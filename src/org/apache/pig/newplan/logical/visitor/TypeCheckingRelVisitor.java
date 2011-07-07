/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.newplan.logical.visitor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.pig.PigException;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.validators.TypeCheckerException;
import org.apache.pig.impl.plan.CompilationMessageCollector;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.plan.CompilationMessageCollector.MessageType;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.newplan.DependencyOrderWalker;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.expression.CastExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.relational.LOCogroup;
import org.apache.pig.newplan.logical.relational.LOCross;
import org.apache.pig.newplan.logical.relational.LODistinct;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOGenerate;
import org.apache.pig.newplan.logical.relational.LOInnerLoad;
import org.apache.pig.newplan.logical.relational.LOJoin;
import org.apache.pig.newplan.logical.relational.LOLimit;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LOSort;
import org.apache.pig.newplan.logical.relational.LOSplit;
import org.apache.pig.newplan.logical.relational.LOSplitOutput;
import org.apache.pig.newplan.logical.relational.LOStore;
import org.apache.pig.newplan.logical.relational.LOUnion;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalNodesVisitor;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.newplan.logical.relational.LogicalSchema.LogicalFieldSchema;

/*
All the getType() of these operators always return BAG.
We just have to :-
1) Check types of inputs, expression plans
2) Compute output schema with type information
   (At the moment, the parser does only return GetSchema with correct aliases)
3) Insert casting if necessary

 */
public class TypeCheckingRelVisitor extends LogicalRelationalNodesVisitor {

    private CompilationMessageCollector msgCollector;

    public TypeCheckingRelVisitor(OperatorPlan plan, CompilationMessageCollector msgCollector)
    throws FrontendException {
        super(plan, new DependencyOrderWalker(plan));
        this.msgCollector = msgCollector;

    }

    @Override
    public void visit(LOLoad load){
        // do nothing
    }

    @Override
    public void visit(LOStore store)
    throws FrontendException {
        store.resetSchema();
        store.getSchema();
    }

    /***
     * The schema of filter output will be the same as filter input
     * @throws FrontendException 
     */
    @Override
    public void visit(LOFilter filter) throws FrontendException {
        filter.resetSchema();
        LogicalExpressionPlan comparisonPlan = filter.getFilterPlan() ;

        // Check that the inner plan has only 1 output port
        if (comparisonPlan.getSources().size() > 1) {
            int errCode = 1057;
            String msg = "Filter's cond plan can only have one output" ;
            msgCollector.collect(msg, MessageType.Error) ;
            throwTypeCheckerException(filter, msg, errCode, PigException.INPUT, null) ;
        }

        // visit the filter expression
        visitExpressionPlan(comparisonPlan, filter);


        //check filter expression type
        byte innerCondType = ((LogicalExpression)comparisonPlan.getSources().get(0)).getType();
        if (innerCondType != DataType.BOOLEAN) {
            int errCode = 1058;
            String msg = "Filter's condition must evaluate to boolean. Found: " + 
                         DataType.findTypeName(innerCondType);
            msgCollector.collect(msg, MessageType.Error) ;
            throwTypeCheckerException(filter, msg, errCode, PigException.INPUT, null) ;
        }       

        try {
            // re-compute the schema
            filter.resetSchema();
            filter.getSchema() ;
        } 
        catch (FrontendException fe) {
            int errCode = 1059;
            String msg = "Problem while reconciling output schema of Filter" ;
            msgCollector.collect(msg, MessageType.Error);
            throwTypeCheckerException(filter, msg, errCode, PigException.INPUT, fe) ;
        }
    }
    
    private void throwTypeCheckerException(Operator op, String msg,
            int errCode, byte input, FrontendException fe) throws TypeCheckerException {
        if( fe == null ) {
            throw new TypeCheckerException(op, msg, errCode, PigException.INPUT);
        }
        throw new TypeCheckerException(op, msg, errCode, PigException.INPUT, fe);
    }

	@Override
    public void visit(LOGenerate gen) throws FrontendException {
        for(int i=0; i < gen.getOutputPlans().size(); i++) {
            LogicalExpressionPlan expPlan = gen.getOutputPlans().get(i);
            // Check that the inner plan has only 1 output port
            if (expPlan.getSources().size() > 1) {
                int errCode = 1057;
                String msg = "LOGenerate expression plan can only have one output" ;
                msgCollector.collect(msg, MessageType.Error) ;
                throwTypeCheckerException( gen, msg, errCode, PigException.BUG, null) ;
            }
            // visit the filter expression
            visitExpressionPlan( expPlan, gen );

       }
        gen.resetSchema();
        gen.getSchema();
    }

    @Override
    public void visit(LOInnerLoad innerLoad) throws FrontendException{
        innerLoad.resetSchema();
        innerLoad.getSchema();
    }
    
    @Override
    public void visit(LOForEach forEach) throws FrontendException {
        try {
            // visit inner plan
            new TypeCheckingRelVisitor( forEach.getInnerPlan(), msgCollector ).visit();
            // re-compute the schema
            forEach.resetSchema();
            forEach.getSchema() ;
        }  catch (FrontendException fe) {
            int errCode = 1059;
            String msg = "Problem while reconciling output schema of ForEach" ;
            msgCollector.collect(msg, MessageType.Error);
            throwTypeCheckerException(forEach, msg, errCode, PigException.INPUT, fe) ;
        }
    }

    private void visitExpressionPlan(LogicalExpressionPlan explPlan,
            LogicalRelationalOperator relOp)
    throws FrontendException {
        TypeCheckingExpVisitor expTypeCheck =
            new TypeCheckingExpVisitor(explPlan, msgCollector, relOp);
        expTypeCheck.visit();

    }

    /* (non-Javadoc)
     * @see org.apache.pig.newplan.logical.relational.LogicalRelationalNodesVisitor#visit(org.apache.pig.newplan.logical.relational.LOUnion)
     *     The output schema of LOUnion is the merge of all input schemas.
     *     Operands on left side always take precedance on aliases.
     *     We allow type promotion here
     */
    @Override
    public void visit(LOUnion u) throws FrontendException {
        u.resetSchema();
        // Have to make a copy, because as we insert operators, this list will
        // change under us.
        List<Operator> inputs = new ArrayList<Operator>(u.getInputs());

        // There is no point to union only one operand
        // it should be a problem in the parser
        if (inputs.size() < 2) {
            throw new AssertionError("Union with Count(Operand) < 2") ;
        }

        LogicalSchema schema = null ;
        try {
            // Compute the schema
            schema = u.getSchema() ;

        }
        catch (FrontendException fee) {
            int errCode = 1055;
            String msg = "Problem while reading schemas from inputs of Union" ;
            msgCollector.collect(msg, MessageType.Error) ;
            throwTypeCheckerException(u, msg, errCode, PigException.INPUT, fee) ;
        }

        // Do cast insertion only if we are typed 
        // and if its not union-onschema. In case of union-onschema the
        // foreach with cast is added in UnionOnSchemaSetter
        if (schema != null && !u.isOnSchema()) {
            // Insert casting to inputs if necessary
            for (int i=0; i< inputs.size() ;i++) {
                LOForEach insertedOp
                = insertCastForEachInBetweenIfNecessary((LogicalRelationalOperator)inputs.get(i), u) ;

                // We may have to compute the schema of the input again
                // because we have just inserted
                if (insertedOp != null) {
                    if(insertedOp.getAlias()==null){
                        insertedOp.setAlias(((LogicalRelationalOperator)inputs.get(i)).getAlias());
                    }
                    try {
                        this.visit(insertedOp);
                    }
                    catch (FrontendException fee) {
                        int errCode = 1056;
                        String msg = "Problem while casting inputs of Union" ;
                        msgCollector.collect(msg, MessageType.Error) ;
                        throwTypeCheckerException(u, msg, errCode, PigException.INPUT, fee) ;
                    }
                }
            }
        }
        u.resetSchema();
        u.getSchema();
    }

    /***
     * For casting insertion for relational operators
     * only if it's necessary
     * Currently this only does "shallow" casting
     * @param fromOp
     * @param toOp
     * @return the inserted operator. null is no insertion
     * @throws FrontendException 
     */
    private LOForEach insertCastForEachInBetweenIfNecessary(
            LogicalRelationalOperator fromOp,
            LogicalRelationalOperator toOp)
    throws FrontendException {


        // Make sure that they are adjacent and the direction
        // is from "fromOp" to "toOp"
        List<Operator> preList = plan.getPredecessors(toOp) ;
        boolean found = false ;
        for(Operator tmpOp: preList) {
            // compare by reference
            if (tmpOp == fromOp) {
                found = true ;
                break ;
            }
        }

        if (!found) {
            int errCode = 1077;
            String msg = "Two operators that require a cast in between are not adjacent.";
            throwTypeCheckerException(fromOp, msg, errCode, PigException.INPUT, null);
        }

        // retrieve input schema to be casted
        // this will be used later
        LogicalSchema fromSchema = null ;
        LogicalSchema toSchema = null ;
        try {
            fromSchema = fromOp.getSchema() ;
            toSchema = toOp.getSchema();
        }
        catch(FrontendException fe) {
            int errCode = 1055;
            String msg = "Problem while reading schema from input of " 
                + fromOp.getClass().getSimpleName();
            throwTypeCheckerException(fromOp, msg, errCode, PigException.BUG, fe);
        }

        // make sure the supplied targetSchema has the same number of members
        // as number of output fields from "fromOp"
        if (fromSchema.size() != toSchema.size()) {
            int errCode = 1078;
            String msg = "Schema size mismatch for casting. Input schema size: " 
                + fromSchema.size() + ". Target schema size: " + toSchema.size();
            throwTypeCheckerException(toOp, msg, errCode, PigException.INPUT, null);
        }

        // Plans inside Generate. Fields that do not need casting will only
        // have Project. Fields that need casting will have Project + Cast
        ArrayList<LogicalExpressionPlan> generatePlans = new ArrayList<LogicalExpressionPlan>() ;
        LogicalPlan innerPlan = new LogicalPlan();

        // create LOGenerate for foreach
        LOGenerate loGen = new LOGenerate(innerPlan, generatePlans, 
                new boolean[toSchema.size()]);
        innerPlan.add(loGen);

        // Create ForEach to be inserted
        LOForEach foreach = new LOForEach(plan);
        foreach.setInnerPlan(innerPlan);


        int castNeededCounter = 0 ;
        for(int i=0;i < fromSchema.size(); i++) {

            LOInnerLoad innerLoad = new LOInnerLoad(innerPlan, foreach, i);
            innerPlan.add(innerLoad);
            innerPlan.connect(innerLoad, loGen);

            LogicalExpressionPlan genPlan = new LogicalExpressionPlan() ;
            ProjectExpression project = new ProjectExpression(genPlan, i, 0, loGen);
            genPlan.add(project);

            // add casting if necessary by comparing target types
            // to the input schema
            LogicalFieldSchema fs = null ;
            fs = fromSchema.getField(i) ;

            // This only does "shallow checking"

            LogicalFieldSchema outFieldSchema ;

            outFieldSchema = toSchema.getField(i) ;

            if (outFieldSchema.type != fs.type) {
                castNeededCounter++ ;
                new CastExpression(genPlan, project, outFieldSchema);
            }

            generatePlans.add(genPlan) ;
        }

        // if we really need casting
        if (castNeededCounter > 0)  {
            // Flatten List
            // This is just cast insertion so we don't have any flatten
            ArrayList<Boolean> flattenList = new ArrayList<Boolean>() ;
            for(int i=0;i < toSchema.size(); i++) {
                flattenList.add(Boolean.valueOf(false)) ;
            }

            // Manipulate the plan structure
            plan.add(foreach);
            plan.insertBetween(fromOp, foreach, toOp);
            return foreach;

        }
        else {
            plan.remove(foreach);
            return null ;
        }
    }


    @Override
    public void visit(LOSplitOutput op) throws FrontendException {
        op.resetSchema();
        OperatorPlan lp = op.getPlan();
        // LOSplitOutput can only have 1 input
        List<Operator> list = lp.getPredecessors(op) ;
        if (list.size() != 1) {
            int errCode = 2008;
            String msg = "LOSplitOutput cannot have more than one input. Found: " + list.size() + " input(s).";
            throwTypeCheckerException(op, msg, errCode, PigException.BUG, null) ;
        }

        LogicalExpressionPlan condPlan = op.getFilterPlan() ;

        // Check that the inner plan has only 1 output port
        if (condPlan.getSources().size() != 1) {
            int errCode = 1057;
            String msg = "Split's inner plan can only have one output (leaf)" ;
            msgCollector.collect(msg, MessageType.Error) ;
            throwTypeCheckerException(op, msg, errCode, PigException.INPUT, null) ;
        }

        visitExpressionPlan(condPlan, op);

        byte innerCondType = ((LogicalExpression)condPlan.getSources().get(0)).getType() ;
        if (innerCondType != DataType.BOOLEAN) {
            int errCode = 1058;
            String msg = "Split's condition must evaluate to boolean. Found: " + DataType.findTypeName(innerCondType) ;
            msgCollector.collect(msg, MessageType.Error) ;
            throwTypeCheckerException(op, msg, errCode, PigException.INPUT, null) ;
        }

        try {
            // Compute the schema
            op.getSchema() ;
        }
        catch (FrontendException fe) {
            int errCode = 1055;
            String msg = "Problem while reading"
                + " schemas from inputs of SplitOutput" ;
            msgCollector.collect(msg, MessageType.Error) ;
            throwTypeCheckerException(op, msg, errCode, PigException.INPUT, fe) ;
        }
    }

    /***
     *  LODistinct, output schema should be the same as input
     * @param op
     * @throws VisitorException
     */

    @Override
    public void visit(LODistinct op) throws VisitorException {
        op.resetSchema();

        try {
            // Compute the schema
            op.getSchema() ;
        }
        catch (FrontendException fe) {
            int errCode = 1055;
            String msg = "Problem while reading"
                + " schemas from inputs of Distinct" ;
            msgCollector.collect(msg, MessageType.Error) ;
            throwTypeCheckerException(op, msg, errCode, PigException.INPUT, fe) ;
        }
    }

    @Override
    public void visit(LOLimit limit) throws FrontendException {
        limit.resetSchema();
        LogicalExpressionPlan expressionPlan = limit.getLimitPlan();
        if (expressionPlan != null) {
            // Check that the inner plan has only 1 output port
            if (expressionPlan.getSources().size() > 1) {
                int errCode = 1057;
                String msg = "Limit's expression plan can only have one output";
                msgCollector.collect(msg, MessageType.Error);
                throwTypeCheckerException(limit, msg, errCode, PigException.INPUT, null);
            }

            // visit the limit expression
            visitExpressionPlan(expressionPlan, limit);

            // check limit expression type
            byte innerCondType = ((LogicalExpression) expressionPlan.getSources().get(0))
                    .getType();
            // cast to long if it is a bytearray
            if (innerCondType == DataType.BYTEARRAY)
                insertAtomicCastForInnerPlan(expressionPlan, limit, DataType.LONG);
            // else it must be an int or a long
            else if (innerCondType != DataType.LONG && innerCondType != DataType.INTEGER) {
                int errCode = 1058;
                String msg = "Limit's expression must evaluate to Long or Integer. Found: "
                        + DataType.findTypeName(innerCondType);
                msgCollector.collect(msg, MessageType.Error);
                throwTypeCheckerException(limit, msg, errCode, PigException.INPUT, null);
            }
        }
        try {
            // Compute the schema
            limit.getSchema();
        } catch (FrontendException fe) {
            int errCode = 1055;
            String msg = "Problem while reading schemas from inputs of Limit";
            msgCollector.collect(msg, MessageType.Error) ;
            throwTypeCheckerException(limit, msg, errCode, PigException.INPUT, fe);
        }
    }

    /***
     * Return concatenated of all fields from all input operators
     * If one of the inputs have no schema then we cannot construct
     * the output schema.
     * @param cs
     * @throws VisitorException
     */
    public void visit(LOCross cs) throws VisitorException {
        cs.resetSchema();

        try {
            // Compute the schema
            cs.getSchema() ;
        }
        catch (FrontendException fe) {
            int errCode = 1055;
            String msg = "Problem while reading"
                + " schemas from inputs of Cross" ;
            msgCollector.collect(msg, MessageType.Error) ;
            throwTypeCheckerException(cs, msg, errCode, PigException.INPUT, fe) ;
        }
    }

    /***
     * The schema of sort output will be the same as sort input.
     * @throws FrontendException 
     *
     */
    public void visit(LOSort sort) throws FrontendException {
        sort.resetSchema();
        // Type checking internal plans.
        for(int i=0;i < sort.getSortColPlans().size(); i++) {

            LogicalExpressionPlan sortColPlan = sort.getSortColPlans().get(i) ;

            // Check that the inner plan has only 1 output port
            if (sortColPlan.getSources().size() != 1) {
                int errCode = 1057;
                String msg = "Sort's inner plan can only have one output (leaf)" ;
                msgCollector.collect(msg, MessageType.Error) ;
                throwTypeCheckerException(sort, msg, errCode, PigException.INPUT, null) ;
            }

            visitExpressionPlan(sortColPlan, sort);
        }

        try {
            // Compute the schema
            sort.getSchema() ;
        }
        catch (FrontendException fee) {
            int errCode = 1059;
            String msg = "Problem while reconciling output schema of Sort" ;
            msgCollector.collect(msg, MessageType.Error);
            throwTypeCheckerException(sort, msg, errCode, PigException.INPUT, fee) ;
        }
    }

    /***
     * The schema of split output will be the same as split input
     */

    public void visit(LOSplit split) throws VisitorException {
        OperatorPlan lp = split.getPlan();
        List<Operator> inputList = lp.getPredecessors(split);

        if (inputList.size() != 1) {            
            int errCode = 2008;
            String msg = "LOSplit cannot have more than one input. Found: " + inputList.size() + " input(s).";
            throwTypeCheckerException(split, msg, errCode, PigException.BUG, null) ;
        }

        split.resetSchema();
        try {
            // Compute the schema
            split.getSchema();
        }
        catch (FrontendException fe) {
            int errCode = 1059;
            String msg = "Problem while reconciling output schema of Split" ;
            msgCollector.collect(msg, MessageType.Error);
            throwTypeCheckerException(split, msg, errCode, PigException.INPUT, fe) ;
        }
    }

    /**
     * LOJoin visitor
     * @throws FrontendException 
     */
    public void visit(LOJoin join) throws FrontendException {
        try {
            join.resetSchema();
            join.getSchema();
        } catch (FrontendException fe) {
            int errCode = 1060;
            String msg = "Cannot resolve Join output schema" ;
            msgCollector.collect(msg, MessageType.Error) ;
            throwTypeCheckerException(join, msg, errCode, PigException.INPUT, fe) ;
        }

        MultiMap<Integer, LogicalExpressionPlan> joinColPlans
        = join.getExpressionPlans() ;
        List<Operator> inputs = join.getInputs((LogicalPlan) plan) ;

        // Type checking internal plans.
        for(int i=0;i < inputs.size(); i++) {
            ArrayList<LogicalExpressionPlan> innerPlans
            = new ArrayList<LogicalExpressionPlan>(joinColPlans.get(i)) ;

            for(int j=0; j < innerPlans.size(); j++) {

                LogicalExpressionPlan innerPlan = innerPlans.get(j) ;

                // Check that the inner plan has only 1 output port
                if (innerPlan.getSources().size() != 1) {
                    int errCode = 1057;
                    String msg = "Join's inner plans can only"
                        + " have one output (leaf)" ;
                    msgCollector.collect(msg, MessageType.Error) ;
                    throwTypeCheckerException(join, msg, errCode, PigException.INPUT, null) ;
                }
                visitExpressionPlan(innerPlan, join);
            }
        }

        try {

            if (!isJoinOnMultiCols(join)) {
                // merge all the inner plan outputs so we know what type
                // our group column should be
                byte groupType = getAtomicJoinColType(join);

                // go through all inputs again to add cast if necessary
                for(int i=0;i < inputs.size(); i++) {
                    Collection<LogicalExpressionPlan> exprPlans = join.getJoinPlan(i);

                    //there should be one and only expression plan - that gets 
                    // checked in getAtomicJoinColType()
                    LogicalExpressionPlan exprPlan = exprPlans.iterator().next();

                    // Checking innerPlan size already done above
                    byte innerType =
                        ((LogicalExpression)exprPlan.getSources().get(0)).getType();

                    if (innerType != groupType) {
                        insertAtomicCastForInnerPlan(exprPlan, join, groupType);
                    }
                }
            }
            else {
                //schema of the group-by key
                LogicalSchema groupBySchema = getSchemaFromInnerPlans(join.getExpressionPlans(), join) ;

                // go through all inputs again to add cast if necessary
                for(int i=0;i < inputs.size(); i++) {
                    List<LogicalExpressionPlan> innerPlans = 
                        new ArrayList<LogicalExpressionPlan>(join.getJoinPlan(i)) ;
                    for(int j=0;j < innerPlans.size(); j++) {
                        LogicalExpressionPlan innerPlan = innerPlans.get(j) ;
                        LogicalExpression outputExp = ((LogicalExpression)innerPlan.getSources().get(0));
                        byte innerType = outputExp.getType() ;

                        byte expectedType = groupBySchema.getField(j).type ;

                        if (!DataType.isAtomic(innerType) && (DataType.TUPLE != innerType)) {
                            int errCode = 1057;
                            String msg = "Join's inner plans can only"
                                + "have one output (leaf)" ;
                            msgCollector.collect(msg, MessageType.Error) ;
                            throwTypeCheckerException(join, msg, errCode, PigException.INPUT, null) ;
                        }
                        if (innerType != expectedType) {
                            insertAtomicCastForInnerPlan(
                                    innerPlan,join, expectedType
                            ) ;
                        }
                    }
                }
            }
        }
        catch (FrontendException fe) {
            int errCode = 1060;
            String msg = "Cannot resolve Join output schema" ;
            msgCollector.collect(msg, MessageType.Error) ;
            throwTypeCheckerException(join, msg, errCode, PigException.INPUT, fe) ;
        }

        try {
            join.resetSchema();
            join.getSchema(); 
        }
        catch (FrontendException fe) {
            int errCode = 1060;
            String msg = "Cannot resolve Join output schema" ;
            msgCollector.collect(msg, MessageType.Error) ;
            throwTypeCheckerException(join, msg, errCode, PigException.INPUT, fe) ;
        }
    }

    /**
     * @param join
     * @return true if there is more than one join column for an input
     */
    private boolean isJoinOnMultiCols(LOJoin join) {
        MultiMap<Integer, LogicalExpressionPlan> exprPlans = join.getExpressionPlans();
        if(exprPlans == null || exprPlans.size() == 0){
            throw new AssertionError("LOJoin.isJoinOnMultiCols() can only be called "
                    + " after it has an join expression plans ") ;
        }
        return exprPlans.get(0).size() > 1;
    }

    /**
     * This can be used to get the merged type of output join col
     * only when the join col is of atomic type
     * @return The type of the join col 
     * @throws FrontendException 
     */
    private byte getAtomicJoinColType(LOJoin join) throws FrontendException {
        if (isJoinOnMultiCols(join)) {
            int errCode = 1010;
            String msg = "getAtomicJoinColType is used only when"
                + " dealing with atomic group col";
            throw new FrontendException(msg, errCode, PigException.INPUT, false, null) ;
        }

        byte groupType = DataType.BYTEARRAY ;
        // merge all the inner plan outputs so we know what type
        // our group column should be
        for(int i=0;i < plan.getPredecessors(join).size() ; i++) {
            List<LogicalExpressionPlan> innerPlans = 
                new ArrayList<LogicalExpressionPlan>(join.getJoinPlan(i)) ;
            if (innerPlans.size() != 1) {
                int errCode = 1012;
                String msg = "Each COGroup input has to have "
                    + "the same number of inner plans";
                throw new FrontendException(msg, errCode, PigException.INPUT, false, null) ;
            }
            byte innerType = ((LogicalExpression)innerPlans.get(0).getSources().get(0)).getType() ;
            groupType = DataType.mergeType(groupType, innerType) ;
            if (groupType == -1)
            {
                int errCode = 1107;
                String msg = "Cannot merge join keys, incompatible types";
                throw new FrontendException(msg, errCode, PigException.INPUT) ;
            }
        }

        return groupType ; 
    }

    /**
     * This can be used to get the merged type of output join col
     * only when the join/cogroup col is of atomic type
     * @return The type of the join col 
     * @throws FrontendException 
     */
    private byte getAtomicColType(MultiMap<Integer, LogicalExpressionPlan> allExprPlans) throws FrontendException {
        if (isMultiExprPlanPerInput(allExprPlans)) {
            int errCode = 1010;
            String msg = "getAtomicJoinColType is used only when"
                + " dealing with atomic group col";
            throw new FrontendException(msg, errCode, PigException.INPUT, false, null) ;
        }

        byte groupType = DataType.BYTEARRAY ;
        // merge all the inner plan outputs so we know what type
        // our group column should be
        for(int i=0;i < allExprPlans.size() ; i++) {
            List<LogicalExpressionPlan> innerPlans = 
                new ArrayList<LogicalExpressionPlan>(allExprPlans.get(i)) ;
            if (innerPlans.size() != 1) {
                int errCode = 1012;
                String msg = "Each COGroup input has to have "
                    + "the same number of inner plans";
                throw new FrontendException(msg, errCode, PigException.INPUT, false, null) ;
            }
            byte innerType = ((LogicalExpression)innerPlans.get(0).getSources().get(0)).getType() ;
            groupType = DataType.mergeType(groupType, innerType) ;
            if (groupType == -1)
            {
                int errCode = 1107;
                String msg = "Cannot merge join keys, incompatible types";
                throw new FrontendException(msg, errCode, PigException.INPUT) ;
            }
        }

        return groupType ; 
    }



    private boolean isMultiExprPlanPerInput(
            MultiMap<Integer, LogicalExpressionPlan> exprPlans) {
        if(exprPlans == null || exprPlans.size() == 0){
            throw new AssertionError("LOJoin.isJoinOnMultiCols() can only be called "
                    + " after it has an join expression plans ") ;
        }
        return exprPlans.get(0).size() > 1;
    }

    /**
     * Cast the single output operator of innerPlan to toType
     * @param innerPlan
     * @param relOp - join or cogroup
     * @param toType
     * @throws FrontendException
     */
    private void insertAtomicCastForInnerPlan(LogicalExpressionPlan innerPlan,
            LogicalRelationalOperator relOp, byte toType) throws FrontendException {
        if (!DataType.isUsableType(toType)) {
            int errCode = 1051;
            String msg = "Cannot cast to "
                + DataType.findTypeName(toType);
            throwTypeCheckerException(relOp, msg, errCode, PigException.INPUT, null);
        }

        List<Operator> outputs = innerPlan.getSources();
        if (outputs.size() > 1) {
            int errCode = 2060;
            String msg = "Expected one output. Found " + outputs.size() + "  outputs.";
            throwTypeCheckerException(relOp, msg, errCode, PigException.BUG, null);
        }
        LogicalExpression currentOutput = (LogicalExpression) outputs.get(0);
        TypeCheckingExpVisitor.collectCastWarning(
                relOp, currentOutput.getType(),
                toType, msgCollector
        );
        LogicalFieldSchema newFS = new LogicalFieldSchema(
                currentOutput.getFieldSchema().alias, null, toType
        );
        //add cast
        new CastExpression(innerPlan, currentOutput, newFS);

        //visit modified inner plan
        visitExpressionPlan(innerPlan, relOp);
    }

    /**
     * Create combined group-by/join column schema based on join/cogroup 
     * expression plans for all inputs.
     * This implementation is based on the assumption that all the 
     * inputs have the same join col tuple arity.
     * 
     * @param exprPlans
     * @return
     * @throws FrontendException
     */
    private LogicalSchema getSchemaFromInnerPlans(
            MultiMap<Integer, LogicalExpressionPlan> exprPlans,
            LogicalRelationalOperator op
    )
    throws FrontendException {
        // this fsList represents all the columns in group tuple
        List<LogicalFieldSchema> fsList = new ArrayList<LogicalFieldSchema>() ;

        int outputSchemaSize = exprPlans.get(0).size();

        // by default, they are all bytearray
        // for type checking, we don't care about aliases
        for(int i=0; i<outputSchemaSize; i++) {
            fsList.add(new LogicalFieldSchema(null, null, DataType.BYTEARRAY));
        }

        // merge all the inner plan outputs so we know what type
        // our group column should be
        for(int i=0;i < exprPlans.size(); i++) {
            List<LogicalExpressionPlan> innerPlans =
                new ArrayList<LogicalExpressionPlan>(exprPlans.get(i)) ;

            for(int j=0;j < innerPlans.size(); j++) {
                LogicalExpression eOp = (LogicalExpression)innerPlans.get(j).getSources().get(0);
                byte innerType = eOp.getType();

                if(eOp instanceof ProjectExpression) {
                    if(((ProjectExpression)eOp).isProjectStar()) {
                        //there is a project star and there is more than one 
                        // expression plan
                        int errCode = 1013;
                        String msg = "Grouping attributes can either be star (*) " +
                        "or a list of expressions, but not both.";
                        msgCollector.collect(msg, MessageType.Error) ;
                        throw new FrontendException(
                                msg, errCode, PigException.INPUT, false, null
                        );         
                    }
                }
                //merge the type
                LogicalFieldSchema groupFs = fsList.get(j);
                groupFs.type = DataType.mergeType(groupFs.type, innerType) ;
                if(groupFs.type == DataType.ERROR){
                    String colType = "join";
                    if(op instanceof LOCogroup){
                        colType = "group";
                    }
                    String msg =
                        colType + " column no. " +
                        (j+1) + " in relation no. " + (i+1) + " of  " + colType + 
                        " statement has datatype " + DataType.findTypeName(innerType) +
                        " which is incompatible with type of corresponding column" +
                        " in earlier relation(s) in the statement";
                    msgCollector.collect(msg, MessageType.Error) ;
                    TypeCheckerException ex =
                        new TypeCheckerException(op, msg, 1130, PigException.INPUT);
                    ex.setMarkedAsShowToUser(true);
                    throw ex;
                }
            }

        }
        //create schema from field schemas
        LogicalSchema tupleSchema = new LogicalSchema();
        for(LogicalFieldSchema fs : fsList){
            tupleSchema.addField(fs);
        }
        return tupleSchema;
    }

    /**
     * COGroup
     * All group by cols from all inputs have to be of the
     * same type
     * @throws FrontendException 
     */
    @Override
    public void visit(LOCogroup cg) throws FrontendException {
        try {
            cg.resetSchema();
            cg.getSchema();
        } catch (FrontendException fe) {
            int errCode = 1060;
            String msg = "Cannot resolve COGroup output schema" ;
            msgCollector.collect(msg, MessageType.Error) ;
            throwTypeCheckerException(cg, msg, errCode, PigException.INPUT, fe) ;
        }

        MultiMap<Integer, LogicalExpressionPlan> groupByPlans = 
            cg.getExpressionPlans();

        List<Operator> inputs = cg.getInputs((LogicalPlan)plan);

        // Type checking internal plans.
        for(int i=0;i < inputs.size(); i++) {
            List<LogicalExpressionPlan> innerPlans =
                new ArrayList<LogicalExpressionPlan>(cg.getExpressionPlans().get(i)) ;

            for(int j=0; j < innerPlans.size(); j++) {

                LogicalExpressionPlan innerPlan = innerPlans.get(j) ;

                // Check that the inner plan has only 1 output port
                if (innerPlan.getSources().size() != 1) {
                    int errCode = 1057;
                    String msg = "COGroup's inner plans can only"
                        + "have one output (leaf)" ;
                    msgCollector.collect(msg, MessageType.Error) ;
                    throwTypeCheckerException(cg, msg, errCode, PigException.INPUT, null) ;
                }
                visitExpressionPlan(innerPlan, cg);
            }

        }

        try {

            if (!isCoGroupOnMultiCols(cg)) {
                // merge all the inner plan outputs so we know what type
                // our group column should be
                byte groupType = getAtomicColType(cg.getExpressionPlans());

                // go through all inputs again to add cast if necessary
                for(int i=0;i < inputs.size(); i++) {
                    List<LogicalExpressionPlan> innerPlans =
                        new ArrayList<LogicalExpressionPlan>(cg.getExpressionPlans().get(i)) ;
                    // Checking innerPlan size already done above
                    byte innerType = ((LogicalExpression)innerPlans.get(0).getSources().get(0)).getType() ;
                    if (innerType != groupType) {
                        insertAtomicCastForInnerPlan(
                                innerPlans.get(0),cg, groupType
                        ) ;
                    }
                }
            }
            else {

                LogicalSchema groupBySchema = getSchemaFromInnerPlans(cg.getExpressionPlans(), cg);

                // go through all inputs again to add cast if necessary
                for(int i=0;i < inputs.size(); i++) {

                    List<LogicalExpressionPlan> innerPlans = 
                        new ArrayList<LogicalExpressionPlan>(groupByPlans.get(i)) ;
                    for(int j=0;j < innerPlans.size(); j++) {
                        LogicalExpressionPlan innerPlan = innerPlans.get(j) ;
                        byte innerType = ((LogicalExpression)innerPlan.getSources().get(0)).getType() ;
                        byte expectedType = DataType.BYTEARRAY ;

                        if (!DataType.isAtomic(innerType) && (DataType.TUPLE != innerType)) {
                            int errCode = 1061;
                            String msg = "Sorry, group by complex types"
                                + " will be supported soon" ;
                            msgCollector.collect(msg, MessageType.Error) ;
                            throwTypeCheckerException(cg, msg, errCode, PigException.INPUT, null) ;
                        }

                        expectedType = groupBySchema.getField(j).type ;

                        if (innerType != expectedType) {
                            insertAtomicCastForInnerPlan(
                                    innerPlan, cg, expectedType
                            );
                        }
                    }
                }
            }
        }
        catch (FrontendException fe) {
            int errCode = 1060;
            String msg = "Cannot resolve COGroup output schema" ;
            msgCollector.collect(msg, MessageType.Error) ;
            throwTypeCheckerException(cg, msg, errCode, PigException.INPUT, fe) ;
        }

        try {
            cg.resetSchema();
            cg.getSchema();
        }
        catch (FrontendException fe) {
            int errCode = 1060;
            String msg = "Cannot resolve COGroup output schema" ;
            msgCollector.collect(msg, MessageType.Error) ;
            throwTypeCheckerException(cg, msg, errCode, PigException.INPUT, fe) ;
        }
    }

    /**
     * @param coGroup
     * @return true if there is more than one join column for an input
     */
    private boolean isCoGroupOnMultiCols(LOCogroup coGroup) {
        MultiMap<Integer, LogicalExpressionPlan> exprPlans = coGroup.getExpressionPlans();
        if(exprPlans == null || exprPlans.size() == 0){
            throw new AssertionError("LOCoGroup.isJoinOnMultiCols() can only becalled "
                    + " after it has an join expression plans ") ;
        }
        return exprPlans.get(0).size() > 1;
    }

}
