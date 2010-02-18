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
package org.apache.pig.experimental.logical.relational;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.LogicalToPhysicalTranslatorException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POProject;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFRJoin;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFilter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POGlobalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POMergeJoin;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSkewedJoin;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.experimental.logical.expression.ExpToPhyTranslationVisitor;
import org.apache.pig.experimental.logical.expression.LogicalExpressionPlan;
import org.apache.pig.experimental.logical.expression.ProjectExpression;
import org.apache.pig.experimental.logical.relational.LogicalSchema.LogicalFieldSchema;
import org.apache.pig.experimental.plan.DependencyOrderWalker;
import org.apache.pig.experimental.plan.Operator;
import org.apache.pig.experimental.plan.OperatorPlan;
import org.apache.pig.experimental.plan.PlanWalker;
import org.apache.pig.experimental.plan.ReverseDependencyOrderWalker;
import org.apache.pig.experimental.plan.SubtreeDependencyOrderWalker;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.CompilerUtils;
import org.apache.pig.impl.util.LinkedMultiMap;
import org.apache.pig.impl.util.MultiMap;

public class LogToPhyTranslationVisitor extends LogicalPlanVisitor {
    
    public LogToPhyTranslationVisitor(OperatorPlan plan) {
        super(plan, new DependencyOrderWalker(plan));
        currentPlan = new PhysicalPlan();
        logToPhyMap = new HashMap<Operator, PhysicalOperator>();
        currentPlans = new Stack<PhysicalPlan>();
    }

    protected Map<Operator, PhysicalOperator> logToPhyMap;

    protected Stack<PhysicalPlan> currentPlans;

    protected PhysicalPlan currentPlan;

    protected NodeIdGenerator nodeGen = NodeIdGenerator.getGenerator();

    protected PigContext pc;
    
    public void setPigContext(PigContext pc) {
        this.pc = pc;
    }

    public PhysicalPlan getPhysicalPlan() {
        return currentPlan;
    }
    
    @Override
    public void visitLOLoad(LOLoad loLoad) throws IOException {
        String scope = DEFAULT_SCOPE;
//        System.err.println("Entering Load");
        // The last parameter here is set to true as we assume all files are 
        // splittable due to LoadStore Refactor
        POLoad load = new POLoad(new OperatorKey(scope, nodeGen
                .getNextNodeId(scope)));
        load.setAlias(loLoad.getAlias());
        load.setLFile(loLoad.getFileSpec());
        load.setPc(pc);
        load.setResultType(DataType.BAG);
        load.setSignature(loLoad.getAlias());
        currentPlan.add(load);
        logToPhyMap.put(loLoad, load);

        // Load is typically a root operator, but in the multiquery
        // case it might have a store as a predecessor.
        List<Operator> op = loLoad.getPlan().getPredecessors(loLoad);
        PhysicalOperator from;
        
        if(op != null) {
            from = logToPhyMap.get(op.get(0));
            try {
                currentPlan.connect(from, load);
            } catch (PlanException e) {
                int errCode = 2015;
                String msg = "Invalid physical operators in the physical plan" ;
                throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
            }
        }
//        System.err.println("Exiting Load");
    }
    
    @Override
    public void visitLOFilter(LOFilter filter) throws IOException {
        String scope = DEFAULT_SCOPE;
//        System.err.println("Entering Filter");
        POFilter poFilter = new POFilter(new OperatorKey(scope, nodeGen
                .getNextNodeId(scope)), filter.getRequestedParallelisam());
        poFilter.setAlias(filter.getAlias());
        poFilter.setResultType(DataType.BAG);
        currentPlan.add(poFilter);
        logToPhyMap.put(filter, poFilter);
        currentPlans.push(currentPlan);

        currentPlan = new PhysicalPlan();

//        PlanWalker childWalker = currentWalker
//                .spawnChildWalker(filter.getFilterPlan());
        PlanWalker childWalker = new ReverseDependencyOrderWalker(filter.getFilterPlan());
        pushWalker(childWalker);
        //currentWalker.walk(this);
        currentWalker.walk(
                new ExpToPhyTranslationVisitor( currentWalker.getPlan(), 
                        childWalker, filter, currentPlan, logToPhyMap ) );
        popWalker();

        poFilter.setPlan(currentPlan);
        currentPlan = currentPlans.pop();

        List<Operator> op = filter.getPlan().getPredecessors(filter);

        PhysicalOperator from;
        if(op != null) {
            from = logToPhyMap.get(op.get(0));
        } else {
            int errCode = 2051;
            String msg = "Did not find a predecessor for Filter." ;
            throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG);
        }
        
        try {
            currentPlan.connect(from, poFilter);
        } catch (PlanException e) {
            int errCode = 2015;
            String msg = "Invalid physical operators in the physical plan" ;
            throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
        }
//        System.err.println("Exiting Filter");
    }
    
    @Override
    public void visitLOInnerLoad(LOInnerLoad load) throws IOException {
        String scope = DEFAULT_SCOPE;
        
        POProject exprOp = new POProject(new OperatorKey(scope, nodeGen
              .getNextNodeId(scope)));
             
        LogicalSchema s = load.getSchema();
        if (s != null) {
            exprOp.setResultType(s.getField(0).type);
        }
        exprOp.setColumn(load.getColNum());
        
        // set input to POProject to the predecessor of foreach
        List<PhysicalOperator> l = new ArrayList<PhysicalOperator>();
        LOForEach foreach = load.getLOForEach();        
        Operator pred = foreach.getPlan().getPredecessors(foreach).get(0);
        l.add(logToPhyMap.get(pred));
        exprOp.setInputs(l);
        
        logToPhyMap.put(load, exprOp);
        currentPlan.add(exprOp);
    }
    
    @Override
    public void visitLOForEach(LOForEach foreach) throws IOException {
        String scope = DEFAULT_SCOPE;
        
        List<PhysicalPlan> innerPlans = new ArrayList<PhysicalPlan>();
        
        org.apache.pig.experimental.logical.relational.LogicalPlan inner = foreach.getInnerPlan();
        LOGenerate gen = (LOGenerate)inner.getSinks().get(0);
       
        List<LogicalExpressionPlan> exps = gen.getOutputPlans();
        List<Operator> preds = inner.getPredecessors(gen);

        currentPlans.push(currentPlan);
        
        // we need to translate each predecessor of LOGenerate into a physical plan.
        // The physical plan should contain the expression plan for this predecessor plus
        // the subtree starting with this predecessor
        for (int i=0; i<preds.size(); i++) {
            currentPlan = new PhysicalPlan();
            // translate the predecessors into a physical plan
            PlanWalker childWalker = new SubtreeDependencyOrderWalker(inner, preds.get(i));
            pushWalker(childWalker);
            childWalker.walk(this);
            popWalker();
            
            // get the leaf of partially translated plan
            PhysicalOperator leaf = currentPlan.getLeaves().get(0);
            
            // add up the expressions
            childWalker = new ReverseDependencyOrderWalker(exps.get(i));
            pushWalker(childWalker);
            childWalker.walk(new ExpToPhyTranslationVisitor(exps.get(i),
                    childWalker, gen, currentPlan, logToPhyMap ));            
            popWalker();
            
            List<Operator> leaves = exps.get(i).getSinks();
            for(Operator l: leaves) {
                PhysicalOperator op = logToPhyMap.get(l);
                if (l instanceof ProjectExpression) {
                    int input = ((ProjectExpression)l).getInputNum();
                    Operator pred = preds.get(input);
                    if (pred instanceof LOInnerLoad) {
                        List<PhysicalOperator> ll = currentPlan.getSuccessors(op);     
                        PhysicalOperator[] ll2 = null;
                        if (ll != null) {
                            ll2 = ll.toArray(new PhysicalOperator[0]);
                        }
                        currentPlan.remove(op);
                        if (ll2 != null) {                        	
                            for(PhysicalOperator suc: ll2) {
                                currentPlan.connect(leaf, suc);
                            }
                        }
                        
                        innerPlans.add(currentPlan);
                        
                        continue;
                    }
                }
                
                currentPlan.connect(leaf, op);                
                innerPlans.add(currentPlan);
            }                        
        }
        
        currentPlan = currentPlans.pop();

        // PhysicalOperator poGen = new POGenerate(new OperatorKey("",
        // r.nextLong()), inputs, toBeFlattened);
        boolean[] flatten = gen.getFlattenFlags();
        List<Boolean> flattenList = new ArrayList<Boolean>();
        for(boolean fl: flatten) {
            flattenList.add(fl);
        }
        POForEach poFE = new POForEach(new OperatorKey(scope, nodeGen
                .getNextNodeId(scope)), foreach.getRequestedParallelisam(), innerPlans, flattenList);
        poFE.setAlias(foreach.getAlias());
        poFE.setResultType(DataType.BAG);
        logToPhyMap.put(foreach, poFE);
        currentPlan.add(poFE); 

        // generate cannot have multiple inputs
        List<Operator> op = foreach.getPlan().getPredecessors(foreach);

        // generate may not have any predecessors
        if (op == null)
            return;

        PhysicalOperator from = logToPhyMap.get(op.get(0));
        try {
           currentPlan.connect(from, poFE);
        } catch (Exception e) {
            int errCode = 2015;
            String msg = "Invalid physical operators in the physical plan" ;
            throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
        }

    }
    

    /**
     * This function translates the new LogicalSchema into old Schema format required
     * by PhysicalOperators
     * @param lSchema LogicalSchema to be converted to Schema
     * @return Schema that is converted from LogicalSchema
     * @throws FrontendException 
     */
    private static Schema translateSchema( LogicalSchema lSchema ) throws FrontendException {
        if( lSchema == null ) {
            return null;
        }
        Schema schema = new Schema();
        List<LogicalFieldSchema> lFields = lSchema.getFields();
        for( LogicalFieldSchema lField : lFields ) {
            FieldSchema field = new FieldSchema( lField.alias, translateSchema(lField.schema),lField.type );
            field.canonicalName = ((Long)lField.uid).toString();
            schema.add(field);
        }        
        return schema;
    }
    
    /**
     * This function takes in a List of LogicalExpressionPlan and converts them to 
     * a list of PhysicalPlans
     * @param plans
     * @return
     * @throws IOException 
     */
    private List<PhysicalPlan> translateExpressionPlans(LogicalRelationalOperator loj,
            List<LogicalExpressionPlan> plans ) throws IOException {
        List<PhysicalPlan> exprPlans = new ArrayList<PhysicalPlan>();
        if( plans == null || plans.size() == 0 ) {
            return exprPlans;
        }
        
        // Save the current plan onto stack
        currentPlans.push(currentPlan);
        
        for( LogicalExpressionPlan lp : plans ) {
            currentPlan = new PhysicalPlan();
            
            // We spawn a new Dependency Walker and use it 
            // PlanWalker childWalker = currentWalker.spawnChildWalker(lp);
            PlanWalker childWalker = new ReverseDependencyOrderWalker(lp);
            
            // Save the old walker and use childWalker as current Walker
            pushWalker(childWalker);
            
            // We create a new ExpToPhyTranslationVisitor to walk the ExpressionPlan
            currentWalker.walk(
                    new ExpToPhyTranslationVisitor( 
                            currentWalker.getPlan(), 
                            childWalker, loj, currentPlan, logToPhyMap ) );
            
            exprPlans.add(currentPlan);
            popWalker();
        }
        
        // Pop the current plan back out
        currentPlan = currentPlans.pop();

        return exprPlans;
    }
    
    @Override
    public void visitLOStore(LOStore loStore) throws IOException {
        String scope = DEFAULT_SCOPE;
//        System.err.println("Entering Store");
        POStore store = new POStore(new OperatorKey(scope, nodeGen
                .getNextNodeId(scope)));
        store.setAlias(((LogicalRelationalOperator)loStore.getPlan().
                getPredecessors(loStore).get(0)).getAlias());
        store.setSFile(loStore.getOutputSpec());
        // TODO Implement this
        //store.setInputSpec(loStore.getInputSpec());
//        try {
            // create a new schema for ourselves so that when
            // we serialize we are not serializing objects that
            // contain the schema - apparently Java tries to
            // serialize the object containing the schema if
            // we are trying to serialize the schema reference in
            // the containing object. The schema here will be serialized
            // in JobControlCompiler
            store.setSchema(translateSchema( loStore.getSchema() ));
//        } catch (FrontendException e1) {
//            int errorCode = 1060;
//            String message = "Cannot resolve Store output schema";  
//            throw new VisitorException(message, errorCode, PigException.BUG, e1);    
//        }
        currentPlan.add(store);
        
        List<Operator> op = loStore.getPlan().getPredecessors(loStore); 
        PhysicalOperator from = null;
        
        if(op != null) {
            from = logToPhyMap.get(op.get(0));
            // TODO Implement sorting when we have a LOSort (new) and LOLimit (new) operator ready
//            SortInfo sortInfo = null;
//            // if store's predecessor is limit,
//            // check limit's predecessor
//            if(op.get(0) instanceof LOLimit) {
//                op = loStore.getPlan().getPredecessors(op.get(0));
//            }
//            PhysicalOperator sortPhyOp = logToPhyMap.get(op.get(0));
//            // if this predecessor is a sort, get
//            // the sort info.
//            if(op.get(0) instanceof LOSort) {
//                sortInfo = ((POSort)sortPhyOp).getSortInfo();
//            }
//            store.setSortInfo(sortInfo);
//        } else {
//            int errCode = 2051;
//            String msg = "Did not find a predecessor for Store." ;
//            throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG);            
        }        

        try {
            currentPlan.connect(from, store);
        } catch (PlanException e) {
            int errCode = 2015;
            String msg = "Invalid physical operators in the physical plan" ;
            throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
        }
        logToPhyMap.put(loStore, store);
//        System.err.println("Exiting Store");
    }
    
    @Override
    public void visitLOJoin(LOJoin loj) throws IOException {
        String scope = DEFAULT_SCOPE;
//        System.err.println("Entering Join");
        
        // List of join predicates
        List<Operator> inputs = plan.getPredecessors(loj);
        
        // mapping of inner join physical plans corresponding to inner physical operators.
        MultiMap<PhysicalOperator, PhysicalPlan> joinPlans = new LinkedMultiMap<PhysicalOperator, PhysicalPlan>();
        
        // Outer list corresponds to join predicates. Inner list is inner physical plan of each predicate.
        List<List<PhysicalPlan>> ppLists = new ArrayList<List<PhysicalPlan>>();
        
        // List of physical operator corresponding to join predicates.
        List<PhysicalOperator> inp = new ArrayList<PhysicalOperator>();
        
        // Outer list corresponds to join predicates and inner list corresponds to type of keys for each predicate.
        List<List<Byte>> keyTypes = new ArrayList<List<Byte>>();
        
        for (int i=0; i<inputs.size(); i++) {
            Operator op = inputs.get(i);
            if( ! ( op instanceof LogicalRelationalOperator ) ) {
                continue;
            }
            LogicalRelationalOperator lop = (LogicalRelationalOperator)op;
            PhysicalOperator physOp = logToPhyMap.get(op);
            inp.add(physOp);
            List<LogicalExpressionPlan> plans = (List<LogicalExpressionPlan>) loj.getJoinPlan(i);
            
            // Convert the expression plan into physical Plan
            List<PhysicalPlan> exprPlans = translateExpressionPlans(loj, plans);

//            currentPlans.push(currentPlan);
//            for (LogicalExpressionPlan lp : plans) {
//                currentPlan = new PhysicalPlan();
//                
//                // We spawn a new Dependency Walker and use it 
//                PlanWalker childWalker = currentWalker.spawnChildWalker(lp);
//                pushWalker(childWalker);
//                // We create a new ExpToPhyTranslationVisitor to walk the ExpressionPlan
//                currentWalker.walk(
//                        new ExpToPhyTranslationVisitor(currentWalker.getPlan(), 
//                                childWalker) );
//                
//                exprPlans.add(currentPlan);
//                popWalker();
//            }
//            currentPlan = currentPlans.pop();
            
            ppLists.add(exprPlans);
            joinPlans.put(physOp, exprPlans);
            
            // Key could potentially be a tuple. So, we visit all exprPlans to get types of members of tuples.
            List<Byte> tupleKeyMemberTypes = new ArrayList<Byte>();
            for(PhysicalPlan exprPlan : exprPlans)
                tupleKeyMemberTypes.add(exprPlan.getLeaves().get(0).getResultType());
            keyTypes.add(tupleKeyMemberTypes);
        }

        if (loj.getJoinType() == LOJoin.JOINTYPE.SKEWED) {
            POSkewedJoin skj;
            try {
                skj = new POSkewedJoin(new OperatorKey(scope,nodeGen.getNextNodeId(scope)),loj.getRequestedParallelisam(),
                                            inp, loj.getInnerFlags());
                skj.setAlias(loj.getAlias());
                skj.setJoinPlans(joinPlans);
            }
            catch (Exception e) {
                int errCode = 2015;
                String msg = "Skewed Join creation failed";
                throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
            }
            skj.setResultType(DataType.TUPLE);
            
            boolean[] innerFlags = loj.getInnerFlags();
            for (int i=0; i < inputs.size(); i++) {
                LogicalRelationalOperator op = (LogicalRelationalOperator) inputs.get(i);
                if (!innerFlags[i]) {
                    try {
                        LogicalSchema s = op.getSchema();
                        // if the schema cannot be determined
                        if (s == null) {
                            throw new FrontendException();
                        }
                        skj.addSchema(translateSchema(s));
                    } catch (FrontendException e) {
                        int errCode = 2015;
                        String msg = "Couldn't set the schema for outer join" ;
                        throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
                    }
                } else {
                    // This will never be retrieved. It just guarantees that the index will be valid when
                    // MRCompiler is trying to read the schema
                    skj.addSchema(null);
                }
            }
            
            currentPlan.add(skj);

            for (Operator op : inputs) {
                try {
                    currentPlan.connect(logToPhyMap.get(op), skj);
                } catch (PlanException e) {
                    int errCode = 2015;
                    String msg = "Invalid physical operators in the physical plan" ;
                    throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
                }
            }
            logToPhyMap.put(loj, skj);
        }
        else if(loj.getJoinType() == LOJoin.JOINTYPE.REPLICATED) {
            
            int fragment = 0;
            POFRJoin pfrj;
            try {
                boolean []innerFlags = loj.getInnerFlags();
                boolean isLeftOuter = false;
                // We dont check for bounds issue as we assume that a join 
                // involves atleast two inputs
                isLeftOuter = !innerFlags[1];
                
                Tuple nullTuple = null;
                if( isLeftOuter ) {
                    try {
                        // We know that in a Left outer join its only a two way 
                        // join, so we assume index of 1 for the right input                        
                        LogicalSchema inputSchema = ((LogicalRelationalOperator)inputs.get(1)).getSchema();                     
                        
                        // We check if we have a schema before the join
                        if(inputSchema == null) {
                            int errCode = 1109;
                            String msg = "Input (" + ((LogicalRelationalOperator) inputs.get(1)).getAlias() + ") " +
                            "on which outer join is desired should have a valid schema";
                            throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.INPUT);
                        }
                        
                        // Using the schema we decide the number of columns/fields 
                        // in the nullTuple
                        nullTuple = TupleFactory.getInstance().newTuple(inputSchema.size());
                        for(int j = 0; j < inputSchema.size(); j++) {
                            nullTuple.set(j, null);
                        }
                        
                    } catch( FrontendException e ) {
                        int errCode = 2104;
                        String msg = "Error while determining the schema of input";
                        throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
                    }
                }
                
                pfrj = new POFRJoin(new OperatorKey(scope,nodeGen.getNextNodeId(scope)),loj.getRequestedParallelisam(),
                                            inp, ppLists, keyTypes, null, fragment, isLeftOuter, nullTuple);
                pfrj.setAlias(loj.getAlias());
            } catch (ExecException e1) {
                int errCode = 2058;
                String msg = "Unable to set index on newly create POLocalRearrange.";
                throw new VisitorException(msg, errCode, PigException.BUG, e1);
            }
            pfrj.setResultType(DataType.TUPLE);
            currentPlan.add(pfrj);
            for (Operator op : inputs) {
                try {
                    currentPlan.connect(logToPhyMap.get(op), pfrj);
                } catch (PlanException e) {
                    int errCode = 2015;
                    String msg = "Invalid physical operators in the physical plan" ;
                    throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
                }
            }
            logToPhyMap.put(loj, pfrj);
        }
        
        else if (loj.getJoinType() == LOJoin.JOINTYPE.MERGE && validateMergeJoin(loj)) {
            
            POMergeJoin smj;
            try {
                smj = new POMergeJoin(new OperatorKey(scope,nodeGen.getNextNodeId(scope)),loj.getRequestedParallelisam(),inp,joinPlans,keyTypes);
            }
            catch (Exception e) {
                int errCode = 2042;
                String msg = "Merge Join creation failed";
                throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
            }

            smj.setResultType(DataType.TUPLE);
            currentPlan.add(smj);

            for (Operator op : inputs) {
                try {
                    currentPlan.connect(logToPhyMap.get(op), smj);
                } catch (PlanException e) {
                    int errCode = 2015;
                    String msg = "Invalid physical operators in the physical plan" ;
                    throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
                }
            }
            logToPhyMap.put(loj, smj);
            return;
        }
        else if (loj.getJoinType() == LOJoin.JOINTYPE.HASH){
            POGlobalRearrange poGlobal = new POGlobalRearrange(new OperatorKey(
                    scope, nodeGen.getNextNodeId(scope)), loj
                    .getRequestedParallelisam());
            poGlobal.setAlias(loj.getAlias());
            POPackage poPackage = new POPackage(new OperatorKey(scope, nodeGen
                    .getNextNodeId(scope)), loj.getRequestedParallelisam());
            poPackage.setAlias(loj.getAlias());
            currentPlan.add(poGlobal);
            currentPlan.add(poPackage);
            
            int count = 0;
            Byte type = null;
            
            try {
                currentPlan.connect(poGlobal, poPackage);
                for (int i=0; i<inputs.size(); i++) {       
                    Operator op = inputs.get(i);
                    List<LogicalExpressionPlan> plans = 
                        (List<LogicalExpressionPlan>) loj.getJoinPlan(i);
                    POLocalRearrange physOp = new POLocalRearrange(new OperatorKey(
                            scope, nodeGen.getNextNodeId(scope)), loj
                            .getRequestedParallelisam());
                    List<PhysicalPlan> exprPlans = translateExpressionPlans(loj, plans);
//                    currentPlans.push(currentPlan);
//                    for (LogicalExpressionPlan lp : plans) {
//                        currentPlan = new PhysicalPlan();
//                        PlanWalker childWalker = currentWalker
//                                .spawnChildWalker(lp);
//                        pushWalker(childWalker);
//                        //currentWalker.walk(this);
//                        currentWalker.walk(
//                                new ExpToPhyTranslationVisitor(currentWalker.getPlan(), 
//                                        childWalker) );
//                        exprPlans.add(currentPlan);
//                        popWalker();
//
//                    }
//                    currentPlan = currentPlans.pop();
                    try {
                        physOp.setPlans(exprPlans);
                    } catch (PlanException pe) {
                        int errCode = 2071;
                        String msg = "Problem with setting up local rearrange's plans.";
                        throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, pe);
                    }
                    try {
                        physOp.setIndex(count++);
                    } catch (ExecException e1) {
                        int errCode = 2058;
                        String msg = "Unable to set index on newly create POLocalRearrange.";
                        throw new VisitorException(msg, errCode, PigException.BUG, e1);
                    }
                    if (plans.size() > 1) {
                        type = DataType.TUPLE;
                        physOp.setKeyType(type);
                    } else {
                        type = exprPlans.get(0).getLeaves().get(0).getResultType();
                        physOp.setKeyType(type);
                    }
                    physOp.setResultType(DataType.TUPLE);

                    currentPlan.add(physOp);

                    try {
                        currentPlan.connect(logToPhyMap.get(op), physOp);
                        currentPlan.connect(physOp, poGlobal);
                    } catch (PlanException e) {
                        int errCode = 2015;
                        String msg = "Invalid physical operators in the physical plan" ;
                        throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
                    }

                }
                
            } catch (PlanException e1) {
                int errCode = 2015;
                String msg = "Invalid physical operators in the physical plan" ;
                throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e1);
            }

            poPackage.setKeyType(type);
            poPackage.setResultType(DataType.TUPLE);
            poPackage.setNumInps(count);
            
            boolean[] innerFlags = loj.getInnerFlags();
            poPackage.setInner(innerFlags);
            
            List<PhysicalPlan> fePlans = new ArrayList<PhysicalPlan>();
            List<Boolean> flattenLst = new ArrayList<Boolean>();
            
            try{
                for(int i=0;i< count;i++){
                    PhysicalPlan fep1 = new PhysicalPlan();
                    POProject feproj1 = new POProject(new OperatorKey(scope, nodeGen.getNextNodeId(scope)), 
                            loj.getRequestedParallelisam(), i+1); //i+1 since the first column is the "group" field
                    feproj1.setAlias(loj.getAlias());
                    feproj1.setResultType(DataType.BAG);
                    feproj1.setOverloaded(false);
                    fep1.add(feproj1);
                    fePlans.add(fep1);
                    // the parser would have marked the side
                    // where we need to keep empty bags on
                    // non matched as outer (innerFlags[i] would be
                    // false)
                    if(!(innerFlags[i])) {
                        Operator joinInput = inputs.get(i);
                        // for outer join add a bincond
                        // which will project nulls when bag is
                        // empty
                        updateWithEmptyBagCheck(fep1, joinInput);
                    }
                    flattenLst.add(true);
                }
                
                POForEach fe = new POForEach(new OperatorKey(scope, nodeGen.getNextNodeId(scope)), 
                        loj.getRequestedParallelisam(), fePlans, flattenLst );
                fe.setAlias(loj.getAlias());
                currentPlan.add(fe);
                currentPlan.connect(poPackage, fe);
                logToPhyMap.put(loj, fe);
            }catch (PlanException e1) {
                int errCode = 2015;
                String msg = "Invalid physical operators in the physical plan" ;
                throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e1);
            }
        }
//        System.err.println("Exiting Join");
    }
    
    /**
     * updates plan with check for empty bag and if bag is empty to flatten a bag
     * with as many null's as dictated by the schema
     * @param fePlan the plan to update
     * @param joinInput the relation for which the corresponding bag is being checked
     * @throws FrontendException 
     */
    public static void updateWithEmptyBagCheck(PhysicalPlan fePlan, Operator joinInput) throws FrontendException {
        LogicalSchema inputSchema = null;
        try {
            inputSchema = ((LogicalRelationalOperator) joinInput).getSchema();
         
          
            if(inputSchema == null) {
                int errCode = 1109;
                String msg = "Input (" + ((LogicalRelationalOperator) joinInput).getAlias() + ") " +
                        "on which outer join is desired should have a valid schema";
                throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.INPUT);
            }
        } catch (FrontendException e) {
            int errCode = 2104;
            String msg = "Error while determining the schema of input";
            throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
        }
        
        CompilerUtils.addEmptyBagOuterJoin(fePlan, translateSchema(inputSchema));
        
    }

    private boolean validateMergeJoin(LOJoin loj) throws IOException{
        
        List<Operator> preds = plan.getPredecessors(loj);

        int errCode = 1101;
        String errMsg = "Merge Join must have exactly two inputs.";
        if(preds.size() != 2)
            throw new LogicalToPhysicalTranslatorException(errMsg+" Found: "+preds.size(),errCode);
        
        return mergeJoinValidator(preds,loj.getPlan());
    }
    
    private boolean mergeJoinValidator(List<Operator> preds,OperatorPlan lp) throws IOException {
        
        int errCode = 1103;
        String errMsg = "Merge join only supports Filter, Foreach, filter and Load as its predecessor. Found : ";
        if(preds != null && !preds.isEmpty()){
            for(Operator lo : preds){
                // TODO Need to add LOForEach in this statement
                if (!(lo instanceof LOFilter || lo instanceof LOLoad)) // || lo instanceof LOForEach
                 throw new LogicalToPhysicalTranslatorException(errMsg, errCode);
                // All is good at this level. Visit predecessors now.
                mergeJoinValidator(lp.getPredecessors(lo),lp);
            }
        }
        // We visited everything and all is good.
        return true;
    }
}
