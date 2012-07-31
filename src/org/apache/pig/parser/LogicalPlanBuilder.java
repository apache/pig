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

package org.apache.pig.parser;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.antlr.runtime.IntStream;
import org.antlr.runtime.RecognitionException;
import org.apache.pig.ExecType;
import org.apache.pig.FuncSpec;
import org.apache.pig.LoadFunc;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.builtin.CubeDimensions;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.builtin.RANDOM;
import org.apache.pig.builtin.RollupDimensions;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.streaming.StreamingCommand;
import org.apache.pig.impl.streaming.StreamingCommand.Handle;
import org.apache.pig.impl.streaming.StreamingCommand.HandleSpec;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.impl.util.StringUtils;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.expression.ConstantExpression;
import org.apache.pig.newplan.logical.expression.LessThanExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.NotExpression;
import org.apache.pig.newplan.logical.expression.OrExpression;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.expression.UserFuncExpression;
import org.apache.pig.newplan.logical.optimizer.SchemaResetter;
import org.apache.pig.newplan.logical.relational.LOCogroup;
import org.apache.pig.newplan.logical.relational.LOCogroup.GROUPTYPE;
import org.apache.pig.newplan.logical.relational.LOCross;
import org.apache.pig.newplan.logical.relational.LOCube;
import org.apache.pig.newplan.logical.relational.LODistinct;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOGenerate;
import org.apache.pig.newplan.logical.relational.LOInnerLoad;
import org.apache.pig.newplan.logical.relational.LOJoin;
import org.apache.pig.newplan.logical.relational.LOJoin.JOINTYPE;
import org.apache.pig.newplan.logical.relational.LOLimit;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LONative;
import org.apache.pig.newplan.logical.relational.LOSort;
import org.apache.pig.newplan.logical.relational.LOSplit;
import org.apache.pig.newplan.logical.relational.LOSplitOutput;
import org.apache.pig.newplan.logical.relational.LOStore;
import org.apache.pig.newplan.logical.relational.LOStream;
import org.apache.pig.newplan.logical.relational.LOUnion;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.newplan.logical.relational.LogicalSchema.LogicalFieldSchema;
import org.apache.pig.newplan.logical.rules.OptimizerUtils;
import org.apache.pig.newplan.logical.visitor.ProjStarInUdfExpander;
import org.apache.pig.newplan.logical.visitor.ProjectStarExpander;

public class LogicalPlanBuilder {

    private LogicalPlan plan = new LogicalPlan();

    private Map<String, Operator> operators = new HashMap<String, Operator>();

    Map<String, String> fileNameMap;

    private PigContext pigContext = null;
    private String scope = null;
    private IntStream intStream;
    private int storeIndex = 0;
    private int loadIndex = 0;
    
    private static NodeIdGenerator nodeIdGen = NodeIdGenerator.getGenerator();
    
    public static long getNextId(String scope) {
        return nodeIdGen.getNextNodeId( scope );
    }
    
    LogicalPlanBuilder(PigContext pigContext, String scope, Map<String, String> fileNameMap,
            IntStream input) {
        this.pigContext = pigContext;
        this.scope = scope;
        this.fileNameMap = fileNameMap;
        this.intStream = input;
    }
    
    LogicalPlanBuilder(IntStream input) throws ExecException {
        pigContext = new PigContext( ExecType.LOCAL, new Properties() );
        pigContext.connect();
        this.scope = "test";
        this.fileNameMap = new HashMap<String, String>();
        this.intStream = input;
    }
    
    Operator lookupOperator(String alias) {
        return operators.get( alias );
    }

    FuncSpec lookupFunction(String alias) {
        return pigContext.getFuncSpecFromAlias( alias );
    }
    
    StreamingCommand lookupCommand(String alias) {
        return pigContext.getCommandForAlias( alias );
    }
    
    void defineCommand(String alias, StreamingCommand command) {
        pigContext.registerStreamCmd( alias, command );
    }
    
    void defineFunction(String alias, FuncSpec fs) {
        pigContext.registerFunction( alias, fs );
    }
    
    LogicalPlan getPlan() {
        return plan;
    }
    
    Map<String, Operator> getOperators() {
        return operators;
    }
    
    LOFilter createFilterOp() {
        return new LOFilter( plan );
    }

    LOLimit createLimitOp() {
        return new LOLimit( plan );
    }
    
    LOFilter createSampleOp() {
        return new LOFilter( plan, true );
    }
    
    String buildFilterOp(SourceLocation loc, LOFilter op, String alias, 
            String inputAlias, LogicalExpressionPlan expr)
                    throws ParserValidationException {
        
        op.setFilterPlan( expr );
        alias = buildOp( loc, op, alias, inputAlias, null ); // it should actually return same alias 
        try {
            (new ProjStarInUdfExpander(op.getPlan())).visit(op);
            new SchemaResetter(op.getPlan(), true).visit(op);
        } catch (FrontendException e) {
            throw new ParserValidationException( intStream, loc, e );
        }   
        return alias;
    }
    
    String buildDistinctOp(SourceLocation loc, String alias, String inputAlias, String partitioner) throws ParserValidationException {
        LODistinct op = new LODistinct( plan );
        return buildOp( loc, op, alias, inputAlias, partitioner );
    }

    String buildLimitOp(SourceLocation loc, String alias, String inputAlias, long limit) throws ParserValidationException {
        LOLimit op = new LOLimit( plan, limit );
        return buildOp( loc, op, alias, inputAlias, null );
    }
    
    String buildLimitOp(SourceLocation loc, LOLimit op, String alias, String inputAlias, LogicalExpressionPlan expr) throws ParserValidationException {
        op.setLimitPlan(expr);
        return buildOp(loc, op, alias, inputAlias, null);
    }
    
    String buildSampleOp(SourceLocation loc, String alias, String inputAlias, double value,
            SourceLocation valLoc)
                    throws ParserValidationException {
        
        LogicalExpressionPlan filterPlan = new LogicalExpressionPlan();
        //  Generate a filter condition.
        LogicalExpression konst = new ConstantExpression( filterPlan, value);
        konst.setLocation( valLoc );
        UserFuncExpression udf = new UserFuncExpression( filterPlan, new FuncSpec( RANDOM.class.getName() ) );
        new LessThanExpression( filterPlan, udf, konst );
        LOFilter filter = new LOFilter( plan, true );
        return buildFilterOp( loc, filter, alias, inputAlias, filterPlan );
    }
    
    String buildSampleOp(SourceLocation loc, LOFilter filter, String alias, String inputAlias,
            LogicalExpressionPlan samplePlan, LogicalExpression expr)
                    throws ParserValidationException {
        
        UserFuncExpression udf = new UserFuncExpression( samplePlan, new FuncSpec( RANDOM.class.getName() ) );
        new LessThanExpression( samplePlan, udf, expr );
        return buildFilterOp( loc, filter, alias, inputAlias, samplePlan );
    }
    
    String buildUnionOp(SourceLocation loc, String alias, List<String> inputAliases, boolean onSchema) throws ParserValidationException {
        LOUnion op = new LOUnion( plan, onSchema );
        return buildOp( loc, op, alias, inputAliases, null );
    }

    String buildSplitOp(SourceLocation loc, String inputAlias) throws ParserValidationException {
        LOSplit op = new LOSplit( plan );
        return buildOp( loc, op, null, inputAlias, null );
    }
    
    LOSplitOutput createSplitOutputOp() {
        return  new LOSplitOutput( plan );
    }
    
    String buildSplitOutputOp(SourceLocation loc, LOSplitOutput op, String alias, String inputAlias,
            LogicalExpressionPlan filterPlan) throws ParserValidationException {
        op.setFilterPlan( filterPlan );
        return buildOp ( loc, op, alias, inputAlias, null );
    }
    
    String buildSplitOtherwiseOp(SourceLocation loc, LOSplitOutput op, String alias, String inputAlias)
            throws ParserValidationException, PlanGenerationFailureException {
        LogicalExpressionPlan splitPlan = new LogicalExpressionPlan();
        Operator losplit = lookupOperator(inputAlias);
        LogicalExpression currentExpr = null;
        for (Operator losplitoutput : plan.getSuccessors(losplit)) {
            // take all the LOSplitOutput and negate their filter plans
            LogicalExpressionPlan fragment = ((LOSplitOutput) losplitoutput)
                    .getFilterPlan();
            try {
                if (OptimizerUtils.planHasNonDeterministicUdf(fragment))
                    throw new ParserValidationException(
                            intStream, loc, new FrontendException(op,
                                    "Can not use Otherwise in Split with an expression containing a @Nondeterministic UDF", 1131));
            } catch (FrontendException e) {
                e.printStackTrace();
                throw new PlanGenerationFailureException(intStream, loc, e);
            }
            LogicalExpression root = null;
            try {
                // get the root expression of the filter plan in LOSplitOutput and copy it
                root = ((LogicalExpression) fragment.getSources().get(0))
                        .deepCopy(splitPlan);
            } catch (FrontendException e) {
                e.printStackTrace();
                throw new PlanGenerationFailureException(intStream, loc, e);
            }
            if (root == null)
                throw new PlanGenerationFailureException(intStream, loc,
                        new FrontendException(op,
                                "Could not retrieve LogicalExpression for LOSplitOutput " + losplitoutput, 2048));
            if (currentExpr == null)
                currentExpr = root;
            else
                currentExpr = new OrExpression(splitPlan, currentExpr, root);
        }
        // using De Morgan's law (!A && !B) == !(A || B)
        currentExpr = new NotExpression(splitPlan, currentExpr);
        op.setFilterPlan(splitPlan);
        return buildOp(loc, op, alias, inputAlias, null);
    }
    
    String buildCrossOp(SourceLocation loc, String alias, List<String> inputAliases, String partitioner) throws ParserValidationException {
        LOCross op = new LOCross( plan );
        return buildOp ( loc, op, alias, inputAliases, partitioner );
    }
    
    LOSort createSortOp() {
        return new LOSort( plan );
    }
    
    String buildSortOp(SourceLocation loc, LOSort sort, String alias, String inputAlias, List<LogicalExpressionPlan> plans, 
            List<Boolean> ascFlags, FuncSpec fs) throws ParserValidationException {
        sort.setSortColPlans( plans );
        sort.setUserFunc( fs );
        if (ascFlags.isEmpty()) {
            for (int i=0;i<plans.size();i++)
                ascFlags.add(true);
        }
        sort.setAscendingCols( ascFlags );
        alias = buildOp( loc, sort, alias, inputAlias, null );
        expandAndResetVisitor(loc, sort);
        return alias;
    }
    
    LOJoin createJoinOp() {
        return new LOJoin( plan );
    }

    String buildJoinOp(SourceLocation loc, LOJoin op, String alias, List<String> inputAliases,
            MultiMap<Integer, LogicalExpressionPlan> joinPlans,
            JOINTYPE jt, List<Boolean> innerFlags, String partitioner)
    throws ParserValidationException {
        if (jt==null)
            jt = JOINTYPE.HASH;
        else {
            op.pinOption(LOJoin.OPTION_JOIN);
        }
        
        int inputCount = inputAliases.size();
        
        if( jt == JOINTYPE.SKEWED ) {
            if( partitioner != null ) {
                throw new ParserValidationException( intStream, loc,
                        "Custom Partitioner is not supported for skewed join" );
            }
            
            if( inputCount != 2 ) {
                throw new ParserValidationException( intStream, loc,
                        "Skewed join can only be applied for 2-way joins" );
            }
        } else if( (jt == JOINTYPE.MERGE || jt == JOINTYPE.MERGESPARSE) && inputCount != 2 ) {
            throw new ParserValidationException( intStream, loc,
                    "Merge join can only be applied for 2-way joins" );
        } else if( jt == JOINTYPE.REPLICATED ) {
            if( innerFlags.size() == 2 && innerFlags.get( 0 ) == false ) {
                throw new ParserValidationException( intStream, loc,
                        "Replicated join does not support (right|full) outer joins" );
            }
        }

        boolean[] flags = new boolean[joinPlans.size()];
        if (innerFlags.size()!=0) {
            for( int i = 0; i < joinPlans.size(); i++ ) {
                flags[i] = innerFlags.get( i );
            }
        }
        else {
            for( int i = 0; i < joinPlans.size(); i++ ) {
                flags[i] = true;
            }
        }
        op.setJoinType( jt );
        op.setInnerFlags( flags );
        op.setJoinPlans( joinPlans );
        alias = buildOp( loc, op, alias, inputAliases, partitioner );
        expandAndResetVisitor(loc, op);
        return alias;
    }

    private void expandAndResetVisitor(SourceLocation loc,
	    LogicalRelationalOperator lrop) throws ParserValidationException {
        try {
	    (new ProjectStarExpander(lrop.getPlan())).visit();
	    (new ProjStarInUdfExpander(lrop.getPlan())).visit();
	    new SchemaResetter(lrop.getPlan(), true).visit();
	} catch (FrontendException e) {
	    throw new ParserValidationException(intStream, loc, e);
	}
    }
    
    LOCube createCubeOp() {
	return new LOCube(plan);
    }

    String buildCubeOp(SourceLocation loc, LOCube op, String alias, String inputAlias,
	    List<String> operations, MultiMap<Integer, LogicalExpressionPlan> expressionPlans)
	    throws ParserValidationException {

	// check if continuously occurring cube operations be combined
	combineCubeOperations((ArrayList<String>) operations, expressionPlans);

	// set the expression plans for cube operator and build cube operator
	op.setExpressionPlans(expressionPlans);
	op.setOperations(operations);
	buildOp(loc, op, alias, inputAlias, null);
	expandAndResetVisitor(loc, op);
	try {
	    alias = convertCubeToFGPlan(loc, op, inputAlias, operations, expressionPlans);
	} catch (FrontendException e) {
	    throw new ParserValidationException(intStream, loc, e);
	}
	return alias;
    }

    // if multiple CUBE operations occur continuously then it can be combined
    // together CUBE rel BY CUBE(a,b), CUBE(c,d); => CUBE rel BY CUBE(a,b,c,d)
    private void combineCubeOperations(ArrayList<String> operations,
	    MultiMap<Integer, LogicalExpressionPlan> expressionPlans) {

	int startIdx = -1;
	int endIdx = -1;
	int i = 0;
	boolean isMerged = false;

	// scan and perform merge of column projections
	for (i = 0; i < operations.size(); i++) {
	    if ((startIdx == -1) && (operations.get(i).equals("CUBE") == true)) {
		startIdx = i;
	    } else {
		if (operations.get(i).equals("CUBE") == true) {
		    endIdx = i;
		} else {
		    if (endIdx > startIdx) {
			mergeAndMarkForDelete(operations, expressionPlans, startIdx, endIdx);
			isMerged = true;
			startIdx = -1;
			endIdx = -1;
		    } else {
			startIdx = -1;
			endIdx = -1;
		    }
		}
	    }
	}

	// this check is required for the case when the sequence of CUBE
	// operations occurs at the end, like (CUBE, ROLLUP, CUBE, CUBE)
	// in which case endIdx will be greater than startIdx
	if (endIdx > startIdx) {
	    isMerged = true;
	    mergeAndMarkForDelete(operations, expressionPlans, startIdx, endIdx);
	}

	// if merged then remove the column projections that were marked for
	// deletion
	if (isMerged) {
	    performDeletion(expressionPlans, operations);
	}
    }

    private void performDeletion(MultiMap<Integer, LogicalExpressionPlan> expressionPlans,
	    ArrayList<String> operations) {

	MultiMap<Integer, LogicalExpressionPlan> ep = new MultiMap<Integer, LogicalExpressionPlan>();
	List<String> op = new ArrayList<String>();
	int idx = 0;
	// rearranging indices
	for (int i = 0; i < operations.size(); i++) {
	    if (operations.get(i) != null) {
		op.add(idx, operations.get(i));
	    }

	    if (expressionPlans.get(i) != null) {
		ep.put(idx, expressionPlans.get(i));
		idx++;
	    }
	}

	// performing deletions
	operations.clear();
	operations.addAll(op);

	expressionPlans.clear();
	for (Integer i : ep.keySet()) {
	    expressionPlans.put(i, ep.get(i));
	}
    }

    // performs merging of dimensions of merged cube operation
    // Ex: CUBE(a,b), CUBE(c,d) ==> CUBE(a,b,c,d)
    // in the above example CUBE operator and dimensions are merged
    private void mergeAndMarkForDelete(ArrayList<String> operations,
	    MultiMap<Integer, LogicalExpressionPlan> expressionPlans, int startIdx, int endIdx) {
	// mark for delete
	for (int i = startIdx + 1; i <= endIdx; i++) {
	    expressionPlans.put(startIdx, expressionPlans.get(i));
	    expressionPlans.removeKey(i);
	    operations.remove(i);
	    operations.add(i, null);
	}
    }

    // This function creates logical plan for foreach and groupby operators.
    // It connects the predecessors of cube operator with foreach plan and
    // disconnects cube operator from the logical plan. It also connects foreach
    // plan with groupby plan.
    private String convertCubeToFGPlan(SourceLocation loc, LOCube op, String inputAlias,
	    List<String> operations, MultiMap<Integer, LogicalExpressionPlan> expressionPlans)
	    throws FrontendException {

	LOForEach foreach = new LOForEach(plan);
	LOCogroup groupby = new LOCogroup(plan);
	LogicalPlan innerPlan = new LogicalPlan();
	LogicalRelationalOperator gen = new LOGenerate(innerPlan);

	injectForeachOperator(loc, op, foreach);

	// Get all column attributes from the input relation.
	// Create ProjectExpression for all columns. Based on the
	// dimensions specified by the user, specified columns will be attached
	// to CubeDimension/RollupDimension UDF and rest will be pushed down
	List<Operator> inpOpers = foreach.getPlan().getPredecessors(foreach);
	List<LogicalExpressionPlan> allExprPlan = new ArrayList<LogicalExpressionPlan>();
	for (Operator oper : inpOpers) {
	    LogicalSchema schema = new LogicalSchema();
	    schema = ((LogicalRelationalOperator) oper).getSchema();

	    if (schema != null) {
		ArrayList<LogicalFieldSchema> fields = (ArrayList<LogicalFieldSchema>) schema
		        .getFields();
		for (int i = 0; i < fields.size(); i++) {
		    LogicalExpressionPlan lEplan = new LogicalExpressionPlan();
		    new ProjectExpression(lEplan, i, fields.get(i).alias, gen);
		    allExprPlan.add(lEplan);
		}
	    }
	}

	// iterate over all operations and generate corresponding UDFs
	for (int operIdx = 0; operIdx < operations.size(); operIdx++) {
	    List<LogicalExpressionPlan> lexpPlanList = new ArrayList<LogicalExpressionPlan>();
	    List<LogicalExpression> lexpList = new ArrayList<LogicalExpression>();

	    lexpPlanList.addAll(expressionPlans.get(operIdx));

	    // If duplicates exists in the dimension list then exception is
	    // thrown
	    checkDuplicateProject(lexpPlanList);

	    // Construct ProjectExpression from the LogicalExpressionPlans
	    lexpList = getProjectExpList(lexpPlanList, gen);

	    for (int i = 0; i < lexpList.size(); i++) {
		// Retain the columns that needs to be pushed down.
		// Remove the dimension columns from the input column list
		// as it will be attached to CubeDimension UDF
		for (int j = 0; j < allExprPlan.size(); j++) {
		    LogicalExpression lexp = (LogicalExpression) allExprPlan.get(j).getSources()
			    .get(0);
		    String colAlias = ((ProjectExpression) lexpList.get(i)).getColAlias();
		    if (colAlias == null) {
			colAlias = ((ProjectExpression) lexpList.get(i)).getFieldSchema().alias;
		    }

		    String projExpAlias = null;
		    try {
			projExpAlias = ((ProjectExpression) lexp).getColAlias();
		    } catch (ClassCastException e) {
			// if it is not projection then it should be
			// UserFuncExpr.
			// ignore and continue till next ProjExpr is encountered
			continue;
		    }
		    if (colAlias.equals(projExpAlias) == true) {
			allExprPlan.remove(j);
		    } else {
			// if projected exp alias is a namespaced alias
			if (projExpAlias.lastIndexOf(":") != -1) {
			    projExpAlias = projExpAlias.substring(
				    projExpAlias.lastIndexOf(":") + 1, projExpAlias.length());
			    if (colAlias.equals(projExpAlias) == true) {
				allExprPlan.remove(j);
			    }
			}
		    }
		}
	    }

	    // Create UDF with user specified dimensions
	    LogicalExpressionPlan uexpPlan = new LogicalExpressionPlan();
	    if (operations.get(operIdx).equals("CUBE")) {
		new UserFuncExpression(uexpPlan, new FuncSpec(CubeDimensions.class.getName()),
		        lexpList);
	    } else {
		new UserFuncExpression(uexpPlan, new FuncSpec(RollupDimensions.class.getName()),
		        lexpList);
	    }

	    for (LogicalExpressionPlan lexp : lexpPlanList) {
		Iterator<Operator> it = lexp.getOperators();
		while (it.hasNext()) {
		    uexpPlan.add(it.next());
		}
	    }
	    // Add the UDF to logical expression plan that contains dependent
	    // attributes (pushed down from input columns)
	    allExprPlan.add(operIdx, uexpPlan);
	}

	// If the operator is a UserFuncExpression then set the flatten flags.
	List<Boolean> flattenFlags = new ArrayList<Boolean>();
	for (int idx = 0; idx < allExprPlan.size(); idx++) {
	    List<Operator> opers = allExprPlan.get(idx).getSources();
	    for (Operator oper : opers) {
		if (oper instanceof ProjectExpression) {
		    flattenFlags.add(false);
		} else if (oper instanceof UserFuncExpression) {
		    flattenFlags.add(true);
		}
	    }
	}

	// Generate and Foreach operator creation
	String falias = null;
	try {
	    buildGenerateOp(loc, (LOForEach) foreach, (LOGenerate) gen, operators, allExprPlan,
		    flattenFlags, getUserDefinedSchema(allExprPlan));
	    falias = buildForeachOp(loc, (LOForEach) foreach, "cube", inputAlias, innerPlan);
	} catch (ParserValidationException pve) {
	    throw new FrontendException(pve);
	}

	List<Boolean> innerFlags = new ArrayList<Boolean>();
	List<String> inpAliases = new ArrayList<String>();
	inpAliases.add(falias);
	innerFlags.add(false);

	// Get the output schema of foreach operator and reconstruct the
	// LogicalExpressionPlan for each dimensional attributes
	MultiMap<Integer, LogicalExpressionPlan> exprPlansCopy = new MultiMap<Integer, LogicalExpressionPlan>();

	for (LogicalExpressionPlan exp : expressionPlans.values()) {
	    LogicalExpression lexp = (LogicalExpression) exp.getSources().get(0);
	    LogicalExpressionPlan epGrp = new LogicalExpressionPlan();
	    new ProjectExpression(epGrp, 0, lexp.getFieldSchema().alias, groupby);
	    exprPlansCopy.put(0, epGrp);
	}

	// build group by operator
	try {
	    return buildGroupOp(loc, (LOCogroup) groupby, op.getAlias(), inpAliases, exprPlansCopy,
		    GROUPTYPE.REGULAR, innerFlags, null);
	} catch (ParserValidationException pve) {
	    throw new FrontendException(pve);
	}
    }

    // User defined schema for generate operator. If not specified output schema
    // of UDF will be used which will prefix "dimensions" namespace to all fields
    private List<LogicalSchema> getUserDefinedSchema(List<LogicalExpressionPlan> allExprPlan)
	    throws FrontendException {
	List<LogicalSchema> genOutputSchema = new ArrayList<LogicalSchema>();
	for (int i = 0; i < allExprPlan.size(); i++) {
	    List<Operator> opers = allExprPlan.get(i).getSources();
	    for (Operator oper : opers) {

		// add a logical schema for dimensions that are pushed from
		// predecessor of cube/rollup
		if (oper instanceof ProjectExpression) {
		    LogicalSchema output = new LogicalSchema();
		    output.addField(new LogicalFieldSchema(
			    ((ProjectExpression) oper).getColAlias(), null, DataType.NULL));
		    genOutputSchema.add(output);
		} else if (oper instanceof UserFuncExpression) {
		    // add logical schema for dimensions specified in
		    // cube/rollup operator
		    LogicalSchema output = new LogicalSchema();
		    for (Operator op : ((UserFuncExpression) oper).getPlan().getSinks()) {
			output.addField(new LogicalFieldSchema(((ProjectExpression) op)
			        .getFieldSchema()));
		    }
		    genOutputSchema.add(output);
		}

	    }
	}
	return genOutputSchema;
    }

    private List<LogicalExpression> getProjectExpList(List<LogicalExpressionPlan> lexpPlanList,
	    LogicalRelationalOperator lro) throws FrontendException {

	List<LogicalExpression> leList = new ArrayList<LogicalExpression>();
	for (int i = 0; i < lexpPlanList.size(); i++) {
	    LogicalExpressionPlan lexp = lexpPlanList.get(i);
	    LogicalExpression lex = (LogicalExpression) lexp.getSources().get(0);
	    Iterator<Operator> opers = lexp.getOperators();

	    // ProjExpr are initially attached to CubeOp. So re-attach it to
	    // specified operator
	    while (opers.hasNext()) {
		Operator oper = opers.next();
		try {
		    ((ProjectExpression) oper).setAttachedRelationalOp(lro);
		} catch (ClassCastException cce) {
		    throw new FrontendException("Column project expected.", cce);
		}
	    }

	    leList.add(lex);
	}

	return leList;
    }

    // This method connects the predecessors of cube operator with foreach
    // operator and disconnects the cube operator from its predecessors
    private void injectForeachOperator(SourceLocation loc, LOCube op, LOForEach foreach)
	    throws FrontendException {
	// connect the foreach operator with predecessors of cube operator
	List<Operator> opers = op.getPlan().getPredecessors(op);
	for (Operator oper : opers) {
	    OperatorPlan foreachPlan = foreach.getPlan();
	    foreachPlan.connect(oper, (Operator) foreach);
	}

	// disconnect the cube operator from the plan
	opers = foreach.getPlan().getPredecessors(foreach);
	for (Operator lop : opers) {
	    List<Operator> succs = lop.getPlan().getSuccessors(lop);
	    for (Operator succ : succs) {
		if (succ instanceof LOCube) {
		    succ.getPlan().disconnect(lop, succ);
		    succ.getPlan().remove(succ);
		}
	    }
	}
    }

    // This methods if the dimensions specified by the user has duplicates
    private void checkDuplicateProject(List<LogicalExpressionPlan> lExprPlan)
	    throws FrontendException {

	for (int i = 0; i < lExprPlan.size(); i++) {
	    for (int j = i + 1; j < lExprPlan.size(); j++) {
		LogicalExpression outer = (LogicalExpression) lExprPlan.get(i).getSources().get(0);
		LogicalExpression inner = (LogicalExpression) lExprPlan.get(j).getSources().get(0);
		String outColAlias = ((ProjectExpression) outer).getColAlias();
		String inColAlias = ((ProjectExpression) inner).getColAlias();

		if (outColAlias == null) {
		    outColAlias = outer.getFieldSchema().alias;
		}

		if (inColAlias == null) {
		    inColAlias = inner.getFieldSchema().alias;
		}

		if (outColAlias.equals(inColAlias) == true) {
		    lExprPlan.remove(j);
		    throw new FrontendException("Duplicate dimensions detected. Dimension name: "
			    + inColAlias);
		}
	    }
	}

    }
	
    LOCogroup createGroupOp() {
        return new LOCogroup( plan );
    }
    
    String buildGroupOp(SourceLocation loc, LOCogroup op, String alias, List<String> inputAliases, 
        MultiMap<Integer, LogicalExpressionPlan> expressionPlans, GROUPTYPE gt, List<Boolean> innerFlags,
        String partitioner) throws ParserValidationException {
        if( gt == GROUPTYPE.COLLECTED ) {
            if( inputAliases.size() > 1 ) {
                throw new ParserValidationException( intStream, loc, 
                        "Collected group is only supported for single input" );
            }
            
            List<LogicalExpressionPlan> exprPlans = expressionPlans.get( 0 );
            for( LogicalExpressionPlan exprPlan : exprPlans ) {
                Iterator<Operator> it = exprPlan.getOperators();
                while( it.hasNext() ) {
                    if( !( it.next() instanceof ProjectExpression ) ) {
                        throw new ParserValidationException( intStream, loc,
                                "Collected group is only supported for columns or star projection" );
                    }
                }
            }
        }
        
        boolean[] flags = new boolean[innerFlags.size()];
        for( int i = 0; i < innerFlags.size(); i++ ) {
            flags[i] = innerFlags.get( i );
        }
        op.setExpressionPlans( expressionPlans );
        op.setGroupType( gt );
        op.setInnerFlags( flags );
        alias = buildOp( loc, op, alias, inputAliases, partitioner );
        expandAndResetVisitor(loc, op);

        return alias;
    }

    String buildLoadOp(SourceLocation loc, String alias, String filename, FuncSpec funcSpec, LogicalSchema schema)
    throws ParserValidationException {
        String absolutePath;
        LoadFunc loFunc;
        try {
            FuncSpec instantiatedFuncSpec =
                    funcSpec == null ?
                        new FuncSpec(PigStorage.class.getName()) :
                        funcSpec;

            loFunc = (LoadFunc)PigContext.instantiateFuncFromSpec(instantiatedFuncSpec);
            String fileNameKey = QueryParserUtils.constructFileNameSignature(filename, instantiatedFuncSpec) + "_" + (loadIndex++);
            absolutePath = fileNameMap.get(fileNameKey);
            if (absolutePath == null) {
                absolutePath = loFunc.relativeToAbsolutePath( filename, QueryParserUtils.getCurrentDir( pigContext ) );

                if (absolutePath!=null) {
                    QueryParserUtils.setHdfsServers( absolutePath, pigContext );
                }
                fileNameMap.put( fileNameKey, absolutePath );
            }
        } catch(Exception ex) {
            throw new ParserValidationException( intStream, loc, ex );
        }

        FileSpec loader = new FileSpec(absolutePath, funcSpec);
        LOLoad op = new LOLoad(
                loader,
                schema,
                plan,
                ConfigurationUtil.toConfiguration(pigContext.getProperties()),
                loFunc,
                alias + "_" + newOperatorKey());
        return buildOp( loc, op, alias, new ArrayList<String>(), null );
    }

    private String buildOp(SourceLocation loc, LogicalRelationalOperator op, String alias, 
    		String inputAlias, String partitioner) throws ParserValidationException {
        List<String> inputAliases = new ArrayList<String>();
        if( inputAlias != null )
            inputAliases.add( inputAlias );
        return buildOp( loc, op, alias, inputAliases, partitioner );
    }
    
    private String buildOp(SourceLocation loc, LogicalRelationalOperator op, String alias, 
    		List<String> inputAliases, String partitioner) throws ParserValidationException {
        setAlias( op, alias );
        setPartitioner( op, partitioner );
        op.setLocation( loc );
        plan.add( op );
        for( String a : inputAliases ) {
            Operator pred = operators.get( a );
            if (pred==null) {
                throw new ParserValidationException( intStream, loc, "Unrecognized alias " + a );
            }
            plan.connect( pred, op );
        }
        operators.put( op.getAlias(), op );
        pigContext.setLastAlias(op.getAlias());	
        return op.getAlias();
    }

    String buildStoreOp(SourceLocation loc, String alias, String inputAlias, String filename, FuncSpec funcSpec)
    throws ParserValidationException {
        try {
            FuncSpec instantiatedFuncSpec =
                    funcSpec == null ?
                            new FuncSpec(PigStorage.class.getName()):
                            funcSpec;

            StoreFuncInterface stoFunc = (StoreFuncInterface)PigContext.instantiateFuncFromSpec(instantiatedFuncSpec);
            String fileNameKey = inputAlias + "_" + (storeIndex++) ;

            String signature = inputAlias + "_" + newOperatorKey();
            stoFunc.setStoreFuncUDFContextSignature(signature);

            String absolutePath = fileNameMap.get(fileNameKey);
            if (absolutePath == null) {
                absolutePath = stoFunc.relToAbsPathForStoreLocation(
                        filename,
                        QueryParserUtils.getCurrentDir(pigContext));
                if (absolutePath!=null) {
                    QueryParserUtils.setHdfsServers(absolutePath, pigContext);
                }
                fileNameMap.put(fileNameKey, absolutePath);
            }
            FileSpec fileSpec = new FileSpec(absolutePath, funcSpec);

            LOStore op = new LOStore(plan, fileSpec, stoFunc, signature);
            return buildOp(loc, op, alias, inputAlias, null);
        } catch(Exception ex) {
            throw new ParserValidationException(intStream, loc, ex);
        }
    }

    private String newOperatorKey() {
        return new OperatorKey( scope, getNextId() ).toString();
    }
    
    public static String newOperatorKey(String scope) {
        return new OperatorKey( scope, getNextId(scope)).toString();
    }
    
    LOForEach createForeachOp() {
        return new LOForEach( plan );
    }
    
    String buildForeachOp(SourceLocation loc, LOForEach op, String alias, String inputAlias, LogicalPlan innerPlan)
    throws ParserValidationException {
        op.setInnerPlan( innerPlan );
        alias = buildOp( loc, op, alias, inputAlias, null );
        expandAndResetVisitor(loc, op);
        return alias;
    }
    
    LOGenerate createGenerateOp(LogicalPlan plan) {
        return new LOGenerate( plan );
    }
    
    void buildGenerateOp(SourceLocation loc, LOForEach foreach, LOGenerate gen,
            Map<String, Operator> operators,
            List<LogicalExpressionPlan> exprPlans, List<Boolean> flattenFlags,
            List<LogicalSchema> schemas)
    throws ParserValidationException{
        
        boolean[] flags = new boolean[ flattenFlags.size() ];
        for( int i = 0; i < flattenFlags.size(); i++ )
            flags[i] = flattenFlags.get( i );
        LogicalPlan innerPlan = (LogicalPlan)gen.getPlan();
        ArrayList<Operator> inputs = new ArrayList<Operator>();
        for( LogicalExpressionPlan exprPlan : exprPlans ) {
            try {
                processExpressionPlan( foreach, innerPlan, exprPlan, operators, inputs );
            } catch (FrontendException e) {
                throw new ParserValidationException(intStream, loc, e);
            }
        }
        
        gen.setOutputPlans( exprPlans );
        gen.setFlattenFlags( flags );
        gen.setUserDefinedSchema( schemas );
        innerPlan.add( gen );
        gen.setLocation( loc );
        for( Operator input : inputs ) {
            innerPlan.connect( input, gen );
        }
    }
    
    /**
     * Process expression plans of LOGenerate and set inputs relation
     * for the ProjectExpression
     * @param foreach 
     * @param lp Logical plan in which the LOGenerate is in
     * @param plan One of the output expression of the LOGenerate
     * @param operators All logical operators in lp;
     * @param inputs  inputs of the LOGenerate
     * @throws FrontendException 
     */
    private static void processExpressionPlan(LOForEach foreach,
                                      LogicalPlan lp,  
                                      LogicalExpressionPlan plan,  
                                      Map<String, Operator> operators,  
                                      ArrayList<Operator> inputs ) throws FrontendException {
        Iterator<Operator> it = plan.getOperators();
        while( it.hasNext() ) {
            Operator sink = it.next();
            //check all ProjectExpression
            if( sink instanceof ProjectExpression ) {
                ProjectExpression projExpr = (ProjectExpression)sink;
                String colAlias = projExpr.getColAlias();
                if( projExpr.isRangeProject()){
                 
                    LOInnerLoad innerLoad = new LOInnerLoad( lp, foreach,
                            new ProjectExpression(projExpr, new LogicalExpressionPlan())
                    );
                    setupInnerLoadAndProj(innerLoad, projExpr, lp, inputs);
                }
                else if( colAlias != null ) {
                    // the project is using a column alias
                    Operator op = operators.get( colAlias );
                    if( op != null ) {
                        // this means the project expression refers to a relation
                        // in the nested foreach

                        //add the relation to inputs of LOGenerate and set 
                        // projection input
                        int index = inputs.indexOf( op );
                        if( index == -1 ) {
                            index = inputs.size();
                            inputs.add( op );
                        }
                        projExpr.setInputNum( index );
                        projExpr.setColNum( -1 );
                    } else {
                        // this means the project expression refers to a column
                        // in the input of foreach. Add a LOInnerLoad and use that
                        // as input
                        LOInnerLoad innerLoad = new LOInnerLoad( lp, foreach, colAlias );
                        setupInnerLoadAndProj(innerLoad, projExpr, lp, inputs);
                    }
                } else {
                    // the project expression is referring to column in ForEach input
                    // using position (eg $1)
                    LOInnerLoad innerLoad = new LOInnerLoad( lp, foreach, projExpr.getColNum() );
                    setupInnerLoadAndProj(innerLoad, projExpr, lp, inputs);
                }
            }
        }
    }
    
    private static void setupInnerLoadAndProj(LOInnerLoad innerLoad,
            ProjectExpression projExpr, LogicalPlan lp,
            ArrayList<Operator> inputs) {
        innerLoad.setLocation( projExpr.getLocation() );
        projExpr.setInputNum( inputs.size() );
        projExpr.setColNum( -1 ); // Projection Expression on InnerLoad is always (*).
        lp.add( innerLoad );
        inputs.add( innerLoad );
        
    }

    Operator buildNestedOperatorInput(SourceLocation loc, LogicalPlan innerPlan, LOForEach foreach, 
            Map<String, Operator> operators, LogicalExpression expr)
    throws NonProjectExpressionException, ParserValidationException {
        OperatorPlan plan = expr.getPlan();
        Iterator<Operator> it = plan.getOperators();
        if( !( it.next() instanceof ProjectExpression ) || it.hasNext() ) {
            throw new NonProjectExpressionException( intStream, loc, expr );
        }
        Operator op = null;
        ProjectExpression projExpr = (ProjectExpression)expr;
        String colAlias = projExpr.getColAlias();
        if( colAlias != null ) {
            op = operators.get( colAlias );
            if( op == null ) {
                op = createInnerLoad(loc, innerPlan, foreach, colAlias );
                op.setLocation( projExpr.getLocation() );
                innerPlan.add( op );
            }
        } else {
            op = new LOInnerLoad( innerPlan, foreach, projExpr.getColNum() );
            op.setLocation( projExpr.getLocation() );
            innerPlan.add( op );
        }
        return op;
    }
    
    private LOInnerLoad createInnerLoad(SourceLocation loc, LogicalPlan innerPlan, LOForEach foreach,
            String colAlias) throws ParserValidationException {
        try {
            return new LOInnerLoad( innerPlan, foreach, colAlias );
        } catch (FrontendException e) {
            throw new ParserValidationException(intStream, loc, e);
        }
    }

    StreamingCommand buildCommand(SourceLocation loc, String cmd, List<String> shipPaths, List<String> cachePaths,
            List<HandleSpec> inputHandleSpecs, List<HandleSpec> outputHandleSpecs,
            String logDir, Integer limit) throws RecognitionException {
        StreamingCommand command = null;
        try {
            command = buildCommand( loc, cmd );
            
            // Process ship paths
            if( shipPaths != null ) {
                if( shipPaths.size() == 0 ) {
                    command.setShipFiles( false );
                } else {
                    for( String path : shipPaths )
                        command.addPathToShip( path );
                }
            }
            
            // Process cache paths
            if( cachePaths != null ) {
                for( String path : cachePaths )
                    command.addPathToCache( path );
            }
            
            // Process input handle specs
            if( inputHandleSpecs != null ) {
                for( HandleSpec spec : inputHandleSpecs )
                    command.addHandleSpec( Handle.INPUT, spec );
            }
            
            // Process output handle specs
            if( outputHandleSpecs != null ) {
                for( HandleSpec spec : outputHandleSpecs )
                    command.addHandleSpec( Handle.OUTPUT, spec );
            }
            
            // error handling
            if( logDir != null )
                command.setLogDir( logDir );
            if( limit != null )
                command.setLogFilesLimit( limit );
        } catch(IOException e) {
            throw new PlanGenerationFailureException( intStream, loc, e );
        }
        
        return command;
    }
    
    StreamingCommand buildCommand(SourceLocation loc, String cmd) throws RecognitionException {
        try {
            String[] args = StreamingCommandUtils.splitArgs( cmd );
            StreamingCommand command = new StreamingCommand( pigContext, args );
            StreamingCommandUtils validator = new StreamingCommandUtils( pigContext );
            validator.checkAutoShipSpecs( command, args );
            return command;
        } catch (ParserException e) {
            throw new InvalidCommandException( intStream, loc, cmd );
        }
    }
    
    String buildStreamOp(SourceLocation loc, String alias, String inputAlias, StreamingCommand command,
            LogicalSchema schema, IntStream input)
    throws RecognitionException {
        try {
            LOStream op = new LOStream( plan, pigContext.createExecutableManager(), command, schema );
            return buildOp( loc, op, alias, inputAlias, null );
        } catch (ExecException ex) {
            throw new PlanGenerationFailureException( input, loc, ex );
        }
    }
    
    String buildNativeOp(SourceLocation loc, String inputJar, String cmd,
            List<String> paths, String storeAlias, String loadAlias, IntStream input)
    throws RecognitionException {
        LONative op;
        try {
            op = new LONative( plan, inputJar, StreamingCommandUtils.splitArgs( cmd ) );
            pigContext.addJar( inputJar );
            for( String path : paths )
                pigContext.addJar( path );
            buildOp( loc, op, null, new ArrayList<String>(), null );
            ((LOStore)operators.get( storeAlias )).setTmpStore(true);
            plan.connect( operators.get( storeAlias ), op );
            LOLoad load = (LOLoad)operators.get( loadAlias );
            plan.connect( op, load );
            return load.getAlias();
        } catch (ParserException e) {
            throw new InvalidCommandException( input, loc, cmd );
        } catch (MalformedURLException e) {
            throw new InvalidPathException( input, loc, e);
        }
    }
    
    void setAlias(LogicalRelationalOperator op, String alias) {
        if( alias == null )
            alias = newOperatorKey();
        op.setAlias( alias );
    }
    
    void setParallel(LogicalRelationalOperator op, Integer parallel) {
        if( parallel != null ) {
            op.setRequestedParallelism( pigContext.getExecType() == ExecType.LOCAL ? 1 : parallel );
        }
    }
    
    static void setPartitioner(LogicalRelationalOperator op, String partitioner) {
        if( partitioner != null )
            op.setCustomPartitioner( partitioner );
    }
    
    FuncSpec buildFuncSpec(SourceLocation loc, String funcName, List<String> args, byte ft) throws RecognitionException {
        String[] argArray = new String[args.size()];
        FuncSpec funcSpec = new FuncSpec( funcName, args.size() == 0 ? null : args.toArray( argArray ) );
        validateFuncSpec( loc, funcSpec, ft );
        return funcSpec;
    }
    
    private void validateFuncSpec(SourceLocation loc, FuncSpec funcSpec, byte ft) throws RecognitionException {
        switch (ft) {
        case FunctionType.COMPARISONFUNC:
        case FunctionType.LOADFUNC:
        case FunctionType.STOREFUNC:
        case FunctionType.STREAMTOPIGFUNC:
        case FunctionType.PIGTOSTREAMFUNC:
            try{
                Class<?> func = PigContext.resolveClassName(funcSpec.getClassName());
                FunctionType.tryCasting(func, ft);
            } catch(Exception ex){
                throw new ParserValidationException(intStream, loc, ex);
            }
        }
    }
    
    static String unquote(String s) {
        return StringUtils.unescapeInputString( s.substring(1, s.length() - 1 ) );
    }
    
    static int undollar(String s) {
        return Integer.parseInt( s.substring( 1, s.length() ) );    
    }
    
    /**
     * Parse the long given as a string such as "34L".
     */
    static long parseLong(String s) {
        String num = s.substring( 0, s.length() - 1 );
        return Long.parseLong( num );
    }

    static Tuple buildTuple(List<Object> objList) {
        TupleFactory tf = TupleFactory.getInstance();
        return tf.newTuple( objList );
    }
    
    static DataBag createDataBag() {
        BagFactory bagFactory = BagFactory.getInstance();
        return bagFactory.newDefaultBag();
    }
    
    /**
     *  Build a project expression in foreach inner plan.
     *  The only difference here is that the projection can be for an expression alias, for which
     *  we will return whatever the expression alias represents.
     * @throws RecognitionException 
     */
    LogicalExpression buildProjectExpr(SourceLocation loc, LogicalExpressionPlan plan, LogicalRelationalOperator op,
            Map<String, LogicalExpressionPlan> exprPlans, String colAlias, int col)
    throws RecognitionException {
        ProjectExpression result = null;
        
        if( colAlias != null ) {
            LogicalExpressionPlan exprPlan = exprPlans.get( colAlias );
            if( exprPlan != null ) {
                LogicalExpressionPlan planCopy = null;
                try {
                    planCopy = exprPlan.deepCopy();
                    plan.merge( planCopy );
                } catch (FrontendException ex) {
                    throw new PlanGenerationFailureException( intStream, loc, ex );
                }
                // The projected alias is actually expression alias, so the projections in the represented
                // expression doesn't have any operator associated with it. We need to set it when we 
                // substitute the expression alias with the its expression.
                if( op != null ) {
                    Iterator<Operator> it = plan.getOperators();
                    while( it.hasNext() ) {
                        Operator o = it.next();
                        if( o instanceof ProjectExpression ) {
                            ProjectExpression projExpr = (ProjectExpression)o;
                            projExpr.setAttachedRelationalOp( op );
                        }
                    }
                }
                return (LogicalExpression)planCopy.getSources().get( 0 );// get the root of the plan
            } else {
                result = new ProjectExpression( plan, 0, colAlias, op );
                result.setLocation( loc );
                return result;
            }
        }
        result = new ProjectExpression( plan, 0, col, op );
        result.setLocation( loc );
        return result;
    }

    /**
     * Build a project expression for a projection present in global plan (not in nested foreach plan).
     * @throws ParserValidationException 
     */
    LogicalExpression buildProjectExpr(SourceLocation loc, 
            LogicalExpressionPlan plan, LogicalRelationalOperator relOp,
            int input, String colAlias, int col)
    throws ParserValidationException {
        ProjectExpression result = null;
        result = colAlias != null ?
            new ProjectExpression( plan, input, colAlias, relOp ) :
            new ProjectExpression( plan, input, col, relOp );
        result.setLocation( loc );
        return result;
    }

    /**
     * Build a project expression that projects a range of columns
     * @param loc 
     * @param plan
     * @param relOp
     * @param input
     * @param startExpr the first expression to be projected, null 
     *        if everything from first is to be projected
     * @param endExpr the last expression to be projected, null 
     *        if everything to the end is to be projected
     * @return project expression
     * @throws ParserValidationException 
     */
    LogicalExpression buildRangeProjectExpr(SourceLocation loc, LogicalExpressionPlan plan, LogicalRelationalOperator relOp,
            int input, LogicalExpression startExpr, LogicalExpression endExpr)
    throws ParserValidationException {
        
        if(startExpr == null && endExpr == null){
            // should not reach here as the parser is enforcing this condition
            String msg = "in range project (..) at least one of start or end " +
            "has to be specified. Use project-star (*) instead.";
            throw new ParserValidationException(intStream, loc, msg);
        }
        
        ProjectExpression proj = new ProjectExpression(plan, input, relOp);

        //set first column to be projected
        if(startExpr != null){
            checkRangeProjectExpr(loc, startExpr);
            ProjectExpression startProj = (ProjectExpression)startExpr;
            if(startProj.getColAlias() != null){
                try {
                    proj.setStartAlias(startProj.getColAlias());
                } catch (FrontendException e) {
                    throw new ParserValidationException(intStream, loc, e);
                }
            }else{
                proj.setStartCol(startProj.getColNum());
            }
        }else{
            proj.setStartCol(0);//project from first column
        }
        
        //set last column to be projected
        if(endExpr != null){
            checkRangeProjectExpr(loc, endExpr);
            ProjectExpression endProj = (ProjectExpression)endExpr;
            if(endProj.getColAlias() != null){
                try {
                    proj.setEndAlias(endProj.getColAlias());
                } catch (FrontendException e) {
                    throw new ParserValidationException(intStream, loc, e);
                }
            }else{
                proj.setEndCol(endProj.getColNum());
            }
        }else{
            proj.setEndCol(-1); //project to last column
        }
        
        try {
            if(startExpr != null)
                plan.removeAndReconnect(startExpr);
            if(endExpr != null)
                plan.removeAndReconnect(endExpr);
        } catch (FrontendException e) {
            throw new ParserValidationException(intStream, loc, e);
        }
        
        
        return proj;
    }

    private void checkRangeProjectExpr(SourceLocation loc, LogicalExpression startExpr)
    throws ParserValidationException {
        if(! (startExpr instanceof ProjectExpression)){
            // should not reach here as the parser is enforcing this condition
            String msg = "range project (..) can have only a simple column." +
            " Found :" + startExpr;
            throw new ParserValidationException(intStream, loc, msg);
        }
        
    }

    LogicalExpression buildUDF(SourceLocation loc, LogicalExpressionPlan plan,
            String funcName, List<LogicalExpression> args)
    throws RecognitionException {

        Class<?> func;
        try {
            func = pigContext.getClassForAlias(funcName);
            FunctionType.tryCasting(func, FunctionType.EVALFUNC);
        } catch (Exception e) {
            throw new PlanGenerationFailureException(intStream, loc, e);
        }
        
        FuncSpec funcSpec = pigContext.getFuncSpecFromAlias(funcName);
        LogicalExpression le;
        if( funcSpec == null ) {
            funcName = func.getName();
            funcSpec = new FuncSpec(funcName);
            //this point is only reached if there was no DEFINE statement for funcName
            //in which case, we pass that information along
            le = new UserFuncExpression(plan, funcSpec, args, false);
        } else {
            le = new UserFuncExpression(plan, funcSpec, args, true);
        }
        
        le.setLocation(loc);
        return le;
    }
    
    private long getNextId() {
        return getNextId(scope);
    }

    static LOFilter createNestedFilterOp(LogicalPlan plan) {
        return new LOFilter( plan );
    }
    
    static LOLimit createNestedLimitOp(LogicalPlan plan) {
        return new LOLimit ( plan );
    }
    
    // Build operator for foreach inner plan.
    Operator buildNestedFilterOp(SourceLocation loc, LOFilter op, LogicalPlan plan, String alias, 
            Operator inputOp, LogicalExpressionPlan expr) {
        op.setFilterPlan( expr );
        buildNestedOp( loc, plan, op, alias, inputOp );
        return op;
    }

    Operator buildNestedDistinctOp(SourceLocation loc, LogicalPlan plan, String alias, Operator inputOp) {
        LODistinct op = new LODistinct( plan );
        buildNestedOp( loc, plan, op, alias, inputOp );
        return op;
    }

    Operator buildNestedLimitOp(SourceLocation loc, LogicalPlan plan, String alias, Operator inputOp, long limit) {
        LOLimit op = new LOLimit( plan, limit );
        buildNestedOp( loc, plan, op, alias, inputOp );
        return op;
    }
    
    Operator buildNestedLimitOp(SourceLocation loc, LOLimit op, LogicalPlan plan, String alias, 
            Operator inputOp, LogicalExpressionPlan expr) {
        op.setLimitPlan( expr );
        buildNestedOp( loc, plan, op, alias, inputOp );
        return op;
    }
    
    Operator buildNestedCrossOp(SourceLocation loc, LogicalPlan plan, String alias, List<Operator> inputOpList) {
        LOCross op = new LOCross( plan );
        op.setNested(true);
        buildNestedOp( loc, plan, op, alias, inputOpList );
        return op;
    }
    
    private void buildNestedOp(SourceLocation loc, LogicalPlan plan, LogicalRelationalOperator op,
            String alias, Operator inputOp) {
        op.setLocation( loc );
        setAlias( op, alias );
        plan.add( op );
        plan.connect( inputOp, op );
    }
    
    private void buildNestedOp(SourceLocation loc, LogicalPlan plan, LogicalRelationalOperator op,
            String alias, List<Operator> inputOpList) {
        op.setLocation( loc );
        setAlias( op, alias );
        plan.add( op );
        for (Operator inputOp : inputOpList) {
            plan.connect( inputOp, op );
        }
    }

    static LOSort createNestedSortOp(LogicalPlan plan) {
        return new LOSort( plan );
    }
    
    /**
     * For any UNKNOWN type in the schema fields, set the type to BYTEARRAY
     * @param sch
     */
    static void setBytearrayForNULLType(LogicalSchema sch){
        for(LogicalFieldSchema fs : sch.getFields()){
            if(fs.type == DataType.NULL){
                fs.type = DataType.BYTEARRAY;
            }
            if(fs.schema != null){
                setBytearrayForNULLType(fs.schema);
            }
        }
    }
    
    static LOForEach createNestedForeachOp(LogicalPlan plan) {
    	return new LOForEach(plan);
    }
    
    Operator buildNestedSortOp(SourceLocation loc, LOSort op, LogicalPlan plan, String alias, Operator inputOp,
            List<LogicalExpressionPlan> plans, 
            List<Boolean> ascFlags, FuncSpec fs) {
        op.setSortColPlans( plans );
        if (ascFlags.isEmpty()) {
            for (int i=0;i<plans.size();i++)
                ascFlags.add(true);
        }
        op.setAscendingCols( ascFlags );
        op.setUserFunc( fs );
        buildNestedOp( loc, plan, op, alias, inputOp );
        return op;
    }
    
    Operator buildNestedForeachOp(SourceLocation loc, LOForEach op, LogicalPlan plan, String alias, 
    		Operator inputOp, LogicalPlan innerPlan)
    throws ParserValidationException
    {
    	op.setInnerPlan(innerPlan);
    	buildNestedOp(loc, plan, op, alias, inputOp);
    	return op;
    }
    
    Operator buildNestedProjectOp(SourceLocation loc, LogicalPlan innerPlan, LOForEach foreach, 
            Map<String, Operator> operators,
            String alias,
            ProjectExpression projExpr,
            List<LogicalExpressionPlan> exprPlans)
    throws ParserValidationException {
        Operator input = null;
        String colAlias = projExpr.getColAlias();
        if( colAlias != null ) {
            // ProjExpr refers to a name, which can be an alias for another operator or col name.
            Operator op = operators.get( colAlias );
            if( op != null ) {
                // ProjExpr refers to an operator alias.
                input = op ;
            } else {
                // Assuming that ProjExpr refers to a column by name. Create an LOInnerLoad
                input = createInnerLoad( loc, innerPlan, foreach, colAlias );
                input.setLocation( projExpr.getLocation() );
            }
        } else {
            // ProjExpr refers to a column by number.
            input = new LOInnerLoad( innerPlan, foreach, projExpr.getColNum() );
            input.setLocation( projExpr.getLocation() );
        }
        
        LogicalPlan lp = new LogicalPlan(); // f's inner plan
        LOForEach f = new LOForEach( innerPlan );
        f.setInnerPlan( lp );
        f.setLocation( loc );
        LOGenerate gen = new LOGenerate( lp );
        boolean[] flatten = new boolean[exprPlans.size()];
        
        List<Operator> innerLoads = new ArrayList<Operator>( exprPlans.size() );
        for( LogicalExpressionPlan plan : exprPlans ) {
            ProjectExpression pe = (ProjectExpression)plan.getSinks().get( 0 );
            String al = pe.getColAlias();
            LOInnerLoad iload = ( al == null ) ?  
                    new LOInnerLoad( lp, f, pe.getColNum() ) : createInnerLoad(loc, lp, f, al );
            iload.setLocation( pe.getLocation() );
            pe.setColNum( -1 );
            pe.setInputNum( innerLoads.size() );
            pe.setAttachedRelationalOp( gen );
            innerLoads.add( iload );
        }
        
        gen.setOutputPlans( exprPlans );
        gen.setFlattenFlags( flatten );
        lp.add( gen );

        for( Operator il : innerLoads ) {
            lp.add( il );
            lp.connect( il, gen );
        }
        
        // Connect the inner load operators to gen
        setAlias( f, alias );
        innerPlan.add( input );
        innerPlan.add( f );
        innerPlan.connect( input, f );
        return f;
    }
    
    GROUPTYPE parseGroupType(String hint, SourceLocation loc) throws ParserValidationException {
        String modifier = unquote( hint );
        
        if( modifier.equalsIgnoreCase( "collected" ) ) {
            return GROUPTYPE.COLLECTED;
        } else if( modifier.equalsIgnoreCase( "regular" ) ){
            return GROUPTYPE.REGULAR;
        } else if( modifier.equalsIgnoreCase( "merge" ) ){
            return GROUPTYPE.MERGE;
        } else {
            throw new ParserValidationException( intStream, loc,
                "Only COLLECTED, REGULAR or MERGE are valid GROUP modifiers." );
        }
    }
    
    JOINTYPE parseJoinType(String hint, SourceLocation loc) throws ParserValidationException {
        String modifier = unquote( hint );

        if( modifier.equalsIgnoreCase( "repl" ) || modifier.equalsIgnoreCase( "replicated" ) ) {
                  return JOINTYPE.REPLICATED; 
          } else if( modifier.equalsIgnoreCase( "hash" ) || modifier.equalsIgnoreCase( "default" ) ) {
                  return LOJoin.JOINTYPE.HASH;
          } else if( modifier.equalsIgnoreCase( "skewed" ) ) {
                 return JOINTYPE.SKEWED;
          } else if (modifier.equalsIgnoreCase("merge")) {
                  return JOINTYPE.MERGE;
          } else if (modifier.equalsIgnoreCase("merge-sparse")) {
                  return JOINTYPE.MERGESPARSE;
          } else {
                  throw new ParserValidationException( intStream, loc,
                      "Only REPL, REPLICATED, HASH, SKEWED, MERGE, and MERGE-SPARSE are vaild JOIN modifiers." );
          }
    }
    
    void putOperator(String alias, Operator op) {
        operators.put(alias, op);
    }

}
