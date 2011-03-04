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
import java.net.URISyntaxException;
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
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.builtin.RANDOM;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.parser.ParseException;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.streaming.StreamingCommand;
import org.apache.pig.impl.streaming.StreamingCommand.Handle;
import org.apache.pig.impl.streaming.StreamingCommand.HandleSpec;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.impl.util.StringUtils;
import org.apache.pig.newplan.logical.expression.ConstantExpression;
import org.apache.pig.newplan.logical.expression.LessThanEqualExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.expression.UserFuncExpression;
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
import org.apache.pig.newplan.logical.relational.LOCogroup.GROUPTYPE;
import org.apache.pig.newplan.logical.relational.LOJoin.JOINTYPE;
import org.apache.pig.newplan.logical.relational.LogicalSchema.LogicalFieldSchema;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;

public class LogicalPlanBuilder {
    private LogicalPlan plan = new LogicalPlan();

    private Map<String, Operator> operators = new HashMap<String, Operator>();
    
    Map<String, String> fileNameMap;
    
    private PigContext pigContext = null;
    private String scope = null;
    private IntStream intStream;
    
    private static NodeIdGenerator nodeIdGen = NodeIdGenerator.getGenerator();
    
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

    String buildFilterOp(LOFilter op, String alias, String inputAlias, LogicalExpressionPlan expr) {
        op.setFilterPlan( expr );
        return buildOp( op, alias, inputAlias, null );
    }
    
    String buildDistinctOp(String alias, String inputAlias, String partitioner) {
        LODistinct op = new LODistinct( plan );
        return buildOp( op, alias, inputAlias, partitioner );
    }

    String buildLimitOp(String alias, String inputAlias, long limit) {
        LOLimit op = new LOLimit( plan, limit );
        return buildOp( op, alias, inputAlias, null );
    }
    
    String buildSampleOp(String alias, String inputAlias, double value) {
        LogicalExpressionPlan filterPlan = new LogicalExpressionPlan();
        //  Generate a filter condition.
        LogicalExpression konst = new ConstantExpression( filterPlan, value);
        UserFuncExpression udf = new UserFuncExpression( filterPlan, new FuncSpec( RANDOM.class.getName() ) );
        new LessThanEqualExpression( filterPlan, udf, konst );
        return buildFilterOp( new LOFilter( plan ), alias, inputAlias, filterPlan );
    }
    
    String buildUnionOp(String alias, List<String> inputAliases, boolean onSchema) {
        LOUnion op = new LOUnion( plan, onSchema );
        return buildOp( op, alias, inputAliases, null );
    }

    String buildSplitOp(String inputAlias) {
        LOSplit op = new LOSplit( plan );
        return buildOp( op, null, inputAlias, null );
    }
    
    LOSplitOutput createSplitOutputOp() {
        return  new LOSplitOutput( plan );
    }
    
    String buildSplitOutputOp(LOSplitOutput op, String alias, String inputAlias,
            LogicalExpressionPlan filterPlan) {
        op.setFilterPlan( filterPlan );
        return buildOp ( op, alias, inputAlias, null );
    }
    
    String buildCrossOp(String alias, List<String> inputAliases, String partitioner) {
        LOCross op = new LOCross( plan );
        return buildOp ( op, alias, inputAliases, partitioner );
    }
    
    LOSort createSortOp() {
        return new LOSort( plan );
    }
    
    String buildSortOp(LOSort sort, String alias, String inputAlias, List<LogicalExpressionPlan> plans, 
            List<Boolean> ascFlags, FuncSpec fs) {
        sort.setSortColPlans( plans );
        sort.setUserFunc( fs );
        if (ascFlags.isEmpty()) {
            for (int i=0;i<plans.size();i++)
                ascFlags.add(true);
        }
        sort.setAscendingCols( ascFlags );
        return buildOp( sort, alias, inputAlias, null );
    }
    
    LOJoin createJoinOp() {
        return new LOJoin( plan );
    }

    String buildJoinOp(LOJoin op, String alias, List<String> inputAliases,
            MultiMap<Integer, LogicalExpressionPlan> joinPlans,
            JOINTYPE jt, List<Boolean> innerFlags, String partitioner) {
        if (jt==null)
            jt = JOINTYPE.HASH;
        else {
            op.pinOption(LOJoin.OPTION_JOIN);
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
        return buildOp( op, alias, inputAliases, partitioner );
    }

    LOCogroup createGroupOp() {
        return new LOCogroup( plan );
    }
    
    String buildGroupOp(LOCogroup op, String alias, List<String> inputAliases, 
        MultiMap<Integer, LogicalExpressionPlan> expressionPlans, GROUPTYPE gt, List<Boolean> innerFlags,
        String partitioner) throws ParserValidationException {
        if( gt == GROUPTYPE.COLLECTED ) {
            List<LogicalExpressionPlan> exprPlans = expressionPlans.get( 0 );
            for( LogicalExpressionPlan exprPlan : exprPlans ) {
                Iterator<Operator> it = exprPlan.getOperators();
                while( it.hasNext() ) {
                    if( !( it.next() instanceof ProjectExpression ) ) {
                        throw new ParserValidationException( intStream, "Collected group is only supported for columns or star projection" );
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
        return buildOp( op, alias, inputAliases, partitioner );
    }
    
    private String getAbolutePathForLoad(String filename, FuncSpec funcSpec)
    throws IOException, URISyntaxException {
        if( funcSpec == null ){
            funcSpec = new FuncSpec( PigStorage.class.getName() );
        }

        LoadFunc loFunc = (LoadFunc)PigContext.instantiateFuncFromSpec( funcSpec );
        String sig = QueryParserUtils.constructFileNameSignature( filename, funcSpec );
        String absolutePath = fileNameMap.get( sig );
        if (absolutePath == null) {
            absolutePath = loFunc.relativeToAbsolutePath( filename, QueryParserUtils.getCurrentDir( pigContext ) );

            if (absolutePath!=null) {
                QueryParserUtils.setHdfsServers( absolutePath, pigContext );
            }
            fileNameMap.put( sig, absolutePath );
        }
        
        return absolutePath;
    }
     
    String buildLoadOp(String alias, String filename, FuncSpec funcSpec, LogicalSchema schema)
    throws ParserValidationException {
        String absolutePath = filename;
        try {
            absolutePath = getAbolutePathForLoad( filename, funcSpec );
        } catch(Exception ex) {
            throw new ParserValidationException( intStream, ex );
        }
        
        FileSpec loader = new FileSpec( absolutePath, funcSpec );
        LOLoad op = new LOLoad( loader, schema, plan, ConfigurationUtil.toConfiguration( pigContext.getProperties() ) );
        return buildOp( op, alias, new ArrayList<String>(), null );
    }
    
    private String buildOp(LogicalRelationalOperator op, String alias, String inputAlias, String partitioner) {
        List<String> inputAliases = new ArrayList<String>();
        if( inputAlias != null )
            inputAliases.add( inputAlias );
        return buildOp( op, alias, inputAliases, partitioner );
    }
    
    private String buildOp(LogicalRelationalOperator op, String alias, List<String> inputAliases, String partitioner) {
        setAlias( op, alias );
        setPartitioner( op, partitioner );
        plan.add( op );
        for( String a : inputAliases ) {
            Operator pred = operators.get( a );
            plan.connect( pred, op );
        }
        operators.put( op.getAlias(), op );
        return op.getAlias();
    }

    private String getAbolutePathForStore(String inputAlias, String filename, FuncSpec funcSpec)
    throws IOException, URISyntaxException {
        if( funcSpec == null ){
            funcSpec = new FuncSpec( PigStorage.class.getName() );
        }
        
        Object obj = PigContext.instantiateFuncFromSpec( funcSpec );
        StoreFuncInterface stoFunc = (StoreFuncInterface)obj;
        String sig = QueryParserUtils.constructSignature( inputAlias, filename, funcSpec);
        stoFunc.setStoreFuncUDFContextSignature( sig );
        String absolutePath = fileNameMap.get( sig );
        if (absolutePath == null) {
            absolutePath = stoFunc.relToAbsPathForStoreLocation( filename, 
                    QueryParserUtils.getCurrentDir( pigContext ) );
            if (absolutePath!=null) {
                QueryParserUtils.setHdfsServers( absolutePath, pigContext );
            }
            fileNameMap.put( sig, absolutePath );
        }
        
        return absolutePath;
    }

    String buildStoreOp(String alias, String inputAlias, String filename, FuncSpec funcSpec)
    throws ParserValidationException {
        String absPath = filename;
        try {
            absPath = getAbolutePathForStore( inputAlias, filename, funcSpec );
        } catch(Exception ex) {
            throw new ParserValidationException( intStream, ex );
        }
        
        FileSpec fileSpec = new FileSpec( absPath, funcSpec );
        LOStore op = new LOStore( plan, fileSpec );
        return buildOp( op, alias, inputAlias, null );
    }
    
    LOForEach createForeachOp() {
        return new LOForEach( plan );
    }
    
    String buildForeachOp(LOForEach op, String alias, String inputAlias, LogicalPlan innerPlan) {
        op.setInnerPlan( innerPlan );
        return buildOp( op, alias, inputAlias, null );
    }
    
    LOGenerate createGenerateOp(LogicalPlan plan) {
        return new LOGenerate( plan );
    }
    
    static void buildGenerateOp(LOForEach foreach, LOGenerate gen, Map<String, Operator> operators,
            List<LogicalExpressionPlan> exprPlans, List<Boolean> flattenFlags,
            List<LogicalSchema> schemas) {
        boolean[] flags = new boolean[ flattenFlags.size() ];
        for( int i = 0; i < flattenFlags.size(); i++ )
            flags[i] = flattenFlags.get( i );
        LogicalPlan innerPlan = (LogicalPlan)gen.getPlan();
        ArrayList<Operator> inputs = new ArrayList<Operator>();
        for( LogicalExpressionPlan exprPlan : exprPlans ) {
            processExpressionPlan( foreach, innerPlan, exprPlan, operators, inputs );
        }
        
        gen.setOutputPlans( exprPlans );
        gen.setFlattenFlags( flags );
        gen.setUserDefinedSchema( schemas );
        innerPlan.add( gen );
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
     */
    private static void processExpressionPlan(LOForEach foreach,
                                      LogicalPlan lp,  
                                      LogicalExpressionPlan plan,  
                                      Map<String, Operator> operators,  
                                      ArrayList<Operator> inputs ) {
        Iterator<Operator> it = plan.getOperators();
        while( it.hasNext() ) {
            Operator sink = it.next();
            //check all ProjectExpression
            if( sink instanceof ProjectExpression ) {
                ProjectExpression projExpr = (ProjectExpression)sink;
                String colAlias = projExpr.getColAlias();
                if( colAlias != null ) {
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
                        projExpr.setInputNum( inputs.size() );
                        LOInnerLoad innerLoad = new LOInnerLoad( lp, foreach, colAlias );
                        projExpr.setColNum( -1 ); // Projection Expression on InnerLoad is always (*).
                        lp.add( innerLoad );
                        inputs.add( innerLoad );
                    }
                } else {
                    // the project expression is referring to column in ForEach input
                    // using position (eg $1)
                    projExpr.setInputNum( inputs.size() );
                    LOInnerLoad innerLoad = new LOInnerLoad( lp, foreach, projExpr.getColNum() );
                    projExpr.setColNum( -1 ); // Projection Expression on InnerLoad is always (*).
                    lp.add( innerLoad );
                    inputs.add( innerLoad );
                }
            }
        }
    }
    
    Operator buildNestedOperatorInput(LogicalPlan innerPlan, LOForEach foreach, 
            Map<String, Operator> operators, LogicalExpression expr)
    throws NonProjectExpressionException {
        OperatorPlan plan = expr.getPlan();
        Iterator<Operator> it = plan.getOperators();
        if( !( it.next() instanceof ProjectExpression ) || it.hasNext() ) {
            throw new NonProjectExpressionException( intStream, expr );
        }
        Operator op = null;
        ProjectExpression projExpr = (ProjectExpression)expr;
        String colAlias = projExpr.getColAlias();
        if( colAlias != null ) {
            op = operators.get( colAlias );
            if( op == null ) {
                op = new LOInnerLoad( innerPlan, foreach, colAlias );
                innerPlan.add( op );
            }
        } else {
            op = new LOInnerLoad( innerPlan, foreach, projExpr.getColNum() );
            innerPlan.add( op );
        }
        return op;
    }
    
    StreamingCommand buildCommand(String cmd, List<String> shipPaths, List<String> cachePaths,
            List<HandleSpec> inputHandleSpecs, List<HandleSpec> outputHandleSpecs,
            String logDir, Integer limit) throws RecognitionException {
        StreamingCommand command = null;
        try {
            command = buildCommand( cmd );
            
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
            throw new PlanGenerationFailureException( intStream, e );
        }
        
        return command;
    }
    
    StreamingCommand buildCommand(String cmd) throws RecognitionException {
        try {
            String[] args = StreamingCommandUtils.splitArgs( cmd );
            StreamingCommand command = new StreamingCommand( pigContext, args );
            StreamingCommandUtils validator = new StreamingCommandUtils( pigContext );
            validator.checkAutoShipSpecs( command, args );
            return command;
        } catch (ParseException e) {
            throw new InvalidCommandException( intStream, cmd );
        }
    }
    
    String buildStreamOp(String alias, String inputAlias, StreamingCommand command,
            LogicalSchema schema, IntStream input)
    throws RecognitionException {
        try {
            LOStream op = new LOStream( plan, pigContext.createExecutableManager(), command, schema );
            return buildOp( op, alias, inputAlias, null );
        } catch (ExecException ex) {
            throw new PlanGenerationFailureException( input, ex );
        }
    }
    
    String buildNativeOp(String inputJar, String cmd,
            List<String> paths, String storeAlias, String loadAlias, IntStream input)
    throws RecognitionException {
        LONative op;
        try {
            op = new LONative( plan, inputJar, StreamingCommandUtils.splitArgs( cmd ) );
            pigContext.addJar( inputJar );
            for( String path : paths )
                pigContext.addJar( path );
            buildOp( op, null, new ArrayList<String>(), null );
            ((LOStore)operators.get( storeAlias )).setTmpStore(true);
            plan.connect( operators.get( storeAlias ), op );
            LOLoad load = (LOLoad)operators.get( loadAlias );
            plan.connect( op, load );
            return load.getAlias();
        } catch (ParseException e) {
            throw new InvalidCommandException( input, cmd );
        } catch (MalformedURLException e) {
            throw new InvalidPathException( input, e);
        }
    }
    
    void setAlias(LogicalRelationalOperator op, String alias) {
        if( alias == null )
            alias = new OperatorKey( scope, getNextId() ).toString();
        op.setAlias( alias );
    }
    
    static void setParallel(LogicalRelationalOperator op, Integer parallel) {
        if( parallel != null )
            op.setRequestedParallelism( parallel );
    }
    
    static void setPartitioner(LogicalRelationalOperator op, String partitioner) {
        if( partitioner != null )
            op.setCustomPartitioner( partitioner );
    }
    
    FuncSpec buildFuncSpec(String funcName, List<String> args, byte ft) throws RecognitionException {
        String[] argArray = new String[args.size()];
        FuncSpec funcSpec = new FuncSpec( funcName, args.size() == 0 ? null : args.toArray( argArray ) );
        validateFuncSpec( funcSpec, ft );
        return funcSpec;
    }
    
    private void validateFuncSpec(FuncSpec funcSpec, byte ft) throws RecognitionException {
        switch( ft ) {
        case FunctionType.COMPARISONFUNC:
        case FunctionType.LOADFUNC:
        case FunctionType.STOREFUNC:
        case FunctionType.STREAMTOPIGFUNC:
        case FunctionType.PIGTOSTREAMFUNC:
            Object func = PigContext.instantiateFuncFromSpec( funcSpec );
            try{
                FunctionType.tryCasting( func, ft );
            } catch(Exception ex){
                throw new ParserValidationException( intStream, ex );
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
    LogicalExpression buildProjectExpr(LogicalExpressionPlan plan, LogicalRelationalOperator op,
            Map<String, LogicalExpressionPlan> exprPlans, String colAlias, int col)
    throws RecognitionException {
        if( colAlias != null ) {
            LogicalExpressionPlan exprPlan = exprPlans.get( colAlias );
            if( exprPlan != null ) {
                LogicalExpressionPlan planCopy = null;
                try {
                    planCopy = exprPlan.deepCopy();
                    plan.merge( planCopy );
                } catch (FrontendException ex) {
                    throw new PlanGenerationFailureException( intStream, ex );
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
                return new ProjectExpression( plan, 0, colAlias, op );
            }
        }
        return new ProjectExpression( plan, 0, col, op );
    }

    /**
     * Build a project expression for a projection present in global plan (not in nested foreach plan).
     */
    LogicalExpression buildProjectExpr(LogicalExpressionPlan plan, LogicalRelationalOperator relOp,
            int input, String colAlias, int col) {
        if( colAlias != null )
            return new ProjectExpression( plan, input, colAlias, relOp );
        return new ProjectExpression( plan, input, col, relOp );
    }
    
    LogicalExpression buildUDF(LogicalExpressionPlan plan, String funcName, List<LogicalExpression> args)
    throws RecognitionException {
        Object func;
        try {
            func = pigContext.instantiateFuncFromAlias( funcName );
            FunctionType.tryCasting( func, FunctionType.EVALFUNC );
        } catch (Exception e) {
            throw new PlanGenerationFailureException( intStream, e );
        }
        
        
        FuncSpec funcSpec = pigContext.getFuncSpecFromAlias( funcName );
        if( funcSpec == null ) {
            funcName = func.getClass().getName();
            funcSpec = new FuncSpec( funcName );
        }
        
        return new UserFuncExpression( plan, funcSpec, args );
    }
    
    private long getNextId() {
        return nodeIdGen.getNextNodeId( scope );
    }

    static LOFilter createNestedFilterOp(LogicalPlan plan) {
        return new LOFilter( plan );
    }
    
    // Build operator for foreach inner plan.
    Operator buildNestedFilterOp(LOFilter op, LogicalPlan plan, String alias, 
            Operator inputOp, LogicalExpressionPlan expr) {
        op.setFilterPlan( expr );
        buildNestedOp( plan, op, alias, inputOp );
        return op;
    }

    Operator buildNestedDistinctOp(LogicalPlan plan, String alias, Operator inputOp) {
        LODistinct op = new LODistinct( plan );
        buildNestedOp( plan, op, alias, inputOp );
        return op;
    }

    Operator buildNestedLimitOp(LogicalPlan plan, String alias, Operator inputOp, long limit) {
        LOLimit op = new LOLimit( plan, limit );
        buildNestedOp( plan, op, alias, inputOp );
        return op;
    }
    
    private void buildNestedOp(LogicalPlan plan, LogicalRelationalOperator op, String alias, Operator inputOp) {
        setAlias( op, alias );
        plan.add( op );
        plan.connect( inputOp, op );
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
    
    Operator buildNestedSortOp(LOSort op, LogicalPlan plan, String alias, Operator inputOp,
            List<LogicalExpressionPlan> plans, 
            List<Boolean> ascFlags, FuncSpec fs) {
        op.setSortColPlans( plans );
        if (ascFlags.isEmpty()) {
            for (int i=0;i<plans.size();i++)
                ascFlags.add(true);
        }
        op.setAscendingCols( ascFlags );
        op.setUserFunc( fs );
        buildNestedOp( plan, op, alias, inputOp );
        return op;
    }
    
    Operator buildNestedProjectOp(LogicalPlan innerPlan, LOForEach foreach, 
            Map<String, Operator> operators,
            String alias, ProjectExpression projExpr, List<LogicalExpressionPlan> exprPlans) {
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
                input = new LOInnerLoad( innerPlan, foreach, colAlias );
            }
        } else {
            // ProjExpr refers to a column by number.
            input = new LOInnerLoad( innerPlan, foreach, projExpr.getColNum() );
        }
        
        LogicalPlan lp = new LogicalPlan(); // f's inner plan
        LOForEach f = new LOForEach( innerPlan );
        f.setInnerPlan( lp );
        LOGenerate gen = new LOGenerate( lp );
        boolean[] flatten = new boolean[exprPlans.size()];
        
        List<Operator> innerLoads = new ArrayList<Operator>( exprPlans.size() );
        for( LogicalExpressionPlan plan : exprPlans ) {
            ProjectExpression pe = (ProjectExpression)plan.getSinks().get( 0 );
            String al = pe.getColAlias();
            LOInnerLoad iload = ( al == null ) ?  
                    new LOInnerLoad( lp, f, pe.getColNum() ) : new LOInnerLoad( lp, f, al );
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
    
}
