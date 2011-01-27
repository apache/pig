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
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.builtin.RANDOM;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
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
    
    // TODO: hook up with the real PigContext instance instead of creating one here.
    private PigContext pigContext = new PigContext( ExecType.LOCAL, new Properties() );
    
    private static NodeIdGenerator nodeIdGen = NodeIdGenerator.getGenerator();

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
    
    LOFilter createFilterOp() {
        return new LOFilter( plan );
    }

    String buildFilterOp(LOFilter op, String alias, Integer parallel, String inputAlias, LogicalExpressionPlan expr) {
        op.setFilterPlan( expr );
        return buildOp( op, alias, parallel, inputAlias, null );
    }
    
    String buildDistinctOp(String alias, Integer parallel, String inputAlias, String partitioner) {
        LODistinct op = new LODistinct( plan );
        return buildOp( op, alias, parallel, inputAlias, partitioner );
    }

    String buildLimitOp(String alias, Integer parallel, String inputAlias, long limit) {
        LOLimit op = new LOLimit( plan, limit );
        return buildOp( op, alias, parallel, inputAlias, null );
    }
    
    String buildSampleOp(String alias, Integer parallel, String inputAlias, double value) {
        LogicalExpressionPlan filterPlan = new LogicalExpressionPlan();
        //  Generate a filter condition.
        LogicalExpression konst = new ConstantExpression( filterPlan, value,
                   new LogicalFieldSchema( null , null, DataType.DOUBLE ) );
        UserFuncExpression udf = new UserFuncExpression( filterPlan, new FuncSpec( RANDOM.class.getName() ) );
        new LessThanEqualExpression( filterPlan, udf, konst );
        return buildFilterOp( new LOFilter( plan ), alias, parallel, inputAlias, filterPlan );
    }
    
    String buildUnionOp(String alias, Integer parallel, List<String> inputAliases, boolean onSchema) {
        LOUnion op = new LOUnion( plan, onSchema );
        return buildOp( op, alias, parallel, inputAliases, null );
    }

    String buildSplitOp(String inputAlias) {
        LOSplit op = new LOSplit( plan );
        return buildOp( op, null, null, inputAlias, null );
    }
    
    LOSplitOutput createSplitOutputOp() {
        return  new LOSplitOutput( plan );
    }
    
    String buildSplitOutputOp(LOSplitOutput op, String alias, Integer parallel, String inputAlias,
            LogicalExpressionPlan filterPlan) {
        op.setFilterPlan( filterPlan );
        return buildOp ( op, alias, parallel, inputAlias, null );
    }
    
    String buildCrossOp(String alias, Integer parallel, List<String> inputAliases, String partitioner) {
        LOCross op = new LOCross( plan );
        return buildOp ( op, alias, parallel, inputAliases, partitioner );
    }
    
    LOSort createSortOp() {
        return new LOSort( plan );
    }
    
    String buildSortOp(LOSort sort, String alias, Integer parallel, String inputAlias, List<LogicalExpressionPlan> plans, 
            List<Boolean> ascFlags, FuncSpec fs) {
        sort.setSortColPlans( plans );
        sort.setUserFunc( fs );
        sort.setAscendingCols( ascFlags );
        return buildOp( sort, alias, parallel, inputAlias, null );
    }
    
    LOJoin createJoinOp() {
        return new LOJoin( plan );
    }

    String buildJoinOp(LOJoin op, String alias, Integer parallel, List<String> inputAliases,
            MultiMap<Integer, LogicalExpressionPlan> joinPlans,
            JOINTYPE jt, List<Boolean> innerFlags, String partitioner) {
        boolean[] flags = new boolean[innerFlags.size()];
        for( int i = 0; i < innerFlags.size(); i++ ) {
            flags[i] = innerFlags.get( i );
        }
        op.setJoinType( jt );
        op.setInnerFlags( flags );
        op.setJoinPlans( joinPlans );
        return buildOp( op, alias, parallel, inputAliases, partitioner );
    }

    LOCogroup createGroupOp() {
        return new LOCogroup( plan );
    }
    
    String buildGroupOp(LOCogroup op, String alias, Integer parallel, List<String> inputAliases, 
        MultiMap<Integer, LogicalExpressionPlan> expressionPlans, GROUPTYPE gt, List<Boolean> innerFlags) {
        boolean[] flags = new boolean[innerFlags.size()];
        for( int i = 0; i < innerFlags.size(); i++ ) {
            flags[i] = innerFlags.get( i );
        }
        op.setExpressionPlans( expressionPlans );
        op.setGroupType( gt );
        op.setInnerFlags( flags );
        return buildOp( op, alias, parallel, inputAliases, null );
    }
    
    String buildLoadOp(String alias, Integer parallel, String filename, FuncSpec funcSpec, LogicalSchema schema) {
        FileSpec loader = new FileSpec( filename, funcSpec );
        LOLoad op = new LOLoad( loader, schema, plan, null );
        return buildOp( op, alias, parallel, new ArrayList<String>(), null );
    }
    
    private String buildOp(LogicalRelationalOperator op, String alias, Integer parallel,
            String inputAlias, String partitioner) {
        List<String> inputAliases = new ArrayList<String>();
        if( inputAlias != null )
            inputAliases.add( inputAlias );
        return buildOp( op, alias, parallel, inputAliases, partitioner );
    }
    
    private String buildOp(LogicalRelationalOperator op, String alias, Integer parallel, 
            List<String> inputAliases, String partitioner) {
        setAlias( op, alias );
        setParallel( op, parallel );
        setPartitioner( op, partitioner );
        plan.add( op );
        for( String a : inputAliases ) {
            Operator pred = operators.get( a );
            plan.connect( pred, op );
        }
        operators.put( op.getAlias(), op );
        return op.getAlias();
    }

    String buildStoreOp(String alias, Integer parallel, String inputAlias, String filename, FuncSpec funcSpec) {
        FileSpec fileSpec = new FileSpec( filename, funcSpec );
        LOStore op = new LOStore( plan, fileSpec );
        return buildOp( op, alias, parallel, inputAlias, null );
    }
    
    LOForEach createForeachOp() {
        return new LOForEach( plan );
    }
    
    String buildForeachOp(LOForEach op, String alias, Integer parallel, String inputAlias, LogicalPlan innerPlan) {
        op.setInnerPlan( innerPlan );
        return buildOp( op, alias, parallel, inputAlias, null );
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
    
    static Operator buildNestedOperatorInput(LogicalPlan innerPlan, LOForEach foreach, 
            Map<String, Operator> operators, LogicalExpression expr, IntStream input)
    throws NonProjectExpressionException {
        OperatorPlan plan = expr.getPlan();
        Iterator<Operator> it = plan.getOperators();
        if( !( it.next() instanceof ProjectExpression ) || it.hasNext() ) {
            throw new NonProjectExpressionException( input, expr );
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
            String logDir, Integer limit, IntStream input) throws RecognitionException {
        StreamingCommand command = null;
        try {
            command = buildCommand( cmd, input );
            
            // Process ship paths
            if( shipPaths.size() == 0 ) {
                command.setShipFiles( false );
            } else {
                for( String path : shipPaths )
                    command.addPathToShip( path );
            }
            
            // Process cache paths
            for( String path : cachePaths )
                command.addPathToCache( path );
            
            // Process input handle specs
            for( HandleSpec spec : inputHandleSpecs )
                command.addHandleSpec( Handle.INPUT, spec );
            
            // Process output handle specs
            for( HandleSpec spec : outputHandleSpecs )
                command.addHandleSpec( Handle.OUTPUT, spec );
            
            // error handling
            command.setLogDir( logDir );
            if( limit != null )
                command.setLogFilesLimit( limit );
        } catch(IOException e) {
            throw new PlanGenerationFailureException( input, e );
        }
        
        return command;
    }
    
    StreamingCommand buildCommand(String cmd, IntStream input) throws RecognitionException {
        try {
            return new StreamingCommand( pigContext, splitArgs( cmd ) );
        } catch (ParseException e) {
            throw new InvalidCommandException( input, cmd );        }
    }
    
    String buildStreamOp(String alias, Integer parallel, String inputAlias, StreamingCommand command,
            LogicalSchema schema, IntStream input)
    throws RecognitionException {
        try {
            LOStream op = new LOStream( plan, pigContext.createExecutableManager(), command, schema );
            return buildOp( op, alias, parallel, inputAlias, null );
        } catch (ExecException ex) {
            throw new PlanGenerationFailureException( input, ex );
        }
    }
    
    String buildNativeOp(Integer parallel, String inputJar, String cmd,
            List<String> paths, String storeAlias, String loadAlias, IntStream input)
    throws RecognitionException {
        LONative op;
        try {
            op = new LONative( plan, inputJar, splitArgs( cmd ) );
            pigContext.addJar( inputJar );
            for( String path : paths )
                pigContext.addJar( path );
           buildOp( op, null, parallel, new ArrayList<String>(), null );
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
    
    static void setAlias(LogicalRelationalOperator op, String alias) {
        if( alias == null )
            alias = new OperatorKey( "test", getNextId() ).toString();
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
    
    static FuncSpec buildFuncSpec(String funcName, List<String> args) {
        String[] argArray = new String[args.size()];
        return new FuncSpec( funcName, args.toArray( argArray ) );
    }
    
    static String unquote(String s) {
        return StringUtils.unescapeInputString( s.substring(1, s.length() - 1 ) );
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
     */
    LogicalExpression buildProjectExpr(LogicalExpressionPlan plan, LogicalRelationalOperator op,
            Map<String, LogicalExpressionPlan> exprPlans, String colAlias, int col) {
        if( colAlias != null ) {
            LogicalExpressionPlan exprPlan = exprPlans.get( colAlias );
            if( exprPlan != null ) {
                return (LogicalExpression)exprPlan.getSources().get( 0 );// get the root of the plan
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
    
    LogicalExpression buildUDF(LogicalExpressionPlan plan, String funcName, List<LogicalExpression> args) {
        FuncSpec funcSpec = null;// TODO: get funcspec from function name.
        return new UserFuncExpression( plan, funcSpec, args );
    }
    
    private static final char SINGLE_QUOTE = '\u005c'';
    private static final char DOUBLE_QUOTE = '"';
    private static String[] splitArgs(String command) throws ParseException {
        List<String> argv = new ArrayList<String>();

        int beginIndex = 0;

        while (beginIndex < command.length()) {
            // Skip spaces
            while (Character.isWhitespace(command.charAt(beginIndex))) {
                ++beginIndex;
            }

            char delim = ' ';
            char charAtIndex = command.charAt(beginIndex);
            if (charAtIndex == SINGLE_QUOTE || charAtIndex == DOUBLE_QUOTE) {
                delim = charAtIndex;
            }

            int endIndex = command.indexOf(delim, beginIndex+1);
            if (endIndex == -1) {
                if (Character.isWhitespace(delim)) {
                    // Reached end of command-line
                    argv.add(command.substring(beginIndex));
                    break;
                } else {
                    // Didn't find the ending quote/double-quote
                    throw new ParseException("Illegal command: " + command);
                }
            }

            if (Character.isWhitespace(delim)) {
                // Do not consume the space
                argv.add(command.substring(beginIndex, endIndex));
            } else {
                argv.add(command.substring(beginIndex, endIndex+1));
            }

            beginIndex = endIndex + 1;
        }

        return argv.toArray(new String[argv.size()]);
    }

    private static long getNextId() {
        return nodeIdGen.getNextNodeId( "test" );
    }

    static LOFilter createNestedFilterOp(LogicalPlan plan) {
        return new LOFilter( plan );
    }
    
    // Build operator for foreach inner plan.
    static Operator buildNestedFilterOp(LOFilter op, LogicalPlan plan, String alias, 
            Operator inputOp, LogicalExpressionPlan expr) {
        op.setFilterPlan( expr );
        buildNestedOp( plan, op, alias, inputOp );
        return op;
    }

    static Operator buildNestedDistinctOp(LogicalPlan plan, String alias, Operator inputOp) {
        LODistinct op = new LODistinct( plan );
        buildNestedOp( plan, op, alias, inputOp );
        return op;
    }

    static Operator buildNestedLimitOp(LogicalPlan plan, String alias, Operator inputOp, long limit) {
        LOLimit op = new LOLimit( plan, limit );
        buildNestedOp( plan, op, alias, inputOp );
        return op;
    }
    
    private static void buildNestedOp(LogicalPlan plan, LogicalRelationalOperator op, String alias, Operator inputOp) {
        setAlias( op, alias );
        plan.add( op );
        plan.connect( inputOp, op );
    }

    static LOSort createNestedSortOp(LogicalPlan plan) {
        return new LOSort( plan );
    }
    
    static Operator buildNestedSortOp(LOSort op, LogicalPlan plan, String alias, Operator inputOp,
            List<LogicalExpressionPlan> plans, 
            List<Boolean> ascFlags, FuncSpec fs) {
        op.setSortColPlans( plans );
        op.setAscendingCols( ascFlags );
        op.setUserFunc( fs );
        buildNestedOp( plan, op, alias, inputOp );
        return op;
    }
    
    static Operator buildNestedProjectOp(LogicalPlan innerPlan, LOForEach foreach, 
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
