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
import java.util.List;
import java.util.Map;
import java.util.Properties;

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

    String buildFilterOp(String alias, Integer parallel, String inputAlias, LogicalExpressionPlan expr) {
        LOFilter op = new LOFilter( plan, expr );
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
        return buildFilterOp( alias, parallel, inputAlias, filterPlan );
    }
    
    String buildUnionOp(String alias, Integer parallel, List<String> inputAliases) {
        LOUnion op = new LOUnion( plan );
        return buildOp( op, alias, parallel, inputAliases, null );
    }

    String buildSplitOp(String inputAlias) {
        LOSplit op = new LOSplit( plan );
        return buildOp( op, null, null, inputAlias, null );
    }
    
    String buildSplitOutputOp(String alias, Integer parallel, String inputAlias, LogicalExpressionPlan filterPlan) {
        LOSplitOutput op = new LOSplitOutput( plan, filterPlan );
        return buildOp ( op, alias, parallel, inputAlias, null );
    }
    
    String buildCrossOp(String alias, Integer parallel, List<String> inputAliases, String partitioner) {
        LOCross op = new LOCross( plan );
        return buildOp ( op, alias, parallel, inputAliases, partitioner );
    }
    
    String buildOrderOp(String alias, Integer parallel, String inputAlias, List<LogicalExpressionPlan> plans, 
            List<Boolean> ascFlags, FuncSpec fs) {
        LOSort op = new LOSort( plan, plans, ascFlags, fs );
        return buildOp( op, alias, parallel, inputAlias, null );
    }

    String buildJoinOp(String alias, Integer parallel, List<String> inputAliases, MultiMap<Integer, LogicalExpressionPlan> joinPlans,
            JOINTYPE jt, List<Boolean> innerFlags, String partitioner) {
        boolean[] flags = new boolean[innerFlags.size()];
        for( int i = 0; i < innerFlags.size(); i++ ) {
            flags[i] = innerFlags.get( i );
        }
        LOJoin op = new LOJoin( plan, joinPlans, jt, flags );
        return buildOp( op, alias, parallel, inputAliases, partitioner );
    }

    String buildGroupOp(String alias, Integer parallel, List<String> inputAliases, 
        MultiMap<Integer, LogicalExpressionPlan> expressionPlans, GROUPTYPE gt, List<Boolean> innerFlags) {
        boolean[] flags = new boolean[innerFlags.size()];
        for( int i = 0; i < innerFlags.size(); i++ ) {
            flags[i] = innerFlags.get( i );
        }
        LOCogroup op = new LOCogroup( plan, expressionPlans, gt, flags );
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
    
    String buildForeachOp(String alias, Integer parallel, String inputAlias, LogicalPlan innerPlan) {
        LOForEach op = new LOForEach( plan );
        op.setInnerPlan( innerPlan );
        return buildOp( op, alias, parallel, inputAlias, null );
    }
    
    void buildGenerateOp(LogicalPlan plan, List<LogicalExpressionPlan> exprPlans, List<Boolean> flattenFlags,
            List<LogicalSchema> schemas) {
        boolean[] flags = new boolean[ flattenFlags.size() ];
        for( int i = 0; i < flattenFlags.size(); i++ )
            flags[i] = flattenFlags.get( i );
        LOGenerate op = new LOGenerate( plan, exprPlans, flags );
        op.setUserDefinedSchema( schemas );
        plan.add( op );
    }
    
    StreamingCommand buildCommand(String cmd, List<String> shipPaths, List<String> cachePaths,
            List<HandleSpec> inputHandleSpecs, List<HandleSpec> outputHandleSpecs,
            String logDir, Integer limit) throws RecognitionException {
        StreamingCommand command = null;
        try {
            command = buildCommand( cmd );
            
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
        }
        
        return command;
    }
    
    StreamingCommand buildCommand(String cmd) throws RecognitionException {
        try {
            return new StreamingCommand( pigContext, splitArgs( cmd ) );
        } catch (ParseException e) {
            throw new RecognitionException();        }
    }
    
    String buildStreamOp(String alias, Integer parallel, String inputAlias, StreamingCommand command, LogicalSchema schema)
    throws RecognitionException {
        try {
            LOStream op = new LOStream( plan, pigContext.createExecutableManager(), command, schema );
            return buildOp( op, alias, parallel, inputAlias, null );
        } catch (ExecException ex) {
            throw new RecognitionException();
        }
    }
    
    // TODO: create specific exceptions to throw.
    String buildNativeOp(Integer parallel, String inputJar, String cmd,
            List<String> paths, String storeAlias, String loadAlias)
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
            throw new RecognitionException();
        } catch (MalformedURLException e) {
            throw new RecognitionException();
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
    
    LogicalExpression buildProjectStar(LogicalExpressionPlan plan, String opAlias) {
        LogicalRelationalOperator op = (LogicalRelationalOperator)operators.get( opAlias );
        return new ProjectExpression( plan, 0, -1, op );
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
}
