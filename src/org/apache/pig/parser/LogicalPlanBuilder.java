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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.pig.FuncSpec;
import org.apache.pig.builtin.RANDOM;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.io.FileSpec;
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
import org.apache.pig.newplan.logical.relational.LOJoin;
import org.apache.pig.newplan.logical.relational.LOLimit;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LOSort;
import org.apache.pig.newplan.logical.relational.LOSplit;
import org.apache.pig.newplan.logical.relational.LOSplitOutput;
import org.apache.pig.newplan.logical.relational.LOStore;
import org.apache.pig.newplan.logical.relational.LOUnion;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.newplan.logical.relational.LOJoin.JOINTYPE;
import org.apache.pig.newplan.logical.relational.LogicalSchema.LogicalFieldSchema;
import org.apache.pig.newplan.Operator;

public class LogicalPlanBuilder {
    private LogicalPlan plan = new LogicalPlan();

    private Map<String, Operator> operators = new HashMap<String, Operator>();
    private Map<String, FuncSpec> functions = new HashMap<String, FuncSpec>();
    
    FuncSpec lookupFunction(String alias) {
        return functions.get( alias );
    }
    
    void defineFunction(String alias, FuncSpec fs) {
        functions.put( alias, fs );
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

    String buildSplitOp(String alias, Integer parallel, String inputAlias) {
        LOSplit op = new LOSplit( plan );
        return buildOp( op, alias, parallel, inputAlias, null );
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

    String buildGroupOp(String alias, Integer parallel, String inputAlias, List<LogicalExpressionPlan> plans) {
        LOCogroup op = new LOCogroup( plan );
        return buildOp( op, alias, parallel, inputAlias, null );
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
    
    static void setAlias(LogicalRelationalOperator op, String alias) {
        if( alias != null ) {
            op.setAlias( alias );
        } else {
            // TODO: generate an alias.
        }
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
    
    LogicalExpressionPlan buildProjectStar(String opAlias) {
        LogicalExpressionPlan plan = new LogicalExpressionPlan();
        LogicalRelationalOperator op = (LogicalRelationalOperator)operators.get( opAlias );
        new ProjectExpression( plan, 0, -1, op );
        return plan;
    }
    
}
