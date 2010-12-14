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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.pig.FuncSpec;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.util.StringUtils;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LOLimit;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LOStore;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
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
        setAlias( op, alias );
        setParallel( op, parallel );
        plan.add( op );
        Operator pred = operators.get( inputAlias );
        if( pred == null ) {
            // error out
        }
        plan.connect( pred, op );
        operators.put( op.getAlias(), op );
        return op.getAlias();
    }

    String buildLimitOp(String alias, Integer parallel, String inputAlias, long limit) {
        LOLimit op = new LOLimit( plan, limit );
        setAlias( op, alias );
        setParallel( op, parallel );
        plan.add( op );
        Operator pred = operators.get( inputAlias );
        if( pred == null ) {
            // error out
        }
        plan.connect( pred, op );
        operators.put( op.getAlias(), op );
        return op.getAlias();
    }
    
    String buildLoadOp(String alias, Integer parallel, String filename, FuncSpec funcSpec, LogicalSchema schema) {
        FileSpec loader = new FileSpec( filename, funcSpec );
        LOLoad op = new LOLoad( loader, schema, plan, null );
        setAlias( op, alias );
        setParallel( op, parallel );
        plan.add( op );
        operators.put( op.getAlias(), op );
        return op.getAlias();
    }
    
    String buildStoreOp(String alias, Integer parallel, String inputAlias, String filename, FuncSpec funcSpec) {
        FileSpec fileSpec = new FileSpec( filename, funcSpec );
        LOStore op = new LOStore( plan, fileSpec );
        setAlias( op, alias );
        setParallel( op, parallel );
        Operator pred = operators.get( inputAlias );
        if( pred == null ) {
            // error out
        }
        plan.connect( pred, op );
        plan.add( op );
        operators.put( op.getAlias(), op );
        return op.getAlias();
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
    
    static FuncSpec buildFuncSpec(String funcName, List<String> args) {
        String[] argArray = new String[args.size()];
        return new FuncSpec( funcName, args.toArray( argArray ) );
    }
    
//    static FuncSpec buildFuncSpec(String funcName, List<LogicalExpression> args) {
//    }
    
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
    
}
