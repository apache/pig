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

package org.apache.pig.test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.builtin.COUNT;
import org.apache.pig.builtin.SIZE;
import org.apache.pig.impl.PigContext;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.optimizer.LogicalPlanOptimizer;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LOStore;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.rules.FilterAboveForeach;
import org.apache.pig.newplan.logical.rules.LoadTypeCastInserter;
import org.apache.pig.newplan.optimizer.PlanOptimizer;
import org.apache.pig.newplan.optimizer.PlanTransformListener;
import org.apache.pig.newplan.optimizer.Rule;
import org.apache.pig.test.TestNewPlanPushDownForeachFlatten.MyFilterFunc;
import org.junit.Assert;
import org.junit.Test;


public class TestNewPlanFilterAboveForeach {
    PigContext pc = new PigContext(ExecType.LOCAL, new Properties());
    
    @Test
    public void testSimple() throws Exception {
        String query = "A =LOAD 'file.txt' AS (name, cuisines:bag{ t : ( cuisine ) } );" +
        "B = FOREACH A GENERATE name, flatten(cuisines);" +
        "C = FILTER B BY name == 'joe';" +
        "D = STORE C INTO 'empty';" ;  
        LogicalPlan newLogicalPlan = buildPlan( query );

        Operator load = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue( load instanceof LOLoad );
        Operator filter = newLogicalPlan.getSuccessors( load ).get( 0 );
        Assert.assertTrue( filter instanceof LOFilter );
        Operator fe1 = newLogicalPlan.getSuccessors( filter ).get( 0 );
        Assert.assertTrue( fe1 instanceof LOForEach );
        Operator fe2 = newLogicalPlan.getSuccessors( fe1 ).get( 0 );
        Assert.assertTrue( fe2 instanceof LOForEach );
    }
    
    @Test
    public void testSimpleNotPossible() throws Exception {
        String query = "A =LOAD 'file.txt' AS (name, cuisines:bag{ t : ( cuisine ) } );" +
                "B = FOREACH A GENERATE name, flatten(cuisines) as cuisines;" +
                "C = FILTER B BY cuisines == 'pizza';" +
                "D = STORE C INTO 'empty';" ;  
        LogicalPlan newLogicalPlan = buildPlan( query );
        
        Operator load = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue( load instanceof LOLoad );
        Operator fe1  = newLogicalPlan.getSuccessors( load ).get( 0 );
        Assert.assertTrue( fe1 instanceof LOForEach );
        Operator fe2 = newLogicalPlan.getSuccessors( fe1 ).get( 0 );
        Assert.assertTrue( fe2 instanceof LOForEach );
        Operator filter = newLogicalPlan.getSuccessors( fe2 ).get( 0 );
        Assert.assertTrue( filter instanceof LOFilter );
    }
    
    /**
     * Non-deterministic filters should not be pushed up (see PIG-2014).
     * In the example below, if Filter gets pushed above flatten, we might remove
     * whole bags of cuisines of random gets pushed up, while the intent is to sample from each bag.
     * @throws Exception
     */
    @Test
    public void testNondeterministicFilter() throws Exception {
        String query = "A =LOAD 'file.txt' AS (name, cuisines:bag{ t : ( cuisine ) }, num:int );" +
        "B = FOREACH A GENERATE name, flatten(cuisines), num;" +
        "C = FILTER B BY RANDOM(num) > 5;" +
        "D = STORE C INTO 'empty';" ;

        LogicalPlan newLogicalPlan = buildPlan( query );

        newLogicalPlan.explain(System.out, "text", true);

        // Expect Filter to not be pushed, so it should be load->foreach-> filter
        Operator load = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue( load instanceof LOLoad );
        Operator fe1 = newLogicalPlan.getSuccessors( load ).get( 0 );
        Assert.assertTrue( fe1 instanceof LOForEach );
        Operator fe2 = newLogicalPlan.getSuccessors( fe1 ).get( 0 );
        Assert.assertTrue( fe2 instanceof LOForEach );
        Operator filter = newLogicalPlan.getSuccessors( fe2 ).get( 0 );
        Assert.assertTrue( filter instanceof LOFilter );
    }

    @Test
    public void testMultipleFilter() throws Exception {
        String query = "A =LOAD 'file.txt' AS (name, cuisines : bag{ t : ( cuisine ) } );" +
        "B = FOREACH A GENERATE name, flatten(cuisines);" +
        "C = FILTER B BY $1 == 'french';" +
        "D = FILTER C BY name == 'joe';" +
        "E = STORE D INTO 'empty';";  
        LogicalPlan newLogicalPlan = buildPlan( query );
        
        Operator load = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue( load instanceof LOLoad );
        Operator filter = newLogicalPlan.getSuccessors( load ).get( 0 );
        Assert.assertTrue( filter instanceof LOFilter );
        Operator fe1 = newLogicalPlan.getSuccessors( filter ).get( 0 );
        Assert.assertTrue( fe1 instanceof LOForEach );
        Operator fe2 = newLogicalPlan.getSuccessors( fe1 ).get( 0 );
        Assert.assertTrue( fe2 instanceof LOForEach );
        Operator filter2 = newLogicalPlan.getSuccessors( fe2 ).get( 0 );
        Assert.assertTrue( filter2 instanceof LOFilter );
    }
    
    @Test
    public void testMultipleFilter2() throws Exception {
        String query = "A =LOAD 'file.txt' AS (name, age, cuisines : bag{ t : ( cuisine ) } );" +
        "B = FOREACH A GENERATE name, age, flatten(cuisines);" +
        "C = FILTER B BY name == 'joe';" +
        "D = FILTER C BY age == 30;" +
        "E = STORE D INTO 'empty';";  
        LogicalPlan newLogicalPlan = buildPlan( query );
        
        Operator load = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue( load instanceof LOLoad );
        Operator filter = newLogicalPlan.getSuccessors( load ).get( 0 );
        Assert.assertTrue( filter instanceof LOFilter );
        Operator filter2 = newLogicalPlan.getSuccessors( filter ).get( 0 );
        Assert.assertTrue( filter2 instanceof LOFilter );
        Operator fe1 = newLogicalPlan.getSuccessors( filter2 ).get( 0 );
        Assert.assertTrue( fe1 instanceof LOForEach );
        Operator fe2 = newLogicalPlan.getSuccessors( fe1 ).get( 0 );
        Assert.assertTrue( fe2 instanceof LOForEach );
    }
    
    @Test
    public void testMultipleFilterNotPossible() throws Exception {
        String query = "A =LOAD 'file.txt' AS (name, cuisines : bag{ t : ( cuisine, region ) } );" +
        "B = FOREACH A GENERATE name, flatten(cuisines);" +
        "C = FILTER B BY $1 == 'French';" +
        "D = FILTER C BY $2 == 'Europe';" +
        "E = STORE D INTO 'empty';";  
        LogicalPlan newLogicalPlan = buildPlan( query );

        Operator load = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue( load instanceof LOLoad );
        Operator fe1 = newLogicalPlan.getSuccessors( load ).get( 0 );
        Assert.assertTrue( fe1 instanceof LOForEach );
        Operator fe2 = newLogicalPlan.getSuccessors( fe1 ).get( 0 );
        Assert.assertTrue( fe2 instanceof LOForEach );
        Operator filter = newLogicalPlan.getSuccessors( fe2 ).get( 0 );
        Assert.assertTrue( filter instanceof LOFilter );
        Operator filter2 = newLogicalPlan.getSuccessors( filter ).get( 0 );
        Assert.assertTrue( filter2 instanceof LOFilter );
    }
    
    @Test
    public void testNotPossibleFilter() throws Exception {
        String query = "A =LOAD 'file.txt' AS (name, cuisines:bag{ t : ( cuisine ) } );" +
        "B = FOREACH A GENERATE name, flatten(cuisines);" +
        "C = FILTER B BY cuisine == 'French';" +
        "D = STORE C INTO 'empty';";  
        LogicalPlan newLogicalPlan = buildPlan( query );

        Operator load = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue( load instanceof LOLoad );
        Operator fe1 = newLogicalPlan.getSuccessors( load ).get( 0 );
        Assert.assertTrue( fe1 instanceof LOForEach );
        Operator fe2 = newLogicalPlan.getSuccessors( fe1 ).get( 0 );
        Assert.assertTrue( fe2 instanceof LOForEach );
        Operator filter = newLogicalPlan.getSuccessors( fe2 ).get( 0 );
        Assert.assertTrue( filter instanceof LOFilter );
    }
    
    @Test
    public void testSimple2() throws Exception {
        String query = "A =LOAD 'file.txt' AS (name, cuisines:bag{ t : ( cuisine ) } );" +
        "B = FOREACH A GENERATE name, cuisines;" +
        "C = FILTER B BY name == 'joe';" +
        "D = STORE C INTO 'empty';";  
        LogicalPlan newLogicalPlan = buildPlan( query );

        Operator load = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue( load instanceof LOLoad );
        Operator filter = newLogicalPlan.getSuccessors( load ).get( 0 );
        Assert.assertTrue( filter instanceof LOFilter );
        Operator fe1 = newLogicalPlan.getSuccessors( filter ).get( 0 );
        Assert.assertTrue( fe1 instanceof LOForEach );
        Operator fe2 = newLogicalPlan.getSuccessors( fe1 ).get( 0 );
        Assert.assertTrue( fe2 instanceof LOForEach );
    }
    
    /**
     * Normal test case: all fields from Foreach are used by exhaustive list.
     * Optimization should kick in.
     * @throws Exception 
     */
    @Test
    public void test1() throws Exception {
        String query = "A =LOAD 'file.txt' AS (a:bag{(u,v)}, b, c);" +
        "B = FOREACH A GENERATE $0, b;" +
        "C = FILTER B BY " + COUNT.class.getName() +"($0) > 5;" +
        "STORE C INTO 'empty';";  
        LogicalPlan newLogicalPlan = buildPlan( query );

        Operator load = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue( load instanceof LOLoad );
        Operator fe1 = newLogicalPlan.getSuccessors( load ).get( 0 );
        Assert.assertTrue( fe1 instanceof LOForEach );
        Operator filter = newLogicalPlan.getSuccessors( fe1 ).get( 0 );
        Assert.assertTrue( filter instanceof LOFilter );
        Operator fe2 = newLogicalPlan.getSuccessors( filter ).get( 0 );
        Assert.assertTrue( fe2 instanceof LOForEach );
    }
    
    /**
     * Identical to test1() except that it use project *.
     * @throws Exception 
     */
    @Test
    public void test2() throws Exception {
        String query = "A =LOAD 'file.txt' AS (a:(u,v), b, c);" +
        "B = FOREACH A GENERATE $0, b;" +
        "C = FILTER B BY " + SIZE.class.getName() +"(TOTUPLE(*)) > 5;" +
        "STORE C INTO 'empty';";  
        LogicalPlan newLogicalPlan = buildPlan( query );

        Operator load = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue( load instanceof LOLoad );
        Operator fe1 = newLogicalPlan.getSuccessors( load ).get( 0 );
        Assert.assertTrue( fe1 instanceof LOForEach );
        Operator filter = newLogicalPlan.getSuccessors( fe1 ).get( 0 );
        Assert.assertTrue( filter instanceof LOFilter );
        Operator fe2 = newLogicalPlan.getSuccessors( filter ).get( 0 );
        Assert.assertTrue( fe2 instanceof LOForEach );
    }

    /**
     * No fields are used in filter condition at all.
     * @throws Exception 
     */
    @Test
    public void test3() throws Exception {
        String query = "A =LOAD 'file.txt' AS (a:(u,v), b, c);" +
        "B = FOREACH A GENERATE $0, b;" +
        "C = FILTER B BY 8 > 5;" +
        "STORE C INTO 'empty';";  
        LogicalPlan newLogicalPlan = buildPlan( query );

        Operator load = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue( load instanceof LOLoad );
        Operator filter = newLogicalPlan.getSuccessors( load ).get( 0 );
        Assert.assertTrue( filter instanceof LOFilter );
        Operator fe1 = newLogicalPlan.getSuccessors( filter ).get( 0 );
        Assert.assertTrue( fe1 instanceof LOForEach );
        Operator fe2 = newLogicalPlan.getSuccessors( fe1 ).get( 0 );
        Assert.assertTrue( fe2 instanceof LOForEach );
    }
    
    /**
     * Similar to test2, but not all fields are available from the operator before foreach.
     * Optimziation doesn't kick in.
     * @throws Exception 
     */
    @Test
    public void test4() throws Exception {
        String query = "A =LOAD 'file.txt' AS (a:(u,v), b, c);" +
        "B = FOREACH A GENERATE $0, b, flatten(1);" +
        "C = FILTER B BY " + SIZE.class.getName() +"(TOTUPLE(*)) > 5;" +
        "STORE C INTO 'empty';";  
        LogicalPlan newLogicalPlan = buildPlan( query );

        Operator load = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue( load instanceof LOLoad );
        Operator fe1 = newLogicalPlan.getSuccessors( load ).get( 0 );
        Assert.assertTrue( fe1 instanceof LOForEach );
        Operator fe2 = newLogicalPlan.getSuccessors( fe1 ).get( 0 );
        Assert.assertTrue( fe2 instanceof LOForEach );
        Operator filter = newLogicalPlan.getSuccessors( fe2 ).get( 0 );
        Assert.assertTrue( filter instanceof LOFilter );
    }

    @Test
    public void testFilterForeach() throws Exception {
        String query = "A =load 'myfile' as (name, age, gpa);" +
        "B = foreach A generate $1, $2;" +        
        "C = filter B by $0 < 18;" +
         "D = STORE C INTO 'empty';"; 

        LogicalPlan newLogicalPlan = buildPlan( query );

        Operator load = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue( load instanceof LOLoad );
        Operator filter = newLogicalPlan.getSuccessors( load ).get( 0 );
        Assert.assertTrue( filter instanceof LOFilter );
        Operator fe = newLogicalPlan.getSuccessors( filter ).get( 0 );
        Assert.assertTrue( fe instanceof LOForEach );
        Operator store = newLogicalPlan.getSuccessors( fe ).get( 0 );
        Assert.assertTrue( store instanceof LOStore );
    }

    @Test
    public void testFilterForeachAddedField() throws Exception {
        String query = "A =load 'myfile' as (name, age, gpa);" +
        "B = foreach A generate $1, $2, COUNT({(1)});" +        
        "C = filter B by $2 < 18;" +
         "D = STORE C INTO 'empty';"; 

        LogicalPlan newLogicalPlan = buildPlan( query );

        Operator load = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue( load instanceof LOLoad );
        Operator fe = newLogicalPlan.getSuccessors( load ).get( 0 );
        Assert.assertTrue( fe instanceof LOForEach );
        Operator filter = newLogicalPlan.getSuccessors( fe ).get( 0 );
        Assert.assertTrue( filter instanceof LOFilter );
        Operator store = newLogicalPlan.getSuccessors( filter ).get( 0 );
        Assert.assertTrue( store instanceof LOStore );
    }

    @Test
    public void testFilterForeachCast() throws Exception {
        String query = "A =load 'myfile' as (name, age, gpa);" +
        "B = foreach A generate (int)$1, $2;" +        
        "C = filter B by $0 < 18;" +
         "D = STORE C INTO 'empty';"; 

        LogicalPlan newLogicalPlan = buildPlan( query );

        Operator load = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue( load instanceof LOLoad );
        Operator fe = newLogicalPlan.getSuccessors( load ).get( 0 );
        Assert.assertTrue( fe instanceof LOForEach );
        Operator filter = newLogicalPlan.getSuccessors( fe ).get( 0 );
        Assert.assertTrue( filter instanceof LOFilter );
        Operator store = newLogicalPlan.getSuccessors( filter ).get( 0 );
        Assert.assertTrue( store instanceof LOStore );
    }

    @Test
    public void testFilterCastForeach() throws Exception {
        String query = "A =load 'myfile' as (name, age, gpa);" +
        "B = foreach A generate $1, $2;" +        
        "C = filter B by (int)$0 < 18;" +
         "D = STORE C INTO 'empty';"; 

        LogicalPlan newLogicalPlan = buildPlan( query );

        Operator load = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue( load instanceof LOLoad );
        Operator filter = newLogicalPlan.getSuccessors( load ).get( 0 );
        Assert.assertTrue( filter instanceof LOFilter );
        Operator fe = newLogicalPlan.getSuccessors( filter ).get( 0 );
        Assert.assertTrue( fe instanceof LOForEach );
        Operator store = newLogicalPlan.getSuccessors( fe ).get( 0 );
        Assert.assertTrue( store instanceof LOStore );
    }


    @Test
    public void testFilterConstantConditionForeach() throws Exception {
        String query = "A =load 'myfile' as (name, age, gpa);" +
        "B = foreach A generate $1, $2;" +        
        "C = filter B by 1 == 1;" +
         "D = STORE C INTO 'empty';"; 

        LogicalPlan newLogicalPlan = buildPlan( query );

        Operator load = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue( load instanceof LOLoad );
        Operator filter = newLogicalPlan.getSuccessors( load ).get( 0 );
        Assert.assertTrue( filter instanceof LOFilter );
        Operator fe = newLogicalPlan.getSuccessors( filter ).get( 0 );
        Assert.assertTrue( fe instanceof LOForEach );
        Operator store = newLogicalPlan.getSuccessors( fe ).get( 0 );
        Assert.assertTrue( store instanceof LOStore );
    }

    @Test
    public void testFilterUDFForeach() throws Exception {
        String query = "A =load 'myfile' as (name, age, gpa);" +
        "B = foreach A generate $1, $2;" +        
        "C = filter B by " + MyFilterFunc.class.getName() + "($1) ;" +
         "D = STORE C INTO 'empty';"; 

        LogicalPlan newLogicalPlan = buildPlan( query );

        Operator load = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue( load instanceof LOLoad );
        Operator filter = newLogicalPlan.getSuccessors( load ).get( 0 );
        Assert.assertTrue( filter instanceof LOFilter );
        Operator fe = newLogicalPlan.getSuccessors( filter ).get( 0 );
        Assert.assertTrue( fe instanceof LOForEach );
        Operator store = newLogicalPlan.getSuccessors( fe ).get( 0 );
        Assert.assertTrue( store instanceof LOStore );
    }
    
    @Test
    public void testFilterForeachFlatten() throws Exception {
        String query = "A =load 'myfile' as (name, age, gpa);" +
        "B = foreach A generate $1, flatten($2);" +        
        "C = filter B by $0 < 18;" +
         "D = STORE C INTO 'empty';"; 

        LogicalPlan newLogicalPlan = buildPlan( query );

        Operator load = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue( load instanceof LOLoad );
        Operator filter = newLogicalPlan.getSuccessors( load ).get( 0 );
        Assert.assertTrue( filter instanceof LOFilter );
        Operator fe = newLogicalPlan.getSuccessors( filter ).get( 0 );
        Assert.assertTrue( fe instanceof LOForEach );
        Operator store = newLogicalPlan.getSuccessors( fe ).get( 0 );
        Assert.assertTrue( store instanceof LOStore );
    }
    
    // See PIG-1669
    @Test
    public void testPushUpFilterWithScalar() throws Exception {
        String query = "a = load 'studenttab10k' as (name, age, gpa);" +
        "b = group a all;" +
        "c = foreach b generate AVG(a.age) as age;" +
        "d = foreach a generate name, age;" +
        "e = filter d by age > c.age;" +
        "f = store e into 'empty';";

        LogicalPlan newLogicalPlan = buildPlan( query );

        Operator store = newLogicalPlan.getSinks().get( 0 );
        Operator foreach = newLogicalPlan.getPredecessors(store).get(0);
        Assert.assertTrue( foreach instanceof LOForEach );
    }
    
    // See PIG-1935
    @Test
    public void testPushUpFilterAboveBinCond() throws Exception {
        String query = "data = LOAD 'data.txt' as (referrer:chararray, canonical_url:chararray, ip:chararray);" +
        "best_url = FOREACH data GENERATE ((canonical_url != '' and canonical_url is not null) ? canonical_url : referrer) AS url, ip;" +
        "filtered = FILTER best_url BY url == 'badsite.com';" +
        "store filtered into 'empty';";

        LogicalPlan newLogicalPlan = buildPlan( query );

        Operator store = newLogicalPlan.getSinks().get( 0 );
        Operator filter = newLogicalPlan.getPredecessors(store).get(0);
        Assert.assertTrue( filter instanceof LOFilter );
    }

    private LogicalPlan buildPlan(String query) throws Exception {
        PigServer pigServer = new PigServer( pc );
        LogicalPlan newLogicalPlan = Util.buildLp(pigServer, query);
        PlanOptimizer optimizer = new MyPlanOptimizer( newLogicalPlan, 3 );
        optimizer.optimize();
        return newLogicalPlan;
    }
    
    public class MyPlanOptimizer extends LogicalPlanOptimizer {
        protected MyPlanOptimizer(OperatorPlan p,  int iterations) {
            super(p, iterations, new HashSet<String>());
        }
        
        @Override
        public void addPlanTransformListener(PlanTransformListener listener) {
            super.addPlanTransformListener(listener);
        }
        
       @Override
       protected List<Set<Rule>> buildRuleSets() {            
            List<Set<Rule>> ls = new ArrayList<Set<Rule>>();
            
            Set<Rule> s = new HashSet<Rule>();
            // add split filter rule
            Rule r = new LoadTypeCastInserter( "TypeCastInserter" );
            s.add(r);
            ls.add(s);
             
            s = new HashSet<Rule>();
            r = new FilterAboveForeach( "FilterAboveForeach" );
            s.add(r);            
            ls.add(s);
            
            return ls;
        }
    }    
}
