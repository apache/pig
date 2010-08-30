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
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.LogicalPlanMigrationVistor;
import org.apache.pig.newplan.logical.optimizer.LogicalPlanOptimizer;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.rules.FilterAboveForeach;
import org.apache.pig.newplan.logical.rules.LoadTypeCastInserter;
import org.apache.pig.newplan.optimizer.PlanOptimizer;
import org.apache.pig.newplan.optimizer.PlanTransformListener;
import org.apache.pig.newplan.optimizer.Rule;
import org.apache.pig.test.utils.Identity;
import org.apache.pig.test.utils.LogicalPlanTester;
import org.junit.Assert;
import org.junit.Test;


public class TestNewPlanFilterAboveForeach {
    PigContext pc = new PigContext(ExecType.LOCAL, new Properties());
    LogicalPlanTester planTester = new LogicalPlanTester(pc) ;
    
    @Test
    public void testSimple() throws Exception {
        LogicalPlanTester lpt = new LogicalPlanTester( pc );
        lpt.buildPlan( "A = LOAD 'file.txt' AS (name, cuisines:bag{ t : ( cuisine ) } );" );
        lpt.buildPlan( "B = FOREACH A GENERATE name, flatten(cuisines);" );
        lpt.buildPlan( "C = FILTER B BY name == 'joe';" );
        org.apache.pig.impl.logicalLayer.LogicalPlan plan = lpt.buildPlan( "D = STORE C INTO 'empty';" );  
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( plan );

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
    public void testMultipleFilter() throws Exception {
        LogicalPlanTester lpt = new LogicalPlanTester( pc );
        lpt.buildPlan( "A = LOAD 'file.txt' AS (name, cuisines : bag{ t : ( cuisine ) } );" );
        lpt.buildPlan( "B = FOREACH A GENERATE name, flatten(cuisines);" );
        lpt.buildPlan( "C = FILTER B BY $1 == 'french';" );
        lpt.buildPlan( "D = FILTER C BY name == 'joe';" );
        org.apache.pig.impl.logicalLayer.LogicalPlan plan = lpt.buildPlan( "E = STORE D INTO 'empty';" );  
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( plan );
        
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
        LogicalPlanTester lpt = new LogicalPlanTester( pc );
        lpt.buildPlan( "A = LOAD 'file.txt' AS (name, age, cuisines : bag{ t : ( cuisine ) } );" );
        lpt.buildPlan( "B = FOREACH A GENERATE name, age, flatten(cuisines);" );
        lpt.buildPlan( "C = FILTER B BY name == 'joe';" );
        lpt.buildPlan( "D = FILTER C BY age == 30;" );
        org.apache.pig.impl.logicalLayer.LogicalPlan plan = lpt.buildPlan( "E = STORE D INTO 'empty';" );  
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( plan );
        
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
        LogicalPlanTester lpt = new LogicalPlanTester( pc );
        lpt.buildPlan( "A = LOAD 'file.txt' AS (name, cuisines : bag{ t : ( cuisine, region ) } );" );
        lpt.buildPlan( "B = FOREACH A GENERATE name, flatten(cuisines);" );
        lpt.buildPlan( "C = FILTER B BY $1 == 'French';" );
        lpt.buildPlan( "D = FILTER C BY $2 == 'Europe';" );
        org.apache.pig.impl.logicalLayer.LogicalPlan plan = lpt.buildPlan( "E = STORE D INTO 'empty';" );  
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( plan );

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
        LogicalPlanTester lpt = new LogicalPlanTester( pc );
        lpt.buildPlan( "A = LOAD 'file.txt' AS (name, cuisines:bag{ t : ( cuisine ) } );" );
        lpt.buildPlan( "B = FOREACH A GENERATE name, flatten(cuisines);" );
        lpt.buildPlan( "C = FILTER B BY cuisine == 'French';" );
        org.apache.pig.impl.logicalLayer.LogicalPlan plan = lpt.buildPlan( "D = STORE C INTO 'empty';" );  
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( plan );

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
        LogicalPlanTester lpt = new LogicalPlanTester( pc );
        lpt.buildPlan( "A = LOAD 'file.txt' AS (name, cuisines:bag{ t : ( cuisine ) } );" );
        lpt.buildPlan( "B = FOREACH A GENERATE name, cuisines;" );
        lpt.buildPlan( "C = FILTER B BY name == 'joe';" );
        org.apache.pig.impl.logicalLayer.LogicalPlan plan = lpt.buildPlan( "D = STORE C INTO 'empty';" );  
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( plan );

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
     */
    @Test
    public void test1() throws FrontendException {
        LogicalPlanTester lpt = new LogicalPlanTester( pc );
        lpt.buildPlan( "A = LOAD 'file.txt' AS (a(u,v), b, c);" );
        lpt.buildPlan( "B = FOREACH A GENERATE $0, b;" );
        lpt.buildPlan( "C = FILTER B BY " + Identity.class.getName() +"($0, $1) > 5;" );
        org.apache.pig.impl.logicalLayer.LogicalPlan plan = lpt.buildPlan( "STORE C INTO 'empty';" );  
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( plan );

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
     */
    @Test
    public void test2() throws FrontendException {
        LogicalPlanTester lpt = new LogicalPlanTester( pc );
        lpt.buildPlan( "A = LOAD 'file.txt' AS (a(u,v), b, c);" );
        lpt.buildPlan( "B = FOREACH A GENERATE $0, b;" );
        lpt.buildPlan( "C = FILTER B BY " + Identity.class.getName() +"(*) > 5;" );
        org.apache.pig.impl.logicalLayer.LogicalPlan plan = lpt.buildPlan( "STORE C INTO 'empty';" );  
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( plan );

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
     */
    @Test
    public void test3() throws FrontendException {
        LogicalPlanTester lpt = new LogicalPlanTester( pc );
        lpt.buildPlan( "A = LOAD 'file.txt' AS (a(u,v), b, c);" );
        lpt.buildPlan( "B = FOREACH A GENERATE $0, b;" );
        lpt.buildPlan( "C = FILTER B BY 8 > 5;" );
        org.apache.pig.impl.logicalLayer.LogicalPlan plan = lpt.buildPlan( "STORE C INTO 'empty';" );  
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( plan );

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
     */
    @Test
    public void test4() throws FrontendException {
        LogicalPlanTester lpt = new LogicalPlanTester( pc );
        lpt.buildPlan( "A = LOAD 'file.txt' AS (a(u,v), b, c);" );
        lpt.buildPlan( "B = FOREACH A GENERATE $0, b, flatten(1);" );
        lpt.buildPlan( "C = FILTER B BY " + Identity.class.getName() +"(*) > 5;" );
        org.apache.pig.impl.logicalLayer.LogicalPlan plan = lpt.buildPlan( "STORE C INTO 'empty';" );  
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( plan );

        Operator load = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue( load instanceof LOLoad );
        Operator fe1 = newLogicalPlan.getSuccessors( load ).get( 0 );
        Assert.assertTrue( fe1 instanceof LOForEach );
        Operator fe2 = newLogicalPlan.getSuccessors( fe1 ).get( 0 );
        Assert.assertTrue( fe2 instanceof LOForEach );
        Operator filter = newLogicalPlan.getSuccessors( fe2 ).get( 0 );
        Assert.assertTrue( filter instanceof LOFilter );
    }
    
    private LogicalPlan migrateAndOptimizePlan(org.apache.pig.impl.logicalLayer.LogicalPlan plan) throws FrontendException {
        LogicalPlan newLogicalPlan = migratePlan( plan );
        PlanOptimizer optimizer = new MyPlanOptimizer( newLogicalPlan, 3 );
        optimizer.optimize();
        return newLogicalPlan;
    }
    
    private LogicalPlan migratePlan(org.apache.pig.impl.logicalLayer.LogicalPlan lp) throws VisitorException{
        LogicalPlanMigrationVistor visitor = new LogicalPlanMigrationVistor(lp);        
        visitor.visit();
        org.apache.pig.newplan.logical.relational.LogicalPlan newPlan = visitor.getNewLogicalPlan();
        return newPlan;
    }

    public class MyPlanOptimizer extends LogicalPlanOptimizer {
        protected MyPlanOptimizer(OperatorPlan p,  int iterations) {
            super(p, iterations, new HashSet<String>());
        }
        
        public void addPlanTransformListener(PlanTransformListener listener) {
            super.addPlanTransformListener(listener);
        }
        
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
