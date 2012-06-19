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
import org.apache.pig.data.DataType;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.expression.AndExpression;
import org.apache.pig.newplan.logical.expression.ConstantExpression;
import org.apache.pig.newplan.logical.expression.EqualExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.optimizer.LogicalPlanOptimizer;
import org.apache.pig.newplan.logical.optimizer.ProjectionPatcher;
import org.apache.pig.newplan.logical.optimizer.SchemaPatcher;
import org.apache.pig.newplan.logical.relational.LOCogroup;
import org.apache.pig.newplan.logical.relational.LODistinct;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOJoin;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LOStore;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.newplan.logical.rules.LoadTypeCastInserter;
import org.apache.pig.newplan.logical.rules.MergeFilter;
import org.apache.pig.newplan.logical.rules.PushUpFilter;
import org.apache.pig.newplan.logical.rules.SplitFilter;
import org.apache.pig.newplan.optimizer.PlanOptimizer;
import org.apache.pig.newplan.optimizer.PlanTransformListener;
import org.apache.pig.newplan.optimizer.Rule;
import org.junit.Assert;
import org.junit.Test;

public class TestNewPlanFilterRule {
    PigContext pc = new PigContext(ExecType.LOCAL, new Properties());

    LogicalPlan plan = null;
    LogicalRelationalOperator load1 = null;
    LogicalRelationalOperator load2 = null;
    LogicalRelationalOperator filter = null;
    LogicalRelationalOperator join = null;
    LogicalRelationalOperator store = null;    
    
    private void prep() {
        plan = new LogicalPlan();
        LogicalSchema schema = new LogicalSchema();
        schema.addField(new LogicalSchema.LogicalFieldSchema("id", null, DataType.INTEGER));
        schema.addField(new LogicalSchema.LogicalFieldSchema("name", null, DataType.CHARARRAY));
        schema.addField(new LogicalSchema.LogicalFieldSchema("age", null, DataType.INTEGER));    
        schema.getField(0).uid = 1;
        schema.getField(1).uid = 2;
        schema.getField(2).uid = 3;
        LogicalRelationalOperator l1 = new LOLoad(schema, plan);
        l1.setAlias("A");
        plan.add(l1);

        schema = new LogicalSchema();
        schema.addField(new LogicalSchema.LogicalFieldSchema("id", null, DataType.INTEGER));
        schema.addField(new LogicalSchema.LogicalFieldSchema("dept", null, DataType.INTEGER));
        schema.addField(new LogicalSchema.LogicalFieldSchema("salary", null, DataType.FLOAT));    
        schema.getField(0).uid = 4;
        schema.getField(1).uid = 5;
        schema.getField(2).uid = 6;
        LogicalRelationalOperator l2 = new LOLoad(schema, plan);
        l2.setAlias("B");
        plan.add(l2);
        
        MultiMap<Integer, LogicalExpressionPlan> joinPlans = new MultiMap<Integer, LogicalExpressionPlan>();
        
        LogicalRelationalOperator j1 = new LOJoin(plan, joinPlans, LOJoin.JOINTYPE.HASH, new boolean[]{true, true});
        LogicalExpressionPlan p1 = new LogicalExpressionPlan();
        ProjectExpression lp1 = new ProjectExpression(p1, 0, 1, j1);
        p1.add(lp1);
        joinPlans.put(0, p1);
        
        LogicalExpressionPlan p2 = new LogicalExpressionPlan();
        ProjectExpression lp2 = new ProjectExpression(p2, 1, 1, j1);
        p2.add(lp2);
        joinPlans.put(1, p2);
        
        j1.setAlias("C");
        plan.add(j1);
        
        // build an expression with no AND
        LogicalExpressionPlan p3 = new LogicalExpressionPlan();
        LogicalRelationalOperator f1 = new LOFilter(plan, p3);
        
        LogicalExpression lp3 = new ProjectExpression(p3, 0, 2, f1);
        LogicalExpression cont = new ConstantExpression(p3, new Integer(3));
        p3.add(lp3);
        p3.add(cont);       
        LogicalExpression eq = new EqualExpression(p3, lp3, cont);        
        
        
        f1.setAlias("D");
        plan.add(f1);
        
        LogicalRelationalOperator s1 = new LOStore(plan, null, null, null);
        plan.add(s1);       
        
        // load --|-join - filter - store
        // load --|   
        plan.connect(l1, j1);
        plan.connect(l2, j1);
        plan.connect(j1, f1);        
        plan.connect(f1, s1);      
        
        filter = f1;
        store = s1;
        join = j1;
        load1 = l1;
        load2 = l2;
    }
    
    @Test
    public void testFilterRule() throws Exception  {
        prep();
        // run split filter rule
        Rule r = new SplitFilter("SplitFilter");
        Set<Rule> s = new HashSet<Rule>();
        s.add(r);
        List<Set<Rule>> ls = new ArrayList<Set<Rule>>();
        ls.add(s);
        MyPlanOptimizer optimizer = new MyPlanOptimizer(plan, ls, 3);
        optimizer.optimize();
        
        Assert.assertEquals(plan.getPredecessors(filter).get(0), join);
        Assert.assertEquals(plan.getSuccessors(filter).get(0), store);
        
        // run push up filter rule
        r = new PushUpFilter("PushUpFilter");
        s = new HashSet<Rule>();
        s.add(r);
        ls = new ArrayList<Set<Rule>>();
        ls.add(s);
        optimizer = new MyPlanOptimizer(plan, ls, 3);
        optimizer.optimize();
        
        // the filter should be moved up to be after load
        Assert.assertEquals(plan.getSuccessors(load1).get(0), filter);
        Assert.assertEquals(plan.getSuccessors(filter).get(0), join);
        Assert.assertEquals(plan.getSuccessors(join).get(0), store);
        
        // run merge filter rule
        r = new MergeFilter("MergeFilter");
        s = new HashSet<Rule>();
        s.add(r);
        ls = new ArrayList<Set<Rule>>();
        ls.add(s);
        optimizer = new MyPlanOptimizer(plan, ls, 3);
        optimizer.optimize();
        
        // the filter should the same as before, nothing to merge
        Assert.assertEquals(plan.getSuccessors(load1).get(0), filter);
        Assert.assertEquals(plan.getSuccessors(filter).get(0), join);
        Assert.assertEquals(plan.getSuccessors(join).get(0), store);
    }
        
    // build an expression with 1 AND, it should split into 2 filters
    @Test
    public void testFilterRuleWithAnd() throws Exception  {
        prep();
        
        LogicalExpressionPlan p4 = new LogicalExpressionPlan();        
        LogicalExpression lp3 = new ProjectExpression(p4, 0, 2, filter);
        LogicalExpression cont = new ConstantExpression(p4, new Integer(3));
        p4.add(lp3);
        p4.add(cont);
        LogicalExpression eq = new EqualExpression(p4, lp3, cont);
      
        LogicalExpression lp4 = new ProjectExpression(p4, 0, 5, filter);
        LogicalExpression cont2 = new ConstantExpression(p4, new Float(100));
        p4.add(lp4);
        p4.add(cont2);
        LogicalExpression eq2 = new EqualExpression(p4, lp4, cont2);        
    
        LogicalExpression and = new AndExpression(p4, eq, eq2);        
        
        ((LOFilter)filter).setFilterPlan(p4);
        
        // run split filter rule
        Rule r = new SplitFilter("SplitFilter");
        Set<Rule> s = new HashSet<Rule>();
        s.add(r);
        List<Set<Rule>> ls = new ArrayList<Set<Rule>>();
        ls.add(s);
        PlanOptimizer optimizer = new MyPlanOptimizer(plan, ls, 3);
        optimizer.optimize();
        
        Assert.assertEquals(plan.getPredecessors(filter).get(0), join);
        Operator next = plan.getSuccessors(filter).get(0);
        Assert.assertEquals(LOFilter.class, next.getClass());        
        next = plan.getSuccessors(next).get(0);
        Assert.assertEquals(LOStore.class, next.getClass());
        
        // run push up filter rule
        r = new PushUpFilter("PushUpFilter");
        s = new HashSet<Rule>();
        s.add(r);
        ls = new ArrayList<Set<Rule>>();
        ls.add(s);
        optimizer = new MyPlanOptimizer(plan, ls, 3);
        optimizer.optimize();
        
        // both filters should be moved up to be after each load
        next = plan.getSuccessors(load1).get(0);
        Assert.assertEquals(next.getClass(), LOFilter.class);
        Assert.assertEquals(plan.getSuccessors(next).get(0), join);
        
        next = plan.getSuccessors(load2).get(0);
        Assert.assertEquals(next.getClass(), LOFilter.class);
        Assert.assertEquals(plan.getSuccessors(next).get(0), join);
        
        Assert.assertEquals(plan.getSuccessors(join).get(0), store);
        
        // run merge filter rule
        r = new MergeFilter("MergeFilter");
        s = new HashSet<Rule>();
        s.add(r);
        ls = new ArrayList<Set<Rule>>();
        ls.add(s);
        optimizer = new MyPlanOptimizer(plan, ls, 3);
        optimizer.optimize();
        
        // the filters should the same as before, nothing to merge
        next = plan.getSuccessors(load1).get(0);
        Assert.assertEquals(next.getClass(), LOFilter.class);
        Assert.assertEquals(plan.getSuccessors(next).get(0), join);
        
        next = plan.getSuccessors(load2).get(0);
        Assert.assertEquals(next.getClass(), LOFilter.class);
        Assert.assertEquals(plan.getSuccessors(next).get(0), join);
        
        Assert.assertEquals(plan.getSuccessors(join).get(0), store);
    }
    
    @Test
    public void testFilterRuleWith2And() throws Exception  {
        prep();
        // build an expression with 2 AND, it should split into 3 filters
        LogicalExpressionPlan p5 = new LogicalExpressionPlan();
        
       
        LogicalExpression lp3 = new ProjectExpression(p5, 0, 2, filter);
        LogicalExpression cont = new ConstantExpression(p5, new Integer(3));
        p5.add(lp3);
        p5.add(cont);       
        LogicalExpression eq = new EqualExpression(p5, lp3, cont);
        
        LogicalExpression lp4 = new ProjectExpression(p5, 0, 3, filter);
        LogicalExpression cont2 = new ConstantExpression(p5, new Integer(3));        
        p5.add(lp4);
        p5.add(cont2);
        LogicalExpression eq2 = new EqualExpression(p5, lp4, cont2);        
        
        LogicalExpression and1 = new AndExpression(p5, eq, eq2);
       
        lp3 = new ProjectExpression(p5, 0, 0, filter);
        lp4 = new ProjectExpression(p5, 0, 3, filter);
        p5.add(lp3);
        p5.add(lp4);   
        eq2 = new EqualExpression(p5, lp3, lp4);        
              
        LogicalExpression and2 = new AndExpression(p5, and1, eq2);        
        
        ((LOFilter)filter).setFilterPlan(p5);
        
        Rule r = new SplitFilter("SplitFilter");
        Set<Rule> s = new HashSet<Rule>();
        s.add(r);
        List<Set<Rule>> ls = new ArrayList<Set<Rule>>();
        ls.add(s);
        MyPlanOptimizer optimizer = new MyPlanOptimizer(plan, ls, 3);
        MyPlanTransformListener listener = new MyPlanTransformListener();
        optimizer.addPlanTransformListener(listener);
        optimizer.optimize();
        
        Assert.assertEquals(plan.getPredecessors(filter).get(0), join);
        Operator next = plan.getSuccessors(filter).get(0);
        Assert.assertEquals(next.getClass(), LOFilter.class);
        next = plan.getSuccessors(next).get(0);
        Assert.assertEquals(next.getClass(), LOFilter.class);
        next = plan.getSuccessors(next).get(0);
        Assert.assertEquals(LOStore.class, next.getClass());
        
        OperatorPlan transformed = listener.getTransformed();
        Assert.assertEquals(transformed.size(), 3);
        
        // run push up filter rule
        r = new PushUpFilter("PushUpFilter");
        s = new HashSet<Rule>();
        s.add(r);
        ls = new ArrayList<Set<Rule>>();
        ls.add(s);
        optimizer = new MyPlanOptimizer(plan, ls, 3);
        listener = new MyPlanTransformListener();
        optimizer.addPlanTransformListener(listener);
        optimizer.optimize();
        
        // 2 filters should be moved up to be after each load, and one filter should remain
        next = plan.getSuccessors(load1).get(0);
        Assert.assertEquals(next.getClass(), LOFilter.class);
        Assert.assertEquals(plan.getSuccessors(next).get(0), join);
        
        next = plan.getSuccessors(load2).get(0);
        Assert.assertEquals(next.getClass(), LOFilter.class);
        Assert.assertEquals(plan.getSuccessors(next).get(0), join);
        
        next = plan.getSuccessors(join).get(0);
        Assert.assertEquals(next.getClass(), LOFilter.class);
        
        next = plan.getSuccessors(next).get(0);
        Assert.assertEquals(next.getClass(), LOStore.class);
        
        transformed = listener.getTransformed();
        Assert.assertEquals(transformed.size(), 7);
        
        // run merge filter rule
        r = new MergeFilter("MergeFilter");
        s = new HashSet<Rule>();
        s.add(r);
        ls = new ArrayList<Set<Rule>>();
        ls.add(s);
        optimizer = new MyPlanOptimizer(plan, ls, 3);
        listener = new MyPlanTransformListener();
        optimizer.addPlanTransformListener(listener);
        optimizer.optimize();
        
        // the filters should the same as before, nothing to merge
        next = plan.getSuccessors(load1).get(0);
        Assert.assertEquals(next.getClass(), LOFilter.class);
        Assert.assertEquals(plan.getSuccessors(next).get(0), join);
        
        next = plan.getSuccessors(load2).get(0);
        Assert.assertEquals(next.getClass(), LOFilter.class);
        Assert.assertEquals(plan.getSuccessors(next).get(0), join);
        
        next = plan.getSuccessors(join).get(0);
        Assert.assertEquals(next.getClass(), LOFilter.class);
        
        next = plan.getSuccessors(next).get(0);
        Assert.assertEquals(next.getClass(), LOStore.class);
        
        transformed = listener.getTransformed();
        Assert.assertNull(transformed);
    }   
    
    @Test
    public void testFilterRuleWith2And2() throws Exception  {
        prep();
        // build an expression with 2 AND, it should split into 3 filters
        LogicalExpressionPlan p5 = new LogicalExpressionPlan();
        
        LogicalExpression lp3 = new ProjectExpression(p5, 0, 2, filter);
        LogicalExpression cont = new ConstantExpression(p5, new Integer(3));
        p5.add(lp3);
        p5.add(cont);
        LogicalExpression eq = new EqualExpression(p5, lp3, cont);      
        
        lp3 = new ProjectExpression(p5, 0, 0, filter);
        LogicalExpression lp4 = new ProjectExpression(p5, 0, 3, filter);        
        p5.add(lp4);
        p5.add(lp3);
        LogicalExpression eq2 = new EqualExpression(p5, lp3, lp4);
        
        LogicalExpression and1 = new AndExpression(p5, eq, eq2);
        
        lp3 = new ProjectExpression(p5, 0, 2, filter);
        lp4 = new ProjectExpression(p5, 0, 5, filter);
        p5.add(lp3);
        p5.add(lp4);
        eq2 = new EqualExpression(p5, lp3, lp4);
        
        LogicalExpression and2 = new AndExpression(p5, and1, eq2);    
        
        ((LOFilter)filter).setFilterPlan(p5);
        
        Rule r = new SplitFilter("SplitFilter");
        Set<Rule> s = new HashSet<Rule>();
        s.add(r);
        List<Set<Rule>> ls = new ArrayList<Set<Rule>>();
        ls.add(s);
        MyPlanOptimizer optimizer = new MyPlanOptimizer(plan, ls, 3);
        optimizer.optimize();
        
        Assert.assertEquals(plan.getPredecessors(filter).get(0), join);
        Operator next = plan.getSuccessors(filter).get(0);
        Assert.assertEquals(next.getClass(), LOFilter.class);
        next = plan.getSuccessors(next).get(0);
        Assert.assertEquals(next.getClass(), LOFilter.class);
        next = plan.getSuccessors(next).get(0);
        Assert.assertEquals(LOStore.class, next.getClass());
        
        // run push up filter rule
        r = new PushUpFilter("PushUpFilter");
        s = new HashSet<Rule>();
        s.add(r);
        ls = new ArrayList<Set<Rule>>();
        ls.add(s);
        optimizer = new MyPlanOptimizer(plan, ls, 3);
        optimizer.optimize();
        
        // 1 filter should be moved up to be after a load, and 2 filters should remain
        next = plan.getSuccessors(load1).get(0);
        Assert.assertEquals(next.getClass(), LOFilter.class);
        Assert.assertEquals(plan.getSuccessors(next).get(0), join);
        
        next = plan.getSuccessors(load2).get(0);
        Assert.assertEquals(next, join);     
        
        next = plan.getSuccessors(join).get(0);
        Assert.assertEquals(next.getClass(), LOFilter.class);
        
        next = plan.getSuccessors(next).get(0);
        Assert.assertEquals(next.getClass(), LOFilter.class);
                
        next = plan.getSuccessors(next).get(0);
        Assert.assertEquals(next.getClass(), LOStore.class);
        
        // run merge filter rule
        r = new MergeFilter("MergeFilter");
        s = new HashSet<Rule>();
        s.add(r);
        ls = new ArrayList<Set<Rule>>();
        ls.add(s);
        optimizer = new MyPlanOptimizer(plan, ls, 3);
        MyPlanTransformListener listener = new MyPlanTransformListener();
        optimizer.addPlanTransformListener(listener);
        optimizer.optimize();
        
        // the 2 filters after join should merge
        next = plan.getSuccessors(load1).get(0);
        Assert.assertEquals(next.getClass(), LOFilter.class);
        Assert.assertEquals(plan.getSuccessors(next).get(0), join);
        
        next = plan.getSuccessors(load2).get(0);
        Assert.assertEquals(next, join);        
        
        next = plan.getSuccessors(join).get(0);
        Assert.assertEquals(next.getClass(), LOFilter.class);
        
        next = plan.getSuccessors(next).get(0);
        Assert.assertEquals(next.getClass(), LOStore.class);
        
        OperatorPlan transformed = listener.getTransformed();
        Assert.assertEquals(transformed.size(), 2);
    }   
    
    // See pig-1639
    @Test
    public void testFilterUDFNegative() throws Exception {
        String query = "A = load 'myfile' as (name, age, gpa);" +
        "B = group A by age;"+        
        "C = filter B by COUNT(A) < 18;" + "D = STORE C INTO 'empty';" ; 

        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );

        Operator load = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue( load instanceof LOLoad );
        Operator fe = newLogicalPlan.getSuccessors( load ).get( 0 );
        Assert.assertTrue( fe instanceof LOForEach );
        Operator group = newLogicalPlan.getSuccessors( fe ).get( 0 );
        Assert.assertTrue( group instanceof LOCogroup );
        Operator filter = newLogicalPlan.getSuccessors( group ).get( 0 );
        Assert.assertTrue( filter instanceof LOFilter );
        Operator store = newLogicalPlan.getSuccessors( filter ).get( 0 );
        Assert.assertTrue( store instanceof LOStore );
    }

    /**
     * Test that SAMPLE doesn't get pushed up (see PIG-2014)
     */
    @Test
    public void testSample() throws Exception {
        String query = "A = LOAD 'file.txt' AS (name, cuisines:bag{ t : ( cuisine ) } );" +
        "B = GROUP A by name;" +
        "C = FOREACH B GENERATE group, A;" +
        "D = SAMPLE C 0.1 ; " +
        "E = STORE D INTO 'empty';";
        // expect loload -> foreach -> cogroup -> filter
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );
        newLogicalPlan.explain(System.out, "text", true);

        Operator load = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue( load instanceof LOLoad );
        Operator fe1 = newLogicalPlan.getSuccessors( load ).get( 0 );
        Assert.assertTrue( fe1 instanceof LOForEach );
        Operator cg = newLogicalPlan.getSuccessors( fe1 ).get( 0 );
        Assert.assertTrue( cg instanceof LOCogroup );
        Operator fe2 = newLogicalPlan.getSuccessors( cg ).get( 0 );
        Assert.assertTrue( fe1 instanceof LOForEach );
        Operator filter = newLogicalPlan.getSuccessors( fe2 ).get( 0 );
        Assert.assertTrue( filter instanceof LOFilter );
    }

    /**
     * Test that SAMPLE doesn't get pushed up over Distinct (see PIG-2137)
     */
    @Test
    public void testSampleDistinct() throws Exception {
        String query = "A = LOAD 'file.txt' AS (name, cuisines:bag{ t : ( cuisine ) } );" +
        "B = DISTINCT A;" +
        "C = SAMPLE B 0.1 ; " +
        "D = STORE C INTO 'empty';";
        // expect loload -> foreach -> distinct -> filter
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );
        newLogicalPlan.explain(System.out, "text", true);

        Operator load = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue( load instanceof LOLoad );
        Operator fe1 = newLogicalPlan.getSuccessors( load ).get( 0 );
        Assert.assertTrue( fe1 instanceof LOForEach );
        Operator dist = newLogicalPlan.getSuccessors( fe1 ).get( 0 );
        Assert.assertTrue( dist instanceof LODistinct );
        Operator filter = newLogicalPlan.getSuccessors( dist ).get( 0 );
        Assert.assertTrue( filter instanceof LOFilter );
    }
    
    /**
     * Test that deterministic filter gets get pushed up over Distinct (see PIG-2137)
     */
    @Test
    public void testFilterAfterDistinct() throws Exception {
        String query = "A = LOAD 'file.txt' AS (name : chararray, cuisines:bag{ t : ( cuisine ) } );" +
        "B = DISTINCT A;" +
        "C = filter B by SIZE(name) > 10;" +
        "D = STORE C INTO 'long_name';";
        // filter should be pushed above distinct,
        //ie expect - loload -> foreach -> filter -> distinct 
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( query );
        newLogicalPlan.explain(System.out, "text", true);

        Operator load = newLogicalPlan.getSources().get( 0 );
        Assert.assertTrue( load instanceof LOLoad );
        Operator fe1 = newLogicalPlan.getSuccessors( load ).get( 0 );
        Assert.assertTrue( fe1 instanceof LOForEach );
        Operator filter = newLogicalPlan.getSuccessors(fe1).get( 0 );
        Assert.assertTrue( filter instanceof LOFilter );
   
        Operator dist = newLogicalPlan.getSuccessors(filter).get( 0 );
        Assert.assertTrue( dist instanceof LODistinct );
        
    }

    private LogicalPlan migrateAndOptimizePlan(String query) throws Exception {
    	PigServer pigServer = new PigServer(pc);
        LogicalPlan newLogicalPlan = Util.buildLp(pigServer, query);
        PlanOptimizer optimizer = new NewPlanOptimizer( newLogicalPlan, 3 );
        optimizer.optimize();
        return newLogicalPlan;
    }
    
    public class NewPlanOptimizer extends LogicalPlanOptimizer {
        protected NewPlanOptimizer(OperatorPlan p,  int iterations) {
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
            r = new PushUpFilter( "PushUpFilter" );
            s.add(r);            
            ls.add(s);
            
            return ls;
        }
    }    

    public class MyPlanOptimizer extends PlanOptimizer {

        protected MyPlanOptimizer(OperatorPlan p, List<Set<Rule>> rs,
                int iterations) {
            super(p, rs, iterations);            
            addPlanTransformListener(new SchemaPatcher());
            addPlanTransformListener(new ProjectionPatcher());
        }
        
        @Override
        public void addPlanTransformListener(PlanTransformListener listener) {
            super.addPlanTransformListener(listener);
        }
        
    }
    
    public class MyPlanTransformListener implements PlanTransformListener {

        private OperatorPlan tp;

        @Override
        public void transformed(OperatorPlan fp, OperatorPlan tp)
                throws FrontendException {
            this.tp = tp;
        }
        
        public OperatorPlan getTransformed() {
            return tp;
        }
    }
}
