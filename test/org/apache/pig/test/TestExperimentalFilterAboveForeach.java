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
import java.util.Set;
import org.apache.pig.data.DataType;
import org.apache.pig.experimental.logical.expression.ConstantExpression;
import org.apache.pig.experimental.logical.expression.EqualExpression;
import org.apache.pig.experimental.logical.expression.LogicalExpressionPlan;
import org.apache.pig.experimental.logical.expression.ProjectExpression;
import org.apache.pig.experimental.logical.optimizer.ProjectionPatcher;
import org.apache.pig.experimental.logical.optimizer.SchemaPatcher;
import org.apache.pig.experimental.logical.optimizer.UidStamper;
import org.apache.pig.experimental.logical.relational.LOFilter;
import org.apache.pig.experimental.logical.relational.LOForEach;
import org.apache.pig.experimental.logical.relational.LOGenerate;
import org.apache.pig.experimental.logical.relational.LOInnerLoad;
import org.apache.pig.experimental.logical.relational.LOLoad;
import org.apache.pig.experimental.logical.relational.LOStore;
import org.apache.pig.experimental.logical.relational.LogicalPlan;
import org.apache.pig.experimental.logical.relational.LogicalSchema;
import org.apache.pig.experimental.logical.rules.FilterAboveForeach;
import org.apache.pig.experimental.plan.Operator;
import org.apache.pig.experimental.plan.OperatorPlan;
import org.apache.pig.experimental.plan.optimizer.PlanOptimizer;
import org.apache.pig.experimental.plan.optimizer.PlanTransformListener;
import org.apache.pig.experimental.plan.optimizer.Rule;
import junit.framework.TestCase;

public class TestExperimentalFilterAboveForeach extends TestCase {
    
    public void testSimple() throws Exception {
        
        // Plan here is 
        // Load (name, cuisines{t:(name)}) -> foreach gen name,flatten(cuisines) 
        // -> filter name == 'joe' --> stor
        
        LogicalPlan plan = null;
        LOLoad load = null;
        LOForEach foreach = null;
        LOFilter filter = null;
        LOStore stor = null;
        
        plan = new LogicalPlan();
        
        LogicalSchema schema = new LogicalSchema();
        schema.addField(new LogicalSchema.LogicalFieldSchema("name", null, DataType.CHARARRAY));
        LogicalSchema bagSchema = new LogicalSchema();
        LogicalSchema bagTupleSchema = new LogicalSchema();
        bagTupleSchema.addField( new LogicalSchema.LogicalFieldSchema("name", null, DataType.CHARARRAY) );
        bagSchema.addField( new LogicalSchema.LogicalFieldSchema( "t", bagTupleSchema, DataType.TUPLE ) );
        schema.addField(new LogicalSchema.LogicalFieldSchema("cuisines", bagSchema, DataType.BAG));
        
        load = new LOLoad(null, schema, plan);
        load.setAlias("A");
        plan.add(load);
        
        foreach = new LOForEach(plan);
        
        LogicalPlan innerPlan = new LogicalPlan();
        LOInnerLoad innerLoad = new LOInnerLoad(innerPlan, foreach, 0);
        innerPlan.add(innerLoad);
        
        LOInnerLoad innerLoad2 = new LOInnerLoad(innerPlan, foreach, 1);
        innerPlan.add(innerLoad2);
        
        LogicalExpressionPlan namePrj = new LogicalExpressionPlan();        
        ProjectExpression prjName = new ProjectExpression(namePrj, DataType.CHARARRAY, 0, 0);
        namePrj.add(prjName);
        
        LogicalExpressionPlan cuisinesPrj = new LogicalExpressionPlan();
        ProjectExpression prjCuisines = new ProjectExpression(cuisinesPrj, DataType.BAG, 1, 0);
        cuisinesPrj.add(prjCuisines);
        
        List<LogicalExpressionPlan> expPlans = new ArrayList<LogicalExpressionPlan>();
        expPlans.add(namePrj);
        expPlans.add(cuisinesPrj);
        
        boolean flatten[] = new boolean[2];
        flatten[0] = false;
        flatten[1] = true;
        
        LOGenerate generate = new LOGenerate(innerPlan, expPlans, flatten);        
        innerPlan.add(generate);
        innerPlan.connect(innerLoad, generate);
        innerPlan.connect(innerLoad2, generate);
        
        foreach.setInnerPlan(innerPlan);
        foreach.setAlias("B");
        plan.add(foreach);
        
        plan.connect(load, foreach);
        
        filter = new LOFilter(plan);
        LogicalExpressionPlan filterPlan = new LogicalExpressionPlan();
        ProjectExpression namePrj2 = new ProjectExpression(filterPlan, DataType.CHARARRAY, 0, 0);
        filterPlan.add(namePrj2);
        ConstantExpression constExp = new ConstantExpression(filterPlan, DataType.CHARARRAY, "joe");
        filterPlan.add(constExp);
        EqualExpression equal = new EqualExpression(filterPlan, namePrj2, constExp);
        filterPlan.add(equal);
        
        filter.setFilterPlan(filterPlan);
        filter.setAlias("C");
        plan.add(filter);
        
        plan.connect(foreach, filter);
        
        stor = new LOStore(plan);
        stor.setAlias("D");
        plan.add(stor);
        plan.connect(filter,stor);
        
        try {
            // Stamp everything with a Uid
            UidStamper stamper = new UidStamper(plan);
            stamper.visit();
        }catch(Exception e) {
            assertTrue("Failed to set a valid uid", false );
        }
        
        
        // run filter rule
        Rule r = new FilterAboveForeach("FilterAboveFlatten");
        Set<Rule> s = new HashSet<Rule>();
        s.add(r);
        List<Set<Rule>> ls = new ArrayList<Set<Rule>>();
        ls.add(s);
        
        // Test Plan before optimizing
        List<Operator> list = plan.getSinks();
        assertTrue( list.contains(stor) );
        
        list = plan.getSources();
        assertTrue( list.contains(load) );
        
        assertTrue( plan.getPredecessors(stor).contains(filter) ); 
        assertEquals( 1, plan.getPredecessors(stor).size() );
        
        assertTrue( plan.getPredecessors(filter).contains(foreach) );
        assertEquals( 1, plan.getPredecessors(filter).size() );
        
        assertTrue( foreach.getInnerPlan().getSinks().contains(generate) );
        assertEquals( 1, foreach.getInnerPlan().getSinks().size() );
        assertTrue( foreach.getInnerPlan().getSources().contains(innerLoad) );
        assertTrue( foreach.getInnerPlan().getSources().contains(innerLoad2) );
        assertEquals( 2, foreach.getInnerPlan().getSources().size() );
        
        // Run the optimizer
        MyPlanOptimizer optimizer = new MyPlanOptimizer(plan, ls, 3);
        optimizer.addPlanTransformListener(new ProjectionPatcher());
        optimizer.addPlanTransformListener(new SchemaPatcher());
        optimizer.optimize();
        
        // Test after optimization
        list = plan.getSinks();
        assertTrue( list.contains(stor) );
        
        list = plan.getSources();
        assertTrue( list.contains(load) );
        
        assertTrue( plan.getPredecessors(stor).contains(foreach) ); 
        assertEquals( 1, plan.getPredecessors(stor).size() );
        
        assertTrue( plan.getPredecessors(filter).contains(load) );
        assertEquals( 1, plan.getPredecessors(filter).size() );
        assertEquals( load.getSchema().getField(0).uid, namePrj2.getUid() );
        assertEquals( namePrj2.getUid(), prjName.getUid() );
        
        assertTrue( plan.getPredecessors(foreach).contains(filter) );
        assertEquals( 1, plan.getPredecessors(foreach).size() );
        
        assertTrue( foreach.getInnerPlan().getSinks().contains(generate) );
        assertEquals( 1, foreach.getInnerPlan().getSinks().size() );
        assertTrue( foreach.getInnerPlan().getSources().contains(innerLoad) );
        assertTrue( foreach.getInnerPlan().getSources().contains(innerLoad2) );
        assertEquals( 2, foreach.getInnerPlan().getSources().size() );
    }
    
    public void testMultipleFilter() throws Exception {
        
        // Plan here is 
        // Load (name, cuisines{t:(name)}) -> foreach gen name,flatten(cuisines) 
        // -> filter $1 == 'joe' --> filter name == 'joe' --> stor
        
        LogicalPlan plan = null;
        LOLoad load = null;
        LOForEach foreach = null;
        LOFilter filter = null;
        LOStore stor = null;
        
        plan = new LogicalPlan();
        
        LogicalSchema schema = new LogicalSchema();
        schema.addField(new LogicalSchema.LogicalFieldSchema("name", null, DataType.CHARARRAY));
        LogicalSchema bagSchema = new LogicalSchema();
        LogicalSchema bagTupleSchema = new LogicalSchema();
        bagTupleSchema.addField( new LogicalSchema.LogicalFieldSchema("name", null, DataType.CHARARRAY) );
        bagSchema.addField( new LogicalSchema.LogicalFieldSchema( "t", bagTupleSchema, DataType.TUPLE ) );
        schema.addField(new LogicalSchema.LogicalFieldSchema("cuisines", bagSchema, DataType.BAG));
        
        load = new LOLoad(null, schema, plan);
        load.setAlias("A");
        plan.add(load);
        
        foreach = new LOForEach(plan);
        
        LogicalPlan innerPlan = new LogicalPlan();
        LOInnerLoad innerLoad = new LOInnerLoad(innerPlan, foreach, 0);
        innerPlan.add(innerLoad);
        
        LOInnerLoad innerLoad2 = new LOInnerLoad(innerPlan, foreach, 1);
        innerPlan.add(innerLoad2);
        
        LogicalExpressionPlan namePrj = new LogicalExpressionPlan();        
        ProjectExpression prjName = new ProjectExpression(namePrj, DataType.CHARARRAY, 0, 0);
        namePrj.add(prjName);
        
        LogicalExpressionPlan cuisinesPrj = new LogicalExpressionPlan();
        ProjectExpression prjCuisines = new ProjectExpression(cuisinesPrj, DataType.BAG, 1, 0);
        cuisinesPrj.add(prjCuisines);
        
        List<LogicalExpressionPlan> expPlans = new ArrayList<LogicalExpressionPlan>();
        expPlans.add(namePrj);
        expPlans.add(cuisinesPrj);
        
        boolean flatten[] = new boolean[2];
        flatten[0] = false;
        flatten[1] = true;
        
        LOGenerate generate = new LOGenerate(innerPlan, expPlans, flatten);        
        innerPlan.add(generate);
        innerPlan.connect(innerLoad, generate);
        innerPlan.connect(innerLoad2, generate);
        
        foreach.setInnerPlan(innerPlan);
        foreach.setAlias("B");
        plan.add(foreach);
        
        plan.connect(load, foreach);
        
        filter = new LOFilter(plan);
        LogicalExpressionPlan filterPlan = new LogicalExpressionPlan();
        ProjectExpression namePrj2 = new ProjectExpression(filterPlan, DataType.CHARARRAY, 0, 0);
        filterPlan.add(namePrj2);
        ConstantExpression constExp = new ConstantExpression(filterPlan, DataType.CHARARRAY, "joe");
        filterPlan.add(constExp);
        EqualExpression equal = new EqualExpression(filterPlan, namePrj2, constExp);
        filterPlan.add(equal);
        
        filter.setFilterPlan(filterPlan);
        filter.setAlias("C");
        plan.add(filter);
        
        LOFilter filter2 = new LOFilter(plan);
        LogicalExpressionPlan filter2Plan = new LogicalExpressionPlan();
        ProjectExpression name2Prj2 = new ProjectExpression(filter2Plan, DataType.CHARARRAY, 0, 1);
        filter2Plan.add(name2Prj2);
        ConstantExpression const2Exp = new ConstantExpression(filter2Plan, DataType.CHARARRAY, "joe");
        filter2Plan.add(const2Exp);
        EqualExpression equal2 = new EqualExpression(filter2Plan, namePrj2, constExp);
        filter2Plan.add(equal2);
        
        filter2.setFilterPlan(filter2Plan);
        filter2.setAlias("C1");
        plan.add(filter2);
        
        plan.connect(foreach, filter2);
        plan.connect(filter2, filter);
        
        stor = new LOStore(plan);
        stor.setAlias("D");
        plan.add(stor);
        plan.connect(filter,stor);
        
        try {
            // Stamp everything with a Uid
            UidStamper stamper = new UidStamper(plan);
            stamper.visit();
        }catch(Exception e) {
            assertTrue("Failed to set a valid uid", false );
        }
        
        
        // run filter rule
        Rule r = new FilterAboveForeach("FilterAboveFlatten");
        Set<Rule> s = new HashSet<Rule>();
        s.add(r);
        List<Set<Rule>> ls = new ArrayList<Set<Rule>>();
        ls.add(s);
        
        // Test Plan before optimizing
        List<Operator> list = plan.getSinks();
        assertTrue( list.contains(stor) );
        
        list = plan.getSources();
        assertTrue( list.contains(load) );
        
        assertTrue( plan.getPredecessors(stor).contains(filter) ); 
        assertEquals( 1, plan.getPredecessors(stor).size() );
        
        assertTrue( plan.getPredecessors(filter2).contains(foreach) );
        assertEquals( 1, plan.getPredecessors(filter2).size() );
        
        assertTrue( plan.getPredecessors(foreach).contains(load) );
        assertEquals( 1, plan.getPredecessors(foreach).size() );
        
        assertTrue( plan.getPredecessors(filter).contains(filter2) );
        assertEquals( 1, plan.getPredecessors(filter).size() );
        
        assertTrue( foreach.getInnerPlan().getSinks().contains(generate) );
        assertEquals( 1, foreach.getInnerPlan().getSinks().size() );
        assertTrue( foreach.getInnerPlan().getSources().contains(innerLoad) );
        assertTrue( foreach.getInnerPlan().getSources().contains(innerLoad2) );
        assertEquals( 2, foreach.getInnerPlan().getSources().size() );
        
        // Run the optimizer
        MyPlanOptimizer optimizer = new MyPlanOptimizer(plan, ls, 3);
        optimizer.addPlanTransformListener(new ProjectionPatcher());
        optimizer.addPlanTransformListener(new SchemaPatcher());
        optimizer.optimize();
        
        // Test after optimization
        list = plan.getSinks();
        assertTrue( list.contains(stor) );
        
        list = plan.getSources();
        assertTrue( list.contains(load) );
        
        assertTrue( plan.getPredecessors(stor).contains(filter2) ); 
        assertEquals( 1, plan.getPredecessors(stor).size() );
        
        assertTrue( plan.getPredecessors(filter2).contains(foreach) );
        assertEquals( 1, plan.getPredecessors(filter2).size() );
        
        assertTrue( plan.getPredecessors(foreach).contains(filter) );
        assertEquals( 1, plan.getPredecessors(foreach).size() );
        
        assertTrue( plan.getPredecessors(filter).contains(load) );
        assertEquals( 1, plan.getPredecessors(filter).size() );
        
        assertTrue( foreach.getInnerPlan().getSinks().contains(generate) );
        assertEquals( 1, foreach.getInnerPlan().getSinks().size() );
        assertTrue( foreach.getInnerPlan().getSources().contains(innerLoad) );
        assertTrue( foreach.getInnerPlan().getSources().contains(innerLoad2) );
        assertEquals( 2, foreach.getInnerPlan().getSources().size() );
        
    }
    
    public void testMultipleFilter2() throws Exception {
        
        // Plan here is 
        // Load (name, cuisines{t:(name)}) -> foreach gen name,cuisines 
        // -> filter name == 'joe2' --> filter name == 'joe' --> stor
        
        LogicalPlan plan = null;
        LOLoad load = null;
        LOForEach foreach = null;
        LOFilter filter = null;
        LOStore stor = null;
        
        plan = new LogicalPlan();
        
        LogicalSchema schema = new LogicalSchema();
        schema.addField(new LogicalSchema.LogicalFieldSchema("name", null, DataType.CHARARRAY));
        LogicalSchema bagSchema = new LogicalSchema();
        LogicalSchema bagTupleSchema = new LogicalSchema();
        bagTupleSchema.addField( new LogicalSchema.LogicalFieldSchema("name", null, DataType.CHARARRAY) );
        bagSchema.addField( new LogicalSchema.LogicalFieldSchema( "t", bagTupleSchema, DataType.TUPLE ) );
        schema.addField(new LogicalSchema.LogicalFieldSchema("cuisines", bagSchema, DataType.BAG));
        
        load = new LOLoad(null, schema, plan);
        load.setAlias("A");
        plan.add(load);
        
        foreach = new LOForEach(plan);
        
        LogicalPlan innerPlan = new LogicalPlan();
        LOInnerLoad innerLoad = new LOInnerLoad(innerPlan, foreach, 0);
        innerPlan.add(innerLoad);
        
        LOInnerLoad innerLoad2 = new LOInnerLoad(innerPlan, foreach, 1);
        innerPlan.add(innerLoad2);
        
        LogicalExpressionPlan namePrj = new LogicalExpressionPlan();        
        ProjectExpression prjName = new ProjectExpression(namePrj, DataType.CHARARRAY, 0, 0);
        namePrj.add(prjName);
        
        LogicalExpressionPlan cuisinesPrj = new LogicalExpressionPlan();
        ProjectExpression prjCuisines = new ProjectExpression(cuisinesPrj, DataType.BAG, 1, 0);
        cuisinesPrj.add(prjCuisines);
        
        List<LogicalExpressionPlan> expPlans = new ArrayList<LogicalExpressionPlan>();
        expPlans.add(namePrj);
        expPlans.add(cuisinesPrj);
        
        boolean flatten[] = new boolean[2];
        flatten[0] = false;
        flatten[1] = true;
        
        LOGenerate generate = new LOGenerate(innerPlan, expPlans, flatten);        
        innerPlan.add(generate);
        innerPlan.connect(innerLoad, generate);
        innerPlan.connect(innerLoad2, generate);
        
        foreach.setInnerPlan(innerPlan);
        foreach.setAlias("B");
        plan.add(foreach);
        
        plan.connect(load, foreach);
        
        filter = new LOFilter(plan);
        LogicalExpressionPlan filterPlan = new LogicalExpressionPlan();
        ProjectExpression namePrj2 = new ProjectExpression(filterPlan, DataType.CHARARRAY, 0, 0);
        filterPlan.add(namePrj2);
        ConstantExpression constExp = new ConstantExpression(filterPlan, DataType.CHARARRAY, "joe");
        filterPlan.add(constExp);
        EqualExpression equal = new EqualExpression(filterPlan, namePrj2, constExp);
        filterPlan.add(equal);
        
        filter.setFilterPlan(filterPlan);
        filter.setAlias("C");
        plan.add(filter);
        
        LOFilter filter2 = new LOFilter(plan);
        LogicalExpressionPlan filter2Plan = new LogicalExpressionPlan();
        ProjectExpression name2Prj2 = new ProjectExpression(filter2Plan, DataType.CHARARRAY, 0, 0);
        filter2Plan.add(name2Prj2);
        ConstantExpression const2Exp = new ConstantExpression(filter2Plan, DataType.CHARARRAY, "joe2");
        filter2Plan.add(const2Exp);
        EqualExpression equal2 = new EqualExpression(filter2Plan, namePrj2, constExp);
        filter2Plan.add(equal2);
        
        filter2.setFilterPlan(filter2Plan);
        filter2.setAlias("C1");
        plan.add(filter2);
        
        plan.connect(foreach, filter2);
        plan.connect(filter2, filter);
        
        stor = new LOStore(plan);
        stor.setAlias("D");
        plan.add(stor);
        plan.connect(filter,stor);
        
        try {
            // Stamp everything with a Uid
            UidStamper stamper = new UidStamper(plan);
            stamper.visit();
        }catch(Exception e) {
            assertTrue("Failed to set a valid uid", false );
        }
        
        
        // run filter rule
        Rule r = new FilterAboveForeach("FilterAboveFlatten");
        Set<Rule> s = new HashSet<Rule>();
        s.add(r);
        List<Set<Rule>> ls = new ArrayList<Set<Rule>>();
        ls.add(s);
        
        // Test Plan before optimizing
        List<Operator> list = plan.getSinks();
        assertTrue( list.contains(stor) );
        
        list = plan.getSources();
        assertTrue( list.contains(load) );
        
        assertTrue( plan.getPredecessors(stor).contains(filter) ); 
        assertEquals( 1, plan.getPredecessors(stor).size() );
        
        assertTrue( plan.getPredecessors(filter2).contains(foreach) );
        assertEquals( 1, plan.getPredecessors(filter2).size() );
        
        assertTrue( plan.getPredecessors(foreach).contains(load) );
        assertEquals( 1, plan.getPredecessors(foreach).size() );
        
        assertTrue( plan.getPredecessors(filter).contains(filter2) );
        assertEquals( 1, plan.getPredecessors(filter).size() );
        
        assertTrue( foreach.getInnerPlan().getSinks().contains(generate) );
        assertEquals( 1, foreach.getInnerPlan().getSinks().size() );
        assertTrue( foreach.getInnerPlan().getSources().contains(innerLoad) );
        assertTrue( foreach.getInnerPlan().getSources().contains(innerLoad2) );
        assertEquals( 2, foreach.getInnerPlan().getSources().size() );
        
        // Run the optimizer
        MyPlanOptimizer optimizer = new MyPlanOptimizer(plan, ls, 3);
        optimizer.addPlanTransformListener(new ProjectionPatcher());
        optimizer.addPlanTransformListener(new SchemaPatcher());
        optimizer.optimize();
        
        // Test after optimization
        list = plan.getSinks();
        assertTrue( list.contains(stor) );
        
        list = plan.getSources();
        assertTrue( list.contains(load) );
        
        assertTrue( plan.getPredecessors(stor).contains(foreach) ); 
        assertEquals( 1, plan.getPredecessors(stor).size() );
        
        assertTrue( plan.getPredecessors(filter2).contains(load) );
        assertEquals( 1, plan.getPredecessors(filter2).size() );
        
        assertTrue( plan.getPredecessors(foreach).contains(filter) );
        assertEquals( 1, plan.getPredecessors(foreach).size() );
        
        assertTrue( plan.getPredecessors(filter).contains(filter2) );
        assertEquals( 1, plan.getPredecessors(filter).size() );
        
        assertTrue( foreach.getInnerPlan().getSinks().contains(generate) );
        assertEquals( 1, foreach.getInnerPlan().getSinks().size() );
        assertTrue( foreach.getInnerPlan().getSources().contains(innerLoad) );
        assertTrue( foreach.getInnerPlan().getSources().contains(innerLoad2) );
        assertEquals( 2, foreach.getInnerPlan().getSources().size() );        
    }
    
public void testMultipleFilterNotPossible() throws Exception {
        
        // Plan here is 
        // Load (name, cuisines{t:(name)}) -> foreach gen name,cuisines 
        // -> filter $1 == 'joe2' --> filter $1 == 'joe' --> stor
        
        LogicalPlan plan = null;
        LOLoad load = null;
        LOForEach foreach = null;
        LOFilter filter = null;
        LOStore stor = null;
        
        plan = new LogicalPlan();
        
        LogicalSchema schema = new LogicalSchema();
        schema.addField(new LogicalSchema.LogicalFieldSchema("name", null, DataType.CHARARRAY));
        LogicalSchema bagSchema = new LogicalSchema();
        LogicalSchema bagTupleSchema = new LogicalSchema();
        bagTupleSchema.addField( new LogicalSchema.LogicalFieldSchema("name", null, DataType.CHARARRAY) );
        bagSchema.addField( new LogicalSchema.LogicalFieldSchema( "t", bagTupleSchema, DataType.TUPLE ) );
        schema.addField(new LogicalSchema.LogicalFieldSchema("cuisines", bagSchema, DataType.BAG));
        
        load = new LOLoad(null, schema, plan);
        load.setAlias("A");
        plan.add(load);
        
        foreach = new LOForEach(plan);
        
        LogicalPlan innerPlan = new LogicalPlan();
        LOInnerLoad innerLoad = new LOInnerLoad(innerPlan, foreach, 0);
        innerPlan.add(innerLoad);
        
        LOInnerLoad innerLoad2 = new LOInnerLoad(innerPlan, foreach, 1);
        innerPlan.add(innerLoad2);
        
        LogicalExpressionPlan namePrj = new LogicalExpressionPlan();        
        ProjectExpression prjName = new ProjectExpression(namePrj, DataType.CHARARRAY, 0, 0);
        namePrj.add(prjName);
        
        LogicalExpressionPlan cuisinesPrj = new LogicalExpressionPlan();
        ProjectExpression prjCuisines = new ProjectExpression(cuisinesPrj, DataType.BAG, 1, 0);
        cuisinesPrj.add(prjCuisines);
        
        List<LogicalExpressionPlan> expPlans = new ArrayList<LogicalExpressionPlan>();
        expPlans.add(namePrj);
        expPlans.add(cuisinesPrj);
        
        boolean flatten[] = new boolean[2];
        flatten[0] = false;
        flatten[1] = true;
        
        LOGenerate generate = new LOGenerate(innerPlan, expPlans, flatten);        
        innerPlan.add(generate);
        innerPlan.connect(innerLoad, generate);
        innerPlan.connect(innerLoad2, generate);
        
        foreach.setInnerPlan(innerPlan);
        foreach.setAlias("B");
        plan.add(foreach);
        
        plan.connect(load, foreach);
        
        filter = new LOFilter(plan);
        LogicalExpressionPlan filterPlan = new LogicalExpressionPlan();
        ProjectExpression namePrj2 = new ProjectExpression(filterPlan, DataType.CHARARRAY, 0, 1);
        filterPlan.add(namePrj2);
        ConstantExpression constExp = new ConstantExpression(filterPlan, DataType.CHARARRAY, "joe");
        filterPlan.add(constExp);
        EqualExpression equal = new EqualExpression(filterPlan, namePrj2, constExp);
        filterPlan.add(equal);
        
        filter.setFilterPlan(filterPlan);
        filter.setAlias("C");
        plan.add(filter);
        
        LOFilter filter2 = new LOFilter(plan);
        LogicalExpressionPlan filter2Plan = new LogicalExpressionPlan();
        ProjectExpression name2Prj2 = new ProjectExpression(filter2Plan, DataType.CHARARRAY, 0, 1);
        filter2Plan.add(name2Prj2);
        ConstantExpression const2Exp = new ConstantExpression(filter2Plan, DataType.CHARARRAY, "joe2");
        filter2Plan.add(const2Exp);
        EqualExpression equal2 = new EqualExpression(filter2Plan, namePrj2, constExp);
        filter2Plan.add(equal2);
        
        filter2.setFilterPlan(filter2Plan);
        filter2.setAlias("C1");
        plan.add(filter2);
        
        plan.connect(foreach, filter2);
        plan.connect(filter2, filter);
        
        stor = new LOStore(plan);
        stor.setAlias("D");
        plan.add(stor);
        plan.connect(filter,stor);
        
        try {
            // Stamp everything with a Uid
            UidStamper stamper = new UidStamper(plan);
            stamper.visit();
        }catch(Exception e) {
            assertTrue("Failed to set a valid uid", false );
        }
        
        
        // run filter rule
        Rule r = new FilterAboveForeach("FilterAboveFlatten");
        Set<Rule> s = new HashSet<Rule>();
        s.add(r);
        List<Set<Rule>> ls = new ArrayList<Set<Rule>>();
        ls.add(s);
        
        // Test Plan before optimizing
        List<Operator> list = plan.getSinks();
        assertTrue( list.contains(stor) );
        
        list = plan.getSources();
        assertTrue( list.contains(load) );
        
        assertTrue( plan.getPredecessors(stor).contains(filter) ); 
        assertEquals( 1, plan.getPredecessors(stor).size() );
        
        assertTrue( plan.getPredecessors(filter2).contains(foreach) );
        assertEquals( 1, plan.getPredecessors(filter2).size() );
        
        assertTrue( plan.getPredecessors(foreach).contains(load) );
        assertEquals( 1, plan.getPredecessors(foreach).size() );
        
        assertTrue( plan.getPredecessors(filter).contains(filter2) );
        assertEquals( 1, plan.getPredecessors(filter).size() );
        
        assertTrue( foreach.getInnerPlan().getSinks().contains(generate) );
        assertEquals( 1, foreach.getInnerPlan().getSinks().size() );
        assertTrue( foreach.getInnerPlan().getSources().contains(innerLoad) );
        assertTrue( foreach.getInnerPlan().getSources().contains(innerLoad2) );
        assertEquals( 2, foreach.getInnerPlan().getSources().size() );
        
        // Run the optimizer
        MyPlanOptimizer optimizer = new MyPlanOptimizer(plan, ls, 3);
        optimizer.addPlanTransformListener(new ProjectionPatcher());
        optimizer.addPlanTransformListener(new SchemaPatcher());
        optimizer.optimize();
        
        // Test after optimization
        list = plan.getSinks();
        assertTrue( list.contains(stor) );
        
        list = plan.getSources();
        assertTrue( list.contains(load) );
        
        assertTrue( plan.getPredecessors(stor).contains(filter) ); 
        assertEquals( 1, plan.getPredecessors(stor).size() );
        
        assertTrue( plan.getPredecessors(filter2).contains(foreach) );
        assertEquals( 1, plan.getPredecessors(filter2).size() );
        
        assertTrue( plan.getPredecessors(foreach).contains(load) );
        assertEquals( 1, plan.getPredecessors(foreach).size() );
        
        assertTrue( plan.getPredecessors(filter).contains(filter2) );
        assertEquals( 1, plan.getPredecessors(filter).size() );
        
        assertTrue( foreach.getInnerPlan().getSinks().contains(generate) );
        assertEquals( 1, foreach.getInnerPlan().getSinks().size() );
        assertTrue( foreach.getInnerPlan().getSources().contains(innerLoad) );
        assertTrue( foreach.getInnerPlan().getSources().contains(innerLoad2) );
        assertEquals( 2, foreach.getInnerPlan().getSources().size() );    
    }
    
    public void testNotPossibleFilter() throws Exception {
        // Plan here is 
        // Load (name, cuisines{t:(name)}) -> foreach gen name,flatten(cuisines) 
        // -> filter $1 == 'joe' --> stor

        LogicalPlan plan = null;
        LOLoad load = null;
        LOForEach foreach = null;
        LOFilter filter = null;
        LOStore stor = null;

        plan = new LogicalPlan();

        LogicalSchema schema = new LogicalSchema();
        schema.addField(new LogicalSchema.LogicalFieldSchema("name", null, DataType.CHARARRAY));
        LogicalSchema bagSchema = new LogicalSchema();
        LogicalSchema bagTupleSchema = new LogicalSchema();
        bagTupleSchema.addField( new LogicalSchema.LogicalFieldSchema("name", null, DataType.CHARARRAY) );
        bagSchema.addField( new LogicalSchema.LogicalFieldSchema( "t", bagTupleSchema, DataType.TUPLE ) );
        schema.addField(new LogicalSchema.LogicalFieldSchema("cuisines", bagSchema, DataType.BAG));
        
        load = new LOLoad(null, schema, plan);
        load.setAlias("A");
        plan.add(load);
        
        foreach = new LOForEach(plan);
        
        LogicalPlan innerPlan = new LogicalPlan();
        LOInnerLoad innerLoad = new LOInnerLoad(innerPlan, foreach, 0);
        innerPlan.add(innerLoad);
        
        LOInnerLoad innerLoad2 = new LOInnerLoad(innerPlan, foreach, 1);
        innerPlan.add(innerLoad2);
        
        LogicalExpressionPlan namePrj = new LogicalExpressionPlan();        
        ProjectExpression prjName = new ProjectExpression(namePrj, DataType.CHARARRAY, 0, 0);
        namePrj.add(prjName);
        
        LogicalExpressionPlan cuisinesPrj = new LogicalExpressionPlan();
        ProjectExpression prjCuisines = new ProjectExpression(cuisinesPrj, DataType.BAG, 1, 0);
        cuisinesPrj.add(prjCuisines);
        
        List<LogicalExpressionPlan> expPlans = new ArrayList<LogicalExpressionPlan>();
        expPlans.add(namePrj);
        expPlans.add(cuisinesPrj);
        
        boolean flatten[] = new boolean[2];
        flatten[0] = false;
        flatten[1] = true;
        
        LOGenerate generate = new LOGenerate(innerPlan, expPlans, flatten);        
        innerPlan.add(generate);
        innerPlan.connect(innerLoad, generate);
        innerPlan.connect(innerLoad2, generate);
        
        foreach.setInnerPlan(innerPlan);
        foreach.setAlias("B");
        plan.add(foreach);
        
        plan.connect(load, foreach);
        
        filter = new LOFilter(plan);
        LogicalExpressionPlan filterPlan = new LogicalExpressionPlan();
        ProjectExpression namePrj2 = new ProjectExpression(filterPlan, DataType.CHARARRAY, 0, 1);
        filterPlan.add(namePrj2);
        ConstantExpression constExp = new ConstantExpression(filterPlan, DataType.CHARARRAY, "joe");
        filterPlan.add(constExp);
        EqualExpression equal = new EqualExpression(filterPlan, namePrj2, constExp);
        filterPlan.add(equal);
        
        filter.setFilterPlan(filterPlan);
        filter.setAlias("C");
        plan.add(filter);
        
        plan.connect(foreach, filter);
        
        stor = new LOStore(plan);
        stor.setAlias("D");
        plan.add(stor);
        plan.connect(filter,stor);
        
        try {
            // Stamp everything with a Uid
            UidStamper stamper = new UidStamper(plan);
            stamper.visit();
        }catch(Exception e) {
            assertTrue("Failed to set a valid uid", false );
        }
        
        
        // run filter rule
        Rule r = new FilterAboveForeach("FilterAboveFlatten");
        Set<Rule> s = new HashSet<Rule>();
        s.add(r);
        List<Set<Rule>> ls = new ArrayList<Set<Rule>>();
        ls.add(s);
        
        // Test Plan before optimizing
        List<Operator> list = plan.getSinks();
        assertTrue( list.contains(stor) );
        
        list = plan.getSources();
        assertTrue( list.contains(load) );
        
        assertTrue( plan.getPredecessors(stor).contains(filter) ); 
        assertEquals( 1, plan.getPredecessors(stor).size() );
        
        assertTrue( plan.getPredecessors(filter).contains(foreach) );
        assertEquals( 1, plan.getPredecessors(filter).size() );
        
        assertTrue( foreach.getInnerPlan().getSinks().contains(generate) );
        assertEquals( 1, foreach.getInnerPlan().getSinks().size() );
        assertTrue( foreach.getInnerPlan().getSources().contains(innerLoad) );
        assertTrue( foreach.getInnerPlan().getSources().contains(innerLoad2) );
        assertEquals( 2, foreach.getInnerPlan().getSources().size() );
        
        // Run the optimizer
        MyPlanOptimizer optimizer = new MyPlanOptimizer(plan, ls, 3);
        optimizer.addPlanTransformListener(new ProjectionPatcher());
        optimizer.addPlanTransformListener(new SchemaPatcher());
        optimizer.optimize();
        
        // Test after optimization
        list = plan.getSinks();
        assertTrue( list.contains(stor) );
        
        list = plan.getSources();
        assertTrue( list.contains(load) );
        
        assertTrue( plan.getPredecessors(stor).contains(filter) ); 
        assertEquals( 1, plan.getPredecessors(stor).size() );
        
        assertTrue( plan.getPredecessors(filter).contains(foreach) );
        assertEquals( 1, plan.getPredecessors(filter).size() );
        
        assertTrue( foreach.getInnerPlan().getSinks().contains(generate) );
        assertEquals( 1, foreach.getInnerPlan().getSinks().size() );
        assertTrue( foreach.getInnerPlan().getSources().contains(innerLoad) );
        assertTrue( foreach.getInnerPlan().getSources().contains(innerLoad2) );
        assertEquals( 2, foreach.getInnerPlan().getSources().size() );
    }
    
    public void testSimple2() throws Exception {
        
        // Plan here is 
        // Load (name, cuisines{t:(name)}) -> foreach gen name,cuisines 
        // -> filter name == 'joe' --> stor
        
        LogicalPlan plan = null;
        LOLoad load = null;
        LOForEach foreach = null;
        LOFilter filter = null;
        LOStore stor = null;
        
        plan = new LogicalPlan();
        
        LogicalSchema schema = new LogicalSchema();
        schema.addField(new LogicalSchema.LogicalFieldSchema("name", null, DataType.CHARARRAY));
        LogicalSchema bagSchema = new LogicalSchema();
        LogicalSchema bagTupleSchema = new LogicalSchema();
        bagTupleSchema.addField( new LogicalSchema.LogicalFieldSchema("name", null, DataType.CHARARRAY) );
        bagSchema.addField( new LogicalSchema.LogicalFieldSchema( "t", bagTupleSchema, DataType.TUPLE ) );
        schema.addField(new LogicalSchema.LogicalFieldSchema("cuisines", bagSchema, DataType.BAG));
        
        load = new LOLoad(null, schema, plan);
        load.setAlias("A");
        plan.add(load);
        
        foreach = new LOForEach(plan);
        
        LogicalPlan innerPlan = new LogicalPlan();
        LOInnerLoad innerLoad = new LOInnerLoad(innerPlan, foreach, 0);
        innerPlan.add(innerLoad);
        
        LOInnerLoad innerLoad2 = new LOInnerLoad(innerPlan, foreach, 1);
        innerPlan.add(innerLoad2);
        
        LogicalExpressionPlan namePrj = new LogicalExpressionPlan();        
        ProjectExpression prjName = new ProjectExpression(namePrj, DataType.CHARARRAY, 0, 0);
        namePrj.add(prjName);
        
        LogicalExpressionPlan cuisinesPrj = new LogicalExpressionPlan();
        ProjectExpression prjCuisines = new ProjectExpression(cuisinesPrj, DataType.BAG, 1, 0);
        cuisinesPrj.add(prjCuisines);
        
        List<LogicalExpressionPlan> expPlans = new ArrayList<LogicalExpressionPlan>();
        expPlans.add(namePrj);
        expPlans.add(cuisinesPrj);
        
        boolean flatten[] = new boolean[2];
        flatten[0] = false;
        flatten[1] = false;
        
        LOGenerate generate = new LOGenerate(innerPlan, expPlans, flatten);        
        innerPlan.add(generate);
        innerPlan.connect(innerLoad, generate);
        innerPlan.connect(innerLoad2, generate);
        
        foreach.setInnerPlan(innerPlan);
        foreach.setAlias("B");
        plan.add(foreach);
        
        plan.connect(load, foreach);
        
        filter = new LOFilter(plan);
        LogicalExpressionPlan filterPlan = new LogicalExpressionPlan();
        ProjectExpression namePrj2 = new ProjectExpression(filterPlan, DataType.CHARARRAY, 0, 0);
        filterPlan.add(namePrj2);
        ConstantExpression constExp = new ConstantExpression(filterPlan, DataType.CHARARRAY, "joe");
        filterPlan.add(constExp);
        EqualExpression equal = new EqualExpression(filterPlan, namePrj2, constExp);
        filterPlan.add(equal);
        
        filter.setFilterPlan(filterPlan);
        filter.setAlias("C");
        plan.add(filter);
        
        plan.connect(foreach, filter);
        
        stor = new LOStore(plan);
        stor.setAlias("D");
        plan.add(stor);
        plan.connect(filter,stor);
        
        try {
            // Stamp everything with a Uid
            UidStamper stamper = new UidStamper(plan);
            stamper.visit();
        }catch(Exception e) {
            assertTrue("Failed to set a valid uid", false );
        }
        
        
        // run filter rule
        Rule r = new FilterAboveForeach("FilterAboveFlatten");
        Set<Rule> s = new HashSet<Rule>();
        s.add(r);
        List<Set<Rule>> ls = new ArrayList<Set<Rule>>();
        ls.add(s);
        
        // Test Plan before optimizing
        List<Operator> list = plan.getSinks();
        assertTrue( list.contains(stor) );
        
        list = plan.getSources();
        assertTrue( list.contains(load) );
        
        assertTrue( plan.getPredecessors(stor).contains(filter) ); 
        assertEquals( 1, plan.getPredecessors(stor).size() );
        
        assertTrue( plan.getPredecessors(filter).contains(foreach) );
        assertEquals( 1, plan.getPredecessors(filter).size() );
        
        assertTrue( foreach.getInnerPlan().getSinks().contains(generate) );
        assertEquals( 1, foreach.getInnerPlan().getSinks().size() );
        assertTrue( foreach.getInnerPlan().getSources().contains(innerLoad) );
        assertTrue( foreach.getInnerPlan().getSources().contains(innerLoad2) );
        assertEquals( 2, foreach.getInnerPlan().getSources().size() );
        
        // Run the optimizer
        MyPlanOptimizer optimizer = new MyPlanOptimizer(plan, ls, 3);
        optimizer.addPlanTransformListener(new ProjectionPatcher());
        optimizer.addPlanTransformListener(new SchemaPatcher());
        optimizer.optimize();
        
        // Test after optimization
        list = plan.getSinks();
        assertTrue( list.contains(stor) );
        
        list = plan.getSources();
        assertTrue( list.contains(load) );
        
        assertTrue( plan.getPredecessors(stor).contains(foreach) ); 
        assertEquals( 1, plan.getPredecessors(stor).size() );
        
        assertTrue( plan.getPredecessors(filter).contains(load) );
        assertEquals( 1, plan.getPredecessors(filter).size() );
        
        assertTrue( plan.getPredecessors(foreach).contains(filter) );
        assertEquals( 1, plan.getPredecessors(foreach).size() );
        
        assertTrue( foreach.getInnerPlan().getSinks().contains(generate) );
        assertEquals( 1, foreach.getInnerPlan().getSinks().size() );
        assertTrue( foreach.getInnerPlan().getSources().contains(innerLoad) );
        assertTrue( foreach.getInnerPlan().getSources().contains(innerLoad2) );
        assertEquals( 2, foreach.getInnerPlan().getSources().size() );
    }
    
    public class MyPlanOptimizer extends PlanOptimizer {

        protected MyPlanOptimizer(OperatorPlan p, List<Set<Rule>> rs,
                int iterations) {
            super(p, rs, iterations);           
        }
        
        public void addPlanTransformListener(PlanTransformListener listener) {
            super.addPlanTransformListener(listener);
        }
        
    }
    
//    public class MyPrintVisitor extends AllExpressionVisitor {
//
//        private PrintStream mStream = null;
//        private String TAB1 = "    ";
//        private String TABMore = "|   ";
//        private String LSep = "|\n|---";
//        private String USep = "|   |\n|   ";
//        private int levelCntr = -1;
//        private boolean isVerbose = true;
//        
//        public MyPrintVisitor(OperatorPlan plan, PrintStream ps) {
//            super(plan, new DepthFirstWalker(plan));
//            mStream = ps;
//        }
//
//        @Override
//        protected LogicalExpressionVisitor getVisitor(LogicalExpressionPlan expr) {
//            // TODO Auto-generated method stub
//            return null;
//        }      
//        
//        @Override
//        public void visit() throws VisitorException {
//            try {
//                mStream.write(depthFirstLP().getBytes());
//            } catch (IOException e) {
//                throw new VisitorException(e);
//            }
//        }
//
//        public void setVerbose(boolean verbose) {
//            isVerbose = verbose;
//        }
//
//        public void print(OutputStream printer) throws VisitorException, IOException {
//            printer.write(depthFirstLP().getBytes());
//        }
//
//        class LogicalRelationalOperatorCompare implements Comparator<LogicalRelationalOperator> {
//
//            @Override
//            public int compare(LogicalRelationalOperator o1,
//                    LogicalRelationalOperator o2) {
//                return 0;
//            }
//            
//        }
//
//        protected String depthFirstLP() throws VisitorException, IOException {
//            StringBuilder sb = new StringBuilder();
//            List<Operator> leaves = plan.getSinks();
//            // Collections.sort(leaves, c)
//            for (Operator leaf : leaves) {
//                sb.append(depthFirst(leaf));
//                sb.append("\n");
//            }
//            //sb.delete(sb.length() - "\n".length(), sb.length());
//            //sb.delete(sb.length() - "\n".length(), sb.length());
//            return sb.toString();
//        }
//        
//        private String planString(LogicalPlan lp) throws VisitorException, IOException {
//            StringBuilder sb = new StringBuilder();
//            ByteArrayOutputStream baos = new ByteArrayOutputStream();
//            if(lp!=null)
//                lp.explain(baos, mStream);
//            else
//                return "";
//            sb.append(USep);
//            sb.append(shiftStringByTabs(baos.toString(), 2));
//            return sb.toString();
//        }
//        
//        private String planString(
//                List<LogicalPlan> logicalPlanList) throws VisitorException, IOException {
//            StringBuilder sb = new StringBuilder();
//            if(logicalPlanList!=null)
//                for (LogicalPlan lp : logicalPlanList) {
//                    sb.append(planString(lp));
//                }
//            return sb.toString();
//        }
//
//        private String depthFirst(Operator node) throws VisitorException, IOException {
//            StringBuilder sb = new StringBuilder(node.getName());
//            if(node instanceof LogicalExpression) {
//                sb.append(" FieldSchema: ");
//                try {
//                    sb.append(((LogicalExpression)node).getUid());
//                    sb.append(" Type: " + DataType.findTypeName(((LogicalExpression)node).getType()));
//                } catch (Exception e) {
//                    sb.append("Caught Exception: " + e.getMessage());
//                }
//            } else if( node instanceof LogicalRelationalOperator ){
//                sb.append(" Schema: ");
//                try {
//                    sb.append(((LogicalRelationalOperator)node).getSchema());
//                } catch (Exception e) {
//                    sb.append("Caught exception: " + e.getMessage());
//                }
//            }
//
//            sb.append("\n");
//
//            if (isVerbose) {
//                if(node instanceof LOFilter){
//                    sb.append(planString(((LOFilter)node).getComparisonPlan()));
//                }
//                else if(node instanceof LOForEach){
//                    sb.append(planString(((LOForEach)node).getForEachPlans()));        
//                }
//                else if(node instanceof LOGenerate){
//                    sb.append(planString(((LOGenerate)node).getGeneratePlans())); 
//                    
//                }
//                else if(node instanceof LOCogroup){
//                    MultiMap<LogicalOperator, LogicalPlan> plans = ((LOCogroup)node).getGroupByPlans();
//                    for (LogicalOperator lo : plans.keySet()) {
//                        // Visit the associated plans
//                        for (LogicalPlan plan : plans.get(lo)) {
//                            sb.append(planString(plan));
//                        }
//                    }
//                }
//                else if(node instanceof LOJoin){
//                    MultiMap<LogicalOperator, LogicalPlan> plans = ((LOJoin)node).getJoinPlans();
//                    for (LogicalOperator lo : plans.keySet()) {
//                        // Visit the associated plans
//                        for (LogicalPlan plan : plans.get(lo)) {
//                            sb.append(planString(plan));
//                        }
//                    }
//                }
//                else if(node instanceof LOJoin){
//                    MultiMap<LogicalOperator, LogicalPlan> plans = ((LOJoin)node).getJoinPlans();
//                    for (LogicalOperator lo : plans.keySet()) {
//                        // Visit the associated plans
//                        for (LogicalPlan plan : plans.get(lo)) {
//                            sb.append(planString(plan));
//                        }
//                    }
//                }
//                else if(node instanceof LOSort){
//                    sb.append(planString(((LOSort)node).getSortColPlans())); 
//                }
//                else if(node instanceof LOSplitOutput){
//                    sb.append(planString(((LOSplitOutput)node).getConditionPlan()));
//                }
//                else if (node instanceof LOProject) {
//                    sb.append("Input: ");
//                    sb.append(((LOProject)node).getExpression().name());
//                }
//            }
//            
//            List<LogicalOperator> originalPredecessors =  mPlan.getPredecessors(node);
//            if (originalPredecessors == null)
//                return sb.toString();
//            
//            List<LogicalOperator> predecessors =  new ArrayList<LogicalOperator>(originalPredecessors);
//            
//            Collections.sort(predecessors);
//            int i = 0;
//            for (LogicalOperator pred : predecessors) {
//                i++;
//                String DFStr = depthFirst(pred);
//                if (DFStr != null) {
//                    sb.append(LSep);
//                    if (i < predecessors.size())
//                        sb.append(shiftStringByTabs(DFStr, 2));
//                    else
//                        sb.append(shiftStringByTabs(DFStr, 1));
//                }
//            }
//            return sb.toString();
//        }
//
//        private String shiftStringByTabs(String DFStr, int TabType) {
//            StringBuilder sb = new StringBuilder();
//            String[] spl = DFStr.split("\n");
//
//            String tab = (TabType == 1) ? TAB1 : TABMore;
//
//            sb.append(spl[0] + "\n");
//            for (int i = 1; i < spl.length; i++) {
//                sb.append(tab);
//                sb.append(spl[i]);
//                sb.append("\n");
//            }
//            return sb.toString();
//        }
//
//        private void dispTabs() {
//            for (int i = 0; i < levelCntr; i++)
//                System.out.print(TAB1);
//        }
//    }
}
