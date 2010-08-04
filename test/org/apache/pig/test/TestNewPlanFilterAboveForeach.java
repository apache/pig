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
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.expression.ConstantExpression;
import org.apache.pig.newplan.logical.expression.EqualExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.optimizer.ProjectionPatcher;
import org.apache.pig.newplan.logical.optimizer.SchemaPatcher;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOGenerate;
import org.apache.pig.newplan.logical.relational.LOInnerLoad;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LOStore;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.newplan.logical.relational.LogicalSchema.LogicalFieldSchema;
import org.apache.pig.newplan.logical.rules.FilterAboveForeach;
import org.apache.pig.newplan.optimizer.PlanOptimizer;
import org.apache.pig.newplan.optimizer.PlanTransformListener;
import org.apache.pig.newplan.optimizer.Rule;

import junit.framework.TestCase;

public class TestNewPlanFilterAboveForeach extends TestCase {
    
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
        
        load = new LOLoad(null, schema, plan, null);
        load.setAlias("A");
        plan.add(load);
        
        foreach = new LOForEach(plan);
        
        LogicalPlan innerPlan = new LogicalPlan();
        List<LogicalExpressionPlan> expPlans = new ArrayList<LogicalExpressionPlan>();
        
        boolean flatten[] = new boolean[2];
        flatten[0] = false;
        flatten[1] = true;
        
        LOGenerate generate = new LOGenerate(innerPlan, expPlans, flatten);        
        
        LOInnerLoad innerLoad = new LOInnerLoad(innerPlan, foreach, 0);
        innerPlan.add(innerLoad);
        
        LOInnerLoad innerLoad2 = new LOInnerLoad(innerPlan, foreach, 1);
        innerPlan.add(innerLoad2);
        
        LogicalExpressionPlan namePrj = new LogicalExpressionPlan();        
        ProjectExpression prjName = new ProjectExpression(namePrj, 0, 0, generate);
        namePrj.add(prjName);
        
        LogicalExpressionPlan cuisinesPrj = new LogicalExpressionPlan();
        ProjectExpression prjCuisines = new ProjectExpression(cuisinesPrj, 1, 0, generate);
        cuisinesPrj.add(prjCuisines);
        
        
        expPlans.add(namePrj);
        expPlans.add(cuisinesPrj);
        
        innerPlan.add(generate);
        innerPlan.connect(innerLoad, generate);
        innerPlan.connect(innerLoad2, generate);
        
        foreach.setInnerPlan(innerPlan);
        foreach.setAlias("B");
        plan.add(foreach);
        
        plan.connect(load, foreach);
        
        filter = new LOFilter(plan);
        LogicalExpressionPlan filterPlan = new LogicalExpressionPlan();
        ProjectExpression namePrj2 = new ProjectExpression(filterPlan, 0, 0, filter);
        filterPlan.add(namePrj2);
        ConstantExpression constExp = new ConstantExpression(filterPlan, "joe", 
                new LogicalFieldSchema(null, null, DataType.CHARARRAY));
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
        optimizer.addPlanTransformListener(new SchemaPatcher());
        optimizer.addPlanTransformListener(new ProjectionPatcher());
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
        assertEquals( load.getSchema().getField(0).uid, namePrj2.getFieldSchema().uid );
        assertEquals( namePrj2.getFieldSchema().uid, prjName.getFieldSchema().uid );
        
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
        
        load = new LOLoad(null, schema, plan, null);
        load.setAlias("A");
        plan.add(load);
        
        foreach = new LOForEach(plan);
        
        LogicalPlan innerPlan = new LogicalPlan();
        List<LogicalExpressionPlan> expPlans = new ArrayList<LogicalExpressionPlan>();
        
        boolean flatten[] = new boolean[2];
        flatten[0] = false;
        flatten[1] = true;
        
        LOGenerate generate = new LOGenerate(innerPlan, expPlans, flatten);        
        
        LOInnerLoad innerLoad = new LOInnerLoad(innerPlan, foreach, 0);
        innerPlan.add(innerLoad);
        
        LOInnerLoad innerLoad2 = new LOInnerLoad(innerPlan, foreach, 1);
        innerPlan.add(innerLoad2);
        
        LogicalExpressionPlan namePrj = new LogicalExpressionPlan();        
        ProjectExpression prjName = new ProjectExpression(namePrj, 0, 0, generate);
        namePrj.add(prjName);
        
        LogicalExpressionPlan cuisinesPrj = new LogicalExpressionPlan();
        ProjectExpression prjCuisines = new ProjectExpression(cuisinesPrj, 1, 0, generate);
        cuisinesPrj.add(prjCuisines);
        
        
        expPlans.add(namePrj);
        expPlans.add(cuisinesPrj);
        
        innerPlan.add(generate);
        innerPlan.connect(innerLoad, generate);
        innerPlan.connect(innerLoad2, generate);
        
        foreach.setInnerPlan(innerPlan);
        foreach.setAlias("B");
        plan.add(foreach);
        
        plan.connect(load, foreach);
        
        filter = new LOFilter(plan);
        LogicalExpressionPlan filterPlan = new LogicalExpressionPlan();
        ProjectExpression namePrj2 = new ProjectExpression(filterPlan, 0, 0, filter);
        filterPlan.add(namePrj2);
        ConstantExpression constExp = new ConstantExpression(filterPlan, "joe", 
            new LogicalFieldSchema(null, null, DataType.CHARARRAY));
        filterPlan.add(constExp);
        EqualExpression equal = new EqualExpression(filterPlan, namePrj2, constExp);
        filterPlan.add(equal);
        
        filter.setFilterPlan(filterPlan);
        filter.setAlias("C");
        plan.add(filter);
        
        LOFilter filter2 = new LOFilter(plan);
        LogicalExpressionPlan filter2Plan = new LogicalExpressionPlan();
        ProjectExpression name2Prj2 = new ProjectExpression(filter2Plan, 0, 1, filter2);
        filter2Plan.add(name2Prj2);
        ConstantExpression const2Exp = new ConstantExpression(filter2Plan, "joe", 
                new LogicalFieldSchema(null, null, DataType.CHARARRAY));
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
        optimizer.addPlanTransformListener(new SchemaPatcher());
        optimizer.addPlanTransformListener(new ProjectionPatcher());
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
        assertEquals( load.getSchema().getField(0).uid, namePrj2.getFieldSchema().uid );
        assertEquals( namePrj2.getFieldSchema().uid, prjName.getFieldSchema().uid );
        
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
        
        load = new LOLoad(null, schema, plan, null);
        load.setAlias("A");
        plan.add(load);
        
        foreach = new LOForEach(plan);
        
        LogicalPlan innerPlan = new LogicalPlan();
        
        List<LogicalExpressionPlan> expPlans = new ArrayList<LogicalExpressionPlan>();
        
        boolean flatten[] = new boolean[2];
        flatten[0] = false;
        flatten[1] = true;
        
        LOGenerate generate = new LOGenerate(innerPlan, expPlans, flatten);
        LOInnerLoad innerLoad = new LOInnerLoad(innerPlan, foreach, 0);
        innerPlan.add(innerLoad);
        
        LOInnerLoad innerLoad2 = new LOInnerLoad(innerPlan, foreach, 1);
        innerPlan.add(innerLoad2);
        
        LogicalExpressionPlan namePrj = new LogicalExpressionPlan();        
        ProjectExpression prjName = new ProjectExpression(namePrj, 0, 0, generate);
        namePrj.add(prjName);
        
        LogicalExpressionPlan cuisinesPrj = new LogicalExpressionPlan();
        ProjectExpression prjCuisines = new ProjectExpression(cuisinesPrj, 1, 0, generate);
        cuisinesPrj.add(prjCuisines);
        
        
        expPlans.add(namePrj);
        expPlans.add(cuisinesPrj);
        
        
        innerPlan.add(generate);
        innerPlan.connect(innerLoad, generate);
        innerPlan.connect(innerLoad2, generate);
        
        foreach.setInnerPlan(innerPlan);
        foreach.setAlias("B");
        plan.add(foreach);
        
        plan.connect(load, foreach);
        
        filter = new LOFilter(plan);
        LogicalExpressionPlan filterPlan = new LogicalExpressionPlan();
        ProjectExpression namePrj2 = new ProjectExpression(filterPlan, 0, 0, filter);
        filterPlan.add(namePrj2);
        ConstantExpression constExp = new ConstantExpression(filterPlan, "joe", 
                new LogicalFieldSchema(null, null, DataType.CHARARRAY));
        filterPlan.add(constExp);
        EqualExpression equal = new EqualExpression(filterPlan, namePrj2, constExp);
        filterPlan.add(equal);
        
        filter.setFilterPlan(filterPlan);
        filter.setAlias("C");
        plan.add(filter);
        
        LOFilter filter2 = new LOFilter(plan);
        LogicalExpressionPlan filter2Plan = new LogicalExpressionPlan();
        ProjectExpression name2Prj2 = new ProjectExpression(filter2Plan, 0, 0, filter2);
        filter2Plan.add(name2Prj2);
        ConstantExpression const2Exp = new ConstantExpression(filter2Plan, "joe2", 
                new LogicalFieldSchema(null, null, DataType.CHARARRAY));
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
        optimizer.addPlanTransformListener(new SchemaPatcher());
        optimizer.addPlanTransformListener(new ProjectionPatcher());
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
        
        assertEquals( load.getSchema().getField(0).uid, namePrj2.getFieldSchema().uid );
        assertEquals( namePrj2.getFieldSchema().uid, name2Prj2.getFieldSchema().uid );
        assertEquals( name2Prj2.getFieldSchema().uid, prjName.getFieldSchema().uid );
        
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
        
        load = new LOLoad(null, schema, plan, null);
        load.setAlias("A");
        plan.add(load);
        
        foreach = new LOForEach(plan);
        
        LogicalPlan innerPlan = new LogicalPlan();
        List<LogicalExpressionPlan> expPlans = new ArrayList<LogicalExpressionPlan>();
        
        boolean flatten[] = new boolean[2];
        flatten[0] = false;
        flatten[1] = true;
        
        LOGenerate generate = new LOGenerate(innerPlan, expPlans, flatten);        
        
        
        LOInnerLoad innerLoad = new LOInnerLoad(innerPlan, foreach, 0);
        innerPlan.add(innerLoad);
        
        LOInnerLoad innerLoad2 = new LOInnerLoad(innerPlan, foreach, 1);
        innerPlan.add(innerLoad2);
        
        LogicalExpressionPlan namePrj = new LogicalExpressionPlan();        
        ProjectExpression prjName = new ProjectExpression(namePrj, 0, 0, generate);
        namePrj.add(prjName);
        
        LogicalExpressionPlan cuisinesPrj = new LogicalExpressionPlan();
        ProjectExpression prjCuisines = new ProjectExpression(cuisinesPrj, 1, 0, generate);
        cuisinesPrj.add(prjCuisines);
        
        expPlans.add(namePrj);
        expPlans.add(cuisinesPrj);
        
        innerPlan.add(generate);
        innerPlan.connect(innerLoad, generate);
        innerPlan.connect(innerLoad2, generate);
        
        foreach.setInnerPlan(innerPlan);
        foreach.setAlias("B");
        plan.add(foreach);
        
        plan.connect(load, foreach);
        
        filter = new LOFilter(plan);
        LogicalExpressionPlan filterPlan = new LogicalExpressionPlan();
        ProjectExpression namePrj2 = new ProjectExpression(filterPlan, 0, 1, filter);
        filterPlan.add(namePrj2);
        ConstantExpression constExp = new ConstantExpression(filterPlan, "joe", 
                new LogicalFieldSchema(null, null, DataType.CHARARRAY));
        filterPlan.add(constExp);
        EqualExpression equal = new EqualExpression(filterPlan, namePrj2, constExp);
        filterPlan.add(equal);
        
        filter.setFilterPlan(filterPlan);
        filter.setAlias("C");
        plan.add(filter);
        
        LOFilter filter2 = new LOFilter(plan);
        LogicalExpressionPlan filter2Plan = new LogicalExpressionPlan();
        ProjectExpression name2Prj2 = new ProjectExpression(filter2Plan, 0, 1, filter2);
        filter2Plan.add(name2Prj2);
        ConstantExpression const2Exp = new ConstantExpression(filter2Plan, "joe2", 
                new LogicalFieldSchema(null, null, DataType.CHARARRAY));
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
        optimizer.addPlanTransformListener(new SchemaPatcher());
        optimizer.addPlanTransformListener(new ProjectionPatcher());
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
        
        assertFalse( prjCuisines.getFieldSchema().uid == namePrj2.getFieldSchema().uid );
        assertFalse( prjCuisines.getFieldSchema().uid == name2Prj2.getFieldSchema().uid );
        
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
        
        load = new LOLoad(null, schema, plan, null);
        load.setAlias("A");
        plan.add(load);
        
        foreach = new LOForEach(plan);
        
        LogicalPlan innerPlan = new LogicalPlan();
        boolean flatten[] = new boolean[2];
        flatten[0] = false;
        flatten[1] = true;
        
        List<LogicalExpressionPlan> expPlans = new ArrayList<LogicalExpressionPlan>();

        LOGenerate generate = new LOGenerate(innerPlan, expPlans, flatten);        

        LOInnerLoad innerLoad = new LOInnerLoad(innerPlan, foreach, 0);
        innerPlan.add(innerLoad);
        
        LOInnerLoad innerLoad2 = new LOInnerLoad(innerPlan, foreach, 1);
        innerPlan.add(innerLoad2);
        
        LogicalExpressionPlan namePrj = new LogicalExpressionPlan();        
        ProjectExpression prjName = new ProjectExpression(namePrj, 0, 0, generate);
        namePrj.add(prjName);
        
        LogicalExpressionPlan cuisinesPrj = new LogicalExpressionPlan();
        ProjectExpression prjCuisines = new ProjectExpression(cuisinesPrj, 1, 0, generate);
        cuisinesPrj.add(prjCuisines);
        
        expPlans.add(namePrj);
        expPlans.add(cuisinesPrj);
        
        innerPlan.add(generate);
        innerPlan.connect(innerLoad, generate);
        innerPlan.connect(innerLoad2, generate);
        
        foreach.setInnerPlan(innerPlan);
        foreach.setAlias("B");
        plan.add(foreach);
        
        plan.connect(load, foreach);
        
        filter = new LOFilter(plan);
        LogicalExpressionPlan filterPlan = new LogicalExpressionPlan();
        ProjectExpression namePrj2 = new ProjectExpression(filterPlan, 0, 1, filter);
        filterPlan.add(namePrj2);
        ConstantExpression constExp = new ConstantExpression(filterPlan, "joe", 
                new LogicalFieldSchema(null, null, DataType.CHARARRAY));
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
        optimizer.addPlanTransformListener(new SchemaPatcher());
        optimizer.addPlanTransformListener(new ProjectionPatcher());
        optimizer.optimize();
        
        // Test after optimization
        list = plan.getSinks();
        assertTrue( list.contains(stor) );
        
        list = plan.getSources();
        assertTrue( list.contains(load) );
        
        assertTrue( plan.getPredecessors(stor).contains(filter) ); 
        assertEquals( 1, plan.getPredecessors(stor).size() );
        
        assertFalse( prjCuisines.getFieldSchema().uid == namePrj2.getFieldSchema().uid );
        
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
        
        load = new LOLoad(null, schema, plan, null);
        load.setAlias("A");
        plan.add(load);
        
        foreach = new LOForEach(plan);
        
        LogicalPlan innerPlan = new LogicalPlan();

        List<LogicalExpressionPlan> expPlans = new ArrayList<LogicalExpressionPlan>();
        
        boolean flatten[] = new boolean[2];
        flatten[0] = false;
        flatten[1] = false;
        
        LOGenerate generate = new LOGenerate(innerPlan, expPlans, flatten);        

        LOInnerLoad innerLoad = new LOInnerLoad(innerPlan, foreach, 0);
        innerPlan.add(innerLoad);
        
        LOInnerLoad innerLoad2 = new LOInnerLoad(innerPlan, foreach, 1);
        innerPlan.add(innerLoad2);
        
        LogicalExpressionPlan namePrj = new LogicalExpressionPlan();        
        ProjectExpression prjName = new ProjectExpression(namePrj, 0, 0, generate);
        namePrj.add(prjName);
        
        LogicalExpressionPlan cuisinesPrj = new LogicalExpressionPlan();
        ProjectExpression prjCuisines = new ProjectExpression(cuisinesPrj, 1, 0, generate);
        cuisinesPrj.add(prjCuisines);
        
        expPlans.add(namePrj);
        expPlans.add(cuisinesPrj);
        
        innerPlan.add(generate);
        innerPlan.connect(innerLoad, generate);
        innerPlan.connect(innerLoad2, generate);
        
        foreach.setInnerPlan(innerPlan);
        foreach.setAlias("B");
        plan.add(foreach);
        
        plan.connect(load, foreach);
        
        filter = new LOFilter(plan);
        LogicalExpressionPlan filterPlan = new LogicalExpressionPlan();
        ProjectExpression namePrj2 = new ProjectExpression(filterPlan, 0, 0, filter);
        filterPlan.add(namePrj2);
        ConstantExpression constExp = new ConstantExpression(filterPlan, "joe", 
                new LogicalFieldSchema(null, null, DataType.CHARARRAY));
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
        optimizer.addPlanTransformListener(new SchemaPatcher());
        optimizer.addPlanTransformListener(new ProjectionPatcher());
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
        
        assertEquals( load.getSchema().getField(0).uid , namePrj2.getFieldSchema().uid );
        assertEquals( namePrj2.getFieldSchema().uid, prjName.getFieldSchema().uid );
        
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
}
