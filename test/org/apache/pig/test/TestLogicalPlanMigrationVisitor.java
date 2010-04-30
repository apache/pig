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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.pig.ExecType;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.experimental.logical.LogicalPlanMigrationVistor;
import org.apache.pig.experimental.logical.expression.AndExpression;
import org.apache.pig.experimental.logical.expression.CastExpression;
import org.apache.pig.experimental.logical.expression.ConstantExpression;
import org.apache.pig.experimental.logical.expression.EqualExpression;
import org.apache.pig.experimental.logical.expression.LogicalExpression;
import org.apache.pig.experimental.logical.expression.LogicalExpressionPlan;
import org.apache.pig.experimental.logical.expression.ProjectExpression;
import org.apache.pig.experimental.logical.optimizer.UidStamper;
import org.apache.pig.experimental.logical.relational.LOCogroup;
import org.apache.pig.experimental.logical.relational.LOForEach;
import org.apache.pig.experimental.logical.relational.LOGenerate;
import org.apache.pig.experimental.logical.relational.LOInnerLoad;
import org.apache.pig.experimental.logical.relational.LOJoin;
import org.apache.pig.experimental.logical.relational.LOLoad;
import org.apache.pig.experimental.logical.relational.LOStore;
import org.apache.pig.experimental.logical.relational.LogicalRelationalOperator;
import org.apache.pig.experimental.logical.relational.LogicalSchema;
import org.apache.pig.experimental.logical.relational.LogicalSchema.LogicalFieldSchema;
import org.apache.pig.experimental.plan.Operator;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.test.utils.LogicalPlanTester;

import junit.framework.TestCase;

public class TestLogicalPlanMigrationVisitor extends TestCase {

    PigContext pc = new PigContext(ExecType.LOCAL, new Properties());
    public void testSimplePlan() throws Exception {
        LogicalPlanTester lpt = new LogicalPlanTester(pc);
        lpt.buildPlan("a = load 'd.txt';");
        lpt.buildPlan("b = filter a by $0==NULL;");        
        LogicalPlan plan = lpt.buildPlan("store b into 'empty';");
        
        // check basics
        org.apache.pig.experimental.logical.relational.LogicalPlan newPlan = migratePlan(plan);
        assertEquals(3, newPlan.size());
        assertEquals(newPlan.getSources().size(), 1);
        
        // check load
        LogicalRelationalOperator op = (LogicalRelationalOperator)newPlan.getSources().get(0);
        assertEquals(op.getClass(), org.apache.pig.experimental.logical.relational.LOLoad.class);
        
        // check filter
        op = (LogicalRelationalOperator)newPlan.getSuccessors(op).get(0);
        assertEquals(op.getClass(), org.apache.pig.experimental.logical.relational.LOFilter.class);
        
        LogicalExpressionPlan exp = ((org.apache.pig.experimental.logical.relational.LOFilter)op).getFilterPlan();
        
        EqualExpression eq = (EqualExpression)exp.getSources().get(0);
        assertEquals(eq.getLhs().getClass(), ProjectExpression.class);
        assertEquals(((ProjectExpression)eq.getLhs()).getColNum(), 0);
        assertEquals(((ProjectExpression)eq.getLhs()).getInputNum(), 0);
        
        assertEquals(eq.getRhs().getClass(), ConstantExpression.class);
        
        // check store
        op = (LogicalRelationalOperator)newPlan.getSuccessors(op).get(0);
        assertEquals(op.getClass(), org.apache.pig.experimental.logical.relational.LOStore.class);
    }
    
    public void testPlanWithCast() throws Exception {
        LogicalPlanTester lpt = new LogicalPlanTester(pc);
        lpt.buildPlan("a = load 'd.txt' as (id, c);");
        lpt.buildPlan("b = filter a by (int)id==10;");        
        LogicalPlan plan = lpt.buildPlan("store b into 'empty';");
        
        // check basics
        org.apache.pig.experimental.logical.relational.LogicalPlan newPlan = migratePlan(plan);
        assertEquals(3, newPlan.size());
        assertEquals(newPlan.getSources().size(), 1);
        
        // check load
        LogicalRelationalOperator op = (LogicalRelationalOperator)newPlan.getSources().get(0);
        assertEquals(op.getClass(), org.apache.pig.experimental.logical.relational.LOLoad.class);
        
        // check filter
        op = (LogicalRelationalOperator)newPlan.getSuccessors(op).get(0);
        assertEquals(op.getClass(), org.apache.pig.experimental.logical.relational.LOFilter.class);
        
        LogicalExpressionPlan exp = ((org.apache.pig.experimental.logical.relational.LOFilter)op).getFilterPlan();
        
        EqualExpression eq = (EqualExpression)exp.getSources().get(0);
        assertEquals(eq.getLhs().getClass(), CastExpression.class);
                
        assertEquals(eq.getLhs().getClass(), CastExpression.class);
        LogicalExpression ep = (LogicalExpression)exp.getSuccessors(eq.getLhs()).get(0);
        assertEquals(ep.getClass(), ProjectExpression.class);
        assertEquals(((ProjectExpression)ep).getColNum(), 0);
        assertEquals(((ProjectExpression)ep).getInputNum(), 0);
        
        assertEquals(eq.getRhs().getClass(), ConstantExpression.class);
        
        // check store
        op = (LogicalRelationalOperator)newPlan.getSuccessors(op).get(0);
        assertEquals(op.getClass(), org.apache.pig.experimental.logical.relational.LOStore.class);
    }
    
    public void testJoinPlan() throws Exception {
        LogicalPlanTester lpt = new LogicalPlanTester(pc);
        lpt.buildPlan("a = load 'd1.txt' as (id, c);");
        lpt.buildPlan("b = load 'd2.txt'as (id, c);");
        lpt.buildPlan("c = join a by id, b by c;");
        lpt.buildPlan("d = filter c by a::id==NULL AND b::c==NULL;");        
        LogicalPlan plan = lpt.buildPlan("store d into 'empty';");
      
        // check basics
        org.apache.pig.experimental.logical.relational.LogicalPlan newPlan = migratePlan(plan);
        assertEquals(5, newPlan.size());
        assertEquals(newPlan.getSources().size(), 2);
       
        // check load and join
        LogicalRelationalOperator op = (LogicalRelationalOperator)newPlan.getSuccessors(newPlan.getSources().get(0)).get(0);
        assertEquals(op.getClass(), org.apache.pig.experimental.logical.relational.LOJoin.class);
        assertEquals(((LOJoin)op).getJoinType(), LOJoin.JOINTYPE.HASH);
        
        LogicalRelationalOperator l1 = (LogicalRelationalOperator)newPlan.getPredecessors(op).get(0);
        assertEquals(l1.getClass(), org.apache.pig.experimental.logical.relational.LOLoad.class);
        assertEquals(l1.getAlias(), "a");
        
        LogicalRelationalOperator l2 = (LogicalRelationalOperator)newPlan.getPredecessors(op).get(1);
        assertEquals(l2.getClass(), org.apache.pig.experimental.logical.relational.LOLoad.class);
        assertEquals(l2.getAlias(), "b");

        // check join input plans
        LogicalExpressionPlan p1 = ((LOJoin)op).getJoinPlan(0).iterator().next();
        assertEquals(p1.size(), 1);
        
        ProjectExpression prj = (ProjectExpression)p1.getSources().get(0);
       
        assertEquals(prj.getInputNum(), 0);
        assertEquals(prj.getColNum(), 0);
        
        LogicalExpressionPlan p2 = ((LOJoin)op).getJoinPlan(1).iterator().next();
        assertEquals(p2.size(), 1);
        
        prj = (ProjectExpression)p2.getSources().get(0);
     
        assertEquals(prj.getInputNum(), 1);
        assertEquals(prj.getColNum(), 1);
        
        // check filter
        op = (LogicalRelationalOperator)newPlan.getSuccessors(op).get(0);
        assertEquals(op.getClass(), org.apache.pig.experimental.logical.relational.LOFilter.class);        
        LogicalExpressionPlan exp = ((org.apache.pig.experimental.logical.relational.LOFilter)op).getFilterPlan();
        
        AndExpression ae = (AndExpression)exp.getSources().get(0);
        
        EqualExpression eq = (EqualExpression)exp.getSuccessors(ae).get(0);
        assertEquals(eq.getLhs().getClass(), ProjectExpression.class);
        assertEquals(((ProjectExpression)eq.getLhs()).getColNum(), 0);
        assertEquals(((ProjectExpression)eq.getLhs()).getInputNum(), 0);
        
        assertEquals(eq.getRhs().getClass(), ConstantExpression.class);
        
        eq = (EqualExpression)exp.getSuccessors(ae).get(1);
        assertEquals(eq.getLhs().getClass(), ProjectExpression.class);
        assertEquals(((ProjectExpression)eq.getLhs()).getColNum(), 3);
        assertEquals(((ProjectExpression)eq.getLhs()).getInputNum(), 0);
        
        assertEquals(eq.getRhs().getClass(), ConstantExpression.class);
        
        // check store
        op = (LogicalRelationalOperator)newPlan.getSuccessors(op).get(0);
        assertEquals(op.getClass(), org.apache.pig.experimental.logical.relational.LOStore.class); 
    }
    
    public void testForeachPlan() throws Exception {
        LogicalPlanTester lpt = new LogicalPlanTester(pc);
        lpt.buildPlan("a = load '/test/d.txt' as (id, d);");
        lpt.buildPlan("b = foreach a generate id, FLATTEN(d);");        
        LogicalPlan plan = lpt.buildPlan("store b into '/test/empty';");
        
        // check basics
        org.apache.pig.experimental.logical.relational.LogicalPlan newPlan = migratePlan(plan);
        
        org.apache.pig.experimental.logical.relational.LogicalPlan expected = 
            new org.apache.pig.experimental.logical.relational.LogicalPlan();
        
        LogicalSchema aschema = new LogicalSchema();    	
        aschema.addField(new LogicalSchema.LogicalFieldSchema("id", null, DataType.BYTEARRAY));
        aschema.addField(new LogicalSchema.LogicalFieldSchema("d", null, DataType.BYTEARRAY));
        LOLoad load = new LOLoad(new FileSpec("/test/d.txt", new FuncSpec("org.apache.pig.builtin.PigStorage")), aschema, expected);
        expected.add(load);
        
        LOForEach foreach = new LOForEach(expected);
        org.apache.pig.experimental.logical.relational.LogicalPlan innerPlan = new org.apache.pig.experimental.logical.relational.LogicalPlan();
        LOInnerLoad l1 = new LOInnerLoad(innerPlan, foreach, 0);
        innerPlan.add(l1);
        LOInnerLoad l2 = new LOInnerLoad(innerPlan, foreach, 1);
        
        List<LogicalExpressionPlan> eps = new ArrayList<LogicalExpressionPlan>();
        LogicalExpressionPlan p1 = new LogicalExpressionPlan();
        p1.add(new ProjectExpression(p1, DataType.BYTEARRAY, 0, 0));
        LogicalExpressionPlan p2 = new LogicalExpressionPlan();
        p2.add(new ProjectExpression(p2, DataType.BYTEARRAY, 1, 0));
        eps.add(p1);
        eps.add(p2);
        
        LOGenerate gen = new LOGenerate(innerPlan, eps, new boolean[] {false, true});
        innerPlan.add(gen);
        innerPlan.connect(l1, gen);
        innerPlan.connect(l2, gen);
        
        foreach.setInnerPlan(innerPlan);    	
        expected.add(foreach);
        
        LOStore s = new LOStore(expected, new FileSpec("/test/empty", new FuncSpec("org.apache.pig.builtin.PigStorage")));
      
        expected.add(s);
        
        expected.connect(load, foreach);
        expected.connect(foreach, s);
        
        try {
            UidStamper stamper = new UidStamper(expected);
            stamper.visit();         
        }catch(Exception e) {
            throw new VisitorException(e);
        }

        assertTrue(expected.isEqual(newPlan));
        
        LogicalSchema schema = foreach.getSchema();
        aschema = new LogicalSchema();
        aschema.addField(new LogicalSchema.LogicalFieldSchema("id", null, DataType.BYTEARRAY));
        aschema.addField(new LogicalSchema.LogicalFieldSchema("d", null, DataType.BYTEARRAY));
        assertTrue(schema.isEqual(aschema));
    }

    public void testForeachSchema() throws Exception {
        // test flatten
        LogicalPlanTester lpt = new LogicalPlanTester(pc);
        lpt.buildPlan("a = load '/test/d.txt' as (id, d:tuple(v, s));");
        LogicalPlan plan = lpt.buildPlan("b = foreach a generate id, FLATTEN(d);");  
                
        org.apache.pig.experimental.logical.relational.LogicalPlan newPlan = migratePlan(plan);
        LogicalRelationalOperator op = (LogicalRelationalOperator)newPlan.getSinks().get(0);
        
        LogicalSchema s2 = new LogicalSchema();
        s2.addField(new LogicalSchema.LogicalFieldSchema("id", null, DataType.BYTEARRAY));
        s2.addField(new LogicalSchema.LogicalFieldSchema("v", null, DataType.BYTEARRAY));
        s2.addField(new LogicalSchema.LogicalFieldSchema("s", null, DataType.BYTEARRAY));
        assertTrue(s2.isEqual(op.getSchema()));
        
        // test no flatten
        lpt = new LogicalPlanTester(pc);
        lpt.buildPlan("a = load '/test/d.txt' as (id, d:bag{t:(v, s)});");
        plan = lpt.buildPlan("b = foreach a generate id, d;");  
                
        newPlan = migratePlan(plan);
        op = (LogicalRelationalOperator)newPlan.getSinks().get(0);
        
        LogicalSchema aschema = new LogicalSchema();    	
        aschema.addField(new LogicalSchema.LogicalFieldSchema("id", null, DataType.BYTEARRAY));
        LogicalSchema aschema2 = new LogicalSchema();
        LogicalSchema aschema3 = new LogicalSchema();
        aschema3.addField(new LogicalSchema.LogicalFieldSchema("v", null, DataType.BYTEARRAY));
        aschema3.addField(new LogicalSchema.LogicalFieldSchema("s", null, DataType.BYTEARRAY));
        aschema2.addField(new LogicalSchema.LogicalFieldSchema("t", aschema3, DataType.TUPLE));
        aschema.addField(new LogicalSchema.LogicalFieldSchema("d", aschema2, DataType.BAG));  
        
        assertTrue(aschema.isEqual(op.getSchema()));
        
        // check with defined data type
        lpt = new LogicalPlanTester(pc);
        lpt.buildPlan("a = load '/test/d.txt' as (id, d:bag{t:(v:int, s)});");
        lpt.buildPlan("b = foreach a generate id, FLATTEN(d);");        
        plan = lpt.buildPlan("store b into '/test/empty';");
                
        newPlan = migratePlan(plan);
        op = (LogicalRelationalOperator)newPlan.getSinks().get(0);
        op = (LogicalRelationalOperator)newPlan.getPredecessors(op).get(0);
        LogicalSchema schema = op.getSchema();
        
        aschema = new LogicalSchema();
        aschema.addField(new LogicalSchema.LogicalFieldSchema("id", null, DataType.BYTEARRAY));
        aschema.addField(new LogicalSchema.LogicalFieldSchema("v", null, DataType.INTEGER));
        aschema.addField(new LogicalSchema.LogicalFieldSchema("s", null, DataType.BYTEARRAY));
        assertTrue(schema.isEqual(aschema));
      
    
        // test with add
        lpt = new LogicalPlanTester(pc);
        lpt.buildPlan("a = load '/test/d.txt' as (id, v:int, s:int);");
        lpt.buildPlan("b = foreach a generate id, v+s;");        
        plan = lpt.buildPlan("store b into '/test/empty';");
        
        newPlan = migratePlan(plan);
        op = (LogicalRelationalOperator)newPlan.getSinks().get(0);
        op = (LogicalRelationalOperator)newPlan.getPredecessors(op).get(0);
        schema = op.getSchema();
        
        aschema = new LogicalSchema();
        aschema.addField(new LogicalSchema.LogicalFieldSchema("id", null, DataType.BYTEARRAY));
        aschema.addField(new LogicalSchema.LogicalFieldSchema(null, null, DataType.INTEGER));
        assertTrue(schema.isEqual(aschema));
    }       
    
    public void testForeachPlan2() throws Exception {
        LogicalPlanTester lpt = new LogicalPlanTester(pc);
        lpt.buildPlan("a = load '/test/d.txt' as (id, d:bag{t:(id:int, s)});");
        lpt.buildPlan("b = foreach a generate id, FLATTEN(d);");        
        LogicalPlan plan = lpt.buildPlan("store b into '/test/empty';");
        
        // check basics
        org.apache.pig.experimental.logical.relational.LogicalPlan newPlan = migratePlan(plan);
        
        org.apache.pig.experimental.logical.relational.LogicalPlan expected = 
            new org.apache.pig.experimental.logical.relational.LogicalPlan();
        
        LogicalSchema aschema = new LogicalSchema();    	
        aschema.addField(new LogicalSchema.LogicalFieldSchema("id", null, DataType.BYTEARRAY));
        LogicalSchema aschema2 = new LogicalSchema();
        LogicalSchema aschema3 = new LogicalSchema();
        aschema3.addField(new LogicalSchema.LogicalFieldSchema("id", null, DataType.INTEGER));
        aschema3.addField(new LogicalSchema.LogicalFieldSchema("s", null, DataType.BYTEARRAY));
        aschema2.addField(new LogicalSchema.LogicalFieldSchema("t", aschema3, DataType.TUPLE));
        aschema.addField(new LogicalSchema.LogicalFieldSchema("d", aschema2, DataType.BAG));        
        
        LOLoad load = new LOLoad(new FileSpec("/test/d.txt", new FuncSpec("org.apache.pig.builtin.PigStorage")), aschema, expected);
        expected.add(load);         
        
        LOForEach foreach2 = new LOForEach(expected);
        org.apache.pig.experimental.logical.relational.LogicalPlan innerPlan = new org.apache.pig.experimental.logical.relational.LogicalPlan();
        LOInnerLoad l1 = new LOInnerLoad(innerPlan, foreach2, 0);
        innerPlan.add(l1);
        LOInnerLoad l2 = new LOInnerLoad(innerPlan, foreach2, 1);
        
        List<LogicalExpressionPlan>  eps = new ArrayList<LogicalExpressionPlan>();
        LogicalExpressionPlan p1 = new LogicalExpressionPlan();
        new ProjectExpression(p1, DataType.BYTEARRAY, 0, 0);
        LogicalExpressionPlan p2 = new LogicalExpressionPlan();        
        new ProjectExpression(p2, DataType.BAG, 1, 0);
        eps.add(p1);
        eps.add(p2);
        
        LOGenerate gen = new LOGenerate(innerPlan, eps, new boolean[] {false, true});
        innerPlan.add(gen);
        innerPlan.connect(l1, gen);
        innerPlan.connect(l2, gen);
        
        foreach2.setInnerPlan(innerPlan);    	
        expected.add(foreach2); 
                
        LOStore s = new LOStore(expected, new FileSpec("/test/empty", new FuncSpec("org.apache.pig.builtin.PigStorage")));
      
        expected.add(s);
        
        expected.connect(load, foreach2);
    
        expected.connect(foreach2, s);
        try {
            UidStamper stamper = new UidStamper(expected);
            stamper.visit();         
        }catch(Exception e) {
            throw new VisitorException(e);
        }
        
        assertTrue(expected.isEqual(newPlan));
        
        LogicalSchema schema = foreach2.getSchema();
        aschema = new LogicalSchema();
        aschema.addField(new LogicalSchema.LogicalFieldSchema("id", null, DataType.BYTEARRAY));
        aschema.addField(new LogicalSchema.LogicalFieldSchema("d::id", null, DataType.INTEGER));
        aschema.addField(new LogicalSchema.LogicalFieldSchema("s", null, DataType.BYTEARRAY));
        assertTrue(schema.isEqual(aschema));
        assertTrue(schema.getField("id")==schema.getField(0));
        assertTrue(schema.getField("d::id")==schema.getField(1));
    }
    
    public void testCoGroup() throws Exception {
        LogicalPlanTester lpt = new LogicalPlanTester(pc);
        lpt.buildPlan("a = load '/test/d.txt' as (name:chararray, age:int, gpa:float);");
        lpt.buildPlan("b = group a by name;");        
        LogicalPlan plan = lpt.buildPlan("store b into '/test/empty';");
        
        // check basics
        org.apache.pig.experimental.logical.relational.LogicalPlan newPlan = migratePlan(plan);
        
        LogicalSchema loadSchema = 
            ((LogicalRelationalOperator)newPlan.getSources().get(0)).getSchema();
        
        Set<Long> uids = getAllUids(loadSchema);
        
        LogicalRelationalOperator op = (LogicalRelationalOperator) 
            newPlan.getSuccessors( newPlan.getSources().get(0) ).get(0);
        assertEquals( LOCogroup.class, op.getClass() );
        LogicalSchema schema = op.getSchema();
        
        assertEquals( 2, schema.size() );
        assertEquals( DataType.CHARARRAY, schema.getField(0).type );
        assertEquals( false, uids.contains( schema.getField(0).uid ) );
        assertEquals( 0, schema.getField(0).alias.compareTo("group") );
        
        
        assertEquals( DataType.BAG, schema.getField(1).type );
        
        assertEquals( DataType.CHARARRAY, schema.getField(1).schema.getField(0).type );
        assertEquals( 0, schema.getField(1).schema.getField(0).alias.compareTo("name") );
        assertEquals( loadSchema.getField(0).uid, schema.getField(1).schema.getField(0).uid );
        assertEquals( DataType.INTEGER, schema.getField(1).schema.getField(1).type );
        assertEquals( 0, schema.getField(1).schema.getField(1).alias.compareTo("age") );
        assertEquals( loadSchema.getField(1).uid, schema.getField(1).schema.getField(1).uid );
        assertEquals( DataType.FLOAT, schema.getField(1).schema.getField(2).type );
        assertEquals( 0, schema.getField(1).schema.getField(2).alias.compareTo("gpa") );
        assertEquals( loadSchema.getField(2).uid, schema.getField(1).schema.getField(2).uid );
        
        uids.add(Long.valueOf( schema.getField(0).uid ) );
        assertEquals( false, uids.contains( schema.getField(1).uid ) );
        
        assertEquals( LOCogroup.class, newPlan.getSuccessors(newPlan.getSources().get(0)).get(0).getClass() );
        LOCogroup cogroup = (LOCogroup) newPlan.getSuccessors(newPlan.getSources().get(0)).get(0);
        
        MultiMap<Integer, LogicalExpressionPlan> expressionPlans = cogroup.getExpressionPlans();
        assertEquals( 1, expressionPlans.size() );
        List<LogicalExpressionPlan> plans = (List<LogicalExpressionPlan>) expressionPlans.get(Integer.valueOf(0));
        assertEquals( 1, plans.size() );
        
        LogicalExpressionPlan exprPlan = plans.get(0);
        assertEquals( 1, exprPlan.getSinks().size() );
        assertEquals( ProjectExpression.class, exprPlan.getSinks().get(0).getClass() );
        ProjectExpression prj = (ProjectExpression) exprPlan.getSinks().get(0);
        assertEquals( loadSchema.getField(0).uid, prj.getUid() );
        assertEquals( 0, prj.getColNum() );
        assertEquals( 0, prj.getInputNum() );
    }
    
    public void testCoGroup2() throws Exception {
        LogicalPlanTester lpt = new LogicalPlanTester(pc);
        lpt.buildPlan("a = load '/test/d.txt' as (name:chararray, age:int, gpa:float);");
        lpt.buildPlan("b = group a by ( name, age );");
        LogicalPlan plan = lpt.buildPlan("store b into '/test/empty';");
        
        // check basics
        org.apache.pig.experimental.logical.relational.LogicalPlan newPlan = migratePlan(plan);

        LogicalSchema loadSchema = 
            ((LogicalRelationalOperator)newPlan.getSources().get(0)).getSchema();
        
        Set<Long> uids = getAllUids(loadSchema);
        
        LogicalRelationalOperator op = (LogicalRelationalOperator) 
            newPlan.getSuccessors( newPlan.getSources().get(0) ).get(0);
        assertEquals( LOCogroup.class, op.getClass() );
        LogicalSchema schema = op.getSchema();
        
        assertEquals( 2, schema.size() );
        assertEquals( DataType.TUPLE, schema.getField(0).type );
        assertEquals( false, uids.contains( schema.getField(0).uid ) );
        assertEquals( 0, schema.getField(0).alias.compareTo("group") );
        assertEquals( DataType.CHARARRAY, schema.getField(0).schema.getField(0).type );        
        assertEquals( DataType.INTEGER, schema.getField(0).schema.getField(1).type );
        
        assertEquals( DataType.BAG, schema.getField(1).type );
        
        assertEquals( DataType.CHARARRAY, schema.getField(1).schema.getField(0).type );
        assertEquals( 0, schema.getField(1).schema.getField(0).alias.compareTo("name") );
        assertEquals( loadSchema.getField(0).uid, schema.getField(1).schema.getField(0).uid );
        assertEquals( DataType.INTEGER, schema.getField(1).schema.getField(1).type );
        assertEquals( 0, schema.getField(1).schema.getField(1).alias.compareTo("age") );
        assertEquals( loadSchema.getField(1).uid, schema.getField(1).schema.getField(1).uid );
        assertEquals( DataType.FLOAT, schema.getField(1).schema.getField(2).type );
        assertEquals( 0, schema.getField(1).schema.getField(2).alias.compareTo("gpa") );
        assertEquals( loadSchema.getField(2).uid, schema.getField(1).schema.getField(2).uid );
        
        
        // We are doing Uid tests at the end as the uids should not repeat
        uids.add(Long.valueOf( schema.getField(0).uid ) );
        assertEquals( false, uids.contains( schema.getField(0).schema.getField(0).uid ) );
        uids.add( Long.valueOf( schema.getField(0).schema.getField(0).uid ) );
        assertEquals( false, uids.contains( schema.getField(0).schema.getField(1).uid ) );
        uids.add( Long.valueOf( schema.getField(0).schema.getField(1).uid ) );        
        assertEquals( false, uids.contains( schema.getField(1).uid ) );
        
        assertEquals( LOCogroup.class, newPlan.getSuccessors(newPlan.getSources().get(0)).get(0).getClass() );
        LOCogroup cogroup = (LOCogroup) newPlan.getSuccessors(newPlan.getSources().get(0)).get(0);
        
        MultiMap<Integer, LogicalExpressionPlan> expressionPlans = cogroup.getExpressionPlans();
        assertEquals( 1, expressionPlans.size() );
        List<LogicalExpressionPlan> plans = (List<LogicalExpressionPlan>) expressionPlans.get(Integer.valueOf(0));
        assertEquals( 2, plans.size() );
        
        LogicalExpressionPlan exprPlan = plans.get(0);
        assertEquals( 1, exprPlan.getSinks().size() );
        assertEquals( ProjectExpression.class, exprPlan.getSinks().get(0).getClass() );
        ProjectExpression prj = (ProjectExpression) exprPlan.getSinks().get(0);
        assertEquals( loadSchema.getField(0).uid, prj.getUid() );
        assertEquals( 0, prj.getColNum() );
        assertEquals( 0, prj.getInputNum() );
        
        LogicalExpressionPlan exprPlan2 = plans.get(1);
        assertEquals( 1, exprPlan2.getSinks().size() );
        assertEquals( ProjectExpression.class, exprPlan2.getSinks().get(0).getClass() );
        ProjectExpression prj2 = (ProjectExpression) exprPlan2.getSinks().get(0);
        assertEquals( loadSchema.getField(1).uid, prj2.getUid() );
        assertEquals( 1, prj2.getColNum() );
        assertEquals( 0, prj2.getInputNum() );
    }
    
    public void testCoGroup3() throws Exception {
        LogicalPlanTester lpt = new LogicalPlanTester(pc);
        lpt.buildPlan("a = load '/test/d.txt' as (name:chararray, age:int, gpa:float);");
        lpt.buildPlan("b = load '/test/e.txt' as (name:chararray, blah:chararray );");
        lpt.buildPlan("c = group a by name, b by name;");
        LogicalPlan plan = lpt.buildPlan("store c into '/test/empty';");
        
        // check basics
        org.apache.pig.experimental.logical.relational.LogicalPlan newPlan = migratePlan(plan);
        
        assertEquals( LOCogroup.class, newPlan.getSuccessors( newPlan.getSources().get(0) ).get(0).getClass() );
        LOCogroup cogroup = (LOCogroup) newPlan.getSuccessors( newPlan.getSources().get(0) ).get(0);
        
        // Reason for this strange way of getting the load schema is to maintain the sequence correctly        
        LogicalSchema loadSchema = 
            ((LogicalRelationalOperator)newPlan.getPredecessors(cogroup).get(0)).getSchema();
        
        LogicalSchema load2Schema = 
            ((LogicalRelationalOperator)newPlan.getPredecessors(cogroup).get(1)).getSchema();
        
        Set<Long> uids = getAllUids(loadSchema);
        uids.addAll( getAllUids( load2Schema ) );
        
        LogicalRelationalOperator op = (LogicalRelationalOperator) 
            newPlan.getSuccessors( newPlan.getSources().get(0) ).get(0);
        assertEquals( LOCogroup.class, op.getClass() );
        LogicalSchema schema = op.getSchema();
        
        assertEquals( 3, schema.size() );
        assertEquals( DataType.CHARARRAY, schema.getField(0).type );
        assertEquals( false, uids.contains( schema.getField(0).uid ) );
        assertEquals( 0, schema.getField(0).alias.compareTo("group") );

        assertEquals( DataType.BAG, schema.getField(1).type );
        
        assertEquals( DataType.CHARARRAY, schema.getField(1).schema.getField(0).type );
        assertEquals( 0, schema.getField(1).schema.getField(0).alias.compareTo("name") );
        assertEquals( loadSchema.getField(0).uid, schema.getField(1).schema.getField(0).uid );
        assertEquals( DataType.INTEGER, schema.getField(1).schema.getField(1).type );
        assertEquals( 0, schema.getField(1).schema.getField(1).alias.compareTo("age") );
        assertEquals( loadSchema.getField(1).uid, schema.getField(1).schema.getField(1).uid );
        assertEquals( DataType.FLOAT, schema.getField(1).schema.getField(2).type );
        assertEquals( 0, schema.getField(1).schema.getField(2).alias.compareTo("gpa") );
        assertEquals( loadSchema.getField(2).uid, schema.getField(1).schema.getField(2).uid );
        
        assertEquals( DataType.BAG, schema.getField(2).type );
        
        assertEquals( DataType.CHARARRAY, schema.getField(2).schema.getField(0).type );
        assertEquals( 0, schema.getField(2).schema.getField(0).alias.compareTo("name") );
        assertEquals( load2Schema.getField(0).uid, schema.getField(2).schema.getField(0).uid );
        assertEquals( DataType.CHARARRAY, schema.getField(2).schema.getField(1).type );
        assertEquals( 0, schema.getField(2).schema.getField(1).alias.compareTo("blah") );
        assertEquals( load2Schema.getField(1).uid, schema.getField(2).schema.getField(1).uid );        
        
        
        // We are doing Uid tests at the end as the uids should not repeat                
        assertEquals( false, uids.contains( schema.getField(1).uid ) );
        uids.add( schema.getField(1).uid );
        assertEquals( false, uids.contains( schema.getField(2).uid) );
        
        MultiMap<Integer, LogicalExpressionPlan> expressionPlans = cogroup.getExpressionPlans();
        assertEquals( 2, expressionPlans.size() );
        List<LogicalExpressionPlan> plans = (List<LogicalExpressionPlan>) expressionPlans.get(Integer.valueOf(0));
        assertEquals( 1, plans.size() );
        
        List<LogicalExpressionPlan> plans2 = (List<LogicalExpressionPlan>) expressionPlans.get(Integer.valueOf(1));
        assertEquals( 1, plans2.size() );
        
        LogicalExpressionPlan exprPlan = plans.get(0);
        assertEquals( 1, exprPlan.getSinks().size() );
        assertEquals( ProjectExpression.class, exprPlan.getSinks().get(0).getClass() );
        ProjectExpression prj = (ProjectExpression) exprPlan.getSinks().get(0);
        assertEquals( loadSchema.getField(0).uid, prj.getUid() );
        assertEquals( 0, prj.getColNum() );
        assertEquals( 0, prj.getInputNum() );
        
        LogicalExpressionPlan exprPlan2 = plans2.get(0);
        assertEquals( 1, exprPlan2.getSinks().size() );
        assertEquals( ProjectExpression.class, exprPlan2.getSinks().get(0).getClass() );
        ProjectExpression prj2 = (ProjectExpression) exprPlan2.getSinks().get(0);
        assertEquals( load2Schema.getField(0).uid, prj2.getUid() );
        assertEquals( 0, prj2.getColNum() );
        assertEquals( 1, prj2.getInputNum() );
    }
    
    public void testCoGroup4() throws Exception {
        LogicalPlanTester lpt = new LogicalPlanTester(pc);
        lpt.buildPlan("a = load '/test/d.txt' as (name:chararray, age:int, gpa:float);");
        lpt.buildPlan("b = load '/test/e.txt' as (name:chararray, age:int, blah:chararray );");
        lpt.buildPlan("c = group a by ( name, age ), b by ( name, age );");
        LogicalPlan plan = lpt.buildPlan("store c into '/test/empty';");
        
        // check basics
        org.apache.pig.experimental.logical.relational.LogicalPlan newPlan = migratePlan(plan);
        
        assertEquals( LOCogroup.class, newPlan.getSuccessors( newPlan.getSources().get(0) ).get(0).getClass() );
        LOCogroup cogroup = (LOCogroup) newPlan.getSuccessors( newPlan.getSources().get(0) ).get(0);
        
        // Reason for this strange way of getting the load schema is to maintain the sequence correctly        
        LogicalSchema loadSchema = 
            ((LogicalRelationalOperator)newPlan.getPredecessors(cogroup).get(0)).getSchema();
        
        LogicalSchema load2Schema = 
            ((LogicalRelationalOperator)newPlan.getPredecessors(cogroup).get(1)).getSchema();
        
        Set<Long> uids = getAllUids(loadSchema);
        uids.addAll( getAllUids( load2Schema ) );
        
        LogicalRelationalOperator op = (LogicalRelationalOperator) 
            newPlan.getSuccessors( newPlan.getSources().get(0) ).get(0);
        assertEquals( LOCogroup.class, op.getClass() );
        LogicalSchema schema = op.getSchema();
        
        assertEquals( 3, schema.size() );
        assertEquals( DataType.TUPLE, schema.getField(0).type );
        assertEquals( false, uids.contains( schema.getField(0).uid ) );
        assertEquals( 0, schema.getField(0).alias.compareTo("group") );
        assertEquals( DataType.CHARARRAY, schema.getField(0).schema.getField(0).type );
        assertEquals( 0, schema.getField(0).schema.getField(0).alias.compareTo("name") );
        assertEquals( DataType.INTEGER, schema.getField(0).schema.getField(1).type );
        assertEquals( 0, schema.getField(0).schema.getField(1).alias.compareTo("age") );                
               

        assertEquals( DataType.BAG, schema.getField(1).type );
        
        assertEquals( DataType.CHARARRAY, schema.getField(1).schema.getField(0).type );
        assertEquals( 0, schema.getField(1).schema.getField(0).alias.compareTo("name") );
        assertEquals( loadSchema.getField(0).uid, schema.getField(1).schema.getField(0).uid );
        assertEquals( DataType.INTEGER, schema.getField(1).schema.getField(1).type );
        assertEquals( 0, schema.getField(1).schema.getField(1).alias.compareTo("age") );
        assertEquals( loadSchema.getField(1).uid, schema.getField(1).schema.getField(1).uid );
        assertEquals( DataType.FLOAT, schema.getField(1).schema.getField(2).type );
        assertEquals( 0, schema.getField(1).schema.getField(2).alias.compareTo("gpa") );
        assertEquals( loadSchema.getField(2).uid, schema.getField(1).schema.getField(2).uid );
        
        assertEquals( DataType.BAG, schema.getField(2).type );
        
        assertEquals( DataType.CHARARRAY, schema.getField(2).schema.getField(0).type );
        assertEquals( 0, schema.getField(2).schema.getField(0).alias.compareTo("name") );
        assertEquals( load2Schema.getField(0).uid, schema.getField(2).schema.getField(0).uid );
        assertEquals( DataType.INTEGER, schema.getField(2).schema.getField(1).type );
        assertEquals( 0, schema.getField(2).schema.getField(1).alias.compareTo("age") );
        assertEquals( load2Schema.getField(1).uid, schema.getField(2).schema.getField(1).uid );
        assertEquals( DataType.CHARARRAY, schema.getField(2).schema.getField(2).type );
        assertEquals( 0, schema.getField(2).schema.getField(2).alias.compareTo("blah") );
        assertEquals( load2Schema.getField(2).uid, schema.getField(2).schema.getField(2).uid );        
        
        
        // We are doing Uid tests at the end as the uids should not repeat
        assertEquals( false, uids.contains( schema.getField(0).schema.getField(0).uid ) );
        assertEquals( false, uids.contains( schema.getField(0).schema.getField(1).uid ) );
        assertEquals( false, uids.contains( schema.getField(1).uid ) );
        uids.add( schema.getField(1).uid );
        assertEquals( false, uids.contains( schema.getField(2).uid) );
        
        
        MultiMap<Integer, LogicalExpressionPlan> expressionPlans = cogroup.getExpressionPlans();
        assertEquals( 2, expressionPlans.size() );
        List<LogicalExpressionPlan> plans = (List<LogicalExpressionPlan>) expressionPlans.get(Integer.valueOf(0));
        assertEquals( 2, plans.size() );
        
        List<LogicalExpressionPlan> plans2 = (List<LogicalExpressionPlan>) expressionPlans.get(Integer.valueOf(1));
        assertEquals( 2, plans2.size() );
        
        LogicalExpressionPlan exprPlan = plans.get(0);
        assertEquals( 1, exprPlan.getSinks().size() );
        assertEquals( ProjectExpression.class, exprPlan.getSinks().get(0).getClass() );
        ProjectExpression prj = (ProjectExpression) exprPlan.getSinks().get(0);
        assertEquals( loadSchema.getField(0).uid, prj.getUid() );
        assertEquals( 0, prj.getColNum() );
        assertEquals( 0, prj.getInputNum() );
        
        LogicalExpressionPlan exprPlan2 = plans.get(1);
        assertEquals( 1, exprPlan2.getSinks().size() );
        assertEquals( ProjectExpression.class, exprPlan2.getSinks().get(0).getClass() );
        ProjectExpression prj2 = (ProjectExpression) exprPlan2.getSinks().get(0);
        assertEquals( loadSchema.getField(1).uid, prj2.getUid() );
        assertEquals( 1, prj2.getColNum() );
        assertEquals( 0, prj2.getInputNum() );        
        
        LogicalExpressionPlan exprPlan3 = plans2.get(0);
        assertEquals( 1, exprPlan3.getSinks().size() );
        assertEquals( ProjectExpression.class, exprPlan3.getSinks().get(0).getClass() );
        ProjectExpression prj3 = (ProjectExpression) exprPlan3.getSinks().get(0);
        assertEquals( load2Schema.getField(0).uid, prj3.getUid() );
        assertEquals( 0, prj3.getColNum() );
        assertEquals( 1, prj3.getInputNum() );
        
        LogicalExpressionPlan exprPlan4 = plans2.get(1);
        assertEquals( 1, exprPlan4.getSinks().size() );
        assertEquals( ProjectExpression.class, exprPlan4.getSinks().get(0).getClass() );
        ProjectExpression prj4 = (ProjectExpression) exprPlan4.getSinks().get(0);
        assertEquals( load2Schema.getField(1).uid, prj4.getUid() );
        assertEquals( 1, prj4.getColNum() );
        assertEquals( 1, prj4.getInputNum() );
    }
    
    /**
     * Obtains all the uids from the schema
     * @param schema
     * @return Set of uids from this schema. Its a recursive call
     */
    private Set<Long> getAllUids( LogicalSchema schema ) {
        Set<Long> uids = new HashSet<Long>();
        
        if( schema != null ) {
            for( LogicalFieldSchema fieldSchema : schema.getFields() ) {
                if( ( fieldSchema.type == DataType.BAG || 
                        fieldSchema.type == DataType.TUPLE ) &&
                        fieldSchema.schema != null ) {
                    uids.addAll( getAllUids( fieldSchema.schema ) );
                } else {
                    uids.add( fieldSchema.uid );
                }
            }
        }
        return uids;
    }
    
    private org.apache.pig.experimental.logical.relational.LogicalPlan migratePlan(LogicalPlan lp) throws VisitorException{
        LogicalPlanMigrationVistor visitor = new LogicalPlanMigrationVistor(lp);    	
        visitor.visit();
        
        org.apache.pig.experimental.logical.relational.LogicalPlan newPlan = visitor.getNewLogicalPlan();
        try {
            UidStamper stamper = new UidStamper(newPlan);
            stamper.visit();
            
            return newPlan;
        }catch(Exception e) {
            throw new VisitorException(e);
        }
    }    
}
