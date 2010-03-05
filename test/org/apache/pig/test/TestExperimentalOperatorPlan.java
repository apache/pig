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

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.experimental.logical.expression.AndExpression;
import org.apache.pig.experimental.logical.expression.ConstantExpression;
import org.apache.pig.experimental.logical.expression.EqualExpression;
import org.apache.pig.experimental.logical.expression.LogicalExpressionPlan;
import org.apache.pig.experimental.logical.expression.LogicalExpressionVisitor;
import org.apache.pig.experimental.logical.expression.ProjectExpression;
import org.apache.pig.experimental.logical.relational.LOFilter;
import org.apache.pig.experimental.logical.relational.LOJoin;
import org.apache.pig.experimental.logical.relational.LOLoad;
import org.apache.pig.experimental.logical.relational.LogicalPlan;
import org.apache.pig.experimental.logical.relational.LogicalPlanVisitor;
import org.apache.pig.experimental.logical.relational.LogicalSchema;
import org.apache.pig.experimental.logical.relational.LOJoin.JOINTYPE;
import org.apache.pig.experimental.plan.BaseOperatorPlan;
import org.apache.pig.experimental.plan.DependencyOrderWalker;
import org.apache.pig.experimental.plan.DepthFirstWalker;
import org.apache.pig.experimental.plan.Operator;
import org.apache.pig.experimental.plan.OperatorPlan;
import org.apache.pig.experimental.plan.PlanEdge;
import org.apache.pig.experimental.plan.PlanVisitor;
import org.apache.pig.experimental.plan.PlanWalker;
import org.apache.pig.experimental.plan.ReverseDependencyOrderWalker;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.impl.util.Pair;
import org.junit.Test;

import junit.framework.TestCase;

public class TestExperimentalOperatorPlan extends TestCase {
    
    private static class SillyPlan extends BaseOperatorPlan {
        
        SillyPlan() {
            super();
        }
    }
    
    private static class SillyOperator extends Operator {
        private String name;
        
        SillyOperator(String n, SillyPlan p) {
            super(n, p);
            name = n;
        }

        @Override
        public void accept(PlanVisitor v) {
            if (v instanceof SillyVisitor) {
                ((SillyVisitor)v).visitSillyOperator(this);
            }
        }

        @Override
        public boolean isEqual(Operator operator) {
            return ( name.compareTo(operator.getName()) == 0 );
        }
    }
    
    private static class SillyVisitor extends PlanVisitor {
        
        StringBuffer buf;

        protected SillyVisitor(OperatorPlan plan, PlanWalker walker) {
            super(plan, walker);
            buf = new StringBuffer();
        }
        
        public void visitSillyOperator(SillyOperator so) {
            buf.append(so.getName());
        }
        
        public String getVisitPattern() {
            return buf.toString();
        }
        
    }
    
    // Tests for PlanEdge
    
    @Test
    public void testPlanEdgeInsert() {
        SillyPlan plan = new SillyPlan();
        SillyOperator fred = new SillyOperator("fred", plan);
        SillyOperator joe = new SillyOperator("joe", plan);
        PlanEdge edges = new PlanEdge();
        
        // Test initial entry
        edges.put(fred, joe, 0);
        Collection<Operator> c = edges.get(fred);
        assertEquals(1, c.size());
        Operator[] a = new Operator[1];
        Operator[] b = c.toArray(a);
        assertEquals(joe, b[0]);
        
        // Test entry with no position
        SillyOperator bob = new SillyOperator("bob", plan);
        edges.put(fred, bob);
        c = edges.get(fred);
        assertEquals(2, c.size());
        a = new Operator[2];
        b = c.toArray(a);
        assertEquals(joe, b[0]);
        assertEquals(bob, b[1]);
        
        // Test entry with position
        SillyOperator jill = new SillyOperator("jill", plan);
        edges.put(fred, jill, 1);
        c = edges.get(fred);
        assertEquals(3, c.size());
        a = new Operator[3];
        b = c.toArray(a);
        assertEquals(joe, b[0]);
        assertEquals(jill, b[1]);
        assertEquals(bob, b[2]);
    }
    
    // Test that entry with invalid position cannot be made.
    @Test
    public void testPlanEdgeInsertFirstIndexBad() {
        SillyPlan plan = new SillyPlan();
        SillyOperator fred = new SillyOperator("fred", plan);
        SillyOperator joe = new SillyOperator("joe", plan);
        PlanEdge edges = new PlanEdge();
        boolean caught = false;
        try {
            edges.put(fred, joe, 1);
        } catch (IndexOutOfBoundsException e) {
            caught = true;
        }
        assertTrue(caught);
        
        caught = false;
        edges.put(fred, joe);
        SillyOperator bob = new SillyOperator("bob", plan);
        try {
            edges.put(fred, bob, 2);
        } catch (IndexOutOfBoundsException e) {
            caught = true;
        }
        assertTrue(caught);
    }
    
    // Test OperatorPlan
    @Test
    public void testOperatorPlan() throws IOException {
        SillyPlan plan = new SillyPlan();
        SillyOperator fred = new SillyOperator("fred", plan);
        SillyOperator joe = new SillyOperator("joe", plan);
        SillyOperator bob = new SillyOperator("bob", plan);
        SillyOperator jim = new SillyOperator("jim", plan);
        SillyOperator sam = new SillyOperator("sam", plan);
        
        // Test that roots and leaves are empty when there are no operators in
        // plan.
        List<Operator> list = plan.getSources();
        assertEquals(0, list.size());
        list = plan.getSinks();
        assertEquals(0, list.size());
        
        plan.add(fred);
        plan.add(joe);
        plan.add(bob);
        plan.add(jim);
        plan.add(sam);
        
        // Test that when not connected all nodes are roots and leaves.
        list = plan.getSources();
        assertEquals(5, list.size());
        list = plan.getSinks();
        assertEquals(5, list.size());
        
        // Connect them up
        plan.connect(fred, bob);
        plan.connect(joe, bob);
        plan.connect(bob, jim);
        plan.connect(bob, sam);
        
        // Check that the roots and leaves came out right
        list = plan.getSources();
        assertEquals(2, list.size());
        for (Operator op : list) {
            assertTrue(fred.isEqual(op) || joe.isEqual(op));
        }
        list = plan.getSinks();
        assertEquals(2, list.size());
        for (Operator op : list) {
            assertTrue(jim.isEqual(op) || sam.isEqual(op));
        }
        
        // Check each of their successors and predecessors
        list = plan.getSuccessors(fred);
        assertEquals(1, list.size());
        assertEquals(bob, list.get(0));
        
        list = plan.getSuccessors(joe);
        assertEquals(1, list.size());
        assertEquals(bob, list.get(0));
        
        list = plan.getPredecessors(jim);
        assertEquals(1, list.size());
        assertEquals(bob, list.get(0));
        
        list = plan.getPredecessors(sam);
        assertEquals(1, list.size());
        assertEquals(bob, list.get(0));
        
        list = plan.getPredecessors(bob);
        assertEquals(2, list.size());
        assertEquals(fred, list.get(0));
        assertEquals(joe, list.get(1));
        
        list = plan.getSuccessors(bob);
        assertEquals(2, list.size());
        assertEquals(jim, list.get(0));
        assertEquals(sam, list.get(1));
        
        // Now try swapping two, and check that all comes out as planned
        Pair<Integer, Integer> p1 = plan.disconnect(bob, jim);
        Pair<Integer, Integer> p2 = plan.disconnect(fred, bob);
        
        plan.connect(bob, p1.first, fred, p1.second);
        plan.connect(jim, p2.first, bob, p2.second);
        
         // Check that the roots and leaves came out right
        list = plan.getSources();
        assertEquals(2, list.size());
        for (Operator op : list) {
            assertTrue(jim.isEqual(op) || joe.isEqual(op));
        }
        list = plan.getSinks();
        assertEquals(2, list.size());
        for (Operator op : list) {
            assertTrue(fred.isEqual(op) || sam.isEqual(op));
        }
        
        // Check each of their successors and predecessors
        list = plan.getSuccessors(jim);
        assertEquals(1, list.size());
        assertEquals(bob, list.get(0));
        
        list = plan.getSuccessors(joe);
        assertEquals(1, list.size());
        assertEquals(bob, list.get(0));
        
        list = plan.getPredecessors(fred);
        assertEquals(1, list.size());
        assertEquals(bob, list.get(0));
        
        list = plan.getPredecessors(sam);
        assertEquals(1, list.size());
        assertEquals(bob, list.get(0));
        
        list = plan.getPredecessors(bob);
        assertEquals(2, list.size());
        assertEquals(jim, list.get(0));
        assertEquals(joe, list.get(1));
        
        list = plan.getSuccessors(bob);
        assertEquals(2, list.size());
        assertEquals(fred, list.get(0));
        assertEquals(sam, list.get(1));
        
    }
    
    @Test
    public void testDisconnectAndRemove() throws IOException {
        SillyPlan plan = new SillyPlan();
        SillyOperator fred = new SillyOperator("fred", plan);
        SillyOperator joe = new SillyOperator("joe", plan);
        SillyOperator bob = new SillyOperator("bob", plan);
        
        plan.add(fred);
        plan.add(joe);
        plan.add(bob);
        
        plan.connect(fred, joe);
        
        plan.remove(bob);
        plan.disconnect(fred, joe);
        
        List<Operator> list = plan.getSources();
        assertEquals(2, list.size());
        list = plan.getSinks();
        assertEquals(2, list.size());
        
        plan.remove(fred);
        plan.remove(joe);
        
        assertEquals(0, plan.size());
        
        list = plan.getSources();
        assertEquals(0, list.size());
        list = plan.getSinks();
        assertEquals(0, list.size());
    }
    
    // Test bad remove
    @Test
    public void testRemoveNegative() {
        SillyPlan plan = new SillyPlan();
        SillyOperator fred = new SillyOperator("fred", plan);
        SillyOperator joe = new SillyOperator("joe", plan);
        
        plan.add(fred);
        plan.add(joe);
        
        plan.connect(fred, joe);
        
        boolean caught = false;
        try {
            plan.remove(fred);
        } catch (IOException e) {
            caught = true;
        }
        assertTrue(caught);
        
        caught = false;
        try {
            plan.remove(joe);
        } catch (IOException e) {
            caught = true;
        }
        assertTrue(caught);
        
    }
    
    @Test
    public void testDisconnectNegative() {
        SillyPlan plan = new SillyPlan();
        SillyOperator fred = new SillyOperator("fred", plan);
        SillyOperator joe = new SillyOperator("joe", plan);
        
        plan.add(fred);
        plan.add(joe);
        
        boolean caught = false;
        try {
            plan.disconnect(fred, joe);
        } catch (IOException e) {
            caught = true;
        }
        assertTrue(caught);
        
    }
    
    // Tests for DependencyOrderWalker
    
    @Test
    public void testDependencyOrderWalkerLinear() throws IOException {
        SillyPlan plan = new SillyPlan();
        SillyOperator fred = new SillyOperator("fred", plan);
        SillyOperator joe = new SillyOperator("joe", plan);
        SillyOperator bob = new SillyOperator("bob", plan);
        
        plan.add(fred);
        plan.add(joe);
        plan.add(bob);
        
        plan.connect(fred, joe);
        plan.connect(joe, bob);
        
        SillyVisitor v =
            new SillyVisitor(plan, new DependencyOrderWalker(plan));
        
        v.visit();
        
        String s = v.getVisitPattern();
        
        assertEquals("fredjoebob", s);
    }

    @Test
    public void testDependencyOrderWalkerTree() throws IOException {
        SillyPlan plan = new SillyPlan();
        SillyOperator fred = new SillyOperator("fred", plan);
        SillyOperator joe = new SillyOperator("joe", plan);
        SillyOperator bob = new SillyOperator("bob", plan);
        SillyOperator jill = new SillyOperator("jill", plan);
        SillyOperator jane = new SillyOperator("jane", plan);
        
        plan.add(fred);
        plan.add(joe);
        plan.add(bob);
        plan.add(jill);
        plan.add(jane);
        
        plan.connect(fred, bob);
        plan.connect(joe, bob);
        plan.connect(bob, jill);
        plan.connect(jane, jill);
        
        SillyVisitor v =
            new SillyVisitor(plan, new DependencyOrderWalker(plan));
        
        v.visit();
        
        String s = v.getVisitPattern();
        
        if (!s.equals("fredjoebobjanejill") &&
                !s.equals("joefredbobjanejill") &&
                !s.equals("janefredjoebobjill") &&
                !s.equals("janejoefredbobjill")) {
            System.out.println("Invalid order " + s);
            fail();
        }
    }

    @Test
    public void testDependencyOrderWalkerGraph() throws IOException {
        SillyPlan plan = new SillyPlan();
        SillyOperator fred = new SillyOperator("fred", plan);
        SillyOperator joe = new SillyOperator("joe", plan);
        SillyOperator bob = new SillyOperator("bob", plan);
        SillyOperator jill = new SillyOperator("jill", plan);
        SillyOperator jane = new SillyOperator("jane", plan);
        
        plan.add(fred);
        plan.add(joe);
        plan.add(bob);
        plan.add(jill);
        plan.add(jane);
        
        plan.connect(fred, bob);
        plan.connect(joe, bob);
        plan.connect(bob, jill);
        plan.connect(bob, jane);
        
        SillyVisitor v =
            new SillyVisitor(plan, new DependencyOrderWalker(plan));
        
        v.visit();
        
        String s = v.getVisitPattern();
        
        if (!s.equals("fredjoebobjanejill") &&
                !s.equals("joefredbobjanejill") &&
                !s.equals("fredjoebobjilljane") &&
                !s.equals("joefredbobjilljane")) {
            System.out.println("Invalid order " + s);
            fail();
        }
    }

    // Tests for DepthFirstWalker
    
    @Test
    public void testDepthFirstWalkerLinear() throws IOException {
        SillyPlan plan = new SillyPlan();
        SillyOperator fred = new SillyOperator("fred", plan);
        SillyOperator joe = new SillyOperator("joe", plan);
        SillyOperator bob = new SillyOperator("bob", plan);
        
        plan.add(fred);
        plan.add(joe);
        plan.add(bob);
        
        plan.connect(fred, joe);
        plan.connect(joe, bob);
        
        SillyVisitor v =
            new SillyVisitor(plan, new DepthFirstWalker(plan));
        
        v.visit();
        
        String s = v.getVisitPattern();
        
        assertEquals("fredjoebob", s);
    }

    @Test
    public void testDepthFirstWalkerTree() throws IOException {
        SillyPlan plan = new SillyPlan();
        SillyOperator fred = new SillyOperator("fred", plan);
        SillyOperator joe = new SillyOperator("joe", plan);
        SillyOperator bob = new SillyOperator("bob", plan);
        SillyOperator jill = new SillyOperator("jill", plan);
        SillyOperator jane = new SillyOperator("jane", plan);
        
        plan.add(fred);
        plan.add(joe);
        plan.add(bob);
        plan.add(jill);
        plan.add(jane);
        
        plan.connect(fred, bob);
        plan.connect(fred, joe);
        plan.connect(joe, jill);
        plan.connect(joe, jane);
        
        SillyVisitor v =
            new SillyVisitor(plan, new DepthFirstWalker(plan));
        
        v.visit();
        
        String s = v.getVisitPattern();
        
        assertEquals("fredbobjoejilljane", s);
    }

    @Test
    public void testDepthFirstWalkerGraph() throws IOException {
        SillyPlan plan = new SillyPlan();
        SillyOperator fred = new SillyOperator("fred", plan);
        SillyOperator joe = new SillyOperator("joe", plan);
        SillyOperator bob = new SillyOperator("bob", plan);
        SillyOperator jill = new SillyOperator("jill", plan);
        SillyOperator jane = new SillyOperator("jane", plan);
        
        plan.add(fred);
        plan.add(joe);
        plan.add(bob);
        plan.add(jill);
        plan.add(jane);
        
        plan.connect(fred, bob);
        plan.connect(joe, bob);
        plan.connect(bob, jill);
        plan.connect(bob, jane);
        
        SillyVisitor v =
            new SillyVisitor(plan, new DepthFirstWalker(plan));
        
        v.visit();
        
        String s = v.getVisitPattern();
        
        if (!s.equals("fredbobjilljanejoe") &&
                !s.equals("joebobjilljanefred")) {
            System.out.println("Invalid order " + s);
            fail();
        }
    }

    // Tests for ReverseDependencyOrderWalker
    
    @Test
    public void testReverseDependencyOrderWalkerLinear() throws IOException {
        SillyPlan plan = new SillyPlan();
        SillyOperator fred = new SillyOperator("fred", plan);
        SillyOperator joe = new SillyOperator("joe", plan);
        SillyOperator bob = new SillyOperator("bob", plan);
        
        plan.add(fred);
        plan.add(joe);
        plan.add(bob);
        
        plan.connect(fred, joe);
        plan.connect(joe, bob);
        
        SillyVisitor v =
            new SillyVisitor(plan, new ReverseDependencyOrderWalker(plan));
        
        v.visit();
        
        String s = v.getVisitPattern();
        
        assertEquals("bobjoefred", s);
    }

    @Test
    public void testReverseDependencyOrderWalkerTree() throws IOException {
        SillyPlan plan = new SillyPlan();
        SillyOperator fred = new SillyOperator("fred", plan);
        SillyOperator joe = new SillyOperator("joe", plan);
        SillyOperator bob = new SillyOperator("bob", plan);
        SillyOperator jill = new SillyOperator("jill", plan);
        SillyOperator jane = new SillyOperator("jane", plan);
        
        plan.add(fred);
        plan.add(joe);
        plan.add(bob);
        plan.add(jill);
        plan.add(jane);
        
        plan.connect(fred, bob);
        plan.connect(joe, bob);
        plan.connect(bob, jill);
        plan.connect(jane, jill);
        
        SillyVisitor v =
            new SillyVisitor(plan, new ReverseDependencyOrderWalker(plan));
        
        v.visit();
        
        String s = v.getVisitPattern();
        
        if (!s.equals("jilljanebobjoefred") &&
                !s.equals("jilljanebobfredjoe") &&
                !s.equals("jillbobjoefredjane") &&
                !s.equals("jillbobjoejanefred") &&
                !s.equals("jillbobfredjoejane") &&
                !s.equals("jillbobfredjanejoe") &&
                !s.equals("jillbobjanejoefred") &&
                !s.equals("jillbobjanefredjoe")) {
            System.out.println("Invalid order " + s);
            fail();
        }
    }

    @Test
    public void testReverseDependencyOrderWalkerGraph() throws IOException {
        SillyPlan plan = new SillyPlan();
        SillyOperator fred = new SillyOperator("fred", plan);
        SillyOperator joe = new SillyOperator("joe", plan);
        SillyOperator bob = new SillyOperator("bob", plan);
        SillyOperator jill = new SillyOperator("jill", plan);
        SillyOperator jane = new SillyOperator("jane", plan);
        
        plan.add(fred);
        plan.add(joe);
        plan.add(bob);
        plan.add(jill);
        plan.add(jane);
        
        plan.connect(fred, bob);
        plan.connect(joe, bob);
        plan.connect(bob, jill);
        plan.connect(bob, jane);
        
        SillyVisitor v =
            new SillyVisitor(plan, new ReverseDependencyOrderWalker(plan));
        
        v.visit();
        
        String s = v.getVisitPattern();
        
        if (!s.equals("jilljanebobjoefred") &&
                !s.equals("jilljanebobfredjoe") &&
                !s.equals("janejillbobjoefred") &&
                !s.equals("janejillbobfredjoe")) {
            System.out.println("Invalid order " + s);
            fail();
        }
    }
    
    private static class TestLogicalVisitor extends LogicalPlanVisitor {
        
        StringBuffer bf = new StringBuffer();

        protected TestLogicalVisitor(OperatorPlan plan) {
            super(plan, new DepthFirstWalker(plan));
        }
        
        @Override
        public void visitLOLoad(LOLoad load) {
            bf.append("load ");
        }
        
        String getVisitPlan() {
            return bf.toString();
        }
        
    }
    
    @Test
    public void testLogicalPlanVisitor() throws IOException {
        LogicalPlan lp = new LogicalPlan();
        LOLoad load = new LOLoad(null, null, lp);
        /*lp.add((LogicalRelationalOperator)null, load,
            (LogicalRelationalOperator)null);*/
        lp.add(load);
        
        TestLogicalVisitor v = new TestLogicalVisitor(lp);
        v.visit();
        
        assertEquals("load ", v.getVisitPlan());
    }
    
    @Test
    public void testBinaryOperatorOrder() throws IOException {
        LogicalExpressionPlan ep = new LogicalExpressionPlan();
        ConstantExpression c = new ConstantExpression(ep, DataType.INTEGER, new Integer(5));
        ProjectExpression p = new ProjectExpression(ep, DataType.INTEGER, 0, 0);
        EqualExpression e = new EqualExpression(ep, p, c);
        assertEquals(p, e.getLhs());
        assertEquals(c, e.getRhs());
        
    }
    
    private static class TestExpressionVisitor extends LogicalExpressionVisitor {
        
        StringBuffer bf = new StringBuffer();

        protected TestExpressionVisitor(OperatorPlan plan) {
            super(plan, new DepthFirstWalker(plan));
        }
        
        @Override
        public void visitAnd(AndExpression andExpr) {
            bf.append("and ");
        }
        
        @Override
        public void visitEqual(EqualExpression equal) {
            bf.append("equal ");
        }
        
        @Override
        public void visitProject(ProjectExpression project) {
            bf.append("project ");
        }
        
        @Override
        public void visitConstant(ConstantExpression constant) {
            bf.append("constant ");
        }
        
        String getVisitPlan() {
            return bf.toString();
        }
        
    }
    
    @Test
    public void testExpressionPlanVisitor() throws IOException {
        LogicalExpressionPlan ep = new LogicalExpressionPlan();
        ConstantExpression c = new ConstantExpression(ep, DataType.INTEGER, new Integer(5));
        ProjectExpression p = new ProjectExpression(ep, DataType.INTEGER, 0, 0);
        EqualExpression e = new EqualExpression(ep, p, c);
        ConstantExpression c2 = new ConstantExpression(ep, DataType.BOOLEAN, new Boolean("true"));
        new AndExpression(ep, e, c2);
        
        TestExpressionVisitor v = new TestExpressionVisitor(ep);
        v.visit();
        assertEquals("and equal project constant constant ", v.getVisitPlan());
    }
    
    @Test
    public void testExpressionEquality() {
        LogicalExpressionPlan ep1 = new LogicalExpressionPlan();
        ConstantExpression c1 = new ConstantExpression(ep1, DataType.INTEGER, new Integer(5));
        ProjectExpression p1 = new ProjectExpression(ep1, DataType.INTEGER, 0, 0);
        EqualExpression e1 = new EqualExpression(ep1, p1, c1);
        ConstantExpression ca1 = new ConstantExpression(ep1, DataType.BOOLEAN, new Boolean("true"));
        AndExpression a1 = new AndExpression(ep1, e1, ca1);
        
        LogicalExpressionPlan ep2 = new LogicalExpressionPlan();
        ConstantExpression c2 = new ConstantExpression(ep2, DataType.INTEGER, new Integer(5));
        ProjectExpression p2 = new ProjectExpression(ep2, DataType.INTEGER, 0, 0);
        EqualExpression e2 = new EqualExpression(ep2, p2, c2);
        ConstantExpression ca2 = new ConstantExpression(ep2, DataType.BOOLEAN, new Boolean("true"));
        AndExpression a2 = new AndExpression(ep2, e2, ca2);
        
        assertTrue(ep1.isEqual(ep2));
        assertTrue(c1.isEqual(c2));
        assertTrue(p1.isEqual(p2));
        assertTrue(e1.isEqual(e2));
        assertTrue(ca1.isEqual(ca2));
        assertTrue(a1.isEqual(a2));
        
        LogicalExpressionPlan ep3 = new LogicalExpressionPlan();
        ConstantExpression c3 = new ConstantExpression(ep3, DataType.INTEGER, new Integer(3));
        ProjectExpression p3 = new ProjectExpression(ep3, DataType.INTEGER, 0, 1);
        EqualExpression e3 = new EqualExpression(ep3, p3, c3);
        ConstantExpression ca3 = new ConstantExpression(ep3, DataType.CHARARRAY, "true");
        AndExpression a3 = new AndExpression(ep3, e3, ca3);
        
        assertFalse(ep1.isEqual(ep3));
        assertFalse(c1.isEqual(c3));
        assertFalse(p1.isEqual(p3));
        assertFalse(e1.isEqual(e3));
        assertFalse(ca1.isEqual(ca3));
        assertFalse(a1.isEqual(a3));
        
        LogicalExpressionPlan ep4 = new LogicalExpressionPlan();
        ProjectExpression p4 = new ProjectExpression(ep4, DataType.INTEGER, 1, 0);
        
        assertFalse(ep1.isEqual(ep4));
        assertFalse(p1.isEqual(p4));
    }
    
    @Test
    public void testRelationalEquality() throws IOException {
        // Build a plan that is the logical plan for
        // A = load 'bla' as (x);
        // B = load 'morebla' as (y);
        // C = join A on x, B on y;
        // D = filter C by y > 0;
        
        // A = load
        LogicalPlan lp = new LogicalPlan();
        {
            LogicalSchema aschema = new LogicalSchema();
            aschema.addField(new LogicalSchema.LogicalFieldSchema(
                "x", null, DataType.INTEGER));
            LOLoad A = new LOLoad(new FileSpec("/abc",
                new FuncSpec("/fooload", new String[] {"x", "y"})), aschema, lp);
            lp.add(A);
        
            // B = load
            LogicalSchema bschema = new LogicalSchema();
            bschema.addField(new LogicalSchema.LogicalFieldSchema(
                "y", null, DataType.INTEGER));
            LOLoad B = new LOLoad(new FileSpec("/def",
                new FuncSpec("PigStorage", "\t")), bschema, lp);
            lp.add(B);
        
            // C = join
            LogicalSchema cschema = new LogicalSchema();
            cschema.addField(new LogicalSchema.LogicalFieldSchema(
                "x", null, DataType.INTEGER));
            cschema.addField(new LogicalSchema.LogicalFieldSchema(
                "y", null, DataType.INTEGER));
            LogicalExpressionPlan aprojplan = new LogicalExpressionPlan();
            new ProjectExpression(aprojplan, DataType.INTEGER, 0, 0);
            LogicalExpressionPlan bprojplan = new LogicalExpressionPlan();
            new ProjectExpression(bprojplan, DataType.INTEGER, 1, 0);
            MultiMap<Integer, LogicalExpressionPlan> mm = 
                new MultiMap<Integer, LogicalExpressionPlan>();
            mm.put(0, aprojplan);
            mm.put(1, bprojplan);
            LOJoin C = new LOJoin(lp, mm, JOINTYPE.HASH, new boolean[] {true, true});
            C.neverUseForRealSetSchema(cschema);
            lp.add(C);
            lp.connect(A, C);
            lp.connect(B, C);
            
            // D = filter
            LogicalExpressionPlan filterPlan = new LogicalExpressionPlan();
            ProjectExpression fy = new ProjectExpression(filterPlan, DataType.INTEGER, 0, 1);
            ConstantExpression fc = new ConstantExpression(filterPlan, DataType.INTEGER, new Integer(0));
            new EqualExpression(filterPlan, fy, fc);
            LOFilter D = new LOFilter(lp, filterPlan);
            D.neverUseForRealSetSchema(cschema);
            lp.add(D);
            lp.connect(C, D);
        }
        
        // Build a second similar plan to test equality
        // A = load
        LogicalPlan lp1 = new LogicalPlan();
        {
            LogicalSchema aschema = new LogicalSchema();
            aschema.addField(new LogicalSchema.LogicalFieldSchema(
                "x", null, DataType.INTEGER));
            LOLoad A = new LOLoad(new FileSpec("/abc",
                new FuncSpec("/fooload", new String[] {"x", "y"})), aschema, lp1);
            lp1.add(A);
            
            // B = load
            LogicalSchema bschema = new LogicalSchema();
            bschema.addField(new LogicalSchema.LogicalFieldSchema(
                "y", null, DataType.INTEGER));
            LOLoad B = new LOLoad(new FileSpec("/def",
                new FuncSpec("PigStorage", "\t")), bschema, lp1);
            lp1.add(B);
            
            // C = join
            LogicalSchema cschema = new LogicalSchema();
            cschema.addField(new LogicalSchema.LogicalFieldSchema(
                "x", null, DataType.INTEGER));
            cschema.addField(new LogicalSchema.LogicalFieldSchema(
                "y", null, DataType.INTEGER));
            LogicalExpressionPlan aprojplan = new LogicalExpressionPlan();
            new ProjectExpression(aprojplan, DataType.INTEGER, 0, 0);
            LogicalExpressionPlan bprojplan = new LogicalExpressionPlan();
            new ProjectExpression(bprojplan, DataType.INTEGER, 1, 0);
            MultiMap<Integer, LogicalExpressionPlan> mm = 
                new MultiMap<Integer, LogicalExpressionPlan>();
            mm.put(0, aprojplan);
            mm.put(1, bprojplan);
            LOJoin C = new LOJoin(lp1, mm, JOINTYPE.HASH, new boolean[] {true, true});
            C.neverUseForRealSetSchema(cschema);
            lp1.add(C);
            lp1.connect(A, C);
            lp1.connect(B, C);
                
            
            // D = filter
            LogicalExpressionPlan filterPlan = new LogicalExpressionPlan();
            ProjectExpression fy = new ProjectExpression(filterPlan, DataType.INTEGER, 0, 1);
            ConstantExpression fc = new ConstantExpression(filterPlan, DataType.INTEGER, new Integer(0));
            new EqualExpression(filterPlan, fy, fc);
            LOFilter D = new LOFilter(lp1, filterPlan);
            D.neverUseForRealSetSchema(cschema);
            lp1.add(D);
            lp1.connect(C, D);
                
        }
        
        assertTrue( lp.isEqual(lp1));
    }
    
    @Test
    public void testLoadEqualityDifferentFuncSpecCtorArgs() {
        LogicalPlan lp = new LogicalPlan();
        
        LogicalSchema aschema1 = new LogicalSchema();
        aschema1.addField(new LogicalSchema.LogicalFieldSchema(
            "x", null, DataType.INTEGER));
        LOLoad load1 = new LOLoad(new FileSpec("/abc",
            new FuncSpec("foo", new String[] {"x", "y"})), aschema1, lp);
        lp.add(load1);
        
        LOLoad load2 = new LOLoad(new FileSpec("/abc",
            new FuncSpec("foo", new String[] {"x", "z"})), aschema1, lp);
        lp.add(load2);
        
        assertFalse(load1.isEqual(load2));
    }
    
    @Test
    public void testLoadEqualityDifferentNumFuncSpecCstorArgs() {
        LogicalPlan lp = new LogicalPlan();
        
        LogicalSchema aschema1 = new LogicalSchema();
        aschema1.addField(new LogicalSchema.LogicalFieldSchema(
            "x", null, DataType.INTEGER));
        LOLoad load1 = new LOLoad(new FileSpec("/abc",
            new FuncSpec("foo", new String[] {"x", "y"})), aschema1, lp);
        lp.add(load1);
        
        LOLoad load3 = new LOLoad(new FileSpec("/abc",
            new FuncSpec("foo", "x")), aschema1, lp);
        lp.add(load3);
        
        assertFalse(load1.isEqual(load3));
    }
    
    @Test
    public void testLoadEqualityDifferentFunctionNames() {
        LogicalPlan lp = new LogicalPlan();
        
        LogicalSchema aschema1 = new LogicalSchema();
        aschema1.addField(new LogicalSchema.LogicalFieldSchema(
            "x", null, DataType.INTEGER));
        LOLoad load1 = new LOLoad(new FileSpec("/abc",
            new FuncSpec("foo", new String[] {"x", "y"})), aschema1, lp);
        lp.add(load1);
        
         // Different function names in FuncSpec
        LOLoad load4 = new LOLoad(new FileSpec("/abc",
            new FuncSpec("foobar", new String[] {"x", "z"})), aschema1, lp);
        lp.add(load4);
        
        assertFalse(load1.isEqual(load4));
    }
    
    @Test
    public void testLoadEqualityDifferentFileName() {
        LogicalPlan lp = new LogicalPlan();
        LogicalSchema aschema1 = new LogicalSchema();
        aschema1.addField(new LogicalSchema.LogicalFieldSchema(
            "x", null, DataType.INTEGER));
        LOLoad load1 = new LOLoad(new FileSpec("/abc",
            new FuncSpec("foo", new String[] {"x", "y"})), aschema1, lp);
        lp.add(load1);
    
        // Different file name
        LOLoad load5 = new LOLoad(new FileSpec("/def",
            new FuncSpec("foo", new String[] {"x", "z"})), aschema1, lp);
        lp.add(load5);
        
        assertFalse(load1.isEqual(load5));
    }
    
    @Test
    public void testRelationalEqualityDifferentSchema() {
        LogicalPlan lp = new LogicalPlan();
        LogicalSchema aschema1 = new LogicalSchema();
        aschema1.addField(new LogicalSchema.LogicalFieldSchema(
            "x", null, DataType.INTEGER));
        LOLoad load1 = new LOLoad(new FileSpec("/abc",
            new FuncSpec("foo", new String[] {"x", "y"})), aschema1, lp);
        lp.add(load1);
        
        // Different schema
        LogicalSchema aschema2 = new LogicalSchema();
        aschema2.addField(new LogicalSchema.LogicalFieldSchema(
            "x", null, DataType.CHARARRAY));
        
        LOLoad load6 = new LOLoad(new FileSpec("/abc",
            new FuncSpec("foo", new String[] {"x", "z"})), aschema2, lp);
        lp.add(load6);
            
        assertFalse(load1.isEqual(load6));
    }
    
    @Test
    public void testRelationalEqualityNullSchemas() {
        LogicalPlan lp = new LogicalPlan();
        // Test that two loads with no schema are still equal
        LOLoad load7 = new LOLoad(new FileSpec("/abc",
            new FuncSpec("foo", new String[] {"x", "y"})), null, lp);
        lp.add(load7);
        
        LOLoad load8 = new LOLoad(new FileSpec("/abc",
            new FuncSpec("foo", new String[] {"x", "y"})), null, lp);
        lp.add(load8);
        
        assertTrue(load7.isEqual(load8));
    }
    
    @Test
    public void testRelationalEqualityOneNullOneNotNullSchema() {
        LogicalPlan lp = new LogicalPlan();
        LogicalSchema aschema1 = new LogicalSchema();
        aschema1.addField(new LogicalSchema.LogicalFieldSchema(
            "x", null, DataType.INTEGER));
        LOLoad load1 = new LOLoad(new FileSpec("/abc",
            new FuncSpec("foo", new String[] {"x", "y"})), aschema1, lp);
        lp.add(load1);
        
        // Test that one with schema and one without breaks equality
        LOLoad load9 = new LOLoad(new FileSpec("/abc",
            new FuncSpec("foo", new String[] {"x", "z"})), null, lp);
        lp.add(load9);
        
        assertFalse(load1.isEqual(load9));
    }
        
    @Test
    public void testFilterDifferentPredicates() {
        LogicalPlan lp = new LogicalPlan();
            
        LogicalExpressionPlan fp1 = new LogicalExpressionPlan();
        ProjectExpression fy1 = new ProjectExpression(fp1, DataType.INTEGER, 0, 1);
        ConstantExpression fc1 = new ConstantExpression(fp1, DataType.INTEGER,
            new Integer(0));
        new EqualExpression(fp1, fy1, fc1);
        LOFilter D1 = new LOFilter(lp, fp1);
        LogicalSchema cschema = new LogicalSchema();
        cschema.addField(new LogicalSchema.LogicalFieldSchema(
            "x", null, DataType.INTEGER));
        cschema.addField(new LogicalSchema.LogicalFieldSchema(
            "y", null, DataType.INTEGER));
        D1.neverUseForRealSetSchema(cschema);
        lp.add(D1);
        
        LogicalExpressionPlan fp2 = new LogicalExpressionPlan();
        ProjectExpression fy2 = new ProjectExpression(fp2, DataType.INTEGER, 0, 1);
        ConstantExpression fc2 = new ConstantExpression(fp2, DataType.INTEGER,
            new Integer(1));
        new EqualExpression(fp2, fy2, fc2);
        LOFilter D2 = new LOFilter(lp, fp2);
        D2.neverUseForRealSetSchema(cschema);
        lp.add(D2);
        
        assertFalse(D1.isEqual(D2));
    }
        
    // No tests for LOStore because it tries to actually instantiate the store
    // func, and I don't want to mess with that here.
    
    @Test
    public void testJoinDifferentJoinTypes() throws IOException {
       LogicalPlan lp = new LogicalPlan();
       LogicalSchema jaschema1 = new LogicalSchema();
       jaschema1.addField(new LogicalSchema.LogicalFieldSchema(
           "x", null, DataType.INTEGER));
       LOLoad A1 = new LOLoad(new FileSpec("/abc",
           new FuncSpec("/fooload", new String[] {"x", "y"})), jaschema1, lp);
       lp.add(A1);
        
        // B = load
        LogicalSchema jbschema1 = new LogicalSchema();
        jbschema1.addField(new LogicalSchema.LogicalFieldSchema(
            "y", null, DataType.INTEGER));
        LOLoad B1 = new LOLoad(new FileSpec("/def",
            new FuncSpec("PigStorage", "\t")), jbschema1, lp);
        lp.add(B1);
        
        // C = join
        LogicalSchema jcschema1 = new LogicalSchema();
        jcschema1.addField(new LogicalSchema.LogicalFieldSchema(
            "x", null, DataType.INTEGER));
        jcschema1.addField(new LogicalSchema.LogicalFieldSchema(
            "y", null, DataType.INTEGER));
        LogicalExpressionPlan aprojplan1 = new LogicalExpressionPlan();
        new ProjectExpression(aprojplan1, DataType.INTEGER, 0, 0);
        LogicalExpressionPlan bprojplan1 = new LogicalExpressionPlan();
        new ProjectExpression(bprojplan1, DataType.INTEGER, 1, 0);
        MultiMap<Integer, LogicalExpressionPlan> mm1 = 
            new MultiMap<Integer, LogicalExpressionPlan>();
        mm1.put(0, aprojplan1);
        mm1.put(1, bprojplan1);
        LOJoin C1 = new LOJoin(lp, mm1, JOINTYPE.HASH, new boolean[] {true, true});
        C1.neverUseForRealSetSchema(jcschema1);
        lp.add(C1);
        lp.connect(A1, C1);
        lp.connect(B1, C1);
        
        // A = load
        LogicalSchema jaschema2 = new LogicalSchema();
        jaschema2.addField(new LogicalSchema.LogicalFieldSchema(
           "x", null, DataType.INTEGER));
        LOLoad A2 = new LOLoad(new FileSpec("/abc",
           new FuncSpec("/fooload", new String[] {"x", "y"})), jaschema2, lp);
        lp.add(A2);
        
        // B = load
        LogicalSchema jbschema2 = new LogicalSchema();
        jbschema2.addField(new LogicalSchema.LogicalFieldSchema(
            "y", null, DataType.INTEGER));
        LOLoad B2 = new LOLoad(new FileSpec("/def",
            new FuncSpec("PigStorage", "\t")), jbschema2, lp);
        lp.add(B2);
        
        // C = join
        LogicalSchema jcschema2 = new LogicalSchema();
        jcschema2.addField(new LogicalSchema.LogicalFieldSchema(
            "x", null, DataType.INTEGER));
        jcschema2.addField(new LogicalSchema.LogicalFieldSchema(
            "y", null, DataType.INTEGER));
        LogicalExpressionPlan aprojplan2 = new LogicalExpressionPlan();
        new ProjectExpression(aprojplan2, DataType.INTEGER, 0, 0);
        LogicalExpressionPlan bprojplan2 = new LogicalExpressionPlan();
        new ProjectExpression(bprojplan2, DataType.INTEGER, 1, 0);
        MultiMap<Integer, LogicalExpressionPlan> mm2 = 
            new MultiMap<Integer, LogicalExpressionPlan>();
        mm2.put(0, aprojplan2);
        mm2.put(1, bprojplan2);
        LOJoin C2 = new LOJoin(lp, mm2, JOINTYPE.SKEWED, new boolean[] {true, true});
        C2.neverUseForRealSetSchema(jcschema2);
        lp.add(C2);
        lp.connect(A2, C2);
        lp.connect(B2, C2);
        
        assertFalse(C1.isEqual(C2));
    }
    
    @Test
    public void testJoinDifferentInner() throws IOException {
        LogicalPlan lp = new LogicalPlan();
               LogicalSchema jaschema1 = new LogicalSchema();
       jaschema1.addField(new LogicalSchema.LogicalFieldSchema(
           "x", null, DataType.INTEGER));
       LOLoad A1 = new LOLoad(new FileSpec("/abc",
           new FuncSpec("/fooload", new String[] {"x", "y"})), jaschema1, lp);
       lp.add(A1);
        
        // B = load
        LogicalSchema jbschema1 = new LogicalSchema();
        jbschema1.addField(new LogicalSchema.LogicalFieldSchema(
            "y", null, DataType.INTEGER));
        LOLoad B1 = new LOLoad(new FileSpec("/def",
            new FuncSpec("PigStorage", "\t")), jbschema1, lp);
        lp.add(B1);
        
        // C = join
        LogicalSchema jcschema1 = new LogicalSchema();
        jcschema1.addField(new LogicalSchema.LogicalFieldSchema(
            "x", null, DataType.INTEGER));
        jcschema1.addField(new LogicalSchema.LogicalFieldSchema(
            "y", null, DataType.INTEGER));
        LogicalExpressionPlan aprojplan1 = new LogicalExpressionPlan();
        new ProjectExpression(aprojplan1, DataType.INTEGER, 0, 0);
        LogicalExpressionPlan bprojplan1 = new LogicalExpressionPlan();
        new ProjectExpression(bprojplan1, DataType.INTEGER, 1, 0);
        MultiMap<Integer, LogicalExpressionPlan> mm1 = 
            new MultiMap<Integer, LogicalExpressionPlan>();
        mm1.put(0, aprojplan1);
        mm1.put(1, bprojplan1);
        LOJoin C1 = new LOJoin(lp, mm1, JOINTYPE.HASH, new boolean[] {true, true});
        C1.neverUseForRealSetSchema(jcschema1);
        lp.add(C1);
        lp.connect(A1, C1);
        lp.connect(B1, C1);
        
 
        // Test different inner status
        // A = load
        LogicalSchema jaschema3 = new LogicalSchema();
        jaschema3.addField(new LogicalSchema.LogicalFieldSchema(
           "x", null, DataType.INTEGER));
        LOLoad A3 = new LOLoad(new FileSpec("/abc",
           new FuncSpec("/fooload", new String[] {"x", "y"})), jaschema3, lp);
        lp.add(A3);
        
        // B = load
        LogicalSchema jbschema3 = new LogicalSchema();
        jbschema3.addField(new LogicalSchema.LogicalFieldSchema(
            "y", null, DataType.INTEGER));
        LOLoad B3 = new LOLoad(new FileSpec("/def",
            new FuncSpec("PigStorage", "\t")), jbschema3, lp);
        lp.add(B3);
        
        // C = join
        LogicalSchema jcschema3 = new LogicalSchema();
        jcschema3.addField(new LogicalSchema.LogicalFieldSchema(
            "x", null, DataType.INTEGER));
        jcschema3.addField(new LogicalSchema.LogicalFieldSchema(
            "y", null, DataType.INTEGER));
        LogicalExpressionPlan aprojplan3 = new LogicalExpressionPlan();
        new ProjectExpression(aprojplan3, DataType.INTEGER, 0, 0);
        LogicalExpressionPlan bprojplan3 = new LogicalExpressionPlan();
        new ProjectExpression(bprojplan3, DataType.INTEGER, 1, 0);
        MultiMap<Integer, LogicalExpressionPlan> mm3 = 
            new MultiMap<Integer, LogicalExpressionPlan>();
        mm3.put(0, aprojplan3);
        mm3.put(1, bprojplan3);
        LOJoin C3 = new LOJoin(lp, mm3, JOINTYPE.HASH, new boolean[] {true, false});
        C3.neverUseForRealSetSchema(jcschema3);
        lp.add(C3);
        lp.connect(A3, C3);
        lp.connect(B3, C3);
        
        
        assertFalse(C1.isEqual(C3));
    }
 
    @Test
    public void testJoinDifferentNumInputs() throws IOException {
        LogicalPlan lp = new LogicalPlan();
               LogicalSchema jaschema1 = new LogicalSchema();
       jaschema1.addField(new LogicalSchema.LogicalFieldSchema(
           "x", null, DataType.INTEGER));
       LOLoad A1 = new LOLoad(new FileSpec("/abc",
           new FuncSpec("/fooload", new String[] {"x", "y"})), jaschema1, lp);
       lp.add(A1);
        
        // B = load
        LogicalSchema jbschema1 = new LogicalSchema();
        jbschema1.addField(new LogicalSchema.LogicalFieldSchema(
            "y", null, DataType.INTEGER));
        LOLoad B1 = new LOLoad(new FileSpec("/def",
            new FuncSpec("PigStorage", "\t")), jbschema1, lp);
        lp.add(B1);
        
        // C = join
        LogicalSchema jcschema1 = new LogicalSchema();
        jcschema1.addField(new LogicalSchema.LogicalFieldSchema(
            "x", null, DataType.INTEGER));
        jcschema1.addField(new LogicalSchema.LogicalFieldSchema(
            "y", null, DataType.INTEGER));
        LogicalExpressionPlan aprojplan1 = new LogicalExpressionPlan();
        new ProjectExpression(aprojplan1, DataType.INTEGER, 0, 0);
        LogicalExpressionPlan bprojplan1 = new LogicalExpressionPlan();
        new ProjectExpression(bprojplan1, DataType.INTEGER, 1, 0);
        MultiMap<Integer, LogicalExpressionPlan> mm1 = 
            new MultiMap<Integer, LogicalExpressionPlan>();
        mm1.put(0, aprojplan1);
        mm1.put(1, bprojplan1);
        LOJoin C1 = new LOJoin(lp, mm1, JOINTYPE.HASH, new boolean[] {true, true});
        C1.neverUseForRealSetSchema(jcschema1);
        lp.add(C1);
        lp.connect(A1, C1);
        lp.connect(B1, C1);
 
        // A = load
        LogicalSchema jaschema5 = new LogicalSchema();
        jaschema5.addField(new LogicalSchema.LogicalFieldSchema(
           "x", null, DataType.INTEGER));
        LOLoad A5 = new LOLoad(new FileSpec("/abc",
           new FuncSpec("/fooload", new String[] {"x", "y"})), jaschema5, lp);
        lp.add(A5);
        
        // B = load
        LogicalSchema jbschema5 = new LogicalSchema();
        jbschema5.addField(new LogicalSchema.LogicalFieldSchema(
            "y", null, DataType.INTEGER));
        LOLoad B5 = new LOLoad(new FileSpec("/def",
            new FuncSpec("PigStorage", "\t")), jbschema5, lp);
        lp.add(B5);
        
        // Beta = load
        LogicalSchema jbetaschema5 = new LogicalSchema();
        jbetaschema5.addField(new LogicalSchema.LogicalFieldSchema(
            "y", null, DataType.INTEGER));
        LOLoad Beta5 = new LOLoad(new FileSpec("/ghi",
            new FuncSpec("PigStorage", "\t")), jbetaschema5, lp);
        lp.add(Beta5);
        
        // C = join
        LogicalSchema jcschema5 = new LogicalSchema();
        jcschema5.addField(new LogicalSchema.LogicalFieldSchema(
            "x", null, DataType.INTEGER));
        jcschema5.addField(new LogicalSchema.LogicalFieldSchema(
            "y", null, DataType.INTEGER));
        LogicalExpressionPlan aprojplan5 = new LogicalExpressionPlan();
        new ProjectExpression(aprojplan5, DataType.INTEGER, 0, 0);
        LogicalExpressionPlan bprojplan5 = new LogicalExpressionPlan();
        new ProjectExpression(bprojplan5, DataType.INTEGER, 1, 0);
        LogicalExpressionPlan betaprojplan5 = new LogicalExpressionPlan();
        new ProjectExpression(betaprojplan5, DataType.INTEGER, 1, 0);
        MultiMap<Integer, LogicalExpressionPlan> mm5 = 
            new MultiMap<Integer, LogicalExpressionPlan>();
        mm5.put(0, aprojplan5);
        mm5.put(1, bprojplan5);
        mm5.put(2, betaprojplan5);
        LOJoin C5 = new LOJoin(lp, mm5, JOINTYPE.HASH, new boolean[] {true, true});
        C5.neverUseForRealSetSchema(jcschema5);
        lp.add(C5);
        lp.connect(A5, C5);
        lp.connect(B5, C5);
        lp.connect(Beta5, C5);
        
        assertFalse(C1.isEqual(C5));
    }
        
    @Test
    public void testJoinDifferentJoinKeys() throws IOException {
        LogicalPlan lp = new LogicalPlan();
        
        // Test different join keys
        LogicalSchema jaschema6 = new LogicalSchema();
        jaschema6.addField(new LogicalSchema.LogicalFieldSchema(
           "x", null, DataType.INTEGER));
        LOLoad A6 = new LOLoad(new FileSpec("/abc",
           new FuncSpec("/fooload", new String[] {"x", "y"})), jaschema6, lp);
        lp.add(A6);
        
        // B = load
        LogicalSchema jbschema6 = new LogicalSchema();
        jbschema6.addField(new LogicalSchema.LogicalFieldSchema(
            "y", null, DataType.INTEGER));
        jbschema6.addField(new LogicalSchema.LogicalFieldSchema(
            "z", null, DataType.LONG));
        LOLoad B6 = new LOLoad(new FileSpec("/def",
            new FuncSpec("PigStorage", "\t")), jbschema6, lp);
        lp.add(B6);
        
        // C = join
        LogicalSchema jcschema6 = new LogicalSchema();
        jcschema6.addField(new LogicalSchema.LogicalFieldSchema(
            "x", null, DataType.INTEGER));
        jcschema6.addField(new LogicalSchema.LogicalFieldSchema(
            "y", null, DataType.INTEGER));
        LogicalExpressionPlan aprojplan6 = new LogicalExpressionPlan();
        new ProjectExpression(aprojplan6, DataType.INTEGER, 0, 0);
        LogicalExpressionPlan bprojplan6 = new LogicalExpressionPlan();
        new ProjectExpression(bprojplan6, DataType.INTEGER, 1, 0);
        LogicalExpressionPlan b2projplan6 = new LogicalExpressionPlan();
        new ProjectExpression(b2projplan6, DataType.INTEGER, 1, 1);
        MultiMap<Integer, LogicalExpressionPlan> mm6 = 
            new MultiMap<Integer, LogicalExpressionPlan>();
        mm6.put(0, aprojplan6);
        mm6.put(1, bprojplan6);
        mm6.put(1, b2projplan6);
        LOJoin C6 = new LOJoin(lp, mm6, JOINTYPE.HASH, new boolean[] {true, true});
        C6.neverUseForRealSetSchema(jcschema6);
        lp.add(C6);
        lp.connect(A6, C6);
        lp.connect(B6, C6);
        
        
        LogicalSchema jaschema7 = new LogicalSchema();
        jaschema7.addField(new LogicalSchema.LogicalFieldSchema(
           "x", null, DataType.INTEGER));
        LOLoad A7 = new LOLoad(new FileSpec("/abc",
           new FuncSpec("/fooload", new String[] {"x", "y"})), jaschema7, lp);
        lp.add(A7);
        
        // B = load
        LogicalSchema jbschema7 = new LogicalSchema();
        jbschema7.addField(new LogicalSchema.LogicalFieldSchema(
            "y", null, DataType.INTEGER));
        jbschema7.addField(new LogicalSchema.LogicalFieldSchema(
            "z", null, DataType.LONG));
        LOLoad B7 = new LOLoad(new FileSpec("/def",
            new FuncSpec("PigStorage", "\t")), jbschema7, lp);
        lp.add(B7);
        
        // C = join
        LogicalSchema jcschema7 = new LogicalSchema();
        jcschema7.addField(new LogicalSchema.LogicalFieldSchema(
            "x", null, DataType.INTEGER));
        jcschema7.addField(new LogicalSchema.LogicalFieldSchema(
            "y", null, DataType.INTEGER));
        LogicalExpressionPlan aprojplan7 = new LogicalExpressionPlan();
        new ProjectExpression(aprojplan7, DataType.INTEGER, 0, 0);
        LogicalExpressionPlan bprojplan7 = new LogicalExpressionPlan();
        new ProjectExpression(bprojplan7, DataType.INTEGER, 1, 1);
        LogicalExpressionPlan b2projplan7 = new LogicalExpressionPlan();
        new ProjectExpression(b2projplan7, DataType.INTEGER, 1, 0);
        MultiMap<Integer, LogicalExpressionPlan> mm7 = 
            new MultiMap<Integer, LogicalExpressionPlan>();
        mm7.put(0, aprojplan7);
        mm7.put(1, bprojplan7);
        mm7.put(1, b2projplan7);
        LOJoin C7 = new LOJoin(lp, mm7, JOINTYPE.HASH, new boolean[] {true, true});
        C7.neverUseForRealSetSchema(jcschema7);
        lp.add(C7);
        lp.connect(A7, C7);
        lp.connect(B7, C7);
        
        assertFalse(C6.isEqual(C7));
    }
    
    @Test
    public void testJoinDifferentNumJoinKeys() throws IOException {
        LogicalPlan lp = new LogicalPlan();
        
        // Test different join keys
        LogicalSchema jaschema6 = new LogicalSchema();
        jaschema6.addField(new LogicalSchema.LogicalFieldSchema(
           "x", null, DataType.INTEGER));
        LOLoad A6 = new LOLoad(new FileSpec("/abc",
           new FuncSpec("/fooload", new String[] {"x", "y"})), jaschema6, lp);
        lp.add(A6);
        
        // B = load
        LogicalSchema jbschema6 = new LogicalSchema();
        jbschema6.addField(new LogicalSchema.LogicalFieldSchema(
            "y", null, DataType.INTEGER));
        jbschema6.addField(new LogicalSchema.LogicalFieldSchema(
            "z", null, DataType.LONG));
        LOLoad B6 = new LOLoad(new FileSpec("/def",
            new FuncSpec("PigStorage", "\t")), jbschema6, lp);
        lp.add(B6);
        
        // C = join
        LogicalSchema jcschema6 = new LogicalSchema();
        jcschema6.addField(new LogicalSchema.LogicalFieldSchema(
            "x", null, DataType.INTEGER));
        jcschema6.addField(new LogicalSchema.LogicalFieldSchema(
            "y", null, DataType.INTEGER));
        LogicalExpressionPlan aprojplan6 = new LogicalExpressionPlan();
        new ProjectExpression(aprojplan6, DataType.INTEGER, 0, 0);
        LogicalExpressionPlan bprojplan6 = new LogicalExpressionPlan();
        new ProjectExpression(bprojplan6, DataType.INTEGER, 1, 0);
        LogicalExpressionPlan b2projplan6 = new LogicalExpressionPlan();
        new ProjectExpression(b2projplan6, DataType.INTEGER, 1, 1);
        MultiMap<Integer, LogicalExpressionPlan> mm6 = 
            new MultiMap<Integer, LogicalExpressionPlan>();
        mm6.put(0, aprojplan6);
        mm6.put(1, bprojplan6);
        mm6.put(1, b2projplan6);
        LOJoin C6 = new LOJoin(lp, mm6, JOINTYPE.HASH, new boolean[] {true, true});
        C6.neverUseForRealSetSchema(jcschema6);
        lp.add(C6);
        lp.connect(A6, C6);
        lp.connect(B6, C6);
        
        // Test different different number of join keys
        LogicalSchema jaschema8 = new LogicalSchema();
        jaschema8.addField(new LogicalSchema.LogicalFieldSchema(
           "x", null, DataType.INTEGER));
        LOLoad A8 = new LOLoad(new FileSpec("/abc",
           new FuncSpec("/fooload", new String[] {"x", "y"})), jaschema8, lp);
        lp.add(A8);
        
        // B = load
        LogicalSchema jbschema8 = new LogicalSchema();
        jbschema8.addField(new LogicalSchema.LogicalFieldSchema(
            "y", null, DataType.INTEGER));
        jbschema8.addField(new LogicalSchema.LogicalFieldSchema(
            "z", null, DataType.LONG));
        LOLoad B8 = new LOLoad(new FileSpec("/def",
            new FuncSpec("PigStorage", "\t")), jbschema8, lp);
        lp.add(B8);
        
        // C = join
        LogicalSchema jcschema8 = new LogicalSchema();
        jcschema8.addField(new LogicalSchema.LogicalFieldSchema(
            "x", null, DataType.INTEGER));
        jcschema8.addField(new LogicalSchema.LogicalFieldSchema(
            "y", null, DataType.INTEGER));
        LogicalExpressionPlan aprojplan8 = new LogicalExpressionPlan();
        new ProjectExpression(aprojplan8, DataType.INTEGER, 0, 0);
        LogicalExpressionPlan bprojplan8 = new LogicalExpressionPlan();
        new ProjectExpression(bprojplan8, DataType.INTEGER, 1, 0);
        MultiMap<Integer, LogicalExpressionPlan> mm8 = 
            new MultiMap<Integer, LogicalExpressionPlan>();
        mm8.put(0, aprojplan8);
        mm8.put(1, bprojplan8);
        LOJoin C8 = new LOJoin(lp, mm8, JOINTYPE.HASH, new boolean[] {true, true});
        C8.neverUseForRealSetSchema(jcschema8);
        lp.add(C8);
        lp.connect(A8, C8);
        lp.connect(B8, C8);
        
        assertFalse(C6.isEqual(C8));
    }
    
    @Test
    public void testRelationalSameOpDifferentPreds() throws IOException {
        LogicalPlan lp1 = new LogicalPlan();
        LogicalSchema aschema1 = new LogicalSchema();
        aschema1.addField(new LogicalSchema.LogicalFieldSchema(
            "x", null, DataType.INTEGER));
        LOLoad A1 = new LOLoad(new FileSpec("/abc",
            new FuncSpec("/fooload", new String[] {"x", "y"})), aschema1, lp1);
        lp1.add(A1);
        
        LogicalExpressionPlan fp1 = new LogicalExpressionPlan();
        ProjectExpression fy1 = new ProjectExpression(fp1, DataType.INTEGER, 0, 0);
        ConstantExpression fc1 = new ConstantExpression(fp1, DataType.INTEGER,
            new Integer(0));
        new EqualExpression(fp1, fy1, fc1);
        LOFilter D1 = new LOFilter(lp1, fp1);
        LogicalSchema cschema = new LogicalSchema();
        cschema.addField(new LogicalSchema.LogicalFieldSchema(
            "x", null, DataType.INTEGER));
        D1.neverUseForRealSetSchema(cschema);
        lp1.add(D1);
        lp1.connect(A1, D1);
        
        LogicalPlan lp2 = new LogicalPlan();
        LOLoad A2 = new LOLoad(new FileSpec("/abc",
            new FuncSpec("/foo", new String[] {"x", "z"})), null, lp2);
        lp2.add(A2);
        
        LogicalExpressionPlan fp2 = new LogicalExpressionPlan();
        ProjectExpression fy2 = new ProjectExpression(fp2, DataType.INTEGER, 0, 0);
        ConstantExpression fc2 = new ConstantExpression(fp2, DataType.INTEGER,
            new Integer(0));
        new EqualExpression(fp2, fy2, fc2);
        LOFilter D2 = new LOFilter(lp2, fp2);
        D2.neverUseForRealSetSchema(cschema);
        lp2.add(D2);
        lp2.connect(A2, D2);
        
        assertFalse(D1.isEqual(D2));
    }
    
 
}
