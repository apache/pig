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

import org.apache.pig.data.DataType;
import org.apache.pig.experimental.logical.expression.AndExpression;
import org.apache.pig.experimental.logical.expression.ConstantExpression;
import org.apache.pig.experimental.logical.expression.EqualExpression;
import org.apache.pig.experimental.logical.expression.LogicalExpressionPlan;
import org.apache.pig.experimental.logical.expression.LogicalExpressionVisitor;
import org.apache.pig.experimental.logical.expression.ProjectExpression;
import org.apache.pig.experimental.logical.relational.LOLoad;
import org.apache.pig.experimental.logical.relational.LogicalPlan;
import org.apache.pig.experimental.logical.relational.LogicalPlanVisitor;
import org.apache.pig.experimental.logical.relational.LogicalRelationalOperator;
import org.apache.pig.experimental.plan.BaseOperatorPlan;
import org.apache.pig.experimental.plan.DependencyOrderWalker;
import org.apache.pig.experimental.plan.DepthFirstWalker;
import org.apache.pig.experimental.plan.Operator;
import org.apache.pig.experimental.plan.OperatorPlan;
import org.apache.pig.experimental.plan.PlanEdge;
import org.apache.pig.experimental.plan.PlanVisitor;
import org.apache.pig.experimental.plan.PlanWalker;
import org.apache.pig.experimental.plan.ReverseDependencyOrderWalker;
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
        
        public boolean equals(SillyOperator other) {
            return other.name == name;
        }

        @Override
        public void accept(PlanVisitor v) {
            if (v instanceof SillyVisitor) {
                ((SillyVisitor)v).visitSillyOperator(this);
            }
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
        List<Operator> list = plan.getRoots();
        assertEquals(0, list.size());
        list = plan.getLeaves();
        assertEquals(0, list.size());
        
        plan.add(fred);
        plan.add(joe);
        plan.add(bob);
        plan.add(jim);
        plan.add(sam);
        
        // Test that when not connected all nodes are roots and leaves.
        list = plan.getRoots();
        assertEquals(5, list.size());
        list = plan.getLeaves();
        assertEquals(5, list.size());
        
        // Connect them up
        plan.connect(fred, bob);
        plan.connect(joe, bob);
        plan.connect(bob, jim);
        plan.connect(bob, sam);
        
        // Check that the roots and leaves came out right
        list = plan.getRoots();
        assertEquals(2, list.size());
        for (Operator op : list) {
            assertTrue(fred.equals(op) || joe.equals(op));
        }
        list = plan.getLeaves();
        assertEquals(2, list.size());
        for (Operator op : list) {
            assertTrue(jim.equals(op) || sam.equals(op));
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
        list = plan.getRoots();
        assertEquals(2, list.size());
        for (Operator op : list) {
            assertTrue(jim.equals(op) || joe.equals(op));
        }
        list = plan.getLeaves();
        assertEquals(2, list.size());
        for (Operator op : list) {
            assertTrue(fred.equals(op) || sam.equals(op));
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
        
        List<Operator> list = plan.getRoots();
        assertEquals(2, list.size());
        list = plan.getLeaves();
        assertEquals(2, list.size());
        
        plan.remove(fred);
        plan.remove(joe);
        
        assertEquals(0, plan.size());
        
        list = plan.getRoots();
        assertEquals(0, list.size());
        list = plan.getLeaves();
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
        lp.add((LogicalRelationalOperator)null, load,
            (LogicalRelationalOperator)null);
        
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
 
}
