/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.pig.test;

import static org.apache.pig.newplan.logical.relational.LOTestHelper.newLOLoad;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.ExecType;
import org.apache.pig.FuncSpec;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.newplan.BaseOperatorPlan;
import org.apache.pig.newplan.DependencyOrderWalker;
import org.apache.pig.newplan.DepthFirstWalker;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanEdge;
import org.apache.pig.newplan.PlanVisitor;
import org.apache.pig.newplan.PlanWalker;
import org.apache.pig.newplan.ReverseDependencyOrderWalker;
import org.apache.pig.newplan.logical.expression.AndExpression;
import org.apache.pig.newplan.logical.expression.ConstantExpression;
import org.apache.pig.newplan.logical.expression.EqualExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.LogicalExpressionVisitor;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LOJoin;
import org.apache.pig.newplan.logical.relational.LOJoin.JOINTYPE;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalNodesVisitor;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.junit.Before;
import org.junit.Test;

public class TestNewPlanOperatorPlan {

    public static class FooLoad extends PigStorage {
        public FooLoad(String[] params) {
        }

    }

    private static class SillyPlan extends BaseOperatorPlan {

        SillyPlan() {
            super();
        }
    }

    static public class DummyLoad extends PigStorage {
        public DummyLoad(String a, String b) {
        }

        public DummyLoad(String a) {
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
            return (name.compareTo(operator.getName()) == 0);
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

    Configuration conf = null;

    @Before
    public void setUp() throws Exception {
        PigContext pc = new PigContext(ExecType.LOCAL, new Properties());
        pc.connect();
        conf = new Configuration(
                ConfigurationUtil.toConfiguration(pc.getFs().getConfiguration())
                );
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
    public void testOperatorPlan() throws FrontendException {
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
    public void testDisconnectAndRemove() throws FrontendException {
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
        } catch (FrontendException e) {
            caught = true;
        }
        assertTrue(caught);

        caught = false;
        try {
            plan.remove(joe);
        } catch (FrontendException e) {
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
        } catch (FrontendException e) {
            caught = true;
        }
        assertTrue(caught);

    }

    // Tests for DependencyOrderWalker

    @Test
    public void testDependencyOrderWalkerLinear() throws FrontendException {
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
    public void testDependencyOrderWalkerTree() throws FrontendException {
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
    public void testDependencyOrderWalkerGraph() throws FrontendException {
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
    public void testDepthFirstWalkerLinear() throws FrontendException {
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
    public void testDepthFirstWalkerTree() throws FrontendException {
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
    public void testDepthFirstWalkerGraph() throws FrontendException {
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
    public void testReverseDependencyOrderWalkerLinear() throws FrontendException {
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
    public void testReverseDependencyOrderWalkerTree() throws FrontendException {
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
    public void testReverseDependencyOrderWalkerGraph() throws FrontendException {
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

    private static class TestLogicalVisitor extends LogicalRelationalNodesVisitor {

        StringBuffer bf = new StringBuffer();

        protected TestLogicalVisitor(OperatorPlan plan) throws FrontendException {
            super(plan, new DepthFirstWalker(plan));
        }

        @Override
        public void visit(LOLoad load) {
            bf.append("load ");
        }

        String getVisitPlan() {
            return bf.toString();
        }

    }

    @Test
    public void testLogicalPlanVisitor() throws FrontendException {
        LogicalPlan lp = new LogicalPlan();
        LOLoad load = newLOLoad(null, null, lp, conf);
        /*
         * lp.add((LogicalRelationalOperator)null, load,
         * (LogicalRelationalOperator)null);
         */
        lp.add(load);

        TestLogicalVisitor v = new TestLogicalVisitor(lp);
        v.visit();

        assertEquals("load ", v.getVisitPlan());
    }

    @Test
    public void testBinaryOperatorOrder() throws FrontendException {
        LogicalExpressionPlan ep = new LogicalExpressionPlan();
        ConstantExpression c = new ConstantExpression(ep, new Integer(5));
        ProjectExpression p = new ProjectExpression(ep, 0, 0, null);
        EqualExpression e = new EqualExpression(ep, p, c);
        assertEquals(p, e.getLhs());
        assertEquals(c, e.getRhs());
    }

    private static class TestExpressionVisitor extends LogicalExpressionVisitor {

        StringBuffer bf = new StringBuffer();

        protected TestExpressionVisitor(OperatorPlan plan) throws FrontendException {
            super(plan, new DepthFirstWalker(plan));
        }

        @Override
        public void visit(AndExpression andExpr) {
            bf.append("and ");
        }

        @Override
        public void visit(EqualExpression equal) {
            bf.append("equal ");
        }

        @Override
        public void visit(ProjectExpression project) {
            bf.append("project ");
        }

        @Override
        public void visit(ConstantExpression constant) {
            bf.append("constant ");
        }

        String getVisitPlan() {
            return bf.toString();
        }
    }

    @Test
    public void testExpressionPlanVisitor() throws FrontendException {
        LogicalExpressionPlan ep = new LogicalExpressionPlan();
        ConstantExpression c = new ConstantExpression(ep, new Integer(5));
        ProjectExpression p = new ProjectExpression(ep, 0, 0, null);
        EqualExpression e = new EqualExpression(ep, p, c);
        ConstantExpression c2 = new ConstantExpression(ep, new Boolean("true"));
        new AndExpression(ep, e, c2);

        TestExpressionVisitor v = new TestExpressionVisitor(ep);
        v.visit();
        assertEquals("and equal project constant constant ", v.getVisitPlan());
    }

    @Test
    public void testExpressionEquality() throws FrontendException {
        LogicalExpressionPlan ep1 = new LogicalExpressionPlan();
        ConstantExpression c1 = new ConstantExpression(ep1, new Integer(5));
        ProjectExpression p1 = new ProjectExpression(ep1, 0, 0, null);
        EqualExpression e1 = new EqualExpression(ep1, p1, c1);
        ConstantExpression ca1 = new ConstantExpression(ep1, new Boolean("true"));
        AndExpression a1 = new AndExpression(ep1, e1, ca1);

        LogicalExpressionPlan ep2 = new LogicalExpressionPlan();
        ConstantExpression c2 = new ConstantExpression(ep2, new Integer(5));
        ProjectExpression p2 = new ProjectExpression(ep2, 0, 0, null);
        EqualExpression e2 = new EqualExpression(ep2, p2, c2);
        ConstantExpression ca2 = new ConstantExpression(ep2, new Boolean("true"));
        AndExpression a2 = new AndExpression(ep2, e2, ca2);

        assertTrue(ep1.isEqual(ep2));
        assertTrue(c1.isEqual(c2));
        assertTrue(p1.isEqual(p2));
        assertTrue(e1.isEqual(e2));
        assertTrue(ca1.isEqual(ca2));
        assertTrue(a1.isEqual(a2));

        LogicalExpressionPlan ep3 = new LogicalExpressionPlan();
        ConstantExpression c3 = new ConstantExpression(ep3, new Integer(3));
        ProjectExpression p3 = new ProjectExpression(ep3, 0, 1, null);
        EqualExpression e3 = new EqualExpression(ep3, p3, c3);
        ConstantExpression ca3 = new ConstantExpression(ep3, "true");
        AndExpression a3 = new AndExpression(ep3, e3, ca3);

        assertFalse(ep1.isEqual(ep3));
        assertFalse(c1.isEqual(c3));
        assertFalse(p1.isEqual(p3));
        assertFalse(e1.isEqual(e3));
        assertFalse(ca1.isEqual(ca3));
        assertFalse(a1.isEqual(a3));

        LogicalExpressionPlan ep4 = new LogicalExpressionPlan();
        ProjectExpression p4 = new ProjectExpression(ep4, 1, 0, null);

        assertFalse(ep1.isEqual(ep4));
        assertFalse(p1.isEqual(p4));
    }

    @Test
    public void testRelationalEquality() throws FrontendException {
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
            LOLoad A = newLOLoad(new FileSpec("/abc",
                    new FuncSpec(DummyLoad.class.getName(), new String[] { "x", "y" })), aschema,
                    lp, conf);
            lp.add(A);

            // B = load
            LogicalSchema bschema = new LogicalSchema();
            bschema.addField(new LogicalSchema.LogicalFieldSchema(
                    "y", null, DataType.INTEGER));
            LOLoad B = newLOLoad(new FileSpec("/def",
                    new FuncSpec("PigStorage", "\t")), bschema, lp, conf);
            lp.add(B);

            // C = join
            LogicalSchema cschema = new LogicalSchema();
            cschema.addField(new LogicalSchema.LogicalFieldSchema(
                    "x", null, DataType.INTEGER));
            cschema.addField(new LogicalSchema.LogicalFieldSchema(
                    "y", null, DataType.INTEGER));
            LogicalExpressionPlan aprojplan = new LogicalExpressionPlan();
            new ProjectExpression(aprojplan, 0, 0, null);
            LogicalExpressionPlan bprojplan = new LogicalExpressionPlan();
            new ProjectExpression(bprojplan, 1, 0, null);
            MultiMap<Integer, LogicalExpressionPlan> mm =
                    new MultiMap<Integer, LogicalExpressionPlan>();
            mm.put(0, aprojplan);
            mm.put(1, bprojplan);
            LOJoin C = new LOJoin(lp, mm, JOINTYPE.HASH, new boolean[] { true, true });
            C.neverUseForRealSetSchema(cschema);
            lp.add(C);
            lp.connect(A, C);
            lp.connect(B, C);

            // D = filter
            LogicalExpressionPlan filterPlan = new LogicalExpressionPlan();
            ProjectExpression fy = new ProjectExpression(filterPlan, 0, 1, null);
            ConstantExpression fc = new ConstantExpression(filterPlan, new Integer(0));
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
            LOLoad A = newLOLoad(new FileSpec("/abc",
                    new FuncSpec(DummyLoad.class.getName(), new String[] { "x", "y" })), aschema,
                    lp1, conf);
            lp1.add(A);

            // B = load
            LogicalSchema bschema = new LogicalSchema();
            bschema.addField(new LogicalSchema.LogicalFieldSchema(
                    "y", null, DataType.INTEGER));
            LOLoad B = newLOLoad(new FileSpec("/def",
                    new FuncSpec("PigStorage", "\t")), bschema, lp1, conf);
            lp1.add(B);

            // C = join
            LogicalSchema cschema = new LogicalSchema();
            cschema.addField(new LogicalSchema.LogicalFieldSchema(
                    "x", null, DataType.INTEGER));
            cschema.addField(new LogicalSchema.LogicalFieldSchema(
                    "y", null, DataType.INTEGER));
            LogicalExpressionPlan aprojplan = new LogicalExpressionPlan();
            new ProjectExpression(aprojplan, 0, 0, null);
            LogicalExpressionPlan bprojplan = new LogicalExpressionPlan();
            new ProjectExpression(bprojplan, 1, 0, null);
            MultiMap<Integer, LogicalExpressionPlan> mm =
                    new MultiMap<Integer, LogicalExpressionPlan>();
            mm.put(0, aprojplan);
            mm.put(1, bprojplan);
            LOJoin C = new LOJoin(lp1, mm, JOINTYPE.HASH, new boolean[] { true, true });
            C.neverUseForRealSetSchema(cschema);
            lp1.add(C);
            lp1.connect(A, C);
            lp1.connect(B, C);

            // D = filter
            LogicalExpressionPlan filterPlan = new LogicalExpressionPlan();
            ProjectExpression fy = new ProjectExpression(filterPlan, 0, 1, null);
            ConstantExpression fc = new ConstantExpression(filterPlan, new Integer(0));
            new EqualExpression(filterPlan, fy, fc);
            LOFilter D = new LOFilter(lp1, filterPlan);
            D.neverUseForRealSetSchema(cschema);
            lp1.add(D);
            lp1.connect(C, D);

        }

        assertTrue(lp.isEqual(lp1));
    }

    @Test
    public void testLoadEqualityDifferentFuncSpecCtorArgs() throws FrontendException {
        LogicalPlan lp = new LogicalPlan();

        LogicalSchema aschema1 = new LogicalSchema();
        aschema1.addField(new LogicalSchema.LogicalFieldSchema(
                "x", null, DataType.INTEGER));
        LOLoad load1 = newLOLoad(new FileSpec("/abc",
                new FuncSpec(DummyLoad.class.getName(), new String[] { "x", "y" })), aschema1, lp,
                conf);
        lp.add(load1);

        LOLoad load2 = newLOLoad(new FileSpec("/abc",
                new FuncSpec(DummyLoad.class.getName(), new String[] { "x", "z" })), aschema1, lp,
                conf);
        lp.add(load2);

        assertFalse(load1.isEqual(load2));
    }

    @Test
    public void testLoadEqualityDifferentNumFuncSpecCstorArgs() throws FrontendException {
        LogicalPlan lp = new LogicalPlan();

        LogicalSchema aschema1 = new LogicalSchema();
        aschema1.addField(new LogicalSchema.LogicalFieldSchema(
                "x", null, DataType.INTEGER));
        LOLoad load1 = newLOLoad(new FileSpec("/abc",
                new FuncSpec(DummyLoad.class.getName(), new String[] { "x", "y" })), aschema1, lp,
                conf);
        lp.add(load1);

        LOLoad load3 = newLOLoad(new FileSpec("/abc",
                new FuncSpec(DummyLoad.class.getName(), "x")), aschema1, lp, conf);
        lp.add(load3);

        assertFalse(load1.isEqual(load3));
    }

    @Test
    public void testLoadEqualityDifferentFunctionNames() throws FrontendException {
        LogicalPlan lp = new LogicalPlan();

        LogicalSchema aschema1 = new LogicalSchema();
        aschema1.addField(new LogicalSchema.LogicalFieldSchema(
                "x", null, DataType.INTEGER));
        LOLoad load1 = newLOLoad(new FileSpec("/abc",
                new FuncSpec(DummyLoad.class.getName(), new String[] { "x", "y" })), aschema1, lp,
                conf);
        lp.add(load1);

        // Different function names in FuncSpec
        LOLoad load4 = newLOLoad(new FileSpec("/abc",
                new FuncSpec(DummyLoad.class.getName(), new String[] { "x", "z" })), aschema1, lp,
                conf);
        lp.add(load4);

        assertFalse(load1.isEqual(load4));
    }

    @Test
    public void testLoadEqualityDifferentFileName() throws FrontendException {
        LogicalPlan lp = new LogicalPlan();
        LogicalSchema aschema1 = new LogicalSchema();
        aschema1.addField(new LogicalSchema.LogicalFieldSchema(
                "x", null, DataType.INTEGER));
        LOLoad load1 = newLOLoad(new FileSpec("/abc",
                new FuncSpec(DummyLoad.class.getName(), new String[] { "x", "y" })), aschema1, lp,
                conf);
        lp.add(load1);

        // Different file name
        LOLoad load5 = newLOLoad(new FileSpec("/def",
                new FuncSpec(DummyLoad.class.getName(), new String[] { "x", "z" })), aschema1, lp,
                conf);
        lp.add(load5);

        assertFalse(load1.isEqual(load5));
    }

    @Test
    public void testRelationalEqualityDifferentSchema() throws FrontendException {
        LogicalPlan lp = new LogicalPlan();
        LogicalSchema aschema1 = new LogicalSchema();
        aschema1.addField(new LogicalSchema.LogicalFieldSchema(
                "x", null, DataType.INTEGER));
        LOLoad load1 = newLOLoad(new FileSpec("/abc",
                new FuncSpec(DummyLoad.class.getName(), new String[] { "x", "y" })), aschema1, lp,
                conf);
        lp.add(load1);

        // Different schema
        LogicalSchema aschema2 = new LogicalSchema();
        aschema2.addField(new LogicalSchema.LogicalFieldSchema(
                "x", null, DataType.CHARARRAY));

        LOLoad load6 = newLOLoad(new FileSpec("/abc",
                new FuncSpec(DummyLoad.class.getName(), new String[] { "x", "z" })), aschema2, lp,
                conf);
        lp.add(load6);

        assertFalse(load1.isEqual(load6));
    }

    @Test
    public void testRelationalEqualityNullSchemas() throws FrontendException {
        LogicalPlan lp = new LogicalPlan();
        // Test that two loads with no schema are still equal
        LOLoad load7 = newLOLoad(new FileSpec("/abc",
                new FuncSpec(DummyLoad.class.getName(), new String[] { "x", "y" })), null, lp, conf);
        lp.add(load7);

        LOLoad load8 = newLOLoad(new FileSpec("/abc",
                new FuncSpec(DummyLoad.class.getName(), new String[] { "x", "y" })), null, lp, conf);
        lp.add(load8);

        assertTrue(load7.isEqual(load8));
    }

    @Test
    public void testRelationalEqualityOneNullOneNotNullSchema() throws FrontendException {
        LogicalPlan lp = new LogicalPlan();
        LogicalSchema aschema1 = new LogicalSchema();
        aschema1.addField(new LogicalSchema.LogicalFieldSchema(
                "x", null, DataType.INTEGER));
        LOLoad load1 = newLOLoad(new FileSpec("/abc",
                new FuncSpec(DummyLoad.class.getName(), new String[] { "x", "y" })), aschema1, lp,
                conf);
        lp.add(load1);

        // Test that one with schema and one without breaks equality
        LOLoad load9 = newLOLoad(new FileSpec("/abc",
                new FuncSpec(DummyLoad.class.getName(), new String[] { "x", "z" })), null, lp, conf);
        lp.add(load9);

        assertFalse(load1.isEqual(load9));
    }

    @Test
    public void testFilterDifferentPredicates() throws FrontendException {
        LogicalPlan lp = new LogicalPlan();

        LogicalExpressionPlan fp1 = new LogicalExpressionPlan();
        ProjectExpression fy1 = new ProjectExpression(fp1, 0, 1, null);
        ConstantExpression fc1 = new ConstantExpression(fp1,
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
        ProjectExpression fy2 = new ProjectExpression(fp2, 0, 1, null);
        ConstantExpression fc2 = new ConstantExpression(fp2,
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
    public void testJoinDifferentJoinTypes() throws FrontendException {
        LogicalPlan lp = new LogicalPlan();
        LogicalSchema jaschema1 = new LogicalSchema();
        jaschema1.addField(new LogicalSchema.LogicalFieldSchema(
                "x", null, DataType.INTEGER));
        LOLoad A1 = newLOLoad(new FileSpec("/abc",
                new FuncSpec("org.apache.pig.test.TestNewPlanOperatorPlan$FooLoad", new String[] {
                                "x", "y" })), jaschema1, lp, conf);
        lp.add(A1);

        // B = load
        LogicalSchema jbschema1 = new LogicalSchema();
        jbschema1.addField(new LogicalSchema.LogicalFieldSchema(
                "y", null, DataType.INTEGER));
        LOLoad B1 = newLOLoad(new FileSpec("/def",
                new FuncSpec("PigStorage", "\t")), jbschema1, lp, conf);
        lp.add(B1);

        // C = join
        LogicalSchema jcschema1 = new LogicalSchema();
        jcschema1.addField(new LogicalSchema.LogicalFieldSchema(
                "x", null, DataType.INTEGER));
        jcschema1.addField(new LogicalSchema.LogicalFieldSchema(
                "y", null, DataType.INTEGER));
        LogicalExpressionPlan aprojplan1 = new LogicalExpressionPlan();
        new ProjectExpression(aprojplan1, 0, 0, null);
        LogicalExpressionPlan bprojplan1 = new LogicalExpressionPlan();
        new ProjectExpression(bprojplan1, 1, 0, null);
        MultiMap<Integer, LogicalExpressionPlan> mm1 =
                new MultiMap<Integer, LogicalExpressionPlan>();
        mm1.put(0, aprojplan1);
        mm1.put(1, bprojplan1);
        LOJoin C1 = new LOJoin(lp, mm1, JOINTYPE.HASH, new boolean[] { true, true });
        C1.neverUseForRealSetSchema(jcschema1);
        lp.add(C1);
        lp.connect(A1, C1);
        lp.connect(B1, C1);

        // A = load
        LogicalSchema jaschema2 = new LogicalSchema();
        jaschema2.addField(new LogicalSchema.LogicalFieldSchema(
                "x", null, DataType.INTEGER));
        LOLoad A2 = newLOLoad(new FileSpec("/abc",
                new FuncSpec("org.apache.pig.test.TestNewPlanOperatorPlan$FooLoad", new String[] {
                                "x", "y" })), jaschema2, lp, conf);
        lp.add(A2);

        // B = load
        LogicalSchema jbschema2 = new LogicalSchema();
        jbschema2.addField(new LogicalSchema.LogicalFieldSchema(
                "y", null, DataType.INTEGER));
        LOLoad B2 = newLOLoad(new FileSpec("/def",
                new FuncSpec("PigStorage", "\t")), jbschema2, lp, conf);
        lp.add(B2);

        // C = join
        LogicalSchema jcschema2 = new LogicalSchema();
        jcschema2.addField(new LogicalSchema.LogicalFieldSchema(
                "x", null, DataType.INTEGER));
        jcschema2.addField(new LogicalSchema.LogicalFieldSchema(
                "y", null, DataType.INTEGER));
        LogicalExpressionPlan aprojplan2 = new LogicalExpressionPlan();
        new ProjectExpression(aprojplan2, 0, 0, null);
        LogicalExpressionPlan bprojplan2 = new LogicalExpressionPlan();
        new ProjectExpression(bprojplan2, 1, 0, null);
        MultiMap<Integer, LogicalExpressionPlan> mm2 =
                new MultiMap<Integer, LogicalExpressionPlan>();
        mm2.put(0, aprojplan2);
        mm2.put(1, bprojplan2);
        LOJoin C2 = new LOJoin(lp, mm2, JOINTYPE.SKEWED, new boolean[] { true, true });
        C2.neverUseForRealSetSchema(jcschema2);
        lp.add(C2);
        lp.connect(A2, C2);
        lp.connect(B2, C2);

        assertFalse(C1.isEqual(C2));
    }

    @Test
    public void testJoinDifferentInner() throws FrontendException {
        LogicalPlan lp = new LogicalPlan();
        LogicalSchema jaschema1 = new LogicalSchema();
        jaschema1.addField(new LogicalSchema.LogicalFieldSchema(
                "x", null, DataType.INTEGER));
        LOLoad A1 = newLOLoad(new FileSpec("/abc",
                new FuncSpec("org.apache.pig.test.TestNewPlanOperatorPlan$FooLoad", new String[] {
                                "x", "y" })), jaschema1, lp, conf);
        lp.add(A1);

        // B = load
        LogicalSchema jbschema1 = new LogicalSchema();
        jbschema1.addField(new LogicalSchema.LogicalFieldSchema(
                "y", null, DataType.INTEGER));
        LOLoad B1 = newLOLoad(new FileSpec("/def",
                new FuncSpec("PigStorage", "\t")), jbschema1, lp, conf);
        lp.add(B1);

        // C = join
        LogicalSchema jcschema1 = new LogicalSchema();
        jcschema1.addField(new LogicalSchema.LogicalFieldSchema(
                "x", null, DataType.INTEGER));
        jcschema1.addField(new LogicalSchema.LogicalFieldSchema(
                "y", null, DataType.INTEGER));
        LogicalExpressionPlan aprojplan1 = new LogicalExpressionPlan();
        new ProjectExpression(aprojplan1, 0, 0, null);
        LogicalExpressionPlan bprojplan1 = new LogicalExpressionPlan();
        new ProjectExpression(bprojplan1, 1, 0, null);
        MultiMap<Integer, LogicalExpressionPlan> mm1 =
                new MultiMap<Integer, LogicalExpressionPlan>();
        mm1.put(0, aprojplan1);
        mm1.put(1, bprojplan1);
        LOJoin C1 = new LOJoin(lp, mm1, JOINTYPE.HASH, new boolean[] { true, true });
        C1.neverUseForRealSetSchema(jcschema1);
        lp.add(C1);
        lp.connect(A1, C1);
        lp.connect(B1, C1);

        // Test different inner status
        // A = load
        LogicalSchema jaschema3 = new LogicalSchema();
        jaschema3.addField(new LogicalSchema.LogicalFieldSchema(
                "x", null, DataType.INTEGER));
        LOLoad A3 = newLOLoad(new FileSpec("/abc",
                new FuncSpec("org.apache.pig.test.TestNewPlanOperatorPlan$FooLoad", new String[] {
                                "x", "y" })), jaschema3, lp, conf);
        lp.add(A3);

        // B = load
        LogicalSchema jbschema3 = new LogicalSchema();
        jbschema3.addField(new LogicalSchema.LogicalFieldSchema(
                "y", null, DataType.INTEGER));
        LOLoad B3 = newLOLoad(new FileSpec("/def",
                new FuncSpec("PigStorage", "\t")), jbschema3, lp, conf);
        lp.add(B3);

        // C = join
        LogicalSchema jcschema3 = new LogicalSchema();
        jcschema3.addField(new LogicalSchema.LogicalFieldSchema(
                "x", null, DataType.INTEGER));
        jcschema3.addField(new LogicalSchema.LogicalFieldSchema(
                "y", null, DataType.INTEGER));
        LogicalExpressionPlan aprojplan3 = new LogicalExpressionPlan();
        new ProjectExpression(aprojplan3, 0, 0, null);
        LogicalExpressionPlan bprojplan3 = new LogicalExpressionPlan();
        new ProjectExpression(bprojplan3, 1, 0, null);
        MultiMap<Integer, LogicalExpressionPlan> mm3 =
                new MultiMap<Integer, LogicalExpressionPlan>();
        mm3.put(0, aprojplan3);
        mm3.put(1, bprojplan3);
        LOJoin C3 = new LOJoin(lp, mm3, JOINTYPE.HASH, new boolean[] { true, false });
        C3.neverUseForRealSetSchema(jcschema3);
        lp.add(C3);
        lp.connect(A3, C3);
        lp.connect(B3, C3);

        assertFalse(C1.isEqual(C3));
    }

    @Test
    public void testJoinDifferentNumInputs() throws FrontendException {
        LogicalPlan lp = new LogicalPlan();
        LogicalSchema jaschema1 = new LogicalSchema();
        jaschema1.addField(new LogicalSchema.LogicalFieldSchema(
                "x", null, DataType.INTEGER));
        LOLoad A1 = newLOLoad(new FileSpec("/abc",
                new FuncSpec("org.apache.pig.test.TestNewPlanOperatorPlan$FooLoad", new String[] {
                                "x", "y" })), jaschema1, lp, conf);
        lp.add(A1);

        // B = load
        LogicalSchema jbschema1 = new LogicalSchema();
        jbschema1.addField(new LogicalSchema.LogicalFieldSchema(
                "y", null, DataType.INTEGER));
        LOLoad B1 = newLOLoad(new FileSpec("/def",
                new FuncSpec("PigStorage", "\t")), jbschema1, lp, conf);
        lp.add(B1);

        // C = join
        LogicalSchema jcschema1 = new LogicalSchema();
        jcschema1.addField(new LogicalSchema.LogicalFieldSchema(
                "x", null, DataType.INTEGER));
        jcschema1.addField(new LogicalSchema.LogicalFieldSchema(
                "y", null, DataType.INTEGER));
        LogicalExpressionPlan aprojplan1 = new LogicalExpressionPlan();
        new ProjectExpression(aprojplan1, 0, 0, null);
        LogicalExpressionPlan bprojplan1 = new LogicalExpressionPlan();
        new ProjectExpression(bprojplan1, 1, 0, null);
        MultiMap<Integer, LogicalExpressionPlan> mm1 =
                new MultiMap<Integer, LogicalExpressionPlan>();
        mm1.put(0, aprojplan1);
        mm1.put(1, bprojplan1);
        LOJoin C1 = new LOJoin(lp, mm1, JOINTYPE.HASH, new boolean[] { true, true });
        C1.neverUseForRealSetSchema(jcschema1);
        lp.add(C1);
        lp.connect(A1, C1);
        lp.connect(B1, C1);

        // A = load
        LogicalSchema jaschema5 = new LogicalSchema();
        jaschema5.addField(new LogicalSchema.LogicalFieldSchema(
                "x", null, DataType.INTEGER));
        LOLoad A5 = newLOLoad(new FileSpec("/abc",
                new FuncSpec("org.apache.pig.test.TestNewPlanOperatorPlan$FooLoad", new String[] {
                                "x", "y" })), jaschema5, lp, conf);
        lp.add(A5);

        // B = load
        LogicalSchema jbschema5 = new LogicalSchema();
        jbschema5.addField(new LogicalSchema.LogicalFieldSchema(
                "y", null, DataType.INTEGER));
        LOLoad B5 = newLOLoad(new FileSpec("/def",
                new FuncSpec("PigStorage", "\t")), jbschema5, lp, conf);
        lp.add(B5);

        // Beta = load
        LogicalSchema jbetaschema5 = new LogicalSchema();
        jbetaschema5.addField(new LogicalSchema.LogicalFieldSchema(
                "y", null, DataType.INTEGER));
        LOLoad Beta5 = newLOLoad(new FileSpec("/ghi",
                new FuncSpec("PigStorage", "\t")), jbetaschema5, lp, conf);
        lp.add(Beta5);

        // C = join
        LogicalSchema jcschema5 = new LogicalSchema();
        jcschema5.addField(new LogicalSchema.LogicalFieldSchema(
                "x", null, DataType.INTEGER));
        jcschema5.addField(new LogicalSchema.LogicalFieldSchema(
                "y", null, DataType.INTEGER));
        LogicalExpressionPlan aprojplan5 = new LogicalExpressionPlan();
        new ProjectExpression(aprojplan5, 0, 0, null);
        LogicalExpressionPlan bprojplan5 = new LogicalExpressionPlan();
        new ProjectExpression(bprojplan5, 1, 0, null);
        LogicalExpressionPlan betaprojplan5 = new LogicalExpressionPlan();
        new ProjectExpression(betaprojplan5, 1, 0, null);
        MultiMap<Integer, LogicalExpressionPlan> mm5 =
                new MultiMap<Integer, LogicalExpressionPlan>();
        mm5.put(0, aprojplan5);
        mm5.put(1, bprojplan5);
        mm5.put(2, betaprojplan5);
        LOJoin C5 = new LOJoin(lp, mm5, JOINTYPE.HASH, new boolean[] { true, true });
        C5.neverUseForRealSetSchema(jcschema5);
        lp.add(C5);
        lp.connect(A5, C5);
        lp.connect(B5, C5);
        lp.connect(Beta5, C5);

        assertFalse(C1.isEqual(C5));
    }

    @Test
    public void testJoinDifferentJoinKeys() throws FrontendException {
        LogicalPlan lp = new LogicalPlan();

        // Test different join keys
        LogicalSchema jaschema6 = new LogicalSchema();
        jaschema6.addField(new LogicalSchema.LogicalFieldSchema(
                "x", null, DataType.INTEGER));
        LOLoad A6 = newLOLoad(new FileSpec("/abc",
                new FuncSpec("org.apache.pig.test.TestNewPlanOperatorPlan$FooLoad", new String[] {
                                "x", "y" })), jaschema6, lp, conf);
        lp.add(A6);

        // B = load
        LogicalSchema jbschema6 = new LogicalSchema();
        jbschema6.addField(new LogicalSchema.LogicalFieldSchema(
                "y", null, DataType.INTEGER));
        jbschema6.addField(new LogicalSchema.LogicalFieldSchema(
                "z", null, DataType.LONG));
        LOLoad B6 = newLOLoad(new FileSpec("/def",
                new FuncSpec("PigStorage", "\t")), jbschema6, lp, conf);
        lp.add(B6);

        // C = join
        LogicalSchema jcschema6 = new LogicalSchema();
        jcschema6.addField(new LogicalSchema.LogicalFieldSchema(
                "x", null, DataType.INTEGER));
        jcschema6.addField(new LogicalSchema.LogicalFieldSchema(
                "y", null, DataType.INTEGER));
        LogicalExpressionPlan aprojplan6 = new LogicalExpressionPlan();
        new ProjectExpression(aprojplan6, 0, 0, null);
        LogicalExpressionPlan bprojplan6 = new LogicalExpressionPlan();
        new ProjectExpression(bprojplan6, 1, 0, null);
        LogicalExpressionPlan b2projplan6 = new LogicalExpressionPlan();
        new ProjectExpression(b2projplan6, 1, 1, null);
        MultiMap<Integer, LogicalExpressionPlan> mm6 =
                new MultiMap<Integer, LogicalExpressionPlan>();
        mm6.put(0, aprojplan6);
        mm6.put(1, bprojplan6);
        mm6.put(1, b2projplan6);
        LOJoin C6 = new LOJoin(lp, mm6, JOINTYPE.HASH, new boolean[] { true, true });
        C6.neverUseForRealSetSchema(jcschema6);
        lp.add(C6);
        lp.connect(A6, C6);
        lp.connect(B6, C6);

        LogicalSchema jaschema7 = new LogicalSchema();
        jaschema7.addField(new LogicalSchema.LogicalFieldSchema(
                "x", null, DataType.INTEGER));
        LOLoad A7 = newLOLoad(new FileSpec("/abc",
                new FuncSpec("org.apache.pig.test.TestNewPlanOperatorPlan$FooLoad", new String[] {
                                "x", "y" })), jaschema7, lp, conf);
        lp.add(A7);

        // B = load
        LogicalSchema jbschema7 = new LogicalSchema();
        jbschema7.addField(new LogicalSchema.LogicalFieldSchema(
                "y", null, DataType.INTEGER));
        jbschema7.addField(new LogicalSchema.LogicalFieldSchema(
                "z", null, DataType.LONG));
        LOLoad B7 = newLOLoad(new FileSpec("/def",
                new FuncSpec("PigStorage", "\t")), jbschema7, lp, conf);
        lp.add(B7);

        // C = join
        LogicalSchema jcschema7 = new LogicalSchema();
        jcschema7.addField(new LogicalSchema.LogicalFieldSchema(
                "x", null, DataType.INTEGER));
        jcschema7.addField(new LogicalSchema.LogicalFieldSchema(
                "y", null, DataType.INTEGER));
        LogicalExpressionPlan aprojplan7 = new LogicalExpressionPlan();
        new ProjectExpression(aprojplan7, 0, 0, null);
        LogicalExpressionPlan bprojplan7 = new LogicalExpressionPlan();
        new ProjectExpression(bprojplan7, 1, 1, null);
        LogicalExpressionPlan b2projplan7 = new LogicalExpressionPlan();
        new ProjectExpression(b2projplan7, 1, 0, null);
        MultiMap<Integer, LogicalExpressionPlan> mm7 =
                new MultiMap<Integer, LogicalExpressionPlan>();
        mm7.put(0, aprojplan7);
        mm7.put(1, bprojplan7);
        mm7.put(1, b2projplan7);
        LOJoin C7 = new LOJoin(lp, mm7, JOINTYPE.HASH, new boolean[] { true, true });
        C7.neverUseForRealSetSchema(jcschema7);
        lp.add(C7);
        lp.connect(A7, C7);
        lp.connect(B7, C7);

        assertFalse(C6.isEqual(C7));
    }

    @Test
    public void testJoinDifferentNumJoinKeys() throws FrontendException {
        LogicalPlan lp = new LogicalPlan();

        // Test different join keys
        LogicalSchema jaschema6 = new LogicalSchema();
        jaschema6.addField(new LogicalSchema.LogicalFieldSchema(
                "x", null, DataType.INTEGER));
        LOLoad A6 = newLOLoad(new FileSpec("/abc",
                new FuncSpec("org.apache.pig.test.TestNewPlanOperatorPlan$FooLoad", new String[] {
                                "x", "y" })), jaschema6, lp, conf);
        lp.add(A6);

        // B = load
        LogicalSchema jbschema6 = new LogicalSchema();
        jbschema6.addField(new LogicalSchema.LogicalFieldSchema(
                "y", null, DataType.INTEGER));
        jbschema6.addField(new LogicalSchema.LogicalFieldSchema(
                "z", null, DataType.LONG));
        LOLoad B6 = newLOLoad(new FileSpec("/def",
                new FuncSpec("PigStorage", "\t")), jbschema6, lp, conf);
        lp.add(B6);

        // C = join
        LogicalSchema jcschema6 = new LogicalSchema();
        jcschema6.addField(new LogicalSchema.LogicalFieldSchema(
                "x", null, DataType.INTEGER));
        jcschema6.addField(new LogicalSchema.LogicalFieldSchema(
                "y", null, DataType.INTEGER));
        LogicalExpressionPlan aprojplan6 = new LogicalExpressionPlan();
        new ProjectExpression(aprojplan6, 0, 0, null);
        LogicalExpressionPlan bprojplan6 = new LogicalExpressionPlan();
        new ProjectExpression(bprojplan6, 1, 0, null);
        LogicalExpressionPlan b2projplan6 = new LogicalExpressionPlan();
        new ProjectExpression(b2projplan6, 1, 1, null);
        MultiMap<Integer, LogicalExpressionPlan> mm6 =
                new MultiMap<Integer, LogicalExpressionPlan>();
        mm6.put(0, aprojplan6);
        mm6.put(1, bprojplan6);
        mm6.put(1, b2projplan6);
        LOJoin C6 = new LOJoin(lp, mm6, JOINTYPE.HASH, new boolean[] { true, true });
        C6.neverUseForRealSetSchema(jcschema6);
        lp.add(C6);
        lp.connect(A6, C6);
        lp.connect(B6, C6);

        // Test different different number of join keys
        LogicalSchema jaschema8 = new LogicalSchema();
        jaschema8.addField(new LogicalSchema.LogicalFieldSchema(
                "x", null, DataType.INTEGER));
        LOLoad A8 = newLOLoad(new FileSpec("/abc",
                new FuncSpec("org.apache.pig.test.TestNewPlanOperatorPlan$FooLoad", new String[] {
                                "x", "y" })), jaschema8, lp, conf);
        lp.add(A8);

        // B = load
        LogicalSchema jbschema8 = new LogicalSchema();
        jbschema8.addField(new LogicalSchema.LogicalFieldSchema(
                "y", null, DataType.INTEGER));
        jbschema8.addField(new LogicalSchema.LogicalFieldSchema(
                "z", null, DataType.LONG));
        LOLoad B8 = newLOLoad(new FileSpec("/def",
                new FuncSpec("PigStorage", "\t")), jbschema8, lp, conf);
        lp.add(B8);

        // C = join
        LogicalSchema jcschema8 = new LogicalSchema();
        jcschema8.addField(new LogicalSchema.LogicalFieldSchema(
                "x", null, DataType.INTEGER));
        jcschema8.addField(new LogicalSchema.LogicalFieldSchema(
                "y", null, DataType.INTEGER));
        LogicalExpressionPlan aprojplan8 = new LogicalExpressionPlan();
        new ProjectExpression(aprojplan8, 0, 0, null);
        LogicalExpressionPlan bprojplan8 = new LogicalExpressionPlan();
        new ProjectExpression(bprojplan8, 1, 0, null);
        MultiMap<Integer, LogicalExpressionPlan> mm8 =
                new MultiMap<Integer, LogicalExpressionPlan>();
        mm8.put(0, aprojplan8);
        mm8.put(1, bprojplan8);
        LOJoin C8 = new LOJoin(lp, mm8, JOINTYPE.HASH, new boolean[] { true, true });
        C8.neverUseForRealSetSchema(jcschema8);
        lp.add(C8);
        lp.connect(A8, C8);
        lp.connect(B8, C8);

        assertFalse(C6.isEqual(C8));
    }

    @Test
    public void testRelationalSameOpDifferentPreds() throws FrontendException {
        LogicalPlan lp1 = new LogicalPlan();
        LogicalSchema aschema1 = new LogicalSchema();
        aschema1.addField(new LogicalSchema.LogicalFieldSchema(
                "x", null, DataType.INTEGER));
        LOLoad A1 = newLOLoad(new FileSpec("/abc",
                new FuncSpec(DummyLoad.class.getName(), new String[] { "x", "y" })), aschema1, lp1,
                conf);
        lp1.add(A1);

        LogicalExpressionPlan fp1 = new LogicalExpressionPlan();
        ProjectExpression fy1 = new ProjectExpression(fp1, 0, 0, null);
        ConstantExpression fc1 = new ConstantExpression(fp1,
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
        LOLoad A2 = newLOLoad(new FileSpec("/abc",
                new FuncSpec(DummyLoad.class.getName(), new String[] { "x", "z" })), null, lp2,
                conf);
        lp2.add(A2);

        LogicalExpressionPlan fp2 = new LogicalExpressionPlan();
        ProjectExpression fy2 = new ProjectExpression(fp2, 0, 0, null);
        ConstantExpression fc2 = new ConstantExpression(fp2,
                new Integer(0));
        new EqualExpression(fp2, fy2, fc2);
        LOFilter D2 = new LOFilter(lp2, fp2);
        D2.neverUseForRealSetSchema(cschema);
        lp2.add(D2);
        lp2.connect(A2, D2);

        assertTrue(D1.isEqual(D2));
    }

    @Test
    public void testReplace1() throws FrontendException {
        // has multiple inputs
        SillyPlan plan = new SillyPlan();
        Operator load1 = new SillyOperator("load1", plan);
        Operator load2 = new SillyOperator("load2", plan);
        Operator filter1 = new SillyOperator("filter1", plan);
        Operator join1 = new SillyOperator("join1", plan);
        Operator filter2 = new SillyOperator("filter2", plan);
        plan.add(load1);
        plan.add(load2);
        plan.add(filter1);
        plan.add(filter2);
        plan.add(join1);
        plan.connect(load1, join1);
        plan.connect(load2, filter1);
        plan.connect(filter1, join1);
        plan.connect(join1, filter2);

        Operator join2 = new SillyOperator("join2", plan);
        plan.replace(join1, join2);

        List<Operator> preds = plan.getPredecessors(join2);
        assertEquals(2, preds.size());
        assertTrue(preds.contains(load1));
        assertTrue(preds.contains(filter1));

        List<Operator> succs = plan.getSuccessors(join2);
        assertEquals(1, succs.size());
        assertTrue(succs.contains(filter2));
    }

    @Test
    public void testReplace2() throws FrontendException {
        // has multiple outputs
        SillyPlan plan = new SillyPlan();
        Operator load1 = new SillyOperator("load1", plan);
        Operator split1 = new SillyOperator("split1", plan);
        Operator filter1 = new SillyOperator("filter1", plan);
        Operator filter2 = new SillyOperator("filter2", plan);
        plan.add(load1);
        plan.add(split1);
        plan.add(filter1);
        plan.add(filter2);
        plan.connect(load1, split1);
        plan.connect(split1, filter1);
        plan.connect(split1, filter2);

        Operator split2 = new SillyOperator("split2", plan);
        plan.replace(split1, split2);

        List<Operator> preds = plan.getPredecessors(split2);
        assertEquals(1, preds.size());
        assertTrue(preds.contains(load1));

        List<Operator> succs = plan.getSuccessors(split2);
        assertEquals(2, succs.size());
        assertTrue(succs.contains(filter1));
        assertTrue(succs.contains(filter2));
    }

    @Test
    public void testReplace3() throws FrontendException {
        // single input/output
        SillyPlan plan = new SillyPlan();
        Operator load1 = new SillyOperator("load1", plan);
        Operator filter1 = new SillyOperator("filter1", plan);
        Operator filter2 = new SillyOperator("filter2", plan);
        plan.add(load1);
        plan.add(filter1);
        plan.add(filter2);
        plan.connect(load1, filter1);
        plan.connect(filter1, filter2);

        Operator filter3 = new SillyOperator("filter3", plan);
        plan.replace(filter1, filter3);

        List<Operator> preds = plan.getPredecessors(filter3);
        assertEquals(1, preds.size());
        assertTrue(preds.contains(load1));

        List<Operator> succs = plan.getSuccessors(filter3);
        assertEquals(1, succs.size());
        assertTrue(succs.contains(filter2));
    }

    @Test
    public void testReplace4() throws FrontendException {
        // output is null
        SillyPlan plan = new SillyPlan();
        Operator load1 = new SillyOperator("load1", plan);
        Operator filter1 = new SillyOperator("filter1", plan);
        Operator filter2 = new SillyOperator("filter2", plan);
        plan.add(load1);
        plan.add(filter1);
        plan.add(filter2);
        plan.connect(load1, filter1);
        plan.connect(filter1, filter2);

        Operator filter3 = new SillyOperator("filter3", plan);
        plan.replace(filter2, filter3);

        List<Operator> preds = plan.getPredecessors(filter3);
        assertEquals(1, preds.size());
        assertTrue(preds.contains(filter1));

        List<Operator> succs = plan.getSuccessors(filter3);
        assertNull(succs);
    }

    @Test
    public void testReplace5() throws FrontendException {
        // input is null
        SillyPlan plan = new SillyPlan();
        Operator load1 = new SillyOperator("load1", plan);
        Operator filter1 = new SillyOperator("filter1", plan);
        Operator filter2 = new SillyOperator("filter2", plan);
        plan.add(load1);
        plan.add(filter1);
        plan.add(filter2);
        plan.connect(load1, filter1);
        plan.connect(filter1, filter2);

        Operator load2 = new SillyOperator("load2", plan);
        plan.replace(load1, load2);

        List<Operator> preds = plan.getPredecessors(load2);
        assertNull(preds);

        List<Operator> succs = plan.getSuccessors(load2);
        assertEquals(1, succs.size());
        assertTrue(succs.contains(filter1));
    }

    @Test
    public void testReplace6() throws FrontendException {
        // has multiple inputs/outputs
        SillyPlan plan = new SillyPlan();
        Operator load1 = new SillyOperator("load1", plan);
        Operator load2 = new SillyOperator("load2", plan);
        Operator filter1 = new SillyOperator("filter1", plan);
        // fake operator to take multiple inputs/outputs
        Operator fake1 = new SillyOperator("fake1", plan);
        Operator filter2 = new SillyOperator("filter2", plan);
        Operator filter3 = new SillyOperator("filter3", plan);
        plan.add(load1);
        plan.add(load2);
        plan.add(filter1);
        plan.add(filter2);
        plan.add(filter3);
        plan.add(fake1);
        plan.connect(load1, fake1);
        plan.connect(load2, filter1);
        plan.connect(filter1, fake1);
        plan.connect(fake1, filter2);
        plan.connect(fake1, filter3);

        Operator fake2 = new SillyOperator("fake2", plan);
        plan.replace(fake1, fake2);

        List<Operator> preds = plan.getPredecessors(fake2);
        assertEquals(2, preds.size());
        assertTrue(preds.contains(load1));
        assertTrue(preds.contains(filter1));

        List<Operator> succs = plan.getSuccessors(fake2);
        assertEquals(2, succs.size());
        assertTrue(succs.contains(filter2));
        assertTrue(succs.contains(filter3));
    }

    @Test
    public void testRemove1() throws FrontendException {
        // single input/output
        SillyPlan plan = new SillyPlan();
        Operator load1 = new SillyOperator("load1", plan);
        Operator load2 = new SillyOperator("load2", plan);
        Operator filter1 = new SillyOperator("filter1", plan);
        Operator join1 = new SillyOperator("join1", plan);
        Operator filter2 = new SillyOperator("filter2", plan);
        plan.add(load1);
        plan.add(load2);
        plan.add(filter1);
        plan.add(filter2);
        plan.add(join1);
        plan.connect(load1, join1);
        plan.connect(load2, filter1);
        plan.connect(filter1, join1);
        plan.connect(join1, filter2);

        plan.removeAndReconnect(filter1);

        List<Operator> preds = plan.getPredecessors(join1);
        assertEquals(2, preds.size());
        assertTrue(preds.contains(load2));
    }

    @Test
    public void testRemove2() throws FrontendException {
        // input is null
        SillyPlan plan = new SillyPlan();
        Operator load1 = new SillyOperator("load1", plan);
        Operator load2 = new SillyOperator("load2", plan);
        Operator filter1 = new SillyOperator("filter1", plan);
        Operator join1 = new SillyOperator("join1", plan);
        Operator filter2 = new SillyOperator("filter2", plan);
        plan.add(load1);
        plan.add(load2);
        plan.add(filter1);
        plan.add(filter2);
        plan.add(join1);
        plan.connect(load1, join1);
        plan.connect(load2, filter1);
        plan.connect(filter1, join1);
        plan.connect(join1, filter2);

        plan.removeAndReconnect(load1);

        List<Operator> preds = plan.getPredecessors(join1);
        assertEquals(1, preds.size());
        assertTrue(preds.contains(filter1));

        plan.removeAndReconnect(filter1);
        preds = plan.getPredecessors(join1);
        assertEquals(1, preds.size());
        assertTrue(preds.contains(load2));
    }

    @Test
    public void testRemove3() throws FrontendException {
        // output is null
        SillyPlan plan = new SillyPlan();
        Operator load1 = new SillyOperator("load1", plan);
        Operator filter1 = new SillyOperator("filter1", plan);
        Operator filter2 = new SillyOperator("filter2", plan);
        plan.add(load1);
        plan.add(filter1);
        plan.add(filter2);
        plan.connect(load1, filter1);
        plan.connect(filter1, filter2);

        plan.removeAndReconnect(filter2);

        List<Operator> succs = plan.getSuccessors(filter2);
        assertNull(succs);
    }

    @Test
    public void testRemove4() throws FrontendException {
        // has multiple inputs
        SillyPlan plan = new SillyPlan();
        Operator load1 = new SillyOperator("load1", plan);
        Operator load2 = new SillyOperator("load2", plan);
        Operator filter1 = new SillyOperator("filter1", plan);
        Operator join1 = new SillyOperator("join1", plan);
        Operator fake1 = new SillyOperator("fake1", plan);
        plan.add(load1);
        plan.add(load2);
        plan.add(filter1);
        plan.add(join1);
        plan.connect(load1, join1);
        plan.connect(load2, filter1);
        plan.connect(filter1, join1);
        plan.connect(join1, fake1);

        plan.removeAndReconnect(join1);

        List<Operator> preds = plan.getPredecessors(fake1);
        assertEquals(2, preds.size());
        assertTrue(preds.contains(load1));
        assertTrue(preds.contains(filter1));
    }

    @Test
    public void testRemove5() throws FrontendException {
        // has multiple outputs
        SillyPlan plan = new SillyPlan();
        Operator load1 = new SillyOperator("load1", plan);
        Operator split1 = new SillyOperator("split1", plan);
        Operator split2 = new SillyOperator("split2", plan);
        Operator filter1 = new SillyOperator("filter1", plan);
        Operator filter2 = new SillyOperator("filter2", plan);
        plan.add(load1);
        plan.add(split1);
        plan.add(filter1);
        plan.add(filter2);
        plan.connect(load1, split1);
        plan.connect(split1, split2);
        plan.connect(split2, filter1);
        plan.connect(split2, filter2);

        plan.removeAndReconnect(split2);

        List<Operator> succs = plan.getSuccessors(split1);
        assertEquals(2, succs.size());
        assertTrue(succs.contains(filter1));
        assertTrue(succs.contains(filter2));
    }

    @Test(expected = FrontendException.class)
    public void testRemove6() throws FrontendException {
        // has multiple inputs/outputs
        SillyPlan plan = new SillyPlan();
        Operator load1 = new SillyOperator("load1", plan);
        Operator load2 = new SillyOperator("load2", plan);
        Operator fake1 = new SillyOperator("fake1", plan);
        Operator filter1 = new SillyOperator("filter1", plan);
        Operator filter2 = new SillyOperator("filter2", plan);
        plan.add(load1);
        plan.add(load2);
        plan.add(fake1);
        plan.add(filter1);
        plan.add(filter2);
        plan.connect(load1, fake1);
        plan.connect(load2, fake1);
        plan.connect(fake1, filter1);
        plan.connect(fake1, filter2);

        try {
            plan.removeAndReconnect(fake1);
        } catch (FrontendException e) {
            assertEquals(2256, e.getErrorCode());
            throw e;
        }
    }

    @Test
    public void testInsertBetween1() throws FrontendException {
        // single input
        SillyPlan plan = new SillyPlan();
        Operator load1 = new SillyOperator("load1", plan);
        Operator filter1 = new SillyOperator("filter1", plan);
        plan.add(load1);
        plan.add(filter1);
        plan.connect(load1, filter1);

        Operator filter2 = new SillyOperator("filter2", plan);
        plan.insertBetween(load1, filter2, filter1);

        List<Operator> succs = plan.getSuccessors(filter2);
        assertEquals(1, succs.size());
        assertTrue(succs.contains(filter1));

        List<Operator> preds = plan.getPredecessors(filter2);
        assertEquals(1, preds.size());
        assertTrue(preds.contains(load1));
    }
}
