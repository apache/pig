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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.pig.impl.plan.*;
import org.apache.pig.impl.plan.optimizer.*;

import org.junit.Test;

/**
 * Test the generic operator classes (Operator, OperatorPlan,
 * PlanVisitor).  Also includes tests for optimizer framework, since that
 * can use the same generic test operators.
 */

public class TestOperatorPlan extends junit.framework.TestCase {

    private int mNextKey = 0;
    private static final String SCOPE = "RULE";
    private static NodeIdGenerator nodeIdGen = NodeIdGenerator.getGenerator();
    public static int MAX_OPTIMIZATION_ITERATIONS = 250;

    abstract class TOperator extends Operator implements Comparable {
        protected String mName;

        TOperator(String name) {
            super(new OperatorKey("", mNextKey++));
            mName = name;
        }

        public int compareTo(Object o) {
            if (!(o instanceof TOperator)) {
                return -1;
            }

            TOperator other = (TOperator)o;

            return mName.compareTo(other.mName);
        }
    }

    class SingleOperator extends TOperator {
        SingleOperator(String name) {
            super(name);
        }

        public boolean supportsMultipleInputs() {
            return false;
        }

        public boolean supportsMultipleOutputs() {
            return false;
        }

        @Override
        public void visit(PlanVisitor v) throws VisitorException {
            ((TVisitor)v).visit(this);
        }

        public String name() {
            //return this.getClass().getName() + " " + mName
            return mName;
        }

    }

    class MultiOperator extends TOperator {
        MultiOperator(String name) {
            super(name);
        }

        public boolean supportsMultipleInputs() {
            return true;
        }

        public boolean supportsMultipleOutputs() {
            return true;
        }

        public void visit(PlanVisitor v) throws VisitorException {
            ((TVisitor)v).visit(this);
        }

        public String name() {
            //return this.getClass().getName() + " " + mName;
            return mName;
        }

    }
    
    class MultiInputSingleOutputOperator extends TOperator {
        MultiInputSingleOutputOperator(String name) {
            super(name);
        }

        public boolean supportsMultipleInputs() {
            return true;
        }

        public boolean supportsMultipleOutputs() {
            return false;
        }

        @Override
        public void visit(PlanVisitor v) throws VisitorException {
            ((TVisitor)v).visit(this);
        }

        public String name() {
            //return this.getClass().getName() + " " + mName
            return mName;
        }

    }
    
    class MultiOutputSingleInputOperator extends TOperator {
        MultiOutputSingleInputOperator(String name) {
            super(name);
        }

        public boolean supportsMultipleInputs() {
            return false;
        }

        public boolean supportsMultipleOutputs() {
            return true;
        }

        @Override
        public void visit(PlanVisitor v) throws VisitorException {
            ((TVisitor)v).visit(this);
        }

        public String name() {
            //return this.getClass().getName() + " " + mName
            return mName;
        }

    }

    class TPlan extends OperatorPlan<TOperator> {

        public String display() {
            StringBuilder buf = new StringBuilder();

            buf.append("Nodes: ");
            // Guarantee a sorting
            TreeSet<TOperator> ts = new TreeSet(mOps.keySet());
            for (TOperator op : ts) {
                buf.append(op.name());
                buf.append(' ');
            }

            buf.append("FromEdges: ");
            ts = new TreeSet(mFromEdges.keySet());
            Iterator<TOperator> i = ts.iterator();
            while (i.hasNext()) {
                TOperator from = i.next();
                TreeSet<TOperator> ts2 = new TreeSet(mFromEdges.get(from));
                Iterator<TOperator> j = ts2.iterator();
                while (j.hasNext()) {
                    buf.append(from.name());
                    buf.append("->");
                    buf.append(j.next().name());
                    buf.append(' ');
                }
            }

            buf.append("ToEdges: ");
            ts = new TreeSet(mToEdges.keySet());
            i = ts.iterator();
            while (i.hasNext()) {
                TOperator from = i.next();
                TreeSet<TOperator> ts2 = new TreeSet(mToEdges.get(from));
                Iterator<TOperator> j = ts2.iterator();
                while (j.hasNext()) {
                    buf.append(from.name());
                    buf.append("->");
                    buf.append(j.next().name());
                    buf.append(' ');
                }
            }
            return buf.toString();
        }
    }

    abstract class TVisitor extends PlanVisitor<TOperator, TPlan> {
        protected StringBuilder mJournal;

        TVisitor(TPlan plan, PlanWalker<TOperator, TPlan> walker) {
            super(plan, walker);
            mJournal = new StringBuilder();
        }

        public void visit(SingleOperator so) throws VisitorException {
            mJournal.append(so.name());
            mJournal.append(' ');
        }

        public void visit(MultiOperator mo) throws VisitorException {
            mJournal.append(mo.name());
            mJournal.append(' ');
        }
        
        public void visit(MultiInputSingleOutputOperator miso) throws VisitorException {
            mJournal.append(miso.name());
            mJournal.append(' ');
        }
        
        public void visit(MultiOutputSingleInputOperator mosi) throws VisitorException {
            mJournal.append(mosi.name());
            mJournal.append(' ');
        }

        public String getJournal() {
            return mJournal.toString();
        }
    }

    class TDepthVisitor extends TVisitor {

        TDepthVisitor(TPlan plan) {
            super(plan, new DepthFirstWalker(plan));
        }
    }

    class TDependVisitor extends TVisitor {

        TDependVisitor(TPlan plan) {
            super(plan, new DependencyOrderWalker(plan));
        }
    }

    static class TOptimizer extends PlanOptimizer<TOperator, TPlan> {

        public TOptimizer(TPlan plan) {
            super(plan, TestOperatorPlan.MAX_OPTIMIZATION_ITERATIONS);
        }

        public void addRule(Rule rule) {
            mRules.add(rule);
        }
    }

    class AlwaysTransform extends Transformer<TOperator, TPlan> {
        public boolean mTransformed = false;
        private int mNumChecks = 0;

        AlwaysTransform(TPlan plan) {
            super(plan, new DepthFirstWalker<TOperator, TPlan>(plan));
        }

        public boolean check(List<TOperator> nodes) {
            ++mNumChecks;
            return true;
        }

        public void transform(List<TOperator> nodes) {
            mTransformed = true;
        }
        
        public int getNumberOfChecks() {
            return mNumChecks;
        }
    }

    class NeverTransform extends Transformer<TOperator, TPlan> {
        public boolean mTransformed = false;
        private int mNumChecks = 0;

        NeverTransform(TPlan plan) {
            super(plan, new DepthFirstWalker<TOperator, TPlan>(plan));
        }

        public boolean check(List<TOperator> nodes) {
            ++mNumChecks;
            return false;
        }

        public void transform(List<TOperator> nodes) {
            mTransformed = true;
        }
        
        public int getNumberOfChecks() {
            return mNumChecks;
        }
    }

    @Test
    public void testAddRemove() throws Exception {
        // Test that we can add and remove nodes from the plan.  Also test
        // that we can fetch the nodes by operator key, by operator, by
        // roots, by leaves, that they have no predecessors and no
        // successors.

        TPlan plan = new TPlan();
        TOperator[] ops = new TOperator[3];
        for (int i = 0; i < 3; i++) {
            ops[i] = new SingleOperator(Integer.toString(i));
            plan.add(ops[i]);
        }

        // All should be roots, as none are connected
        List<TOperator> roots = plan.getRoots();
        for (int i = 0; i < 3; i++) {
            assertTrue("Roots should contain operator " + i,
                roots.contains(ops[i]));
        }

        // All should be leaves, as none are connected
        List<TOperator> leaves = plan.getLeaves();
        for (int i = 0; i < 3; i++) {
            assertTrue("Leaves should contain operator " + i,
                leaves.contains(ops[i]));
        }

        // Each operator should have no successors or predecessors.
        assertNull(plan.getSuccessors(ops[1]));
        assertNull(plan.getPredecessors(ops[1]));

        // Make sure we find them all when we iterate through them.
        Set<TOperator> s = new HashSet<TOperator>();
        Iterator<TOperator> j = plan.iterator();
        while (j.hasNext()) {
            s.add(j.next());
        }

        for (int i = 0; i < 3; i++) {
            assertTrue("Iterator should contain operator " + i,
                s.contains(ops[i]));
        }

        // Test that we can find an operator by its key.
        TOperator op = plan.getOperator(new OperatorKey("", 1));
        assertEquals("Expected to get back ops[1]", ops[1], op);

        // Test that we can get an operator key by its operator
        OperatorKey opkey = new OperatorKey("", 1);
        assertTrue("Expected to get back key for ops[1]",
            opkey.equals(plan.getOperatorKey(ops[1])));

        // Test that we can remove operators
        plan.remove(ops[2]);

        assertEquals("Should only have two roots now.", 2,
            plan.getRoots().size());
        assertEquals("Should only have two leaves now.", 2,
            plan.getLeaves().size());

        j = plan.iterator();
        int k;
        for (k = 0; j.hasNext(); k++) j.next();
        assertEquals("Iterator should only return two now", 2, k);

        // Remove all operators
        plan.remove(ops[0]);
        plan.remove(ops[1]);

        assertEquals("Should only have no roots now.", 0,
            plan.getRoots().size());
        assertEquals("Should only have no leaves now.", 0,
            plan.getLeaves().size());

        j = plan.iterator();
        assertFalse("Iterator should return nothing now", j.hasNext());
    }

    @Test
    public void testInsertBetween() throws Exception {
        // Test that insertBetween works.

        TPlan plan = new TPlan();
        TOperator[] ops = new TOperator[3];
        for (int i = 0; i < 3; i++) {
            ops[i] = new SingleOperator(Integer.toString(i));
            plan.add(ops[i]);
        }

        // Connect 0 to 2
        plan.connect(ops[0], ops[2]);

        Collection p = plan.getPredecessors(ops[0]);
        assertNull(p);
        p = plan.getSuccessors(ops[0]);
        assertEquals(1, p.size());
        Iterator i = p.iterator();
        assertEquals(ops[2], i.next());

        p = plan.getPredecessors(ops[1]);
        assertNull(p);
        p = plan.getSuccessors(ops[1]);
        assertNull(p);

        p = plan.getPredecessors(ops[2]);
        assertEquals(1, p.size());
        i = p.iterator();
        assertEquals(ops[0], i.next());
        p = plan.getSuccessors(ops[2]);
        assertNull(p);

        // Insert 1 in between 0 and 2
        plan.insertBetween(ops[0], ops[1], ops[2]);

        p = plan.getPredecessors(ops[0]);
        assertNull(p);
        p = plan.getSuccessors(ops[0]);
        assertEquals(1, p.size());
        i = p.iterator();
        assertEquals(ops[1], i.next());

        p = plan.getPredecessors(ops[1]);
        assertEquals(1, p.size());
        i = p.iterator();
        assertEquals(ops[0], i.next());
        p = plan.getSuccessors(ops[1]);
        assertEquals(1, p.size());
        i = p.iterator();
        assertEquals(ops[2], i.next());

        p = plan.getPredecessors(ops[2]);
        assertEquals(1, p.size());
        i = p.iterator();
        assertEquals(ops[1], i.next());
        p = plan.getSuccessors(ops[2]);
        assertNull(p);
    }

    @Test
    public void testInsertBetweenNegative() throws Exception {
        // Test that insertBetween throws errors when it should.

        TPlan plan = new TPlan();
        TOperator[] ops = new TOperator[4];
        for (int i = 0; i < 4; i++) {
            ops[i] = new MultiOperator(Integer.toString(i));
            plan.add(ops[i]);
        }

        plan.connect(ops[0], ops[1]);

        boolean caughtIt = false;
        try {
            plan.insertBetween(ops[0], ops[3], ops[2]);
        } catch (PlanException pe) {
            caughtIt = true;
        }
        assertTrue(caughtIt);
    }

    @Test
    public void testLinearGraph() throws Exception {
        TPlan plan = new TPlan();
        TOperator[] ops = new TOperator[5];
        for (int i = 0; i < 5; i++) {
            ops[i] = new SingleOperator(Integer.toString(i));
            plan.add(ops[i]);
            if (i > 0) plan.connect(ops[i - 1], ops[i]);
        }

        // Test that connecting a node not yet in the plan is detected.
        TOperator bogus = new SingleOperator("X");
        boolean sawError = false;
        try {
            plan.connect(ops[2], bogus);
        } catch (PlanException ioe) {
            assertEquals("Attempt to connect operator X which is not in "
                + "the plan.", ioe.getMessage());
            sawError = true;
        }
        assertTrue("Should have caught an error when we tried to connect a "
            + "node that was not in the plan", sawError);

        // Get roots should just return ops[0]
        List<TOperator> roots = plan.getRoots();
        assertEquals(1, roots.size());
        assertEquals(roots.get(0), ops[0]);

        // Get leaves should just return ops[4]
        List<TOperator> leaves = plan.getLeaves();
        assertEquals(1, leaves.size());
        assertEquals(leaves.get(0), ops[4]);

        // Test that connecting another input to SingleOperator gives
        // error.
        plan.add(bogus);
        sawError = false;
        try {
            plan.connect(bogus, ops[1]);
        } catch (PlanException ioe) {
            assertEquals("Attempt to give operator of type " +
                "org.apache.pig.test.TestOperatorPlan$SingleOperator " +
                "multiple inputs.  This operator does "
                + "not support multiple inputs.", ioe.getMessage());
            sawError = true;
        }
        assertTrue("Should have caught an error when we tried to connect a "
            + "second input to a Single", sawError);

        // Test that connecting another output to SingleOperator gives
        // error.
        sawError = false;
        try {
            plan.connect(ops[0], bogus);
        } catch (PlanException ioe) {
            assertEquals("Attempt to give operator of type " +
                "org.apache.pig.test.TestOperatorPlan$SingleOperator " +
                "multiple outputs.  This operator does "
                + "not support multiple outputs.", ioe.getMessage());
            sawError = true;
        }
        assertTrue("Should have caught an error when we tried to connect a "
            + "second output to a " +
            "org.apache.pig.test.TestOperatorPlan$SingleOperator", sawError);
        plan.remove(bogus);

        // Successor for ops[1] should be ops[2]
        Collection s = plan.getSuccessors(ops[1]);
        assertEquals(1, s.size());
        Iterator i = s.iterator();
        assertEquals(ops[2], i.next());

        // Predecessor for ops[1] should be ops[0]
        Collection p = plan.getPredecessors(ops[1]);
        assertEquals(1, p.size());
        i = p.iterator();
        assertEquals(ops[0], i.next());

        assertEquals("Nodes: 0 1 2 3 4 FromEdges: 0->1 1->2 2->3 3->4 ToEdges: 1->0 2->1 3->2 4->3 ", plan.display());

        // Visit it depth first
        TVisitor visitor = new TDepthVisitor(plan);
        visitor.visit();
        assertEquals("0 1 2 3 4 ", visitor.getJournal());

        // Visit it dependency order
        visitor = new TDependVisitor(plan);
        visitor.visit();
        assertEquals("0 1 2 3 4 ", visitor.getJournal());

        // Test disconnect
        plan.disconnect(ops[2], ops[3]);
        assertEquals("Nodes: 0 1 2 3 4 FromEdges: 0->1 1->2 3->4 ToEdges: 1->0 2->1 4->3 ", plan.display());

        // Test remove
        plan.remove(ops[1]);
        assertEquals("Nodes: 0 2 3 4 FromEdges: 3->4 ToEdges: 4->3 ", plan.display());
    }

    @Test
    public void testDAG() throws Exception {
        TPlan plan = new TPlan();
        TOperator[] ops = new TOperator[6];
        for (int i = 0; i < 6; i++) {
            ops[i] = new MultiOperator(Integer.toString(i));
            plan.add(ops[i]);
        }
        plan.connect(ops[0], ops[2]);
        plan.connect(ops[1], ops[2]);
        plan.connect(ops[2], ops[3]);
        plan.connect(ops[3], ops[4]);
        plan.connect(ops[3], ops[5]);

        // Get roots should return ops[0] and ops[1]
        List<TOperator> roots = plan.getRoots();
        assertEquals(2, roots.size());
        assertTrue(roots.contains(ops[0]));
        assertTrue(roots.contains(ops[1]));

        // Get leaves should return ops[4] and ops[5]
        List<TOperator> leaves = plan.getLeaves();
        assertEquals(2, leaves.size());
        assertTrue(leaves.contains(ops[4]));
        assertTrue(leaves.contains(ops[5]));

        // Successor for ops[3] should be ops[4] and ops[5]
        List<TOperator> s = new ArrayList<TOperator>(plan.getSuccessors(ops[3]));
        assertEquals(2, s.size());
        assertTrue(s.contains(ops[4]));
        assertTrue(s.contains(ops[5]));
        
        // Predecessor for ops[2] should be ops[0] and ops[1]
        s = new ArrayList<TOperator>(plan.getPredecessors(ops[2]));
        assertEquals(2, s.size());
        assertTrue(s.contains(ops[0]));
        assertTrue(s.contains(ops[1]));

        assertEquals("Nodes: 0 1 2 3 4 5 FromEdges: 0->2 1->2 2->3 3->4 3->5 ToEdges: 2->0 2->1 3->2 4->3 5->3 ", plan.display());

        // Visit it depth first
        TVisitor visitor = new TDepthVisitor(plan);
        visitor.visit();
        // There are a number of valid patterns, make sure we found one of
        // them.
        String result = visitor.getJournal();
        assertTrue(result.equals("1 2 3 4 5 0 ") ||
            result.equals("1 2 3 5 4 0 ") || result.equals("0 2 3 4 5 1 ")
            || result.equals("0 2 3 5 4 1 "));

        // Visit it dependency order
        visitor = new TDependVisitor(plan);
        visitor.visit();
        result = visitor.getJournal();
        assertTrue(result.equals("0 1 2 3 4 5 ") ||
            result.equals("0 1 2 3 5 4 "));

        // Test disconnect
        plan.disconnect(ops[2], ops[3]);
        assertEquals("Nodes: 0 1 2 3 4 5 FromEdges: 0->2 1->2 3->4 3->5 ToEdges: 2->0 2->1 4->3 5->3 ", plan.display());

        // Test remove
        plan.remove(ops[2]);
        assertEquals("Nodes: 0 1 3 4 5 FromEdges: 3->4 3->5 ToEdges: 4->3 5->3 ", plan.display());
    }

    // Test that we don't match when nodes don't match pattern.  Will give
    // a pattern of S->S->M and a plan of S->M->S.
    @Test
    public void testOptimizerDifferentNodes() throws Exception {
        // Build a plan
        TPlan plan = new TPlan();
        TOperator[] ops = new TOperator[3];
        ops[0] = new SingleOperator("1");
        plan.add(ops[0]);
        ops[1] = new MultiOperator("2");
        plan.add(ops[1]);
        ops[2] = new SingleOperator("3");
        plan.add(ops[2]);
        plan.connect(ops[0], ops[1]);
        plan.connect(ops[1], ops[2]);

        // Create our rule
        RulePlan rulePlan = new RulePlan();
        RuleOperator singleOperator_1 = new RuleOperator(SingleOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        RuleOperator singleOperator_2 = new RuleOperator(SingleOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        RuleOperator multiOperator_1 = new RuleOperator(MultiOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        rulePlan.add(singleOperator_1);
        rulePlan.add(singleOperator_2);
        rulePlan.add(multiOperator_1);
        rulePlan.connect(singleOperator_1, singleOperator_2);
        rulePlan.connect(singleOperator_2, multiOperator_1);
        
        AlwaysTransform transformer = new AlwaysTransform(plan);
        Rule<TOperator, TPlan> r =
            new Rule<TOperator, TPlan>(rulePlan, transformer, "TestRule");

        TOptimizer optimizer = new TOptimizer(plan);
        optimizer.addRule(r);

        optimizer.optimize();
        assertFalse(transformer.mTransformed);
    }

    // Test that we don't match when edges don't match pattern.  Will give
    // a pattern of S->S->M and a plan of S->S M.
    @Test
    public void testOptimizerDifferentEdges() throws Exception {
        // Build a plan
        TPlan plan = new TPlan();
        TOperator[] ops = new TOperator[3];
        ops[0] = new SingleOperator("1");
        plan.add(ops[0]);
        ops[1] = new SingleOperator("2");
        plan.add(ops[1]);
        ops[2] = new MultiOperator("3");
        plan.add(ops[2]);
        plan.connect(ops[0], ops[1]);

        // Create our rule
        RulePlan rulePlan = new RulePlan();
        RuleOperator singleOperator_1 = new RuleOperator(SingleOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        RuleOperator singleOperator_2 = new RuleOperator(SingleOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        RuleOperator multiOperator_1 = new RuleOperator(MultiOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        rulePlan.add(singleOperator_1);
        rulePlan.add(singleOperator_2);
        rulePlan.add(multiOperator_1);
        rulePlan.connect(singleOperator_1, singleOperator_2);
        rulePlan.connect(singleOperator_2, multiOperator_1);

        
        AlwaysTransform transformer = new AlwaysTransform(plan);
        Rule<TOperator, TPlan> r =
            new Rule<TOperator, TPlan>(rulePlan, transformer, "TestRule");

        TOptimizer optimizer = new TOptimizer(plan);
        optimizer.addRule(r);
        optimizer.optimize();
        assertFalse(transformer.mTransformed);
    }

    // Test that we match when appropriate.  Will give
    // a pattern of S->S->M and a plan of S->S->M.
    @Test
    public void testOptimizerMatches() throws Exception {
        // Build a plan
        TPlan plan = new TPlan();
        TOperator[] ops = new TOperator[3];
        ops[0] = new SingleOperator("1");
        plan.add(ops[0]);
        ops[1] = new SingleOperator("2");
        plan.add(ops[1]);
        ops[2] = new MultiOperator("3");
        plan.add(ops[2]);
        plan.connect(ops[0], ops[1]);
        plan.connect(ops[1], ops[2]);

        // Create our rule
        RulePlan rulePlan = new RulePlan();
        RuleOperator singleOperator_1 = new RuleOperator(SingleOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        RuleOperator singleOperator_2 = new RuleOperator(SingleOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        RuleOperator multiOperator_1 = new RuleOperator(MultiOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        rulePlan.add(singleOperator_1);
        rulePlan.add(singleOperator_2);
        rulePlan.add(multiOperator_1);
        rulePlan.connect(singleOperator_1, singleOperator_2);
        rulePlan.connect(singleOperator_2, multiOperator_1);
        
        AlwaysTransform transformer = new AlwaysTransform(plan);
        Rule<TOperator, TPlan> r =
            new Rule<TOperator, TPlan>(rulePlan, transformer, "TestRule");

        TOptimizer optimizer = new TOptimizer(plan);
        optimizer.addRule(r);

        optimizer.optimize();

        assertTrue(transformer.mTransformed);
    }

    // Test that we match when the pattern says any.  Will give
    // a pattern of any and a plan of S->S->M.
    @Test
    public void testOptimizerMatchesAny() throws Exception {
        // Build a plan
        TPlan plan = new TPlan();
        TOperator[] ops = new TOperator[3];
        ops[0] = new SingleOperator("1");
        plan.add(ops[0]);
        ops[1] = new SingleOperator("2");
        plan.add(ops[1]);
        ops[2] = new MultiOperator("3");
        plan.add(ops[2]);
        plan.connect(ops[0], ops[1]);
        plan.connect(ops[1], ops[2]);

        // Create our rule
        RulePlan rulePlan = new RulePlan();
        RuleOperator singleOperator_1 = new RuleOperator(SingleOperator.class,
                RuleOperator.NodeType.ANY_NODE,
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        rulePlan.add(singleOperator_1);

        AlwaysTransform transformer = new AlwaysTransform(plan);
        Rule<TOperator, TPlan> r =
            new Rule<TOperator, TPlan>(rulePlan, transformer, "TestRule");

        TOptimizer optimizer = new TOptimizer(plan);
        optimizer.addRule(r);

        optimizer.optimize();
        assertTrue(transformer.mTransformed);
    }

    // Test that we match when the whole plan doesn't match.  Will give
    // a pattern of S->S->M and a plan of S->S->S->M.
    @Test
    public void testOptimizerMatchesPart() throws Exception {
        // Build a plan
        TPlan plan = new TPlan();
        TOperator[] ops = new TOperator[4];
        ops[0] = new SingleOperator("1");
        plan.add(ops[0]);
        ops[1] = new SingleOperator("2");
        plan.add(ops[1]);
        ops[2] = new SingleOperator("3");
        plan.add(ops[2]);
        ops[3] = new MultiOperator("4");
        plan.add(ops[3]);
        plan.connect(ops[0], ops[1]);
        plan.connect(ops[1], ops[2]);
        plan.connect(ops[2], ops[3]);

        // Create our rule
        RulePlan rulePlan = new RulePlan();
        RuleOperator singleOperator_1 = new RuleOperator(SingleOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        RuleOperator singleOperator_2 = new RuleOperator(SingleOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        RuleOperator multiOperator_1 = new RuleOperator(MultiOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        rulePlan.add(singleOperator_1);
        rulePlan.add(singleOperator_2);
        rulePlan.add(multiOperator_1);
        rulePlan.connect(singleOperator_1, singleOperator_2);
        rulePlan.connect(singleOperator_2, multiOperator_1);

        AlwaysTransform transformer = new AlwaysTransform(plan);
        Rule<TOperator, TPlan> r =
            new Rule<TOperator, TPlan>(rulePlan, transformer, "TestRule");

        TOptimizer optimizer = new TOptimizer(plan);
        optimizer.addRule(r);

        optimizer.optimize();
        assertTrue(transformer.mTransformed);
    }

    // Test that we match when a node is optional and the optional node is
    // present.  Will give
    // a pattern of S->S->M (with second S optional) and a plan of S->S->M.
    @Test
    public void testOptimizerOptionalMatches() throws Exception {
        // Build a plan
        TPlan plan = new TPlan();
        TOperator[] ops = new TOperator[3];
        ops[0] = new SingleOperator("1");
        plan.add(ops[0]);
        ops[1] = new SingleOperator("2");
        plan.add(ops[1]);
        ops[2] = new MultiOperator("3");
        plan.add(ops[2]);
        plan.connect(ops[0], ops[1]);
        plan.connect(ops[1], ops[2]);

        // Create our rule
        RulePlan rulePlan = new RulePlan();
        RuleOperator singleOperator_1 = new RuleOperator(SingleOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        RuleOperator singleOperator_2 = new RuleOperator(SingleOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        RuleOperator multiOperator_1 = new RuleOperator(MultiOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        rulePlan.add(singleOperator_1);
        rulePlan.add(singleOperator_2);
        rulePlan.add(multiOperator_1);
        rulePlan.connect(singleOperator_1, singleOperator_2);
        rulePlan.connect(singleOperator_2, multiOperator_1);

        AlwaysTransform transformer = new AlwaysTransform(plan);
        Rule<TOperator, TPlan> r =
            new Rule<TOperator, TPlan>(rulePlan, transformer, "TestRule");

        TOptimizer optimizer = new TOptimizer(plan);
        optimizer.addRule(r);

        optimizer.optimize();
        assertTrue(transformer.mTransformed);
    }

    // Test that we do not match when a node is missing.  Will give
    // a pattern of S->S->M and a plan of S->M.
    @Test
    public void testOptimizerOptionalMissing() throws Exception {
        // Build a plan
        TPlan plan = new TPlan();
        TOperator[] ops = new TOperator[2];
        ops[0] = new SingleOperator("1");
        plan.add(ops[0]);
        ops[1] = new MultiOperator("2");
        plan.add(ops[1]);
        plan.connect(ops[0], ops[1]);

        // Create our rule
        RulePlan rulePlan = new RulePlan();
        RuleOperator singleOperator_1 = new RuleOperator(SingleOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        RuleOperator singleOperator_2 = new RuleOperator(SingleOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        RuleOperator multiOperator_1 = new RuleOperator(MultiOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        rulePlan.add(singleOperator_1);
        rulePlan.add(singleOperator_2);
        rulePlan.add(multiOperator_1);
        rulePlan.connect(singleOperator_1, singleOperator_2);
        rulePlan.connect(singleOperator_2, multiOperator_1);

        AlwaysTransform transformer = new AlwaysTransform(plan);
        Rule<TOperator, TPlan> r =
            new Rule<TOperator, TPlan>(rulePlan, transformer, "TestRule");

        TOptimizer optimizer = new TOptimizer(plan);
        optimizer.addRule(r);

        optimizer.optimize();
        assertFalse(transformer.mTransformed);
    }

    // Test that even if we match, if check returns false then the optimization
    // is not done.
    @Test
    public void testCheck() throws Exception {
        // Build a plan
        TPlan plan = new TPlan();
        TOperator[] ops = new TOperator[3];
        ops[0] = new SingleOperator("1");
        plan.add(ops[0]);
        ops[1] = new SingleOperator("2");
        plan.add(ops[1]);
        ops[2] = new MultiOperator("3");
        plan.add(ops[2]);
        plan.connect(ops[0], ops[1]);
        plan.connect(ops[1], ops[2]);

        // Create our rule
        RulePlan rulePlan = new RulePlan();
        RuleOperator singleOperator_1 = new RuleOperator(SingleOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        RuleOperator singleOperator_2 = new RuleOperator(SingleOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        RuleOperator multiOperator_1 = new RuleOperator(MultiOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        rulePlan.add(singleOperator_1);
        rulePlan.add(singleOperator_2);
        rulePlan.add(multiOperator_1);
        rulePlan.connect(singleOperator_1, singleOperator_2);
        rulePlan.connect(singleOperator_2, multiOperator_1);

        NeverTransform transformer = new NeverTransform(plan);
        Rule<TOperator, TPlan> r =
            new Rule<TOperator, TPlan>(rulePlan, transformer, "TestRule");

        TOptimizer optimizer = new TOptimizer(plan);
        optimizer.addRule(r);

        optimizer.optimize();
        assertFalse(transformer.mTransformed);
    }

    @Test
    public void testReplace() throws Exception {
        // Build a plan
        TPlan plan = new TPlan();
        TOperator[] ops = new TOperator[6];
        ops[0] = new MultiOperator("1");
        plan.add(ops[0]);
        ops[1] = new MultiOperator("2");
        plan.add(ops[1]);
        ops[2] = new MultiOperator("3");
        plan.add(ops[2]);
        ops[3] = new MultiOperator("4");
        plan.add(ops[3]);
        ops[4] = new MultiOperator("5");
        plan.add(ops[4]);
        plan.connect(ops[0], ops[2]);
        plan.connect(ops[1], ops[2]);
        plan.connect(ops[2], ops[3]);
        plan.connect(ops[2], ops[4]);
        ops[5] = new MultiOperator("6");
        plan.replace(ops[2], ops[5]);

        assertEquals("Nodes: 1 2 4 5 6 FromEdges: 1->6 2->6 6->4 6->5 ToEdges: 4->6 5->6 6->1 6->2 ", plan.display());
    }

    @Test
    public void testReplaceNoConnections() throws Exception {
        // Build a plan
        TPlan plan = new TPlan();
        TOperator[] ops = new TOperator[4];
        ops[0] = new MultiOperator("1");
        plan.add(ops[0]);
        ops[1] = new MultiOperator("2");
        plan.add(ops[1]);
        ops[2] = new MultiOperator("3");
        plan.add(ops[2]);
        plan.connect(ops[0], ops[2]);
        ops[3] = new MultiOperator("4");
        plan.replace(ops[1], ops[3]);

        assertEquals("Nodes: 1 3 4 FromEdges: 1->3 ToEdges: 3->1 ", plan.display());
    }
    
    // Input and pattern are both
    // S   S
    //  \ /
    //   M
    // Test that we match
    @Test
    public void testMultiInputPattern() throws Exception {
        // Build a plan
        TPlan plan = new TPlan();
        TOperator[] ops = new TOperator[3];
        ops[0] = new SingleOperator("1");
        plan.add(ops[0]);
        ops[1] = new SingleOperator("2");
        plan.add(ops[1]);
        ops[2] = new MultiOperator("3");
        plan.add(ops[2]);
        plan.connect(ops[0], ops[2]);
        plan.connect(ops[1], ops[2]);

        // Create our rule
        RulePlan rulePlan = new RulePlan();
        RuleOperator singleOperator_1 = new RuleOperator(SingleOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        RuleOperator singleOperator_2 = new RuleOperator(SingleOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        RuleOperator multiOperator_1 = new RuleOperator(MultiOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        rulePlan.add(singleOperator_1);
        rulePlan.add(singleOperator_2);
        rulePlan.add(multiOperator_1);
        rulePlan.connect(singleOperator_1, multiOperator_1);
        rulePlan.connect(singleOperator_2, multiOperator_1);

        AlwaysTransform alwaysTransform = new AlwaysTransform(plan);
        Rule<TOperator, TPlan> r1 =
            new Rule<TOperator, TPlan>(rulePlan, alwaysTransform, "TestRule");

        NeverTransform neverTransform = new NeverTransform(plan);
        Rule<TOperator, TPlan> r2 =
            new Rule<TOperator, TPlan>(rulePlan, neverTransform, "TestRule");

        TOptimizer optimizer = new TOptimizer(plan);
        optimizer.addRule(r1);
        optimizer.addRule(r2);

        optimizer.optimize();
        assertTrue(alwaysTransform.mTransformed);
        assertTrue(alwaysTransform.getNumberOfChecks() == MAX_OPTIMIZATION_ITERATIONS); //default max iterations
        assertFalse(neverTransform.mTransformed);
        assertTrue(neverTransform.getNumberOfChecks() == MAX_OPTIMIZATION_ITERATIONS); //default max iterations
    }
    
    // Pattern
    // S   M
    //  \ /
    //   M
    // Input has the roots swapped
    // M   S
    //  \ /
    //   M
    // Test that we match
    @Test
    public void testIsomorphicMultiInputPattern() throws Exception {
        // Build a plan
        TPlan plan = new TPlan();
        TOperator[] ops = new TOperator[3];
        ops[0] = new SingleOperator("1");
        plan.add(ops[0]);
        ops[1] = new MultiOperator("2");
        plan.add(ops[1]);
        ops[2] = new MultiOperator("3");
        plan.add(ops[2]);
        plan.connect(ops[0], ops[2]);
        plan.connect(ops[1], ops[2]);

        // Create our rule
        RulePlan rulePlan = new RulePlan();
        RuleOperator singleOperator_1 = new RuleOperator(SingleOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        RuleOperator multiOperator_1 = new RuleOperator(MultiOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        RuleOperator multiOperator_2 = new RuleOperator(MultiOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        rulePlan.add(singleOperator_1);
        rulePlan.add(multiOperator_1);
        rulePlan.add(multiOperator_2);
        rulePlan.connect(multiOperator_1, multiOperator_2);
        rulePlan.connect(singleOperator_1, multiOperator_2);

        AlwaysTransform alwaysTransform = new AlwaysTransform(plan);
        Rule<TOperator, TPlan> r1 =
            new Rule<TOperator, TPlan>(rulePlan, alwaysTransform, "TestRule");

        NeverTransform neverTransform = new NeverTransform(plan);
        Rule<TOperator, TPlan> r2 =
            new Rule<TOperator, TPlan>(rulePlan, neverTransform, "TestRule");

        TOptimizer optimizer = new TOptimizer(plan);
        optimizer.addRule(r1);
        optimizer.addRule(r2);

        optimizer.optimize();
        assertTrue(alwaysTransform.mTransformed);
        assertTrue(alwaysTransform.getNumberOfChecks() == MAX_OPTIMIZATION_ITERATIONS); //default max iterations
        assertFalse(neverTransform.mTransformed);
        assertTrue(neverTransform.getNumberOfChecks() == MAX_OPTIMIZATION_ITERATIONS); //default max iterations
    }
    
    // Input and pattern are both
    // M
    // ||
    // M
    // Test that we match
    @Test
    public void testMultiInputMultiOutputPattern() throws Exception {
        // Build a plan
        TPlan plan = new TPlan();
        TOperator[] ops = new TOperator[2];
        ops[0] = new MultiOperator("1");
        plan.add(ops[0]);
        ops[1] = new MultiOperator("2");
        plan.add(ops[1]);
        plan.connect(ops[0], ops[1]);
        plan.connect(ops[0], ops[1]);

        // Create our rule
        RulePlan rulePlan = new RulePlan();
        RuleOperator multiOperator_1 = new RuleOperator(MultiOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        RuleOperator multiOperator_2 = new RuleOperator(MultiOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        rulePlan.add(multiOperator_1);
        rulePlan.add(multiOperator_2);
        rulePlan.connect(multiOperator_1, multiOperator_2);
        rulePlan.connect(multiOperator_1, multiOperator_2);

        AlwaysTransform alwaysTransform = new AlwaysTransform(plan);
        Rule<TOperator, TPlan> r1 =
            new Rule<TOperator, TPlan>(rulePlan, alwaysTransform, "TestRule");
        
        NeverTransform neverTransform = new NeverTransform(plan);
        Rule<TOperator, TPlan> r2 =
            new Rule<TOperator, TPlan>(rulePlan, neverTransform, "TestRule");

        TOptimizer optimizer = new TOptimizer(plan);
        optimizer.addRule(r1);
        optimizer.addRule(r2);

        optimizer.optimize();
        assertTrue(alwaysTransform.mTransformed);
        assertTrue(alwaysTransform.getNumberOfChecks() == MAX_OPTIMIZATION_ITERATIONS); //default max iterations
        assertFalse(neverTransform.mTransformed);
        assertTrue(neverTransform.getNumberOfChecks() == MAX_OPTIMIZATION_ITERATIONS); //default max iterations
    }

    // Input and pattern are both
    //  M
    // / \
    // S  S
    // Test that we match
    @Test
    public void testMultiOutputPattern() throws Exception {
        // Build a plan
        TPlan plan = new TPlan();
        TOperator[] ops = new TOperator[3];
        ops[0] = new MultiOperator("1");
        plan.add(ops[0]);
        ops[1] = new SingleOperator("2");
        plan.add(ops[1]);
        ops[2] = new SingleOperator("3");
        plan.add(ops[2]);
        plan.connect(ops[0], ops[1]);
        plan.connect(ops[0], ops[2]);

        // Create our rule
        RulePlan rulePlan = new RulePlan();
        RuleOperator multiOperator_1 = new RuleOperator(MultiOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        RuleOperator singleOperator_1 = new RuleOperator(SingleOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        RuleOperator singleOperator_2 = new RuleOperator(SingleOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        rulePlan.add(multiOperator_1);
        rulePlan.add(singleOperator_1);
        rulePlan.add(singleOperator_2);
        rulePlan.connect(multiOperator_1, singleOperator_1);
        rulePlan.connect(multiOperator_1, singleOperator_2);

        AlwaysTransform alwaysTransform = new AlwaysTransform(plan);
        Rule<TOperator, TPlan> r1 =
            new Rule<TOperator, TPlan>(rulePlan, alwaysTransform, "TestRule");
        
        NeverTransform neverTransform = new NeverTransform(plan);
        Rule<TOperator, TPlan> r2 =
            new Rule<TOperator, TPlan>(rulePlan, neverTransform, "TestRule");

        TOptimizer optimizer = new TOptimizer(plan);
        optimizer.addRule(r1);
        optimizer.addRule(r2);

        optimizer.optimize();
        assertTrue(alwaysTransform.mTransformed);
        assertTrue(alwaysTransform.getNumberOfChecks() == MAX_OPTIMIZATION_ITERATIONS); //default max iterations
        assertFalse(neverTransform.mTransformed);
        assertTrue(neverTransform.getNumberOfChecks() == MAX_OPTIMIZATION_ITERATIONS); //default max iterations
    }
    
    // Pattern
    //  M
    // / \
    // S  S
    // Input
    //  M
    //  |
    //  S
    // Test that we don't match
    @Test
    public void testNegativeMultiOutputPattern() throws Exception {
        // Build a plan
        TPlan plan = new TPlan();
        TOperator[] ops = new TOperator[2];
        ops[0] = new MultiOperator("1");
        plan.add(ops[0]);
        ops[1] = new SingleOperator("2");
        plan.add(ops[1]);
        plan.connect(ops[0], ops[1]);

        // Create our rule
        RulePlan rulePlan = new RulePlan();
        RuleOperator multiOperator_1 = new RuleOperator(MultiOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        RuleOperator singleOperator_1 = new RuleOperator(SingleOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        RuleOperator singleOperator_2 = new RuleOperator(SingleOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        rulePlan.add(multiOperator_1);
        rulePlan.add(singleOperator_1);
        rulePlan.add(singleOperator_2);
        rulePlan.connect(multiOperator_1, singleOperator_1);
        rulePlan.connect(multiOperator_1, singleOperator_2);

        AlwaysTransform alwaysTransform = new AlwaysTransform(plan);
        Rule<TOperator, TPlan> r1 =
            new Rule<TOperator, TPlan>(rulePlan, alwaysTransform, "TestRule");
        
        NeverTransform neverTransform = new NeverTransform(plan);
        Rule<TOperator, TPlan> r2 =
            new Rule<TOperator, TPlan>(rulePlan, neverTransform, "TestRule");

        TOptimizer optimizer = new TOptimizer(plan);
        optimizer.addRule(r1);
        optimizer.addRule(r2);

        optimizer.optimize();
        assertFalse(alwaysTransform.mTransformed);
        assertTrue(alwaysTransform.getNumberOfChecks() == 0); //default max iterations
        assertFalse(neverTransform.mTransformed);
        assertTrue(neverTransform.getNumberOfChecks() == 0); //default max iterations
    }
    
    // Pattern
    //  M
    //  |
    //  M
    // Input
    //  M
    // ||
    //  M
    // Test that we don't match
    @Test
    public void testNegativeMultiOutputPattern1() throws Exception {
        // Build a plan
        TPlan plan = new TPlan();
        TOperator[] ops = new TOperator[2];
        ops[0] = new MultiOperator("1");
        plan.add(ops[0]);
        ops[1] = new MultiOperator("2");
        plan.add(ops[1]);
        plan.connect(ops[0], ops[1]);
        plan.connect(ops[0], ops[1]);

        // Create our rule
        RulePlan rulePlan = new RulePlan();
        RuleOperator multiOperator_1 = new RuleOperator(MultiOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        RuleOperator multiOperator_2 = new RuleOperator(MultiOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        RuleOperator singleOperator_2 = new RuleOperator(SingleOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        rulePlan.add(multiOperator_1);
        rulePlan.add(multiOperator_2);
        rulePlan.connect(multiOperator_1, multiOperator_2);

        AlwaysTransform alwaysTransform = new AlwaysTransform(plan);
        Rule<TOperator, TPlan> r1 =
            new Rule<TOperator, TPlan>(rulePlan, alwaysTransform, "TestRule");
        
        NeverTransform neverTransform = new NeverTransform(plan);
        Rule<TOperator, TPlan> r2 =
            new Rule<TOperator, TPlan>(rulePlan, neverTransform, "TestRule");

        TOptimizer optimizer = new TOptimizer(plan);
        optimizer.addRule(r1);
        optimizer.addRule(r2);

        optimizer.optimize();
        assertFalse(alwaysTransform.mTransformed);
        assertTrue(alwaysTransform.getNumberOfChecks() == 0); //default max iterations
        assertFalse(neverTransform.mTransformed);
        assertTrue(neverTransform.getNumberOfChecks() == 0); //default max iterations
    }
    
    // Pattern
    // S   S
    //  \ /
    //   M
    // Input
    // S   S    S   S
    //  \ /      \ /
    //   M        M
    // Test that we match multiple instances in the disconnected graph
    @Test
    public void testMultipleMultiInputPatternInDisconnectedGraph() throws Exception {
        // Build a plan
        TPlan plan = new TPlan();
        TOperator[] ops = new TOperator[6];
        ops[0] = new SingleOperator("1");
        plan.add(ops[0]);
        ops[1] = new SingleOperator("2");
        plan.add(ops[1]);
        ops[2] = new MultiOperator("3");
        plan.add(ops[2]);
        plan.connect(ops[0], ops[2]);
        plan.connect(ops[1], ops[2]);
        
        ops[3] = new SingleOperator("4");
        plan.add(ops[3]);
        ops[4] = new SingleOperator("5");
        plan.add(ops[4]);
        ops[5] = new MultiOperator("6");
        plan.add(ops[5]);
        plan.connect(ops[3], ops[5]);
        plan.connect(ops[4], ops[5]);


        // Create our rule
        RulePlan rulePlan = new RulePlan();
        RuleOperator singleOperator_1 = new RuleOperator(SingleOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        RuleOperator singleOperator_2 = new RuleOperator(SingleOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        RuleOperator multiOperator_1 = new RuleOperator(MultiOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        rulePlan.add(singleOperator_1);
        rulePlan.add(singleOperator_2);
        rulePlan.add(multiOperator_1);
        rulePlan.connect(singleOperator_1, multiOperator_1);
        rulePlan.connect(singleOperator_2, multiOperator_1);

        AlwaysTransform alwaysTransform = new AlwaysTransform(plan);
        Rule<TOperator, TPlan> r1 =
            new Rule<TOperator, TPlan>(rulePlan, alwaysTransform, "TestRule");

        NeverTransform neverTransform = new NeverTransform(plan);
        Rule<TOperator, TPlan> r2 =
            new Rule<TOperator, TPlan>(rulePlan, neverTransform, "TestRule");

        TOptimizer optimizer = new TOptimizer(plan);
        optimizer.addRule(r1);
        optimizer.addRule(r2);

        optimizer.optimize();
        assertTrue(alwaysTransform.mTransformed);
        assertTrue(alwaysTransform.getNumberOfChecks() == (2* MAX_OPTIMIZATION_ITERATIONS)); 
        assertFalse(neverTransform.mTransformed);
        assertTrue(neverTransform.getNumberOfChecks() == (2 * MAX_OPTIMIZATION_ITERATIONS));
    }

    // Pattern is
    // S   S
    //  \ /
    //   M
    // Input
    // S   S    S   S
    //  \ /      \ /
    //   M        M
    //    \      /
    //       M
    // Test that we match multiple instances in a connected graph
    @Test
    public void testMultipleMultiInputPattern() throws Exception {
        // Build a plan
        TPlan plan = new TPlan();
        TOperator[] ops = new TOperator[7];
        ops[0] = new SingleOperator("1");
        plan.add(ops[0]);
        ops[1] = new SingleOperator("2");
        plan.add(ops[1]);
        ops[2] = new MultiOperator("3");
        plan.add(ops[2]);
        plan.connect(ops[0], ops[2]);
        plan.connect(ops[1], ops[2]);
        
        ops[3] = new SingleOperator("4");
        plan.add(ops[3]);
        ops[4] = new SingleOperator("5");
        plan.add(ops[4]);
        ops[5] = new MultiOperator("6");
        plan.add(ops[5]);
        plan.connect(ops[3], ops[5]);
        plan.connect(ops[4], ops[5]);

        ops[6] = new MultiOperator("7");
        plan.add(ops[6]);
        plan.connect(ops[2], ops[6]);
        plan.connect(ops[5], ops[6]);

        // Create our rule
        RulePlan rulePlan = new RulePlan();
        RuleOperator singleOperator_1 = new RuleOperator(SingleOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        RuleOperator singleOperator_2 = new RuleOperator(SingleOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        RuleOperator multiOperator_1 = new RuleOperator(MultiOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        rulePlan.add(singleOperator_1);
        rulePlan.add(singleOperator_2);
        rulePlan.add(multiOperator_1);
        rulePlan.connect(singleOperator_1, multiOperator_1);
        rulePlan.connect(singleOperator_2, multiOperator_1);

        AlwaysTransform alwaysTransform = new AlwaysTransform(plan);
        Rule<TOperator, TPlan> r1 =
            new Rule<TOperator, TPlan>(rulePlan, alwaysTransform, "TestRule");

        NeverTransform neverTransform = new NeverTransform(plan);
        Rule<TOperator, TPlan> r2 =
            new Rule<TOperator, TPlan>(rulePlan, neverTransform, "TestRule");

        TOptimizer optimizer = new TOptimizer(plan);
        optimizer.addRule(r1);
        optimizer.addRule(r2);

        optimizer.optimize();
        assertTrue(alwaysTransform.mTransformed);
        assertTrue(alwaysTransform.getNumberOfChecks() == (2 * MAX_OPTIMIZATION_ITERATIONS));
        assertFalse(neverTransform.mTransformed);
        assertTrue(neverTransform.getNumberOfChecks() == (2 * MAX_OPTIMIZATION_ITERATIONS));
    }

    // Pattern is
    // S   S
    //  \ /
    //   M
    // Test that we match only one instance in a connected graph
    @Test
    public void testSingleMultiInputPattern() throws Exception {
        // Build a plan
        TPlan plan = new TPlan();
        TOperator[] ops = new TOperator[6];
        ops[0] = new SingleOperator("1");
        plan.add(ops[0]);
        ops[1] = new SingleOperator("2");
        plan.add(ops[1]);
        ops[2] = new MultiOperator("3");
        plan.add(ops[2]);
        plan.connect(ops[0], ops[2]);
        plan.connect(ops[1], ops[2]);
        
        ops[3] = new SingleOperator("4");
        plan.add(ops[3]);
        ops[4] = new MultiOperator("5");
        plan.add(ops[4]);
        plan.connect(ops[3], ops[4]);

        ops[5] = new MultiOperator("6");
        plan.add(ops[5]);
        plan.connect(ops[4], ops[5]);

        // Create our rule
        RulePlan rulePlan = new RulePlan();
        RuleOperator singleOperator_1 = new RuleOperator(SingleOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        RuleOperator singleOperator_2 = new RuleOperator(SingleOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        RuleOperator multiOperator_1 = new RuleOperator(MultiOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        rulePlan.add(singleOperator_1);
        rulePlan.add(singleOperator_2);
        rulePlan.add(multiOperator_1);
        rulePlan.connect(singleOperator_1, multiOperator_1);
        rulePlan.connect(singleOperator_2, multiOperator_1);

        AlwaysTransform alwaysTransform = new AlwaysTransform(plan);
        Rule<TOperator, TPlan> r1 =
            new Rule<TOperator, TPlan>(rulePlan, alwaysTransform, "TestRule");

        NeverTransform neverTransform = new NeverTransform(plan);
        Rule<TOperator, TPlan> r2 =
            new Rule<TOperator, TPlan>(rulePlan, neverTransform, "TestRule");

        TOptimizer optimizer = new TOptimizer(plan);
        optimizer.addRule(r1);
        optimizer.addRule(r2);

        optimizer.optimize();
        assertTrue(alwaysTransform.mTransformed);
        assertTrue(alwaysTransform.getNumberOfChecks() == MAX_OPTIMIZATION_ITERATIONS);
        assertFalse(neverTransform.mTransformed);
        assertTrue(neverTransform.getNumberOfChecks() == MAX_OPTIMIZATION_ITERATIONS);
    }
    
    // Input and pattern are both
    //  M
    // / \
    // S  S
    // \ /
    //  M
    // Test that we match
    @Test
    public void testDiamondPattern() throws Exception {
        // Build a plan
        TPlan plan = new TPlan();
        TOperator[] ops = new TOperator[4];
        ops[0] = new MultiOperator("1");
        plan.add(ops[0]);
        ops[1] = new SingleOperator("2");
        plan.add(ops[1]);
        ops[2] = new SingleOperator("3");
        plan.add(ops[2]);
        ops[3] = new MultiOperator("4");
        plan.add(ops[3]);
        plan.connect(ops[0], ops[1]);
        plan.connect(ops[0], ops[2]);
        plan.connect(ops[1], ops[3]);
        plan.connect(ops[2], ops[3]);

        // Create our rule
        RulePlan rulePlan = new RulePlan();
        RuleOperator multiOperator_1 = new RuleOperator(MultiOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        RuleOperator singleOperator_1 = new RuleOperator(SingleOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        RuleOperator singleOperator_2 = new RuleOperator(SingleOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        RuleOperator multiOperator_2 = new RuleOperator(MultiOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        rulePlan.add(multiOperator_1);
        rulePlan.add(singleOperator_1);
        rulePlan.add(singleOperator_2);
        rulePlan.add(multiOperator_2);
        rulePlan.connect(multiOperator_1, singleOperator_1);
        rulePlan.connect(multiOperator_1, singleOperator_2);
        rulePlan.connect(singleOperator_1, multiOperator_2);
        rulePlan.connect(singleOperator_2, multiOperator_2);

        AlwaysTransform alwaysTransform = new AlwaysTransform(plan);
        Rule<TOperator, TPlan> r1 =
            new Rule<TOperator, TPlan>(rulePlan, alwaysTransform, "TestRule");
        
        NeverTransform neverTransform = new NeverTransform(plan);
        Rule<TOperator, TPlan> r2 =
            new Rule<TOperator, TPlan>(rulePlan, neverTransform, "TestRule");

        TOptimizer optimizer = new TOptimizer(plan);
        optimizer.addRule(r1);
        optimizer.addRule(r2);

        optimizer.optimize();
        assertTrue(alwaysTransform.mTransformed);
        assertTrue(alwaysTransform.getNumberOfChecks() == MAX_OPTIMIZATION_ITERATIONS); //default max iterations
        assertFalse(neverTransform.mTransformed);
        assertTrue(neverTransform.getNumberOfChecks() == MAX_OPTIMIZATION_ITERATIONS); //default max iterations
    }

    // Pattern 
    //  M
    // / \
    // S  S
    // \ /
    //  M
    // Input has an additional edge from the bottom of the diamond
    //  M
    // / \
    // S  S
    // \ /
    //  M
    //  |
    //  S
    // Test that we match
    @Test
    public void testDiamondWithEdgePattern() throws Exception {
        // Build a plan
        TPlan plan = new TPlan();
        TOperator[] ops = new TOperator[5];
        ops[0] = new MultiOperator("1");
        plan.add(ops[0]);
        ops[1] = new SingleOperator("2");
        plan.add(ops[1]);
        ops[2] = new SingleOperator("3");
        plan.add(ops[2]);
        ops[3] = new MultiOperator("4");
        plan.add(ops[3]);
        ops[4] = new SingleOperator("5");
        plan.add(ops[4]);
        plan.connect(ops[0], ops[1]);
        plan.connect(ops[0], ops[2]);
        plan.connect(ops[1], ops[3]);
        plan.connect(ops[2], ops[3]);
        plan.connect(ops[3], ops[4]);

        // Create our rule
        RulePlan rulePlan = new RulePlan();
        RuleOperator multiOperator_1 = new RuleOperator(MultiOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        RuleOperator singleOperator_1 = new RuleOperator(SingleOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        RuleOperator singleOperator_2 = new RuleOperator(SingleOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        RuleOperator multiOperator_2 = new RuleOperator(MultiOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        rulePlan.add(multiOperator_1);
        rulePlan.add(singleOperator_1);
        rulePlan.add(singleOperator_2);
        rulePlan.add(multiOperator_2);
        rulePlan.connect(multiOperator_1, singleOperator_1);
        rulePlan.connect(multiOperator_1, singleOperator_2);
        rulePlan.connect(singleOperator_1, multiOperator_2);
        rulePlan.connect(singleOperator_2, multiOperator_2);

        AlwaysTransform alwaysTransform = new AlwaysTransform(plan);
        Rule<TOperator, TPlan> r1 =
            new Rule<TOperator, TPlan>(rulePlan, alwaysTransform, "TestRule");
        
        NeverTransform neverTransform = new NeverTransform(plan);
        Rule<TOperator, TPlan> r2 =
            new Rule<TOperator, TPlan>(rulePlan, neverTransform, "TestRule");

        TOptimizer optimizer = new TOptimizer(plan);
        optimizer.addRule(r1);
        optimizer.addRule(r2);

        optimizer.optimize();
        assertTrue(alwaysTransform.mTransformed);
        assertTrue(alwaysTransform.getNumberOfChecks() == MAX_OPTIMIZATION_ITERATIONS); //default max iterations
        assertFalse(neverTransform.mTransformed);
        assertTrue(neverTransform.getNumberOfChecks() == MAX_OPTIMIZATION_ITERATIONS); //default max iterations
    }
    
    // Input and Pattern is
    // S   S
    //  \ /
    //   M
    //   |
    //   M
    //  / \
    // S   S
    // Test that we match once
    @Test
    public void testComplexInputPattern() throws Exception {
        // Build a plan
        TPlan plan = new TPlan();
        TOperator[] ops = new TOperator[6];
        ops[0] = new SingleOperator("1");
        plan.add(ops[0]);
        ops[1] = new SingleOperator("2");
        plan.add(ops[1]);
        ops[2] = new MultiOperator("3");
        plan.add(ops[2]);
        plan.connect(ops[0], ops[2]);
        plan.connect(ops[1], ops[2]);
        
        ops[3] = new SingleOperator("4");
        plan.add(ops[3]);
        ops[4] = new SingleOperator("5");
        plan.add(ops[4]);
        ops[5] = new MultiOperator("6");
        plan.add(ops[5]);
        plan.connect(ops[5], ops[3]);
        plan.connect(ops[5], ops[4]);

        plan.connect(ops[2], ops[5]);

        // Create our rule
        RulePlan rulePlan = new RulePlan();
        RuleOperator singleOperator_1 = new RuleOperator(SingleOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        RuleOperator singleOperator_2 = new RuleOperator(SingleOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        RuleOperator multiOperator_1 = new RuleOperator(MultiOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        rulePlan.add(singleOperator_1);
        rulePlan.add(singleOperator_2);
        rulePlan.add(multiOperator_1);
        rulePlan.connect(singleOperator_1, multiOperator_1);
        rulePlan.connect(singleOperator_2, multiOperator_1);
        
        RuleOperator singleOperator_3 = new RuleOperator(SingleOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        RuleOperator singleOperator_4 = new RuleOperator(SingleOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        RuleOperator multiOperator_2 = new RuleOperator(MultiOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        rulePlan.add(singleOperator_3);
        rulePlan.add(singleOperator_4);
        rulePlan.add(multiOperator_2);
        rulePlan.connect(multiOperator_2, singleOperator_3);
        rulePlan.connect(multiOperator_2, singleOperator_4);
        
        rulePlan.connect(multiOperator_1, multiOperator_2);

        AlwaysTransform alwaysTransform = new AlwaysTransform(plan);
        Rule<TOperator, TPlan> r1 =
            new Rule<TOperator, TPlan>(rulePlan, alwaysTransform, "TestRule");

        NeverTransform neverTransform = new NeverTransform(plan);
        Rule<TOperator, TPlan> r2 =
            new Rule<TOperator, TPlan>(rulePlan, neverTransform, "TestRule");

        TOptimizer optimizer = new TOptimizer(plan);
        optimizer.addRule(r1);
        optimizer.addRule(r2);

        optimizer.optimize();
        assertTrue(alwaysTransform.mTransformed);
        assertTrue(alwaysTransform.getNumberOfChecks() == MAX_OPTIMIZATION_ITERATIONS);
        assertFalse(neverTransform.mTransformed);
        assertTrue(neverTransform.getNumberOfChecks() == MAX_OPTIMIZATION_ITERATIONS);
    }
    
    // Pattern is
    // S   S
    //  \ /
    //   M
    //   ||
    //   M
    //  / \
    // S   S
    // Input is
    // S   S
    //  \ /
    //   M
    //   |
    //   M
    //  / \
    // S   S
    // Test that we don't match
    @Test
    public void testNegativeComplexInputPattern() throws Exception {
        // Build a plan
        TPlan plan = new TPlan();
        TOperator[] ops = new TOperator[6];
        ops[0] = new SingleOperator("1");
        plan.add(ops[0]);
        ops[1] = new SingleOperator("2");
        plan.add(ops[1]);
        ops[2] = new MultiOperator("3");
        plan.add(ops[2]);
        plan.connect(ops[0], ops[2]);
        plan.connect(ops[1], ops[2]);
        
        ops[3] = new SingleOperator("4");
        plan.add(ops[3]);
        ops[4] = new SingleOperator("5");
        plan.add(ops[4]);
        ops[5] = new MultiOperator("6");
        plan.add(ops[5]);
        plan.connect(ops[5], ops[3]);
        plan.connect(ops[5], ops[4]);

        plan.connect(ops[2], ops[5]);

        // Create our rule
        RulePlan rulePlan = new RulePlan();
        RuleOperator singleOperator_1 = new RuleOperator(SingleOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        RuleOperator singleOperator_2 = new RuleOperator(SingleOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        RuleOperator multiOperator_1 = new RuleOperator(MultiOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        rulePlan.add(singleOperator_1);
        rulePlan.add(singleOperator_2);
        rulePlan.add(multiOperator_1);
        rulePlan.connect(singleOperator_1, multiOperator_1);
        rulePlan.connect(singleOperator_2, multiOperator_1);
        
        RuleOperator singleOperator_3 = new RuleOperator(SingleOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        RuleOperator singleOperator_4 = new RuleOperator(SingleOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        RuleOperator multiOperator_2 = new RuleOperator(MultiOperator.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        rulePlan.add(singleOperator_3);
        rulePlan.add(singleOperator_4);
        rulePlan.add(multiOperator_2);
        rulePlan.connect(multiOperator_2, singleOperator_3);
        rulePlan.connect(multiOperator_2, singleOperator_4);
        
        rulePlan.connect(multiOperator_1, multiOperator_2);
        rulePlan.connect(multiOperator_1, multiOperator_2);

        AlwaysTransform alwaysTransform = new AlwaysTransform(plan);
        Rule<TOperator, TPlan> r1 =
            new Rule<TOperator, TPlan>(rulePlan, alwaysTransform, "TestRule");

        NeverTransform neverTransform = new NeverTransform(plan);
        Rule<TOperator, TPlan> r2 =
            new Rule<TOperator, TPlan>(rulePlan, neverTransform, "TestRule");

        TOptimizer optimizer = new TOptimizer(plan);
        optimizer.addRule(r1);
        optimizer.addRule(r2);

        optimizer.optimize();
        assertFalse(alwaysTransform.mTransformed);
        assertTrue(alwaysTransform.getNumberOfChecks() == 0);
        assertFalse(neverTransform.mTransformed);
        assertTrue(neverTransform.getNumberOfChecks() == 0);
    }
    
    //Swap two roots in a graph. Both the roots are disconnected
    //and are the only nodes in the graph
    @Test
    public void testSwapRootsInDisconnectedGraph() throws Exception {
        TPlan plan = new TPlan();
        TOperator[] ops = new TOperator[2];
        
        for(int i = 0; i < ops.length; ++i) {
            ops[i] = new SingleOperator(Integer.toString(i));
            plan.add(ops[i]);
        }
        
        plan.swap(ops[0], ops[1]);
        
        List<TOperator> roots = (ArrayList<TOperator>)plan.getRoots();
        for(int i = 0; i < roots.size(); ++i) {
            assertEquals(roots.get(i), ops[i]);
        }
    }
    
    //Swap two nodes in a graph.
    //Input
    //S1->S2
    //Ouput
    //S2->S1
    //Swap again
    //Output
    //S1->S2
    @Test
    public void testSimpleSwap() throws Exception {
        TPlan plan = new TPlan();
        TOperator[] ops = new TOperator[2];
        
        for(int i = 0; i < ops.length; ++i) {
            ops[i] = new SingleOperator(Integer.toString(i));
            plan.add(ops[i]);
        }
        
        plan.connect(ops[0], ops[1]);

        //PlanPrinter<TOperator, TPlan> planPrinter = new PlanPrinter<TOperator, TPlan>(System.err, plan);
        //planPrinter.visit();
        
        plan.swap(ops[0], ops[1]);
        
        //planPrinter.visit();
        
        List<TOperator> roots = (ArrayList<TOperator>)plan.getRoots();
        assertEquals(roots.get(0), ops[1]);
        
        List<TOperator> rootSuccessors = plan.getSuccessors(roots.get(0));
        assertEquals(rootSuccessors.get(0), ops[0]);
        
        List<TOperator> leaves = (ArrayList<TOperator>)plan.getLeaves();
        assertEquals(leaves.get(0), ops[0]);
        
        List<TOperator> leafPredecessors = plan.getPredecessors(leaves.get(0));
        assertEquals(leafPredecessors.get(0), ops[1]);
        
        plan.swap(ops[0], ops[1]);
        
        //planPrinter.visit();
        
        roots = (ArrayList<TOperator>)plan.getRoots();
        assertEquals(roots.get(0), ops[0]);
        
        rootSuccessors = plan.getSuccessors(roots.get(0));
        assertEquals(rootSuccessors.get(0), ops[1]);
        
        leaves = (ArrayList<TOperator>)plan.getLeaves();
        assertEquals(leaves.get(0), ops[1]);
        
        leafPredecessors = plan.getPredecessors(leaves.get(0));
        assertEquals(leafPredecessors.get(0), ops[0]);
    }
    
    //Swap two nodes in a graph.
    //Swap S1 and S3
    //Input
    //S1->S2->S3
    //Intermediate Output
    //S3->S2->S1
    //Output
    //S1->S2->S3
    @Test
    public void testSimpleSwap2() throws Exception {
        TPlan plan = new TPlan();
        TOperator[] ops = new TOperator[3];
        
        for(int i = 0; i < ops.length; ++i) {
            ops[i] = new SingleOperator(Integer.toString(i));
            plan.add(ops[i]);
        }
        
        plan.connect(ops[0], ops[1]);
        plan.connect(ops[1], ops[2]);

        //PlanPrinter<TOperator, TPlan> planPrinter = new PlanPrinter<TOperator, TPlan>(System.err, plan);
        //planPrinter.visit();
        
        plan.swap(ops[0], ops[2]);

        //planPrinter.visit();
        
        List<TOperator> roots = (ArrayList<TOperator>)plan.getRoots();
        assertEquals(roots.get(0), ops[2]);
        
        List<TOperator> rootSuccessors = plan.getSuccessors(roots.get(0));
        assertEquals(rootSuccessors.get(0), ops[1]);
        
        List<TOperator> leaves = (ArrayList<TOperator>)plan.getLeaves();
        assertEquals(leaves.get(0), ops[0]);
        
        List<TOperator> leafPredecessors = plan.getPredecessors(leaves.get(0));
        assertEquals(leafPredecessors.get(0), ops[1]);
        
        plan.swap(ops[0], ops[2]);
        
        //planPrinter.visit();
        
        roots = (ArrayList<TOperator>)plan.getRoots();
        assertEquals(roots.get(0), ops[0]);
        
        rootSuccessors = plan.getSuccessors(roots.get(0));
        assertEquals(rootSuccessors.get(0), ops[1]);
        
        leaves = (ArrayList<TOperator>)plan.getLeaves();
        assertEquals(leaves.get(0), ops[2]);
        
        leafPredecessors = plan.getPredecessors(leaves.get(0));
        assertEquals(leafPredecessors.get(0), ops[1]);
    }
    
    //Swap two nodes in a graph and then swap it back again.
    //Swap S2 and S3
    //Input
    //S1->S2->S3
    //Intermediate Output
    //S1->S3->S2
    //Output
    //S1->S2->S3
    @Test
    public void testSimpleSwap3() throws Exception {
        TPlan plan = new TPlan();
        TOperator[] ops = new TOperator[3];
        
        for(int i = 0; i < ops.length; ++i) {
            ops[i] = new SingleOperator(Integer.toString(i));
            plan.add(ops[i]);
        }
        
        plan.connect(ops[0], ops[1]);
        plan.connect(ops[1], ops[2]);

        //PlanPrinter<TOperator, TPlan> planPrinter = new PlanPrinter<TOperator, TPlan>(System.err, plan);
        //planPrinter.visit();

        plan.swap(ops[1], ops[2]);
        
        //planPrinter.visit();

        List<TOperator> roots = (ArrayList<TOperator>)plan.getRoots();
        assertEquals(roots.get(0), ops[0]);
        
        List<TOperator> rootSuccessors = plan.getSuccessors(roots.get(0));
        assertEquals(rootSuccessors.get(0), ops[2]);
        
        List<TOperator> leaves = (ArrayList<TOperator>)plan.getLeaves();
        assertEquals(leaves.get(0), ops[1]);
        
        List<TOperator> leafPredecessors = plan.getPredecessors(leaves.get(0));
        assertEquals(leafPredecessors.get(0), ops[2]);
        
        plan.swap(ops[1], ops[2]);
        
        //planPrinter.visit();

        roots = (ArrayList<TOperator>)plan.getRoots();
        assertEquals(roots.get(0), ops[0]);
        
        rootSuccessors = plan.getSuccessors(roots.get(0));
        assertEquals(rootSuccessors.get(0), ops[1]);
        
        leaves = (ArrayList<TOperator>)plan.getLeaves();
        assertEquals(leaves.get(0), ops[2]);
        
        leafPredecessors = plan.getPredecessors(leaves.get(0));
        assertEquals(leafPredecessors.get(0), ops[1]);
    }
    
    //Swap two nodes in a graph and then swap it back again.
    //Swap S1 and S2
    //Input
    //S1->S2->S3
    //Intermediate Output
    //S2->S1->S3
    //Output
    //S1->S2->S3
    @Test
    public void testSimpleSwap4() throws Exception {
        TPlan plan = new TPlan();
        TOperator[] ops = new TOperator[3];
        
        for(int i = 0; i < ops.length; ++i) {
            ops[i] = new SingleOperator(Integer.toString(i));
            plan.add(ops[i]);
        }
        
        plan.connect(ops[0], ops[1]);
        plan.connect(ops[1], ops[2]);

        //PlanPrinter<TOperator, TPlan> planPrinter = new PlanPrinter<TOperator, TPlan>(System.err, plan);
        //planPrinter.visit();

        plan.swap(ops[0], ops[1]);

        //planPrinter.visit();
        
        List<TOperator> roots = (ArrayList<TOperator>)plan.getRoots();
        assertEquals(roots.get(0), ops[1]);
        
        List<TOperator> rootSuccessors = plan.getSuccessors(roots.get(0));
        assertEquals(rootSuccessors.get(0), ops[0]);
        
        List<TOperator> leaves = (ArrayList<TOperator>)plan.getLeaves();
        assertEquals(leaves.get(0), ops[2]);
        
        List<TOperator> leafPredecessors = plan.getPredecessors(leaves.get(0));
        assertEquals(leafPredecessors.get(0), ops[0]);
        
        plan.swap(ops[0], ops[1]);

        //planPrinter.visit();
        
        roots = (ArrayList<TOperator>)plan.getRoots();
        assertEquals(roots.get(0), ops[0]);
        
        rootSuccessors = plan.getSuccessors(roots.get(0));
        assertEquals(rootSuccessors.get(0), ops[1]);
        
        leaves = (ArrayList<TOperator>)plan.getLeaves();
        assertEquals(leaves.get(0), ops[2]);
        
        leafPredecessors = plan.getPredecessors(leaves.get(0));
        assertEquals(leafPredecessors.get(0), ops[1]);
    }
    
    //Swap non-existent nodes in a graph and check for exceptions
    //Swap S1 and S4
    //Swap S4 and S1
    //Swap S5 and S4
    //Swap S1 and null
    //Swap null and S1
    //Swap null and null
    //Input
    //S1->S2->S3 S4 S5
    @Test
    public void testNegativeSimpleSwap() throws Exception {
        TPlan plan = new TPlan();
        TOperator[] ops = new TOperator[5];
        
        for(int i = 0; i < ops.length; ++i) {
            ops[i] = new SingleOperator(Integer.toString(i));
        }

        for(int i = 0; i < ops.length - 2; ++i) {
            plan.add(ops[i]);
        }

        
        plan.connect(ops[0], ops[1]);
        plan.connect(ops[1], ops[2]);

        //PlanPrinter<TOperator, TPlan> planPrinter = new PlanPrinter<TOperator, TPlan>(System.err, plan);
        //planPrinter.visit();

        try {
            plan.swap(ops[0], ops[3]);
            fail("Expected exception for node not in plan.");
        } catch (PlanException pe) {
            assertTrue(pe.getMessage().contains("not in the plan"));
        }
        
        try {
            plan.swap(ops[3], ops[0]);
            fail("Expected exception for node not in plan.");
        } catch (PlanException pe) {
            assertTrue(pe.getMessage().contains("not in the plan"));
        }

        try {
            plan.swap(ops[4], ops[3]);
            fail("Expected exception for node not in plan.");
        } catch (PlanException pe) {
            assertTrue(pe.getMessage().contains("not in the plan"));
        }
        
        try {
            plan.swap(ops[0], null);
            fail("Expected exception for having null as one of the inputs");
        } catch (PlanException pe) {
            assertTrue(pe.getErrorCode() == 1092);
        }
        
        try {
            plan.swap(null, ops[0]);
            fail("Expected exception for having null as one of the inputs");
        } catch (PlanException pe) {
            assertTrue(pe.getErrorCode() == 1092);
        }
        
        try {
            plan.swap(null, null);
            fail("Expected exception for having null as one of the inputs");
        } catch (PlanException pe) {
            assertTrue(pe.getErrorCode() == 1092);
        }

    }
    
    //Swap nodes that have multiple inputs and multiple outs in a graph and check for exceptions
    //Input
    // S1   S2
    //  \ /
    //   M1
    //   |
    //   M2
    //  / \
    // S3   S4
    //Swap S1 and M1
    //Swap M1 and S1
    //Swap M1 and M2
    //Swap M2 and M1
    //Swap M2 and S3
    //Swap S3 and M2
    @Test
    public void testNegativeSimpleSwap1() throws Exception {
        TPlan plan = new TPlan();
        TOperator[] ops = new TOperator[6];
        ops[0] = new SingleOperator("1");
        plan.add(ops[0]);
        ops[1] = new SingleOperator("2");
        plan.add(ops[1]);
        ops[2] = new MultiOperator("3");
        plan.add(ops[2]);
        plan.connect(ops[0], ops[2]);
        plan.connect(ops[1], ops[2]);
        
        ops[3] = new SingleOperator("4");
        plan.add(ops[3]);
        ops[4] = new SingleOperator("5");
        plan.add(ops[4]);
        ops[5] = new MultiOperator("6");
        plan.add(ops[5]);
        plan.connect(ops[5], ops[3]);
        plan.connect(ops[5], ops[4]);

        plan.connect(ops[2], ops[5]);

        //PlanPrinter<TOperator, TPlan> planPrinter = new PlanPrinter<TOperator, TPlan>(System.err, plan);
        //planPrinter.visit();

        try {
            plan.swap(ops[0], ops[2]);
            fail("Expected exception for multi-input operator.");
        } catch (PlanException pe) {
            assertTrue(pe.getErrorCode() == 1093);
        }
        
        try {
            plan.swap(ops[2], ops[0]);
            fail("Expected exception for multi-input operator.");
        } catch (PlanException pe) {
            assertTrue(pe.getErrorCode() == 1093);
        }

        try {
            plan.swap(ops[2], ops[5]);
            fail("Expected exception for multi-input operator.");
        } catch (PlanException pe) {
            assertTrue(pe.getErrorCode() == 1093);
        }
        
        try {
            plan.swap(ops[5], ops[2]);
            fail("Expected exception for multi-output operator.");
        } catch (PlanException pe) {
            assertTrue(pe.getErrorCode() == 1093);
        }
        
        try {
            plan.swap(ops[5], ops[3]);
            fail("Expected exception for multi-output operator.");
        } catch (PlanException pe) {
            assertTrue(pe.getErrorCode() == 1093);
        }
        
        try {
            plan.swap(ops[3], ops[5]);
            fail("Expected exception for multi-output operator.");
        } catch (PlanException pe) {
            assertTrue(pe.getErrorCode() == 1093);
        }

    }
    
    //Push M11 before M10's inputs - 0 through 3
    //Input
    //  S1   S2  S3  S4
    //   \   |   |  /
    //        M10
    //      /  |  \
    //     S5  M11  S6
    //      /  |  \
    //     S7  S8  S9
    //Output when pushed before 1st input
    //       S2
    //       |
    //  S1   M11  S3  S4
    //   \   |   |  /
    //       M10
    //   / / |  \ \
    // S5 S7 S8 S9 S6
    @Test
    public void testpushBefore() throws Exception {
        for(int index = 0; index < 4; index++) {
            TPlan plan = new TPlan();
            TOperator[] ops = new TOperator[11];
            
            for(int i = 0; i < ops.length - 2; ++i) {
                ops[i] = new SingleOperator(Integer.toString(i+1));
                plan.add(ops[i]);
            }
            
            ops[9] = new MultiOperator("10");
            plan.add(ops[9]);
            
            ops[10] = new MultiOperator("11");
            plan.add(ops[10]);
            
            plan.connect(ops[0], ops[9]);
            plan.connect(ops[1], ops[9]);
            plan.connect(ops[2], ops[9]);
            plan.connect(ops[3], ops[9]);
            plan.connect(ops[9], ops[4]);
            plan.connect(ops[9], ops[10]);
            plan.connect(ops[9], ops[5]);
            
            plan.connect(ops[10], ops[6]);
            plan.connect(ops[10], ops[7]);
            plan.connect(ops[10], ops[8]);
            
            //PlanPrinter<TOperator, TPlan> planPrinter = new PlanPrinter<TOperator, TPlan>(System.err, plan);
            //planPrinter.visit();
    
            plan.pushBefore(ops[9], ops[10], index) ;
            
            //planPrinter.visit();
            
            Set<TOperator> rootSet = new HashSet<TOperator>();
            rootSet.add(ops[0]);
            rootSet.add(ops[1]);
            rootSet.add(ops[2]);
            rootSet.add(ops[3]);
            
            Set<TOperator> expectedRootSet = new HashSet<TOperator>(plan.getRoots());
            
            rootSet.retainAll(expectedRootSet);
            assertTrue(rootSet.size() == 4);
            
            Set<TOperator> leafSet = new HashSet<TOperator>();
            leafSet.add(ops[4]);
            leafSet.add(ops[5]);
            leafSet.add(ops[6]);
            leafSet.add(ops[7]);
            leafSet.add(ops[8]);
            
            Set<TOperator> expectedLeafSet = new HashSet<TOperator>(plan.getLeaves());
            
            leafSet.retainAll(expectedLeafSet);
            assertTrue(leafSet.size() == 5);
            
            List<TOperator> m10Predecessors = plan.getPredecessors(ops[9]);            
            assertTrue(m10Predecessors.get(index) == ops[10]);
            
            List<TOperator> m11Predecessors = plan.getPredecessors(ops[10]);
            assertTrue(m11Predecessors.get(0) == ops[index]);
        }
    }
    
    //Push S5 and S6 before M10's input
    //Input
    //  S1   S2  S3  S4
    //   \   |   |  /
    //        M10
    //      /  |  \
    //     S5  M11  S6
    //      /  |  \
    //     S7  S8  S9
    //Output when pushed before 1st input
    //       S2
    //       |
    //  S1   S5  S3  S4
    //   \   |   |  /
    //        M10
    //       /   \
    //      M11   S6
    //    /  |  \
    //   S7  S8  S9
    @Test
    public void testpushBefore2() throws Exception {
        for(int outerIndex = 0; outerIndex < 2; outerIndex++) {
            for(int index = 0; index < 2; index++) {
                TPlan plan = new TPlan();
                TOperator[] ops = new TOperator[11];
                
                for(int i = 0; i < ops.length - 2; ++i) {
                    ops[i] = new SingleOperator(Integer.toString(i+1));
                    plan.add(ops[i]);
                }
                
                ops[9] = new MultiOperator("10");
                plan.add(ops[9]);
                
                ops[10] = new MultiOperator("11");
                plan.add(ops[10]);
                
                plan.connect(ops[0], ops[9]);
                plan.connect(ops[1], ops[9]);
                plan.connect(ops[2], ops[9]);
                plan.connect(ops[3], ops[9]);
                plan.connect(ops[9], ops[4]);
                plan.connect(ops[9], ops[10]);
                plan.connect(ops[9], ops[5]);
                
                plan.connect(ops[10], ops[6]);
                plan.connect(ops[10], ops[7]);
                plan.connect(ops[10], ops[8]);
                
                //PlanPrinter<TOperator, TPlan> planPrinter = new PlanPrinter<TOperator, TPlan>(System.err, plan);
                //planPrinter.visit();
        
                int secondNodeIndex = outerIndex + 4;
                plan.pushBefore(ops[9], ops[secondNodeIndex], index) ;
                
                //planPrinter.visit();
                
                Set<TOperator> rootSet = new HashSet<TOperator>();
                rootSet.add(ops[0]);
                rootSet.add(ops[1]);
                rootSet.add(ops[2]);
                rootSet.add(ops[3]);
                
                Set<TOperator> expectedRootSet = new HashSet<TOperator>(plan.getRoots());
                
                rootSet.retainAll(expectedRootSet);
                assertTrue(rootSet.size() == 4);
                
                Set<TOperator> leafSet = new HashSet<TOperator>();
                for(int leafIndex = 4; leafIndex < 9; ++leafIndex) {
                    if(leafIndex != secondNodeIndex) {
                        leafSet.add(ops[leafIndex]);
                    }
                }
                
                Set<TOperator> expectedLeafSet = new HashSet<TOperator>(plan.getLeaves());
                
                leafSet.retainAll(expectedLeafSet);
                assertTrue(leafSet.size() == 4);
                
                List<TOperator> outerIndexNodePredecessors = plan.getPredecessors(ops[secondNodeIndex]);            
                assertTrue(outerIndexNodePredecessors.get(0) == ops[index]);
                
                List<TOperator> m10Predecessors = plan.getPredecessors(ops[9]);            
                assertTrue(m10Predecessors.get(index) == ops[secondNodeIndex]);
            }
        }
    }

    //Push non-existent nodes in a graph and check for exceptions
    //Push S1 after S4
    //Push S4 after S1
    //Push S5 after S4
    //Push S1 after null
    //Push null after S1
    //Push null after null
    //Input
    //S1->S2->S3 S4 S5
    @Test
    public void testNegativePushBefore() throws Exception {
        TPlan plan = new TPlan();
        TOperator[] ops = new TOperator[5];
        
        for(int i = 0; i < ops.length; ++i) {
            ops[i] = new SingleOperator(Integer.toString(i));
        }

        for(int i = 0; i < ops.length - 2; ++i) {
            plan.add(ops[i]);
        }

        
        plan.connect(ops[0], ops[1]);
        plan.connect(ops[1], ops[2]);

        //PlanPrinter<TOperator, TPlan> planPrinter = new PlanPrinter<TOperator, TPlan>(System.err, plan);
        //planPrinter.visit();

        try {
            plan.pushBefore(ops[0], ops[3], 0);
            fail("Expected exception for node not in plan.");
        } catch (PlanException pe) {
            assertTrue(pe.getMessage().contains("not in the plan"));
        }
        
        try {
            plan.pushBefore(ops[3], ops[0], 0);
            fail("Expected exception for node not in plan.");
        } catch (PlanException pe) {
            assertTrue(pe.getMessage().contains("not in the plan"));
        }

        try {
            plan.pushBefore(ops[4], ops[3], 0);
            fail("Expected exception for node not in plan.");
        } catch (PlanException pe) {
            assertTrue(pe.getMessage().contains("not in the plan"));
        }
        
        try {
            plan.pushBefore(ops[0], null, 0);
            fail("Expected exception for having null as one of the inputs");
        } catch (PlanException pe) {
            assertTrue(pe.getErrorCode() == 1085);
        }
        
        try {
            plan.pushBefore(null, ops[0], 0);
            fail("Expected exception for having null as one of the inputs");
        } catch (PlanException pe) {
            assertTrue(pe.getErrorCode() == 1085);
        }
        
        try {
            plan.pushBefore(null, null, 0);
            fail("Expected exception for having null as one of the inputs");
        } catch (PlanException pe) {
            assertTrue(pe.getErrorCode() == 1085);
        }

    }

    //Negative test cases
    //Input
    //  S1   S2  S3  S4
    //   \   |   |  /
    //        M10
    //      /  |  \
    //     S5  M11  S6
    //      /  |  \
    //     S7  S8  S9
    @Test
    public void testNegativePushBefore2() throws Exception {
        TPlan plan = new TPlan();
        TOperator[] ops = new TOperator[11];
        
        for(int i = 0; i < ops.length - 2; ++i) {
            ops[i] = new SingleOperator(Integer.toString(i+1));
            plan.add(ops[i]);
        }
        
        ops[9] = new MultiOperator("10");
        plan.add(ops[9]);
        
        ops[10] = new MultiOperator("11");
        plan.add(ops[10]);
        
        plan.connect(ops[0], ops[9]);
        plan.connect(ops[1], ops[9]);
        plan.connect(ops[2], ops[9]);
        plan.connect(ops[3], ops[9]);
        plan.connect(ops[9], ops[4]);
        plan.connect(ops[9], ops[10]);
        plan.connect(ops[9], ops[5]);
        
        plan.connect(ops[10], ops[6]);
        plan.connect(ops[10], ops[7]);
        plan.connect(ops[10], ops[8]);
        
        //PlanPrinter<TOperator, TPlan> planPrinter = new PlanPrinter<TOperator, TPlan>(System.err, plan);
        //planPrinter.visit();

        try {
            plan.pushBefore(ops[0], ops[9], 0) ;
            fail("Expected exception for first operator having null predecessors");
        } catch (PlanException pe) {
            assertTrue(pe.getErrorCode() == 1086);
        }
        
        try {
            plan.pushBefore(ops[10], ops[6], 0) ;
            fail("Expected exception for first operator having one predecessor");
        } catch (PlanException pe) {
            assertTrue(pe.getErrorCode() == 1086);
        }
        
        try {
            plan.pushBefore(ops[9], ops[10], 4) ;
            fail("Expected exception for inputNum exceeding number of predecessors");
        } catch (PlanException pe) {
            assertTrue(pe.getErrorCode() == 1087);
        }
        
        try {
            plan.pushBefore(ops[9], ops[0], 0) ;
            fail("Expected exception for second operator having null predecessors");
        } catch (PlanException pe) {
            assertTrue(pe.getErrorCode() == 1088);
        }

        try {
            plan.pushBefore(ops[9], ops[9], 0) ;
            fail("Expected exception for second operator having more than one predecessor");
        } catch (PlanException pe) {
            assertTrue(pe.getErrorCode() == 1088);
        }
        
        try {
            plan.pushBefore(ops[9], ops[8], 0) ;
            fail("Expected exception for second operator not being a successor of first operator");
        } catch (PlanException pe) {
            assertTrue(pe.getErrorCode() == 1089);
        }
        
        plan.disconnect(ops[9], ops[4]);
        plan.disconnect(ops[9], ops[5]);
        
        MultiInputSingleOutputOperator miso = new MultiInputSingleOutputOperator("12");
        
        plan.replace(ops[9], miso);
        
        try {
            plan.pushBefore(miso, ops[10], 0) ;
            fail("Expected exception for trying to connect multiple outputs to the first operator");
        } catch (PlanException pe) {
            assertTrue(pe.getErrorCode() == 1091);
        }
        
    }

    //Push M10 after M11's outputs - 0 through 2
    //Input
    //  S1   S2  S3  S4
    //   \   |   |  /
    //        M10
    //    S5   |  S6
    //     \   |  /
    //        M11
    //      /  |  \
    //     S7  S8  S9
    //Output when pushed after 1st output
    //  S5 S1  S2  S3  S4 S6
    //   \   \  \  /  /  /
    //           M10
    //         /  |  \
    //        S7  M11  S9
    //            |
    //            S8

    @Test
    public void testpushAfter() throws Exception {
        for(int index = 0; index < 3; index++) {
            TPlan plan = new TPlan();
            TOperator[] ops = new TOperator[11];
            
            for(int i = 0; i < ops.length - 2; ++i) {
                ops[i] = new SingleOperator(Integer.toString(i+1));
                plan.add(ops[i]);
            }
            
            ops[9] = new MultiOperator("10");
            plan.add(ops[9]);
            
            ops[10] = new MultiOperator("11");
            plan.add(ops[10]);
            
            plan.connect(ops[0], ops[9]);
            plan.connect(ops[1], ops[9]);
            plan.connect(ops[2], ops[9]);
            plan.connect(ops[3], ops[9]);
            plan.connect(ops[9], ops[10]);
            plan.connect(ops[4], ops[10]);
            plan.connect(ops[5], ops[10]);
            
            plan.connect(ops[10], ops[6]);
            plan.connect(ops[10], ops[7]);
            plan.connect(ops[10], ops[8]);
            
            //PlanPrinter<TOperator, TPlan> planPrinter = new PlanPrinter<TOperator, TPlan>(System.err, plan);
            //planPrinter.visit();
    
            plan.pushAfter(ops[10], ops[9], index) ;
            
            //planPrinter.visit();
            
            Set<TOperator> rootSet = new HashSet<TOperator>();
            rootSet.add(ops[0]);
            rootSet.add(ops[1]);
            rootSet.add(ops[2]);
            rootSet.add(ops[3]);
            rootSet.add(ops[4]);
            rootSet.add(ops[5]);
            
            Set<TOperator> expectedRootSet = new HashSet<TOperator>(plan.getRoots());
            
            rootSet.retainAll(expectedRootSet);
            assertTrue(rootSet.size() == 6);
            
            Set<TOperator> leafSet = new HashSet<TOperator>();
            leafSet.add(ops[6]);
            leafSet.add(ops[7]);
            leafSet.add(ops[8]);
            
            Set<TOperator> expectedLeafSet = new HashSet<TOperator>(plan.getLeaves());
            
            leafSet.retainAll(expectedLeafSet);
            assertTrue(leafSet.size() == 3);
            
            List<TOperator> m10Successors = plan.getSuccessors(ops[9]);            
            assertTrue(m10Successors.get(0) == ops[index + 6]);
            
            List<TOperator> m11Successors = plan.getSuccessors(ops[10]);
            assertTrue(m11Successors.get(index) == ops[9]);
        }
    }

    //Push S5 and S6 after M11's outputs - 0 through 2
    //Input
    //  S1   S2  S3  S4
    //   \   |   |  /
    //        M10
    //    S5   |  S6
    //     \   |  /
    //        M11
    //      /  |  \
    //     S7  S8  S9
    //Output when S5 is pushed after 1st output
    //  S1   S2  S3  S4
    //   \   |   |  /
    //        M10
    //         |  S6
    //         |  /
    //        M11
    //      /  |  \
    //     S7  S5  S9
    //         |
    //         S8

    @Test
    public void testpushAfter1() throws Exception {
        for(int outerIndex = 0; outerIndex < 2; outerIndex++) {
            for(int index = 0; index < 3; index++) {
                TPlan plan = new TPlan();
                TOperator[] ops = new TOperator[11];
                
                for(int i = 0; i < ops.length - 2; ++i) {
                    ops[i] = new SingleOperator(Integer.toString(i+1));
                    plan.add(ops[i]);
                }
                
                ops[9] = new MultiOperator("10");
                plan.add(ops[9]);
                
                ops[10] = new MultiOperator("11");
                plan.add(ops[10]);
                
                plan.connect(ops[0], ops[9]);
                plan.connect(ops[1], ops[9]);
                plan.connect(ops[2], ops[9]);
                plan.connect(ops[3], ops[9]);
                plan.connect(ops[9], ops[10]);
                plan.connect(ops[4], ops[10]);
                plan.connect(ops[5], ops[10]);
                
                plan.connect(ops[10], ops[6]);
                plan.connect(ops[10], ops[7]);
                plan.connect(ops[10], ops[8]);
                
                //PlanPrinter<TOperator, TPlan> planPrinter = new PlanPrinter<TOperator, TPlan>(System.err, plan);
                //planPrinter.visit();
        
                int secondNodeIndex = outerIndex + 4;
                plan.pushAfter(ops[10], ops[secondNodeIndex], index) ;
                
                //planPrinter.visit();
                
                Set<TOperator> rootSet = new HashSet<TOperator>();
                rootSet.add(ops[0]);
                rootSet.add(ops[1]);
                rootSet.add(ops[2]);
                rootSet.add(ops[3]);

                for(int rootIndex = 0; rootIndex < 6; ++rootIndex) {
                    if(rootIndex != secondNodeIndex) {
                        rootSet.add(ops[rootIndex]);
                    }
                }

                Set<TOperator> expectedRootSet = new HashSet<TOperator>(plan.getRoots());
                
                rootSet.retainAll(expectedRootSet);
                assertTrue(rootSet.size() == 5);

                Set<TOperator> leafSet = new HashSet<TOperator>();
                leafSet.add(ops[6]);
                leafSet.add(ops[7]);
                leafSet.add(ops[8]);
                
                Set<TOperator> expectedLeafSet = new HashSet<TOperator>(plan.getLeaves());
                
                leafSet.retainAll(expectedLeafSet);
                assertTrue(leafSet.size() == 3);
                
                List<TOperator> outerIndexNodeSuccessors = plan.getSuccessors(ops[secondNodeIndex]);
                assertTrue(outerIndexNodeSuccessors.get(0) == ops[index + 6]);
                
                List<TOperator> m11Successors = plan.getSuccessors(ops[10]);            
                assertTrue(m11Successors.get(index) == ops[secondNodeIndex]);
            }
        }
    }
    
    //Push non-existent nodes in a graph and check for exceptions
    //Push S1 after S4
    //Push S4 after S1
    //Push S5 after S4
    //Push S1 after null
    //Push null after S1
    //Push null after null
    //Input
    //S1->S2->S3 S4 S5
    @Test
    public void testNegativePushAfter() throws Exception {
        TPlan plan = new TPlan();
        TOperator[] ops = new TOperator[5];
        
        for(int i = 0; i < ops.length; ++i) {
            ops[i] = new SingleOperator(Integer.toString(i));
        }

        for(int i = 0; i < ops.length - 2; ++i) {
            plan.add(ops[i]);
        }

        
        plan.connect(ops[0], ops[1]);
        plan.connect(ops[1], ops[2]);

        //PlanPrinter<TOperator, TPlan> planPrinter = new PlanPrinter<TOperator, TPlan>(System.err, plan);
        //planPrinter.visit();

        try {
            plan.pushAfter(ops[0], ops[3], 0);
            fail("Expected exception for node not in plan.");
        } catch (PlanException pe) {
            assertTrue(pe.getMessage().contains("not in the plan"));
        }
        
        try {
            plan.pushAfter(ops[3], ops[0], 0);
            fail("Expected exception for node not in plan.");
        } catch (PlanException pe) {
            assertTrue(pe.getMessage().contains("not in the plan"));
        }

        try {
            plan.pushAfter(ops[4], ops[3], 0);
            fail("Expected exception for node not in plan.");
        } catch (PlanException pe) {
            assertTrue(pe.getMessage().contains("not in the plan"));
        }
        
        try {
            plan.pushAfter(ops[0], null, 0);
            fail("Expected exception for having null as one of the inputs");
        } catch (PlanException pe) {
            assertTrue(pe.getErrorCode() == 1085);
        }
        
        try {
            plan.pushAfter(null, ops[0], 0);
            fail("Expected exception for having null as one of the inputs");
        } catch (PlanException pe) {
            assertTrue(pe.getErrorCode() == 1085);
        }
        
        try {
            plan.pushAfter(null, null, 0);
            fail("Expected exception for having null as one of the inputs");
        } catch (PlanException pe) {
            assertTrue(pe.getErrorCode() == 1085);
        }

    }

    //Negative test cases
    //Input
    //  S1   S2  S3  S4
    //   \   |   |  /
    //        M10
    //    S5   |  S6
    //     \   |  /
    //        M11
    //      /  |  \
    //     S7  S8  S9
    @Test
    public void testNegativePushAfter2() throws Exception {
        TPlan plan = new TPlan();
        TOperator[] ops = new TOperator[11];
        
        for(int i = 0; i < ops.length - 2; ++i) {
            ops[i] = new SingleOperator(Integer.toString(i+1));
            plan.add(ops[i]);
        }
        
        ops[9] = new MultiOperator("10");
        plan.add(ops[9]);
        
        ops[10] = new MultiOperator("11");
        plan.add(ops[10]);
        
        plan.connect(ops[0], ops[9]);
        plan.connect(ops[1], ops[9]);
        plan.connect(ops[2], ops[9]);
        plan.connect(ops[3], ops[9]);
        plan.connect(ops[9], ops[10]);
        plan.connect(ops[4], ops[10]);
        plan.connect(ops[5], ops[10]);
        
        plan.connect(ops[10], ops[6]);
        plan.connect(ops[10], ops[7]);
        plan.connect(ops[10], ops[8]);
        
        //PlanPrinter<TOperator, TPlan> planPrinter = new PlanPrinter<TOperator, TPlan>(System.err, plan);
        //planPrinter.visit();

        try {
            plan.pushAfter(ops[6], ops[9], 0) ;
            fail("Expected exception for first operator having null successors");
        } catch (PlanException pe) {
            assertTrue(pe.getErrorCode() == 1086);
        }
        
        try {
            plan.pushAfter(ops[0], ops[9], 0) ;
            fail("Expected exception for first operator having no inputs");
        } catch (PlanException pe) {
            assertTrue(pe.getErrorCode() == 1088);
        }
        
        try {
            plan.pushAfter(ops[9], ops[6], 0) ;
            fail("Expected exception for first operator having one successor");
        } catch (PlanException pe) {
            assertTrue(pe.getErrorCode() == 1086);
        }
        
        try {
            plan.pushAfter(ops[6], ops[10], 0) ;
            fail("Expected exception for first operator having one successors");
        } catch (PlanException pe) {
            assertTrue(pe.getErrorCode() == 1086);
        }

        
        try {
            plan.pushAfter(ops[10], ops[9], 4) ;
            fail("Expected exception for outputNum exceeding the number of outputs of first operator");
        } catch (PlanException pe) {
            assertTrue(pe.getErrorCode() == 1087);
        }

        try {
            plan.pushAfter(ops[10], ops[6], 0) ;
            fail("Expected exception for second operator having null successors");
        } catch (PlanException pe) {
            assertTrue(pe.getErrorCode() == 1088);
        }
        
        try {
            plan.pushAfter(ops[10], ops[10], 0) ;
            fail("Expected exception for second operator having more than one successor");
        } catch (PlanException pe) {
            assertTrue(pe.getErrorCode() == 1088);
        }
        
        try {
            plan.pushAfter(ops[10], ops[0], 0) ;
            fail("Expected exception for second operator not being a predecessor of first operator");
        } catch (PlanException pe) {
            assertTrue(pe.getErrorCode() == 1089);
        }
        
        plan.disconnect(ops[4], ops[10]);
        plan.disconnect(ops[5], ops[10]);
        
        MultiOutputSingleInputOperator mosi = new MultiOutputSingleInputOperator("12");
        
        plan.replace(ops[10], mosi);
        
        try {
            plan.pushAfter(mosi, ops[9], 0) ;
            fail("Expected exception for trying to connect multiple inputs to the first operator");
        } catch (PlanException pe) {
            assertTrue(pe.getErrorCode() == 1091);
        }
        
    }

}

