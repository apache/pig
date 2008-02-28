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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.pig.impl.logicalLayer.OperatorKey;
import org.apache.pig.impl.logicalLayer.parser.ParseException;
import org.apache.pig.impl.plan.*;

import org.junit.Test;

/**
 * Test the generic operator classes (Operator, OperatorPlan,
 * PlanVisitor).
 */

public class TestOperatorPlan extends junit.framework.TestCase {

    private int mNextKey = 0;

    abstract class TOperator extends Operator {
        TOperator(OperatorKey key) {
            super(key);
        }
    }

    class SingleOperator extends TOperator {
        private String mName;

        SingleOperator(String name) {
            super(new OperatorKey("", mNextKey++));
            mName = name;
        }

        public boolean supportsMultipleInputs() {
            return false;
        }

        public boolean supportsMultipleOutputs() {
            return false;
        }

        @Override
        public void visit(PlanVisitor v) throws ParseException {
            ((TVisitor)v).visitSingleOperator(this);
        }

        public String name() {
            return mName;
        }

        public String typeName() {
            return "Single";
        }
    }

    class MultiOperator extends TOperator {
        private String mName;

        MultiOperator(String name) {
            super(new OperatorKey("", mNextKey++));
            mName = name;
        }

        public boolean supportsMultipleInputs() {
            return true;
        }

        public boolean supportsMultipleOutputs() {
            return true;
        }

        public void visit(PlanVisitor v) throws ParseException {
            ((TVisitor)v).visitMultiOperator(this);
        }

        public String name() {
            return mName;
        }

        public String typeName() {
            return "Multi";
        }
    }

    class TPlan extends OperatorPlan {

        public String display() {
            StringBuilder buf = new StringBuilder();

            buf.append("Nodes: ");
            for (Operator op : mOps.keySet()) {
                buf.append(op.name());
                buf.append(' ');
            }

            buf.append("FromEdges: ");
            Iterator i = mFromEdges.keySet().iterator();
            while (i.hasNext()) {
                Operator from = (Operator)i.next();
                Iterator j = mFromEdges.iterator(from);
                while (j.hasNext()) {
                    buf.append(from.name());
                    buf.append("->");
                    buf.append(((Operator)j.next()).name());
                    buf.append(' ');
                }
            }

            buf.append("ToEdges: ");
            i = mToEdges.keySet().iterator();
            while (i.hasNext()) {
                Operator from = (Operator)i.next();
                Iterator j = mToEdges.iterator(from);
                while (j.hasNext()) {
                    buf.append(from.name());
                    buf.append("->");
                    buf.append(((Operator)j.next()).name());
                    buf.append(' ');
                }
            }
            return buf.toString();
        }
    }

    abstract class TVisitor extends PlanVisitor {
        protected StringBuilder mJournal;

        TVisitor(TPlan plan) {
            super(plan);
            mJournal = new StringBuilder();
        }

        public void visitSingleOperator(SingleOperator so) throws ParseException {
            mJournal.append(so.name());
            mJournal.append(' ');
        }

        public void visitMultiOperator(MultiOperator mo) throws ParseException {
            mJournal.append(mo.name());
            mJournal.append(' ');
        }

        public String getJournal() {
            return mJournal.toString();
        }
    }

    class TDepthVisitor extends TVisitor {

        TDepthVisitor(TPlan plan) {
            super(plan);
        }

        public void visit() throws ParseException {
            depthFirst();
        }
    }

    class TDependVisitor extends TVisitor {

        TDependVisitor(TPlan plan) {
            super(plan);
        }

        public void visit() throws ParseException {
            dependencyOrder();
        }
    }

    @Test
    public void testAddRemove() throws Exception {
        // Test that we can add and remove nodes from the plan.  Also test
        // that we can fetch the nodes by operator key, by operator, by
        // roots, by leaves, that they have no predecessors and no
        // successors.

        TPlan plan = new TPlan();
        Operator[] ops = new Operator[3];
        for (int i = 0; i < 3; i++) {
            ops[i] = new SingleOperator(Integer.toString(i));
            plan.add(ops[i]);
        }

        // All should be roots, as none are connected
        List<Operator> roots = plan.getRoots();
        for (int i = 0; i < 3; i++) {
            assertTrue("Roots should contain operator " + i,
                roots.contains(ops[i]));
        }

        // All should be leaves, as none are connected
        List<Operator> leaves = plan.getLeaves();
        for (int i = 0; i < 3; i++) {
            assertTrue("Leaves should contain operator " + i,
                leaves.contains(ops[i]));
        }

        // Each operator should have no successors or predecessors.
        assertNull(plan.getSuccessors(ops[1]));
        assertNull(plan.getPredecessors(ops[1]));

        // Make sure we find them all when we iterate through them.
        Set<Operator> s = new HashSet<Operator>();
        Iterator<Operator> j = plan.iterator();
        while (j.hasNext()) {
            s.add(j.next());
        }

        for (int i = 0; i < 3; i++) {
            assertTrue("Iterator should contain operator " + i,
                s.contains(ops[i]));
        }

        // Test that we can find an operator by its key.
        Operator op = plan.getOperator(new OperatorKey("", 1));
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
    public void testLinearGraph() throws Exception {
        TPlan plan = new TPlan();
        Operator[] ops = new Operator[5];
        for (int i = 0; i < 5; i++) {
            ops[i] = new SingleOperator(Integer.toString(i));
            plan.add(ops[i]);
            if (i > 0) plan.connect(ops[i - 1], ops[i]);
        }

        // Test that connecting a node not yet in the plan is detected.
        Operator bogus = new SingleOperator("X");
        boolean sawError = false;
        try {
            plan.connect(ops[2], bogus);
        } catch (IOException ioe) {
            assertEquals("Attempt to connect operator X which is not in "
                + "the plan.", ioe.getMessage());
            sawError = true;
        }
        assertTrue("Should have caught an error when we tried to connect a "
            + "node that was not in the plan", sawError);

        // Get roots should just return ops[0]
        List<Operator> roots = plan.getRoots();
        assertEquals(1, roots.size());
        assertEquals(roots.get(0), ops[0]);

        // Get leaves should just return ops[4]
        List<Operator> leaves = plan.getLeaves();
        assertEquals(1, leaves.size());
        assertEquals(leaves.get(0), ops[4]);

        // Test that connecting another input to SingleOperator gives
        // error.
        plan.add(bogus);
        sawError = false;
        try {
            plan.connect(bogus, ops[1]);
        } catch (IOException ioe) {
            assertEquals("Attempt to give operator of type Single " +
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
        } catch (IOException ioe) {
            assertEquals("Attempt to give operator of type Single " +
                "multiple outputs.  This operator does "
                + "not support multiple outputs.", ioe.getMessage());
            sawError = true;
        }
        assertTrue("Should have caught an error when we tried to connect a "
            + "second output to a Single", sawError);
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

        assertEquals("Nodes: 2 1 4 0 3 FromEdges: 2->3 1->2 0->1 3->4 ToEdges: 2->1 1->0 4->3 3->2 ", plan.display());

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
        assertEquals("Nodes: 2 1 4 0 3 FromEdges: 1->2 0->1 3->4 ToEdges: 2->1 1->0 4->3 ", plan.display());

        // Test remove
        plan.remove(ops[1]);
        assertEquals("Nodes: 2 4 0 3 FromEdges: 3->4 ToEdges: 4->3 ", plan.display());
    }

    @Test
    public void testDAG() throws Exception {
        TPlan plan = new TPlan();
        Operator[] ops = new Operator[6];
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
        List<Operator> roots = plan.getRoots();
        assertEquals(2, roots.size());
        assertTrue(roots.contains(ops[0]));
        assertTrue(roots.contains(ops[1]));

        // Get leaves should return ops[4] and ops[5]
        List<Operator> leaves = plan.getLeaves();
        assertEquals(2, leaves.size());
        assertTrue(leaves.contains(ops[4]));
        assertTrue(leaves.contains(ops[5]));

        // Successor for ops[3] should be ops[4] and ops[5]
        List<Operator> s = new ArrayList<Operator>(plan.getSuccessors(ops[3]));
        assertEquals(2, s.size());
        assertTrue(s.contains(ops[4]));
        assertTrue(s.contains(ops[5]));
        
        // Predecessor for ops[2] should be ops[0] and ops[1]
        s = new ArrayList<Operator>(plan.getPredecessors(ops[2]));
        assertEquals(2, s.size());
        assertTrue(s.contains(ops[0]));
        assertTrue(s.contains(ops[1]));

        assertEquals("Nodes: 0 3 5 1 4 2 FromEdges: 0->2 3->4 3->5 1->2 2->3 ToEdges: 3->2 5->3 4->3 2->0 2->1 ", plan.display());

        // Visit it depth first
        TVisitor visitor = new TDepthVisitor(plan);
        visitor.visit();
        assertEquals("0 2 3 4 5 1 ", visitor.getJournal());

        // Visit it dependency order
        visitor = new TDependVisitor(plan);
        visitor.visit();
        assertEquals("0 1 2 3 5 4 ", visitor.getJournal());

        // Test disconnect
        plan.disconnect(ops[2], ops[3]);
        assertEquals("Nodes: 0 3 5 1 4 2 FromEdges: 0->2 3->4 3->5 1->2 ToEdges: 5->3 4->3 2->0 2->1 ", plan.display());

        // Test remove
        plan.remove(ops[2]);
        assertEquals("Nodes: 0 3 5 1 4 FromEdges: 3->4 3->5 ToEdges: 5->3 4->3 ", plan.display());
    }


}

