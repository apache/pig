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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.BaseOperatorPlan;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanVisitor;
import org.apache.pig.newplan.optimizer.Rule;
import org.apache.pig.newplan.optimizer.Transformer;
import org.junit.Before;
import org.junit.Test;

public class TestNewPlanRule {

    private static class SillyRule extends Rule {

        public SillyRule(String n, OperatorPlan p) {
            super(n, p);
        }

        @Override
        public Transformer getNewTransformer() {
            return null;
        }

        @Override
        protected OperatorPlan buildPattern() {
            // TODO Auto-generated method stub
            return null;
        }

    }

    private static class SillyPlan extends BaseOperatorPlan {

        SillyPlan() {
            super();
        }

        @Override
        public boolean isEqual(OperatorPlan other) {
            return false;
        }

    }

    private static class OP extends Operator {
        OP(String n, OperatorPlan p) {
            super(n, p);
        }

        public void accept(PlanVisitor v) {

        }

        @Override
        public boolean isEqual(Operator operator) {
            return false;
        }
    }

    private static class OP_Load extends OP {
        OP_Load(String n, OperatorPlan p) {
            super(n, p);
        }
    }

    private static class OP_Filter extends OP {
        OP_Filter(String n, OperatorPlan p) {
            super(n, p);
        }
    }

    private static class OP_Split extends OP {
        OP_Split(String n, OperatorPlan p) {
            super(n, p);
        }
    }

    private static class OP_Store extends OP {
        OP_Store(String n, OperatorPlan p) {
            super(n, p);
        }
    }

    private static class OP_Join extends OP {
        OP_Join(String n, OperatorPlan p) {
            super(n, p);
        }
    }


    OperatorPlan plan = null;
    Operator join;

    @Before
    public void setUp() {
        plan = new SillyPlan();
        Operator l1 = new OP_Load("p1", plan);
        plan.add(l1);
        Operator l2 = new OP_Load("p2", plan);
        plan.add(l2);
        Operator j1 = new OP_Join("j1", plan);
        plan.add(j1);
        Operator f1 = new OP_Filter("f1", plan);
        plan.add(f1);
        Operator f2 = new OP_Filter("f2", plan);
        plan.add(f2);
        Operator t1 = new OP_Split("t1",plan);
        plan.add(t1);
        Operator f3 = new OP_Filter("f3", plan);
        plan.add(f3);
        Operator f4 = new OP_Filter("f4", plan);
        plan.add(f4);
        Operator s1 = new OP_Store("s1", plan);
        plan.add(s1);
        Operator s2 = new OP_Store("s2", plan);
        plan.add(s2);

        // load --|-join - filter - filter - split |- filter - store
        // load --|                                |- filter - store
        plan.connect(l1, j1);
        plan.connect(l2, j1);
        plan.connect(j1, f1);
        plan.connect(f1, f2);
        plan.connect(f2, t1);
        plan.connect(t1, f3);
        plan.connect(t1, f4);
        plan.connect(f3, s1);
        plan.connect(f4, s2);

        join = j1;
    }

    @Test
    public void testMultiNode() throws Exception {
        //         load --|-join - filter - filter - split |- filter - store
        //         load --|
        // load -- filter-|
        Operator l3 = new OP_Load("p3", plan);
        Operator f5 = new OP_Filter("f5", plan);
        plan.add(l3);
        plan.add(f5);
        plan.connect(l3, f5);

         plan.connect(f5, join);


         OperatorPlan pattern = new SillyPlan();
         Operator op1 = new OP_Load("mmm1", pattern);
         Operator op2 = new OP_Filter("mmm2", pattern);
         Operator op3 = new OP_Join("mmm3", pattern);
         pattern.add(op1);
         pattern.add(op2);
         pattern.add(op3);
         pattern.connect(op1, op3);
         pattern.connect(op2, op3);

         Rule r = new SillyRule("basic", pattern);
         List<OperatorPlan> l = r.match(plan);
         assertEquals(1, l.size());
         OperatorPlan match = l.get(0);
         assertEquals(3, match.size());
         assertEquals(1, match.getSinks().size());
         assertEquals(join, match.getSinks().get(0));

         assertEquals(2, match.getSources().size());
         assertTrue(match.getSources().get(0).getClass().equals(OP_Load.class) || match.getSources().get(0).equals(f5) );
         assertTrue(match.getSources().get(1).getClass().equals(OP_Load.class) || match.getSources().get(1).equals(f5) );
         assertNotSame(match.getSources().get(0), match.getSources().get(1));
    }

    @Test
    public void testSingleNodeMatch() throws FrontendException {
        // search for Load
        OperatorPlan pattern = new SillyPlan();
        pattern.add(new OP_Load("mmm", pattern));

        Rule r = new SillyRule("basic", pattern);
        List<OperatorPlan> l = r.match(plan);
        assertEquals(2, l.size());

        Operator m1 = l.get(0).getSources().get(0);
        assertTrue(m1.getName().equals("p1") || m1.getName().equals("p2"));
        assertEquals(1, l.get(0).size());

        Operator m2 = l.get(1).getSources().get(0);
        assertTrue(m2.getName().equals("p1") || m2.getName().equals("p2"));
        assertEquals(1, l.get(1).size());
        assertNotSame(m1.getName(), m2.getName());

        // search for filter
        pattern = new SillyPlan();
        pattern.add(new OP_Filter("mmm",pattern));
        r = new SillyRule("basic", pattern);
        l = r.match(plan);
        assertEquals(4, l.size());

        m1 = l.get(0).getSources().get(0);
        assertTrue(m1.getName().equals("f1") || m1.getName().equals("f2")
                || m1.getName().equals("f3") || m1.getName().equals("f4"));
        assertEquals(1, l.get(0).size());

        m2 = l.get(1).getSources().get(0);
        assertTrue(m1.getName().equals("f1") || m1.getName().equals("f2")
                || m1.getName().equals("f3") || m1.getName().equals("f4"));
        assertEquals(1, l.get(1).size());
        assertNotSame(m1.getName(), m2.getName());

        // search for store
        pattern = new SillyPlan();
        pattern.add(new OP_Store("mmm",pattern));
        r = new SillyRule("basic", pattern);
        l = r.match(plan);
        assertEquals(l.size(), 2);

        m1 = l.get(0).getSources().get(0);
        assertTrue(m1.getName().equals("s1") || m1.getName().equals("s2"));
        assertEquals(l.get(0).size(), 1);

        m2 = l.get(1).getSources().get(0);
        assertTrue(m2.getName().equals("s1") || m2.getName().equals("s2"));
        assertEquals(l.get(1).size(), 1);
        assertNotSame(m1.getName(), m2.getName());

        // search for split
        pattern = new SillyPlan();
        pattern.add(new OP_Split("mmm",pattern));
        r = new SillyRule("basic", pattern);
        l = r.match(plan);
        assertEquals(l.size(), 1);

        m1 = l.get(0).getSources().get(0);
        assertTrue(m1.getName().equals("t1"));
        assertEquals(l.get(0).size(), 1);

        // search for join
        pattern = new SillyPlan();
        pattern.add(new OP_Join("mmm",pattern));
        r = new SillyRule("basic", pattern);
        l = r.match(plan);
        assertEquals(l.size(), 1);

        m1 = l.get(0).getSources().get(0);
        assertTrue(m1.getName().equals("j1"));
        assertEquals(l.get(0).size(), 1);

    }

    @Test
    public void testTwoNodeMatch() throws FrontendException {
        // search for 2 Loads at the same time
        OperatorPlan pattern = new SillyPlan();
        pattern.add(new OP_Load("mmm1", pattern));
        pattern.add(new OP_Load("mmm2", pattern));

        Rule r = new SillyRule("basic", pattern);
        List<OperatorPlan> l = r.match(plan);
        assertEquals(l.size(), 1);

        assertEquals(l.get(0).getSources().size(), 2);
        assertEquals(l.get(0).getSinks().size(), 2);
        assertEquals(l.get(0).size(), 2);

        Operator m1 = l.get(0).getSources().get(0);
        assertTrue(m1.getName().equals("p1") || m1.getName().equals("p2"));
        Operator m2 = l.get(0).getSources().get(1);
        assertTrue(m2.getName().equals("p1") || m2.getName().equals("p2"));
        assertNotSame(m1.getName(), m2.getName());


        // search for join then filter
        pattern = new SillyPlan();
        Operator s1 = new OP_Join("mmm1", pattern);
        Operator s2 = new OP_Filter("mmm2", pattern);
        pattern.add(s1);
        pattern.add(s2);
        pattern.connect(s1, s2);

        r = new SillyRule("basic", pattern);
        l = r.match(plan);
        assertEquals(1, l.size());

        assertEquals(1, l.get(0).getSources().size());
        assertEquals(1, l.get(0).getSinks().size());
        assertEquals(2, l.get(0).size());

        m1 = l.get(0).getSources().get(0);
        assertTrue(m1.getName().equals("j1"));
        m2 = l.get(0).getSinks().get(0);
        assertTrue(m2.getName().equals("f1"));


        // search for filter, then store
        pattern = new SillyPlan();
        s1 = new OP_Filter("mmm1", pattern);
        s2 = new OP_Store("mmm2", pattern);
        pattern.add(s1);
        pattern.add(s2);
        pattern.connect(s1, s2);

        r = new SillyRule("basic", pattern);
        l = r.match(plan);
        assertEquals(2, l.size());

        assertEquals(1, l.get(0).getSources().size());
        assertEquals(1, l.get(0).getSinks().size());

        // search for 2 loads, then join
        pattern = new SillyPlan();
        s1 = new OP_Load("mmm1", pattern);
        s2 = new OP_Load("mmm2", pattern);
        Operator s3 = new OP_Join("jjj", pattern);
        pattern.add(s1);
        pattern.add(s2);
        pattern.add(s3);
        pattern.connect(s1, s3);
        pattern.connect(s2, s3);

        r = new SillyRule("basic", pattern);
        l = r.match(plan);
        assertEquals(l.size(), 1);

        // search for split then 2 filters
        pattern = new SillyPlan();
        s1 = new OP_Split("mmm1", pattern);
        s2 = new OP_Filter("mmm2", pattern);
        s3 = new OP_Filter("mmm3", pattern);
        pattern.add(s1);
        pattern.add(s2);
        pattern.add(s3);
        pattern.connect(s1, s2);
        pattern.connect(s1, s3);

        r = new SillyRule("basic", pattern);
        l = r.match(plan);
        assertEquals(1, l.size());

        assertEquals(1, l.get(0).getSources().size());
        assertEquals(2, l.get(0).getSinks().size());
        assertEquals(3, l.get(0).size());

        m1 = l.get(0).getSources().get(0);
        assertEquals("t1", m1.getName());
        m2 = l.get(0).getSinks().get(0);
        assertTrue(m2.getName().equals("f3") || m2.getName().equals("f4"));
        m2 = l.get(0).getSinks().get(1);
        assertTrue(m2.getName().equals("f3") || m2.getName().equals("f4"));
    }

    @Test
    public void testThreeNodeMatch() throws FrontendException {
        // load --|                                |- filter - store
        // load --|-join - filter - filter - split |- filter - store
        //               | filter - store
        //               | filter - store
        Operator f5 = new OP_Filter("f5", plan);
        Operator f6 = new OP_Filter("f6", plan);
        Operator s5 = new OP_Store("s5", plan);
        Operator s6 = new OP_Store("s6", plan);
        plan.add(f5);
        plan.add(f6);
        plan.add(s5);
        plan.add(s6);
        plan.connect(f5, s5);
        plan.connect(f6, s6);
        plan.connect(join, f5);
        plan.connect(join, f6);

        OperatorPlan pattern = new SillyPlan();
        Operator s1 = new OP_Join("mmm1", pattern);
        Operator s2 = new OP_Filter("mmm2", pattern);
        Operator s3 = new OP_Filter("mmm3", pattern);
        Operator s4 = new OP_Filter("mmm4", pattern);
        pattern.add(s1);
        pattern.add(s2);
        pattern.add(s3);
        pattern.add(s4);
        pattern.connect(s1, s2);
        pattern.connect(s1, s3);
        pattern.connect(s1, s4);

        Rule r = new SillyRule("basic", pattern);
        List<OperatorPlan> l = r.match(plan);
        assertEquals(1, l.size());

        assertEquals(1, l.get(0).getSources().size());
        assertEquals(3, l.get(0).getSinks().size());
        assertEquals(4, l.get(0).size());

        Operator m1,m2;
        m1 = l.get(0).getSources().get(0);
        assertEquals("j1", m1.getName());
        for( int i=0; i < 3; i++ ) {
          m2 = l.get(0).getSinks().get(i);
          assertTrue(m2.getName().equals("f1")
                   || m2.getName().equals("f5")
                   || m2.getName().equals("f6")
          );
        }
    }
}