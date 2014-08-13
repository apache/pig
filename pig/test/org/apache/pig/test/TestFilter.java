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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.ConstantExpression;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.EqualToExpr;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.GreaterThanExpr;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POAnd;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POProject;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFilter;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.test.utils.GenPhyOp;
import org.apache.pig.test.utils.GenRandomData;
import org.apache.pig.test.utils.TestHelper;
import org.junit.Before;
import org.junit.Test;

public class TestFilter {
    Random r = new Random(42L);
    POFilter pass;
    POFilter fail;
    Tuple t;
    DataBag inp;
    POFilter projFil;

    boolean[] nullFlags = new boolean[] { false, true };

    @Before
    public void setUp() throws Exception {
        pass = GenPhyOp.topFilterOpWithExPlan(50, 25);
        fail = GenPhyOp.topFilterOpWithExPlan(25, 50);
    }

    private void setUpProjFil(boolean withNulls) throws Exception {
        if (withNulls)
            inp = GenRandomData.genRandSmallTupDataBagWithNulls(r, 10, 100);
        else
            inp = GenRandomData.genRandSmallTupDataBag(r, 10, 100);
        t = GenRandomData.genRandSmallBagTuple(r, 10, 100);
        projFil = GenPhyOp.topFilterOpWithProj(1, 50);
        POProject inpPrj = GenPhyOp.exprProject();
        Tuple tmpTpl = new DefaultTuple();
        tmpTpl.append(inp);
        inpPrj.setColumn(0);
        inpPrj.setResultType(DataType.TUPLE);
        inpPrj.setOverloaded(true);
        inpPrj.attachInput(tmpTpl);
        List<PhysicalOperator> inputs = new ArrayList<PhysicalOperator>();
        inputs.add(inpPrj);
        projFil.setInputs(inputs);
    }

    @Test
    public void testGetNextTuple() throws Exception {
        pass.attachInput(t);
        Result res = pass.getNextTuple();
        assertEquals(t, res.result);
        fail.attachInput(t);
        res = fail.getNextTuple();
        assertEquals(res.returnStatus, POStatus.STATUS_EOP);

        for (int i = 0; i < nullFlags.length; i++) {
            int count = 0;
            setUpProjFil(nullFlags[i]);
            while (true) {
                res = projFil.getNextTuple();
                if (res.returnStatus == POStatus.STATUS_EOP)
                    break;
                count++;
                assertEquals(POStatus.STATUS_OK, res.returnStatus);
                Tuple output = (Tuple)res.result;
                assertEquals(
                        "Running testGetNextTuple with nullFlags set to " + nullFlags[i] + ":",
                        true, TestHelper.bagContains(inp, output));
                assertEquals(
                        "Running testGetNextTuple with nullFlags set to " + nullFlags[i] + ":",
                        true, (Integer)((Tuple)res.result).get(1) > 50);
            }
            assertEquals("Running testGetNextTuple with nullFlags set to " + nullFlags[i] + ":",
                    getExpCount(inp), count);

        }
    }

    /**
     * @param inp2
     * @return
     * @throws ExecException
     */
    private int getExpCount(DataBag inp2) throws ExecException {
        // TODO Auto-generated method stub
        int count = 0;
        for (Iterator<Tuple> it = inp2.iterator(); it.hasNext();) {

            Tuple t = it.next();
            if (t.get(1) != null && (Integer)t.get(1) > 50)
                count++;
        }

        return count;
    }

    @Test
    public void testSimpleFilter() throws Exception {
        for (int i = 0; i < nullFlags.length; i++) {
            // Build the inner expression
            POProject p1 = GenPhyOp.exprProject(0);
            POProject p2 = GenPhyOp.exprProject(1);
            GreaterThanExpr gt = GenPhyOp.compGreaterThanExpr(p1, p2, DataType.INTEGER);

            PhysicalPlan ip = new PhysicalPlan();
            ip.add(p1);
            ip.add(p2);
            ip.add(gt);
            ip.connect(p1, gt);
            ip.connect(p2, gt);

            int[] ints = { 0, 1, 1, 0, 1, 1 };
            TupleFactory tf = TupleFactory.getInstance();
            DataBag inbag = BagFactory.getInstance().newDefaultBag();
            Random r = new Random();
            for (int j = 0; j < ints.length; j += 2) {
                // if we are testing with nulls
                // introduce nulls randomly
                if (nullFlags[i] == true) {
                    int rand = r.nextInt(100);
                    if (rand <= 20) {
                        Tuple t = tf.newTuple(2);
                        t.set(0, new Integer(ints[j]));
                        t.set(1, null);
                        inbag.add(t);
                    } else if (rand > 20 && rand <= 40) {
                        Tuple t = tf.newTuple(2);
                        t.set(0, null);
                        t.set(1, new Integer(ints[j + 1]));
                        inbag.add(t);
                    } else if (rand > 40 && rand <= 60) {
                        Tuple t = tf.newTuple(2);
                        t.set(0, null);
                        t.set(1, null);
                        inbag.add(t);
                    }
                }
                Tuple t = tf.newTuple(2);
                t.set(0, new Integer(ints[j]));
                t.set(1, new Integer(ints[j + 1]));
                inbag.add(t);
            }

            PORead read = GenPhyOp.topReadOp(inbag);
            POFilter filter = GenPhyOp.connectedFilterOp(read);
            filter.setPlan(ip);

            PhysicalPlan op = new PhysicalPlan();
            op.add(filter);
            op.add(read);
            op.connect(read, filter);

            DataBag outbag = BagFactory.getInstance().newDefaultBag();
            Result res;
            Tuple t = tf.newTuple();
            do {
                res = filter.getNextTuple();
                if (res.returnStatus == POStatus.STATUS_OK) {
                    outbag.add((Tuple)res.result);
                }
            } while (res.returnStatus == POStatus.STATUS_OK);
            assertEquals("Running " + this.getClass().getName() + "with nullFlags set to "
                    + nullFlags[i] + ":", POStatus.STATUS_EOP, res.returnStatus);
            assertEquals("Running " + this.getClass().getName() + "with nullFlags set to "
                    + nullFlags[i] + ":", 1, outbag.size());
            Iterator<Tuple> it = outbag.iterator();
            assertTrue("Running " + this.getClass().getName() + "with nullFlags set to "
                    + nullFlags[i] + ":", it.hasNext());
            t = it.next();
            assertEquals("Running " + this.getClass().getName() + "with nullFlags set to "
                    + nullFlags[i] + ":", 2, t.size());
            assertTrue("Running " + this.getClass().getName() + "with nullFlags set to "
                    + nullFlags[i] + ":", t.get(0) instanceof Integer);
            assertTrue("Running " + this.getClass().getName() + "with nullFlags set to "
                    + nullFlags[i] + ":", t.get(1) instanceof Integer);
            Integer i1 = (Integer)t.get(0);
            Integer i2 = (Integer)t.get(1);
            assertEquals("Running " + this.getClass().getName() + "with nullFlags set to "
                    + nullFlags[i] + ":", 1, (int)i1);
            assertEquals("Running " + this.getClass().getName() + "with nullFlags set to "
                    + nullFlags[i] + ":", 0, (int)i2);
        }
    }

    @Test
    public void testAndFilter() throws Exception {
        for (int i = 0; i < nullFlags.length; i++) {
            // Build the inner expression
            POProject p1 = GenPhyOp.exprProject(0);
            ConstantExpression c2 = GenPhyOp.exprConst();
            c2.setValue(new Integer(0));
            GreaterThanExpr gt = GenPhyOp.compGreaterThanExpr(p1, c2, DataType.INTEGER);

            POProject p3 = GenPhyOp.exprProject(1);
            ConstantExpression c = GenPhyOp.exprConst();
            c.setValue(new Integer(1));
            EqualToExpr eq = GenPhyOp.compEqualToExpr(p3, c, DataType.INTEGER);
            POAnd and = GenPhyOp.compAndExpr(gt, eq);

            PhysicalPlan ip = new PhysicalPlan();
            ip.add(p1);
            ip.add(c2);
            ip.add(gt);
            ip.add(p3);
            ip.add(c);
            ip.add(eq);
            ip.add(and);
            ip.connect(p1, gt);
            ip.connect(c2, gt);
            ip.connect(p3, eq);
            ip.connect(c, eq);
            ip.connect(eq, and);
            ip.connect(gt, and);

            int[] ints = { 0, 1, 1, 0, 1, 1 };
            TupleFactory tf = TupleFactory.getInstance();
            DataBag inbag = BagFactory.getInstance().newDefaultBag();
            Random r = new Random();
            for (int j = 0; j < ints.length; j += 2) {
                // if we are testing with nulls
                // introduce nulls randomly
                if (nullFlags[i] == true) {
                    int rand = r.nextInt(100);
                    if (rand <= 20) {
                        Tuple t = tf.newTuple(2);
                        t.set(0, new Integer(ints[j]));
                        t.set(1, null);
                        inbag.add(t);
                    } else if (rand > 20 && rand <= 40) {
                        Tuple t = tf.newTuple(2);
                        t.set(0, null);
                        t.set(1, new Integer(ints[j + 1]));
                        inbag.add(t);
                    } else if (rand > 40 && rand <= 60) {
                        Tuple t = tf.newTuple(2);
                        t.set(0, null);
                        t.set(1, null);
                        inbag.add(t);
                    }
                }
                Tuple t = tf.newTuple(2);
                t.set(0, new Integer(ints[j]));
                t.set(1, new Integer(ints[j + 1]));
                inbag.add(t);
            }

            PORead read = GenPhyOp.topReadOp(inbag);
            POFilter filter = GenPhyOp.connectedFilterOp(read);
            filter.setPlan(ip);

            PhysicalPlan op = new PhysicalPlan();
            op.add(filter);
            op.add(read);
            op.connect(read, filter);

            DataBag outbag = BagFactory.getInstance().newDefaultBag();
            Result res;
            Tuple t = tf.newTuple();
            do {
                res = filter.getNextTuple();
                if (res.returnStatus == POStatus.STATUS_OK) {
                    outbag.add((Tuple)res.result);
                }
            } while (res.returnStatus == POStatus.STATUS_OK);
            assertEquals(POStatus.STATUS_EOP, res.returnStatus);
            assertEquals(1, outbag.size());
            Iterator<Tuple> it = outbag.iterator();
            assertTrue(it.hasNext());
            t = it.next();
            assertEquals(2, t.size());
            assertTrue(t.get(0) instanceof Integer);
            assertTrue(t.get(1) instanceof Integer);
            Integer i1 = (Integer)t.get(0);
            Integer i2 = (Integer)t.get(1);
            assertEquals(1, (int)i1);
            assertEquals(1, (int)i2);
        }
    }
}