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

import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POProject;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrangeForIllustrate;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.PORead;
import org.apache.pig.backend.local.executionengine.physicalLayer.relationalOperators.POCogroup;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanException;

public class TestPOCogroup extends TestCase {
    Random r = new Random();

    public void testCogroup2Inputs() throws Exception {
        DataBag bag1 = BagFactory.getInstance().newDefaultBag();
        Tuple t = TupleFactory.getInstance().newTuple();
        t.append(new Integer(2));
        bag1.add(t);
        t = TupleFactory.getInstance().newTuple();
        t.append(new Integer(1));
        bag1.add(t);
        t = TupleFactory.getInstance().newTuple();
        t.append(new Integer(1));
        bag1.add(t);

        DataBag bag2 = BagFactory.getInstance().newDefaultBag();
        t = TupleFactory.getInstance().newTuple();
        t.append(new Integer(2));
        bag2.add(t);
        t = TupleFactory.getInstance().newTuple();
        t.append(new Integer(2));
        bag2.add(t);
        t = TupleFactory.getInstance().newTuple();
        t.append(new Integer(1));
        bag2.add(t);

        PORead poread1 = new PORead(new OperatorKey("", r.nextLong()), bag1);
        PORead poread2 = new PORead(new OperatorKey("", r.nextLong()), bag2);

        List<PhysicalOperator> inputs1 = new LinkedList<PhysicalOperator>();
        inputs1.add(poread1);

        POProject prj1 = new POProject(new OperatorKey("", r.nextLong()), -1, 0);
        prj1.setResultType(DataType.INTEGER);
        PhysicalPlan p1 = new PhysicalPlan();
        p1.add(prj1);
        List<PhysicalPlan> in1 = new LinkedList<PhysicalPlan>();
        in1.add(p1);
        POLocalRearrangeForIllustrate lr1 = new POLocalRearrangeForIllustrate(new OperatorKey("", r
                .nextLong()), -1, inputs1);
        lr1.setPlans(in1);
        lr1.setIndex(0);

        List<PhysicalOperator> inputs2 = new LinkedList<PhysicalOperator>();
        inputs2.add(poread2);

        POProject prj2 = new POProject(new OperatorKey("", r.nextLong()), -1, 0);
        prj2.setResultType(DataType.INTEGER);
        PhysicalPlan p2 = new PhysicalPlan();
        p2.add(prj2);
        List<PhysicalPlan> in2 = new LinkedList<PhysicalPlan>();
        in2.add(p2);
        POLocalRearrangeForIllustrate lr2 = new POLocalRearrangeForIllustrate(new OperatorKey("", r
                .nextLong()), -1, inputs2);
        lr2.setPlans(in2);
        lr2.setIndex(1);

        List<PhysicalOperator> inputs = new LinkedList<PhysicalOperator>();
        inputs.add(lr1);
        inputs.add(lr2);

        POCogroup poc = new POCogroup(new OperatorKey("", r.nextLong()), -1,
                inputs);

        List<Tuple> expected = new LinkedList<Tuple>();

        Tuple t1 = TupleFactory.getInstance().newTuple();
        t1.append(1);
        DataBag b1 = BagFactory.getInstance().newDefaultBag();
        Tuple temp = TupleFactory.getInstance().newTuple();
        temp.append(1);
        b1.add(temp);
        b1.add(temp);
        t1.append(b1);

        DataBag b2 = BagFactory.getInstance().newDefaultBag();
        b2.add(temp);
        t1.append(b2);

        expected.add(t1);

        t1 = TupleFactory.getInstance().newTuple();
        t1.append(2);
        DataBag b3 = BagFactory.getInstance().newDefaultBag();
        temp = TupleFactory.getInstance().newTuple();
        temp.append(2);
        b3.add(temp);
        t1.append(b3);

        DataBag b4 = BagFactory.getInstance().newDefaultBag();
        b4.add(temp);
        b4.add(temp);
        t1.append(b4);

        expected.add(t1);
        // System.out.println(expected.get(0) + " " + expected.get(1));
        List<Tuple> obtained = new LinkedList<Tuple>();
        for (Result res = poc.getNext(t); res.returnStatus != POStatus.STATUS_EOP; res = poc
                .getNext(t)) {
            System.out.println(res.result);
            obtained.add((Tuple) res.result);
            assertTrue(expected.contains((Tuple) res.result));
        }
        assertEquals(expected.size(), obtained.size());
    }

    public void testCogroup1Input() throws ExecException, PlanException {
        DataBag input = BagFactory.getInstance().newDefaultBag();
        Tuple t = TupleFactory.getInstance().newTuple();
        t.append(1);
        t.append(2);
        input.add(t);
        input.add(t);
        Tuple t2 = TupleFactory.getInstance().newTuple();
        t2.append(2);
        t2.append(2);
        Tuple t3 = TupleFactory.getInstance().newTuple();
        t3.append(3);
        t3.append(4);
        input.add(t2);
        input.add(t3);

        PORead poread1 = new PORead(new OperatorKey("", r.nextLong()), input);
        List<PhysicalOperator> inputs1 = new LinkedList<PhysicalOperator>();
        inputs1.add(poread1);

        POProject prj1 = new POProject(new OperatorKey("", r.nextLong()), -1, 1);
        prj1.setResultType(DataType.INTEGER);
        PhysicalPlan p1 = new PhysicalPlan();
        p1.add(prj1);
        List<PhysicalPlan> in1 = new LinkedList<PhysicalPlan>();
        in1.add(p1);
        POLocalRearrangeForIllustrate lr1 = new POLocalRearrangeForIllustrate(new OperatorKey("", r
                .nextLong()), -1, inputs1);
        lr1.setPlans(in1);
        lr1.setIndex(0);

        List<PhysicalOperator> inputs = new LinkedList<PhysicalOperator>();
        inputs.add(lr1);

        List<Tuple> expected = new LinkedList<Tuple>();
        Tuple tOut1 = TupleFactory.getInstance().newTuple();
        tOut1.append(2);
        DataBag t11 = BagFactory.getInstance().newDefaultBag();
        t11.add(t);
        t11.add(t);
        t11.add(t2);
        tOut1.append(t11);
        expected.add(tOut1);
        Tuple tOut2 = TupleFactory.getInstance().newTuple();
        tOut2.append(4);
        DataBag t22 = BagFactory.getInstance().newDefaultBag();
        t22.add(t3);
        tOut2.append(t22);
        expected.add(tOut2);
        POCogroup poc = new POCogroup(new OperatorKey("", r.nextLong()), -1,
                inputs);
        int count = 0;
        for (Result res = poc.getNext(t); res.returnStatus != POStatus.STATUS_EOP; res = poc
                .getNext(t)) {
            System.out.println(res.result);
            count++;
            assertTrue(expected.contains(res.result));
        }
        assertEquals(expected.size(), count);
    }

}
