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
import static org.junit.Assert.assertTrue;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.ExpressionOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POProject;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.plan.OperatorKey;
import org.junit.Before;
import org.junit.Test;

public class TestPOGenerate {

    DataBag cogroup;
    DataBag partialFlatten;
    DataBag simpleGenerate;
    Random r = new Random();
    BagFactory bf = BagFactory.getInstance();
    TupleFactory tf = TupleFactory.getInstance();

    @Before
    public void setUp() throws Exception {
        Tuple [] inputA = new Tuple[4];
        Tuple [] inputB = new Tuple[4];
        for(int i = 0; i < 4; i++) {
            inputA[i] = tf.newTuple(2);
            inputB[i] = tf.newTuple(1);
        }
        inputA[0].set(0, 'a');
        inputA[0].set(1, '1');
        inputA[1].set(0, 'b');
        inputA[1].set(1, '1');
        inputA[2].set(0, 'a');
        inputA[2].set(1, '1');
        inputA[3].set(0, 'c');
        inputA[3].set(1, '1');
        inputB[0].set(0, 'b');
        inputB[1].set(0, 'b');
        inputB[2].set(0, 'a');
        inputB[3].set(0, 'd');
        DataBag cg11 = bf.newDefaultBag();
        cg11.add(inputA[0]);
        cg11.add(inputA[2]);
        DataBag cg21 = bf.newDefaultBag();
        cg21.add(inputA[1]);
        DataBag cg31 = bf.newDefaultBag();
        cg31.add(inputA[3]);
        DataBag emptyBag = bf.newDefaultBag();
        DataBag cg12 = bf.newDefaultBag();
        cg12.add(inputB[2]);
        DataBag cg22 = bf.newDefaultBag();
        cg22.add(inputB[0]);
        cg22.add(inputB[1]);
        DataBag cg42 = bf.newDefaultBag();
        cg42.add(inputB[3]);
        Tuple [] tIn = new Tuple[4];
        for(int i = 0; i < 4; ++i) {
            tIn[i] = tf.newTuple(2);
        }
        tIn[0].set(0, cg11);
        tIn[0].set(1, cg12);
        tIn[1].set(0, cg21);
        tIn[1].set(1, cg22);
        tIn[2].set(0, cg31);
        tIn[2].set(1, emptyBag);
        tIn[3].set(0, emptyBag);
        tIn[3].set(1, cg42);

        cogroup = bf.newDefaultBag();
        for(int i = 0; i < 4; ++i) {
            cogroup.add(tIn[i]);
        }

        Tuple[] tPartial = new Tuple[4];
        for(int i = 0; i < 4; ++i) {
            tPartial[i] = tf.newTuple(2);
            tPartial[i].set(0, inputA[i].get(0));
            tPartial[i].set(1, inputA[i].get(1));
        }

        tPartial[0].append(cg12);

        tPartial[1].append(cg22);

        tPartial[2].append(cg12);

        tPartial[3].append(emptyBag);

        partialFlatten = bf.newDefaultBag();
        for(int i = 0; i < 4; ++i) {
            partialFlatten.add(tPartial[i]);
        }

        simpleGenerate = bf.newDefaultBag();
        for(int i = 0; i < 4; ++i) {
            simpleGenerate.add(inputA[i]);
        }

        //System.out.println("Cogroup : " + cogroup);
        //System.out.println("Partial : " + partialFlatten);
        //System.out.println("Simple : " + simpleGenerate);

    }

    @Test
    public void testJoin() throws Exception {
        ExpressionOperator prj1 = new POProject(new OperatorKey("", r.nextLong()), -1, 0);
        ExpressionOperator prj2 = new POProject(new OperatorKey("", r.nextLong()), -1, 1);
        prj1.setResultType(DataType.BAG);
        prj2.setResultType(DataType.BAG);
        List<Boolean> toBeFlattened = new LinkedList<Boolean>();
        toBeFlattened.add(true);
        toBeFlattened.add(true);
        PhysicalPlan plan1 = new PhysicalPlan();
        plan1.add(prj1);
        PhysicalPlan plan2 = new PhysicalPlan();
        plan2.add(prj2);
        List<PhysicalPlan> inputs = new LinkedList<PhysicalPlan>();
        inputs.add(plan1);
        inputs.add(plan2);
        PhysicalOperator poGen = new POForEach(new OperatorKey("", r.nextLong()), 1, inputs, toBeFlattened);
        //DataBag obtained = bf.newDefaultBag();
        for (Tuple t : cogroup) {
            /*plan1.attachInput(t);
            plan2.attachInput(t);*/
            poGen.attachInput(t);
            Result output = poGen.getNextTuple();
            while(output.result != null && output.returnStatus != POStatus.STATUS_EOP) {
                //System.out.println(output.result);
                Tuple tObtained = (Tuple) output.result;
                assertTrue(tObtained.get(0).toString().equals(tObtained.get(2).toString()));
                //obtained.add((Tuple) output.result);
                output = poGen.getNextTuple();
            }
        }

    }

    @Test
    public void testPartialJoin() throws Exception {
        ExpressionOperator prj1 = new POProject(new OperatorKey("", r.nextLong()), -1, 0);
        ExpressionOperator prj2 = new POProject(new OperatorKey("", r.nextLong()), -1, 1);
        prj1.setResultType(DataType.BAG);
        prj2.setResultType(DataType.BAG);
        List<Boolean> toBeFlattened = new LinkedList<Boolean>();
        toBeFlattened.add(true);
        toBeFlattened.add(false);
        PhysicalPlan plan1 = new PhysicalPlan();
        plan1.add(prj1);
        PhysicalPlan plan2 = new PhysicalPlan();
        plan2.add(prj2);
        List<PhysicalPlan> inputs = new LinkedList<PhysicalPlan>();
        inputs.add(plan1);
        inputs.add(plan2);
        PhysicalOperator poGen = new POForEach(new OperatorKey("", r.nextLong()), 1, inputs, toBeFlattened);

        //DataBag obtained = bf.newDefaultBag();
        List<String> obtained = new LinkedList<String>();
        for (Tuple t : cogroup) {
            /*plan1.attachInput(t);
            plan2.attachInput(t);*/
            poGen.attachInput(t);
            Result output = poGen.getNextTuple();
            while(output.result != null && output.returnStatus != POStatus.STATUS_EOP) {
                //System.out.println(output.result);
                obtained.add(((Tuple) output.result).toString());
                output = poGen.getNextTuple();
            }
        }
        int count = 0;
        for (Tuple t : partialFlatten) {
            assertTrue(obtained.contains(t.toString()));
            ++count;
        }
        assertEquals(partialFlatten.size(), count);

    }

    @Test
    public void testSimpleGenerate() throws Exception {
        ExpressionOperator prj1 = new POProject(new OperatorKey("", r.nextLong()), -1, 0);
        ExpressionOperator prj2 = new POProject(new OperatorKey("", r.nextLong()), -1, 1);
        prj1.setResultType(DataType.INTEGER);
        prj2.setResultType(DataType.INTEGER);
        List<Boolean> toBeFlattened = new LinkedList<Boolean>();
        toBeFlattened.add(true);
        toBeFlattened.add(false);
        PhysicalPlan plan1 = new PhysicalPlan();
        plan1.add(prj1);
        PhysicalPlan plan2 = new PhysicalPlan();
        plan2.add(prj2);
        List<PhysicalPlan> inputs = new LinkedList<PhysicalPlan>();
        inputs.add(plan1);
        inputs.add(plan2);
        PhysicalOperator poGen = new POForEach(new OperatorKey("", r.nextLong()), 1, inputs, toBeFlattened);

        //DataBag obtained = bf.newDefaultBag();
        List<String> obtained = new LinkedList<String>();
        for (Tuple t : simpleGenerate) {
            /*plan1.attachInput(t);
            plan2.attachInput(t);*/
            poGen.attachInput(t);
            Result output = poGen.getNextTuple();
            while(output.result != null && output.returnStatus != POStatus.STATUS_EOP) {
                //System.out.println(output.result);
                obtained.add(((Tuple) output.result).toString());
                output = poGen.getNextTuple();
            }
        }

        int count = 0;
        for (Tuple t : simpleGenerate) {
            assertTrue(obtained.contains(t.toString()));
            ++count;
        }
        assertEquals(simpleGenerate.size(), count);

    }
}
