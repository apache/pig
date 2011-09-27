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
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.FuncSpec;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POProject;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POUserFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPartialAgg;
import org.apache.pig.builtin.IntSum;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.parser.ParserException;
import org.apache.pig.test.utils.GenPhyOp;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test POPartialAgg runtime
 */
public class TestPOPartialAgg {
    POPartialAgg partAggOp;
    PhysicalPlan parentPlan;
    Tuple dummyTuple = null;

    @Before
    public void setUp() throws Exception {
        createPOPartialPlan();
    }

    private void createPOPartialPlan() throws PlanException {
        parentPlan = new PhysicalPlan();
        partAggOp = GenPhyOp.topPOPartialAgg();
        partAggOp.setParentPlan(parentPlan);

        // setup key plan
        PhysicalPlan keyPlan = new PhysicalPlan();
        POProject keyProj = new POProject(GenPhyOp.getOK(), -1, 0);
        keyProj.setResultType(DataType.INTEGER);
        keyPlan.add(keyProj);
        partAggOp.setKeyPlan(keyPlan);

        // setup value plan
        // project arg for udf
        PhysicalPlan valPlan1 = new PhysicalPlan();
        POProject projVal1 = new POProject(GenPhyOp.getOK(), -1, 1);
        projVal1.setResultType(DataType.BAG);
        valPlan1.add(projVal1);

        // setup udf
        List<PhysicalOperator> udfInps = new ArrayList<PhysicalOperator>();
        udfInps.add(projVal1);
        FuncSpec sumSpec = new FuncSpec(IntSum.Intermediate.class.getName());
        POUserFunc sumUdf = new POUserFunc(GenPhyOp.getOK(), -1, udfInps,
                sumSpec);
        valPlan1.add(sumUdf);
        valPlan1.connect(projVal1, sumUdf);

        List<PhysicalPlan> valuePlans = new ArrayList<PhysicalPlan>();
        valuePlans.add(valPlan1);

        partAggOp.setValuePlans(valuePlans);
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testPartialOneInput1() throws ExecException, ParserException {
        // input tuple has key, and bag containing SUM.Init output
        String[] tups1 = { "(1,(2L))" };
        Tuple t = Util.getTuplesFromConstantTupleStrings(tups1).get(0);
        checkSingleRow(t);
    }

    @Test
    public void testPartialOneInput2() throws ExecException, ParserException {
        // input tuple has key, and bag containing SUM.Init output
        String[] tups1 = { "(null,(2L))" };
        Tuple t = Util.getTuplesFromConstantTupleStrings(tups1).get(0);
        checkSingleRow(t);
    }

    @Test
    public void testPartialOneInput3() throws ExecException, ParserException {
        // input tuple has key, and bag containing SUM.Init output
        String[] tups1 = { "(1,(null))" };
        Tuple t = Util.getTuplesFromConstantTupleStrings(tups1).get(0);
        checkSingleRow(t);
    }

    private void checkSingleRow(Tuple t) throws ExecException {
        Result res;
        // attaching one input tuple, result tuple stays in operator, expect EOP
        partAggOp.attachInput(t);
        res = partAggOp.getNext(dummyTuple);
        assertEquals(POStatus.STATUS_EOP, res.returnStatus);

        // end of all input, now expecting results
        parentPlan.endOfAllInput = true;
        res = partAggOp.getNext(dummyTuple);
        assertEquals(POStatus.STATUS_OK, res.returnStatus);
        assertEquals(t, res.result);
    }

    @Test
    public void testPartialAggNoInput() throws ExecException, ParserException {

        // nothing attached, expecting EOP
        Result res = partAggOp.getNext(dummyTuple);
        assertEquals(POStatus.STATUS_EOP, res.returnStatus);

        // end of all input, still no results
        parentPlan.endOfAllInput = true;
        res = partAggOp.getNext(dummyTuple);
        assertEquals(POStatus.STATUS_EOP, res.returnStatus);

    }

    @Test
    public void testPartialMultiInput1() throws Exception {
        // input tuple has key, and bag containing SUM.Init output
        String[] inputTups = { "(1,(1L))", "(1,(2L))", "(2,(1L))" };
        String[] outputTups = { "(1,(3L))", "(2,(1L))" };
        checkInputAndOutput(inputTups, outputTups, false);
    }

    @Test
    public void testPartialMultiInput2() throws Exception {
        // input tuple has key, and bag containing SUM.Init output
        String[] inputTups = { "(1,(1L))", "(2,(2L))", "(1,(2L))" };
        String[] outputTups = { "(1,(3L))", "(2,(2L))" };
        checkInputAndOutput(inputTups, outputTups, false);
    }

    @Test
    public void testPartialMultiInput3() throws Exception {
        // input tuple has key, and bag containing SUM.Init output
        String[] inputTups = { "(null,(1L))", "(null,(2L))", "(null,(2L))" };
        String[] outputTups = { "(null,(5L))" };
        checkInputAndOutput(inputTups, outputTups, false);
    }

    @Test
    public void testPartialMultiInput4() throws Exception {
        // input tuple has key, and bag containing SUM.Init output
        String[] inputTups = { "(1,(1L))", "(2,(2L))", "(null,(2L))" };
        String[] outputTups = { "(1,(1L))", "(2,(2L))", "(null,(2L))" };
        checkInputAndOutput(inputTups, outputTups, false);
    }

    @Test
    // The case where there is no memory for use by hashmap
    public void testPartialMultiInputHashMemEmpty1() throws Exception {
        // input tuple has key, and bag containing SUM.Init output
        String[] inputTups = { "(1,(1L))", "(2,(2L))", "(null,(2L))" };
        String[] outputTups = { "(1,(1L))", "(2,(2L))", "(null,(2L))" };
        checkInputAndOutput(inputTups, outputTups, true);
    }

    @Test
    // The case where there is no memory for use by hashmap
    public void testPartialMultiInputHashMemEmpty2() throws Exception {
        // input tuple has key, and bag containing SUM.Init output
        String[] inputTups = { "(1,(1L))", "(2,(2L))", "(1,(2L))" };
        // since the group keys with same value are not in consecutive rows
        // and hashmap is not given any memory they don't get
        // aggreated with POPartialAgg
        String[] outputTups = { "(1,(1L))", "(2,(2L))", "(1,(2L))" };
        checkInputAndOutput(inputTups, outputTups, true);
    }

    @Test
    public void testPartialMultiInput1HashMemEmpty() throws Exception {
        // input tuple has key, and bag containing SUM.Init output
        // gby keys in consecutive row, they get aggregated even when
        // hashmap is not given any memory
        String[] inputTups = { "(1,(1L))", "(1,(2L))", "(2,(1L))" };
        String[] outputTups = { "(1,(3L))", "(2,(1L))" };
        checkInputAndOutput(inputTups, outputTups, true);
    }

    @Test
    public void testMultiInput1HashMemEmpty() throws Exception {
        // input tuple has key, and bag containing SUM.Init output
        String[] inputTups = { "(1,(1L))", "(2,(2L))", "(1,(2L))" };
        String[] outputTups = { "(1,(3L))", "(2,(2L))" };
        checkInputAndOutput(inputTups, outputTups, false);
    }

    @Test
    public void testPartialMultiInputMultiInput1HashMemEmpty() throws Exception {
        // input tuple has key, and bag containing SUM.Init output
        String[] inputTups = { "(null,(1L))", "(null,(2L))", "(null,(2L))" };
        String[] outputTups = { "(null,(5L))" };
        checkInputAndOutput(inputTups, outputTups, false);
    }


    /**
     * run the plan on inputTups and check if output matches outputTups if
     * isMapMemEmpty is set to true, set memory available for the hash-map to
     * zero
     * 
     * @param inputTups
     * @param outputTups
     * @param isMapMemEmpty
     * @throws ParserException
     * @throws ExecException
     * @throws PlanException
     */
    private void checkInputAndOutput(String[] inputTups, String[] outputTups,
            boolean isMapMemEmpty) throws Exception {

        PigMapReduce.sJobConfInternal.set(new Configuration());
        if (isMapMemEmpty) {
            PigMapReduce.sJobConfInternal.get().set("pig.cachedbag.memusage",
                    "0");
        }

        List<Tuple> inputs = Util.getTuplesFromConstantTupleStrings(inputTups);
        List<Tuple> expectedOuts = Util
                .getTuplesFromConstantTupleStrings(outputTups);
        List<Tuple> outputs = new ArrayList<Tuple>();

        // run through the inputs
        for (Tuple t : inputs) {
            Result res;
            // attaching one input tuple, result tuple stays in operator, expect
            // EOP
            partAggOp.attachInput(t);
            res = partAggOp.getNext(dummyTuple);
            if (isMapMemEmpty) {
                addResults(res, outputs);
            } else {
                assertEquals(POStatus.STATUS_EOP, res.returnStatus);
            }
        }

        // start getting the outputs

        // end of all input, now expecting results
        parentPlan.endOfAllInput = true;

        if (isMapMemEmpty) {
            Result res = partAggOp.getNext(dummyTuple);
            // only one last output expected
            addResults(res, outputs);

            res = partAggOp.getNext(dummyTuple);
            assertEquals(POStatus.STATUS_EOP, res.returnStatus);
            Util.compareActualAndExpectedResults(outputs, expectedOuts);
        } else {
            while (true) {
                Result res = partAggOp.getNext(dummyTuple);
                if (!addResults(res, outputs)) {
                    break;
                }
            }
            Util.compareActualAndExpectedResults(outputs, expectedOuts);
        }

    }

    private boolean addResults(Result res, List<Tuple> outputs) {
        if (res.returnStatus == POStatus.STATUS_EOP) {
            return false;
        } else if (res.returnStatus == POStatus.STATUS_OK) {
            outputs.add((Tuple) res.result);
            return true;
        } else {
            fail("Invalid result status " + res.returnStatus);
            return false; // to keep compiler happy
        }
    }

}
