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

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.ExecType;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.builtin.ARITY;
import org.apache.pig.builtin.BagSize;
import org.apache.pig.builtin.CONCAT;
import org.apache.pig.builtin.COR;
import org.apache.pig.builtin.COUNT;
import org.apache.pig.builtin.COUNT_STAR;
import org.apache.pig.builtin.COV;
import org.apache.pig.builtin.DIFF;
import org.apache.pig.builtin.Distinct;
import org.apache.pig.builtin.INDEXOF;
import org.apache.pig.builtin.INVERSEMAP;
import org.apache.pig.builtin.KEYSET;
import org.apache.pig.builtin.LAST_INDEX_OF;
import org.apache.pig.builtin.LCFIRST;
import org.apache.pig.builtin.LOWER;
import org.apache.pig.builtin.MapSize;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.builtin.REGEX_EXTRACT;
import org.apache.pig.builtin.REGEX_EXTRACT_ALL;
import org.apache.pig.builtin.REPLACE;
import org.apache.pig.builtin.SIZE;
import org.apache.pig.builtin.STRSPLIT;
import org.apache.pig.builtin.SUBSTRING;
import org.apache.pig.builtin.StringConcat;
import org.apache.pig.builtin.StringSize;
import org.apache.pig.builtin.TOBAG;
import org.apache.pig.builtin.TOKENIZE;
import org.apache.pig.builtin.TOMAP;
import org.apache.pig.builtin.TOP;
import org.apache.pig.builtin.TOTUPLE;
import org.apache.pig.builtin.TRIM;
import org.apache.pig.builtin.TextLoader;
import org.apache.pig.builtin.TupleSize;
import org.apache.pig.builtin.UCFIRST;
import org.apache.pig.builtin.UPPER;
import org.apache.pig.builtin.VALUELIST;
import org.apache.pig.builtin.VALUESET;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.ReadToEndLoader;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.logicalLayer.validators.TypeCheckerException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestBuiltin {

    private String initString = "local";

    PigServer pigServer;

    // This should only be used when absolutely necessary -- eg, when using ReadToEndLoader.
    private static MiniCluster cluster = MiniCluster.buildCluster();

    TupleFactory tupleFactory = TupleFactory.getInstance();
    BagFactory bagFactory = DefaultBagFactory.getInstance();

    // some inputs
    private static Integer[] intInput = { 3, 1, 2, 4, 5, 7, null, 6, 8, 9, 10 };
    private static Long[] intAsLong = { 3L, 1L, 2L, 4L, 5L, 7L, null, 6L, 8L, 9L, 10L };

    private static Long[] longInput = { 145769183483345L, null, 4345639849L, 3435543121L, 2L, 5L, 9L, 7L, 8L, 6L, 10L };

    private static Float[] floatInput = { 10.4f, 2.35f, 3.099f, null, 4.08495f, 5.350f, 6.78f, 7.0f, 8.0f, 9.0f, 0.09f };
    private static Double[] floatAsDouble = { 10.4, 2.35, 3.099, null, 4.08495, 5.350, 6.78, 7.0, 8.0, 9.0, 0.09 };

    private static Double[] doubleInput = { 5.5673910, 121.0, 3.0, 0.000000834593, 1.0, 6.0, 7.0, 8.0, 9.0, 10.0, null };

    private static String[] ba = { "7", "2", "3", null, "4", "5", "6", "1", "8", "9", "10"};
    private static Double[] baAsDouble = { 7.0, 2.0, 3.0, null, 4.0, 5.0, 6.0, 1.0, 8.0, 9.0, 10.0};

    private static String[] stringInput = {"unit", "test", null, "input", "string"};
    private static DataByteArray[] ByteArrayInput = Util.toDataByteArrays(ba);

    // The HashMaps below are used to set up the appropriate EvalFunc,
    // the allowed input and expected output for the different aggregate functions
    // which have different implementations for different input types
    // This way rather than quickly exploding the test cases (one per input type
    // per aggregate), all cases for a given aggregate stage are handled
    // in one test case in a loop

    // A mapping between name of Aggregate function to its corresponding EvalFunc object
    private static HashMap<String, EvalFunc<?>> evalFuncMap = new HashMap<String, EvalFunc<?>>();

    // A mapping between a type name (example: "Integer") and a tuple containing
    // a bag of inputs of that type
    private static HashMap<String, Tuple> inputMap = new HashMap<String, Tuple>();

    // A mapping between name of Aggregate function and the input type of its
    // argument
    private static HashMap<String, String> allowedInput = new HashMap<String, String>();

    // A mapping between name of Aggregate function and the output value (based on the
    // inputs above)
    private static HashMap<String, Object> expectedMap = new HashMap<String, Object>();

    String[] stages = {"Initial", "Intermediate", "Final"};

    String[][] aggs = {
            {"SUM", "IntSum", "LongSum", "FloatSum", "DoubleSum"},
            {"AVG", "IntAvg", "LongAvg", "FloatAvg", "DoubleAvg"},
            {"MIN", "IntMin", "LongMin", "FloatMin", "DoubleMin", "StringMin"},
            {"MAX", "IntMax", "LongMax", "FloatMax", "DoubleMax", "StringMax"},
            {"COUNT"},
            };

    String[] inputTypeAsString = {"ByteArray", "Integer", "Long", "Float", "Double", "String" };

    @Before
    public void setUp() throws Exception {

        pigServer = new PigServer(ExecType.LOCAL, new Properties());
        pigServer.setValidateEachStatement(true);
        // First set up data structs for "base" SUM, MIN and MAX and AVG.
        // The allowed input and expected output data structs for
        // the "Intermediate" and "Final" stages can be based on the
        // "base" case - the allowed inputs for Initial stage can be based
        // on the "base" case.  In the test cases, the
        // output of Initial is sent to Intermediate, so we don't
        // explicitly test the output of Initial and hence do not
        // need to set up expectedMap.

        // first set up EvalFuncMap and expectedMap
        setupEvalFuncMap();

        expectedMap.put("SUM", new Double(55));
        expectedMap.put("DoubleSum", new Double(170.567391834593));
        expectedMap.put("IntSum", new Long(55));
        expectedMap.put("LongSum", new Long(145776964666362L));
        expectedMap.put("FloatSum", new Double(56.15395));

        expectedMap.put("AVG", new Double(5.5));
        expectedMap.put("DoubleAvg", new Double(17.0567391834593));
        expectedMap.put("LongAvg", new Double(14577696466636.2));
        expectedMap.put("IntAvg", new Double(5.5));
        expectedMap.put("FloatAvg", new Double(5.615394958853722));

        expectedMap.put("MIN", new Double(1));
        expectedMap.put("IntMin", new Integer(1));
        expectedMap.put("LongMin", new Long(2));
        expectedMap.put("FloatMin", new Float(0.09f));
        expectedMap.put("DoubleMin", new Double(0.000000834593));
        expectedMap.put("StringMin", "input");

        expectedMap.put("MAX", new Double(10));
        expectedMap.put("IntMax", new Integer(10));
        expectedMap.put("LongMax", new Long(145769183483345L));
        expectedMap.put("FloatMax", new Float(10.4f));
        expectedMap.put("DoubleMax", new Double(121.0));
        expectedMap.put("StringMax", "unit");

        expectedMap.put("COUNT", new Long(10));

        // set up allowedInput
        for (String[] aggGroups : aggs) {
            int i = 0;
            for(String agg: aggGroups) {
                allowedInput.put(agg, inputTypeAsString[i++]);
            }
        }

        // The idea here is that we can reuse the same input
        // and expected output of the algebraic functions
        // for their Intermediate and Final Stages. For the
        // Initial stage we can reuse the input of the algebraic
        // function.

        for (String[] aggGroups : aggs) {
            for(String agg: aggGroups) {
                for (String stage : stages) {
                    if(stage.equals("Initial")) {
                        // For the Initial function, the input should match the input
                        // for the aggregate function itself. In the test cases, the
                        // output of Initial is sent to Intermediate, so we don't
                        // explicitly test the output of Initial and hence do not
                        // need to set up expectedMap.
                        allowedInput.put(agg + stage, allowedInput.get(agg));
                    } else {
                        // For IntSumIntermediate and IntSumFinal and
                        // FloatSumIntermediate and FloatSumFinal, the input is expected
                        // be of types Long and Double respectively (Initial version
                        // of these functions is supposed to convert the Int to Long
                        // and Float to Double respectively) - Likewise for SUMIntermediate
                        // and SumFinal the input is expected to be Double - The Initial
                        // version is supposed to convert byteArrays to Double
                        if((agg).equals("IntSum") || (agg).equals("IntAvg")) {
                            allowedInput.put(agg + stage, "IntegerAsLong");
                        } else if ((agg).equals("FloatSum") || agg.equals("FloatAvg")) {
                            allowedInput.put(agg + stage, "FloatAsDouble");
                        }else if ((agg).equals("MIN") || agg.equals("MAX") ||
                                (agg.equals("SUM")) || agg.equals("AVG")) {
                            // For MIN and MAX the Intermediate and Final functions
                            // expect input to be Doubles (Initial is supposed to
                            // convert the ByteArray to Double)
                            allowedInput.put(agg + stage, "ByteArrayAsDouble");
                        } else {
                            // In all other cases, the input and expected output
                            // for "Intermediate" and "Final" stages should match the input
                            // and expected output for the aggregate function itself
                            allowedInput.put(agg + stage, allowedInput.get(agg));

                        }
                        // For Average, we set up expectedMap only for the "Final" stage
                        // For other aggs, set up expected Map for both "Intermediate"
                        // and "Final"
                        if(! agg.matches("(?i)avg") || stage.equals("Final")) {
                            expectedMap.put(agg + stage, expectedMap.get(agg));
                        }
                    }
                }
            }
        }

        // For Avg, the expected output (for the sum part) for Intermediate are the
        // same as SUM - so handled a little differently accordingly
        expectedMap.put("AVGIntermediate", expectedMap.get("SUM"));
        expectedMap.put("DoubleAvgIntermediate", expectedMap.get("DoubleSum"));
        expectedMap.put("LongAvgIntermediate", expectedMap.get("LongSum"));
        expectedMap.put("IntAvgIntermediate", expectedMap.get("IntSum"));
        expectedMap.put("FloatAvgIntermediate", expectedMap.get("FloatSum"));

        // set up input hash
        try{
            inputMap.put("Integer", Util.loadNestTuple(TupleFactory.getInstance().newTuple(1), intInput));
            inputMap.put("IntegerAsLong", Util.loadNestTuple(TupleFactory.getInstance().newTuple(1), intAsLong));
            inputMap.put("Long", Util.loadNestTuple(TupleFactory.getInstance().newTuple(1), longInput));
            inputMap.put("Float", Util.loadNestTuple(TupleFactory.getInstance().newTuple(1), floatInput));
            inputMap.put("FloatAsDouble", Util.loadNestTuple(TupleFactory.getInstance().newTuple(1), floatAsDouble));
            inputMap.put("Double", Util.loadNestTuple(TupleFactory.getInstance().newTuple(1), doubleInput));
            inputMap.put("ByteArray", Util.loadNestTuple(TupleFactory.getInstance().newTuple(1), ByteArrayInput));
            inputMap.put("ByteArrayAsDouble", Util.loadNestTuple(TupleFactory.getInstance().newTuple(1), baAsDouble));
            inputMap.put("String", Util.loadNestTuple(TupleFactory.getInstance().newTuple(1), stringInput));

        }catch(ExecException e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void shutDown() {
      cluster.shutDown();
    }

    /**
     *
     */
    private void setupEvalFuncMap() {

        for (String[] aggGroup : aggs) {
            for (String agg : aggGroup) {
                // doing this as a two step process because PigContext.instantiateFuncFromSpec("SUM.Intermediate")
                // fails with class resolution error.
                EvalFunc<?> func = (EvalFunc<?>)PigContext.instantiateFuncFromSpec(agg);
                evalFuncMap.put(agg, func);
                evalFuncMap.put(agg + "Initial", (EvalFunc<?>)PigContext.instantiateFuncFromSpec(((Algebraic)func).getInitial()));
                evalFuncMap.put(agg + "Intermediate", (EvalFunc<?>)PigContext.instantiateFuncFromSpec(((Algebraic)func).getIntermed()));
                evalFuncMap.put(agg + "Final", (EvalFunc<?>)PigContext.instantiateFuncFromSpec(((Algebraic)func).getFinal()));
            }
        }
    }

    /**
     * Test the case where the combiner is not called - so initial is called
     * and then final is called
     * @throws Exception
     */
    @Test
    public void testAggNoCombine() throws Exception {

        for (String[] aggGroup : aggs) {
            String[] aggFinalTypes = null; // will contains AVGFinal, DoubleAvgFinal etc
            String[] aggInitialTypes = null; // will contains AVGInitial, DoubleAvgInitial etc

            for (String stage: stages) {
                String[] aggTypesArray = null;
                if(stage.equals("Initial")) {
                    aggInitialTypes = new String[aggGroup.length];
                    aggTypesArray = aggInitialTypes;
                } else  if(stage.equals("Final")) {
                    aggFinalTypes = new String[aggGroup.length];
                    aggTypesArray = aggFinalTypes;
                } else { // Intermediate
                    continue;
                }
                for (int i = 0; i < aggTypesArray.length; i++) {
                    aggTypesArray[i] = aggGroup[i] + stage;
                }
            }

            for(int k = 0; k < aggFinalTypes.length; k++) {
                EvalFunc<?> avgInitial = evalFuncMap.get(aggInitialTypes[k]);
                Tuple tup = inputMap.get(getInputType(aggInitialTypes[k]));

                // To test this case, first AVGInitial is called for each input
                // value and output of it is put into a bag. The bag containing
                // all AVGInitial output is provided as input to AVGFinal

                // The tuple we got above has a bag with input
                // values. Lets call AVGInitial with each value:
                DataBag bg = (DataBag) tup.get(0);
                DataBag  finalInputBg = bagFactory.newDefaultBag();
                for (Tuple tuple : bg) {
                    DataBag initialInputBg = bagFactory.newDefaultBag();
                    initialInputBg.add(tuple);
                    Tuple initialInputTuple = tupleFactory.newTuple(initialInputBg);
                    finalInputBg.add((Tuple)avgInitial.exec(initialInputTuple));
                }

                Tuple finalInputTuple = tupleFactory.newTuple(finalInputBg);
                EvalFunc<?> aggFinal = evalFuncMap.get(aggFinalTypes[k]);
                String msg = "[Testing " + aggGroup[k] + " on input type: " + getInputType(aggFinalTypes[k]);
                System.err.println(msg + " for no combiner case]");
                Object output = aggFinal.exec(finalInputTuple);
                msg += " ( (output) " + output + " == " + getExpected(aggFinalTypes[k]) + " (expected) )]";
                // for doubles, precisions can be a problem - so check
                // if the type is double for expected result and check
                // within some precision
                if(getExpected(aggFinalTypes[k]) instanceof Double) {
                    assertEquals(msg, (Double)getExpected(aggFinalTypes[k]), (Double)output, 0.00001);
                } else {
                    assertEquals(msg, getExpected(aggFinalTypes[k]), output);
                }
            }
        }
    }

    /**
     * Test the case where the combiner is called once - so initial is called
     * and then Intermediate and then final is called
     * @throws Exception
     */
    @Test
    public void testAggSingleCombine() throws Exception {

        for (String[] aggGroup : aggs) {
            String[] aggFinalTypes = null; // will contains AVGFinal, DoubleAvgFinal etc
            String[] aggInitialTypes = null; // will contains AVGInitial, DoubleAvgInitial etc
            String[] aggIntermediateTypes = null; // will contains AVGIntermediate, DoubleAvgIntermediate etc
            for (String stage: stages) {
                String[] aggTypesArray = null;
                if(stage.equals("Initial")) {
                    aggInitialTypes = new String[aggGroup.length];
                    aggTypesArray = aggInitialTypes;
                } else if (stage.equals("Intermediate")) {
                    aggIntermediateTypes = new String[aggGroup.length];
                    aggTypesArray = aggIntermediateTypes;
                } else  {// final
                    aggFinalTypes = new String[aggGroup.length];
                    aggTypesArray = aggFinalTypes;
                }

                for (int i = 0; i < aggTypesArray.length; i++) {
                    aggTypesArray[i] = aggGroup[i] + stage;
                }
            }
            for(int k = 0; k < aggFinalTypes.length; k++) {
                EvalFunc<?> aggInitial = evalFuncMap.get(aggInitialTypes[k]);
                Tuple tup = inputMap.get(getInputType(aggInitialTypes[k]));
                // To test this case, first <Agg>Initial is called for each input
                // value. The output from <Agg>Initial for the first half of inputs is
                // put into one bag and the next half into another. Then these two
                // bags are provided as inputs to two separate calls of <Agg>Intermediate.
                // The outputs from the two calls to <Agg>Intermediate are put into a bag
                // and sent as input to <Agg>Final

                // The tuple we got above has a bag with input
                // values. Lets call <Agg>Initial with each value:
                DataBag bg = (DataBag) tup.get(0);
                DataBag  intermediateInputBg1 = bagFactory.newDefaultBag();
                DataBag  intermediateInputBg2 = bagFactory.newDefaultBag();
                int i = 0;
                for (Tuple tuple : bg) {
                    DataBag initialInputBg = bagFactory.newDefaultBag();
                    initialInputBg.add(tuple);
                    Tuple initialInputTuple = tupleFactory.newTuple(initialInputBg);
                    if(i < bg.size()/2) {
                        intermediateInputBg1.add((Tuple)aggInitial.exec(initialInputTuple));
                    } else {
                        intermediateInputBg2.add((Tuple)aggInitial.exec(initialInputTuple));
                    }
                    i++;
                }

                EvalFunc<?> avgIntermediate = evalFuncMap.get(aggIntermediateTypes[k]);
                DataBag finalInputBg = bagFactory.newDefaultBag();
                Tuple intermediateInputTuple = tupleFactory.newTuple(intermediateInputBg1);
                finalInputBg.add((Tuple)avgIntermediate.exec(intermediateInputTuple));
                intermediateInputTuple = tupleFactory.newTuple(intermediateInputBg2);
                finalInputBg.add((Tuple)avgIntermediate.exec(intermediateInputTuple));

                Tuple finalInputTuple = tupleFactory.newTuple(finalInputBg);
                EvalFunc<?> aggFinal = evalFuncMap.get(aggFinalTypes[k]);
                String msg = "[Testing " + aggGroup[k] + " on input type: " + getInputType(aggFinalTypes[k]);
                System.err.println(msg + " for single combiner case]");
                Object output = aggFinal.exec(finalInputTuple);
                msg += " ( (output) " + output + " == " + getExpected(aggFinalTypes[k]) + " (expected) )]";
                // for doubles, precisions can be a problem - so check
                // if the type is double for expected result and check
                // within some precision
                if(getExpected(aggFinalTypes[k]) instanceof Double) {
                    assertEquals(msg, (Double)getExpected(aggFinalTypes[k]), (Double)output, 0.00001);
                } else {
                    assertEquals(msg, getExpected(aggFinalTypes[k]), output);
                }
            }
        }

    }


    /**
     * Test the case where the combiner is called more than once - so initial is called
     * and then Intermediate called couple of times and then final is called
     * @throws Exception
     */
    @Test
    public void testAggMultipleCombine() throws Exception {

        for (String[] aggGroup : aggs) {
            String[] aggFinalTypes = null; // will contains AVGFinal, DoubleAvgFinal etc
            String[] aggInitialTypes = null; // will contains AVGInitial, DoubleAvgInitial etc
            String[] aggIntermediateTypes = null; // will contains AVGIntermediate, DoubleAvgIntermediate etc
            for (String stage: stages) {
                String[] aggTypesArray = null;
                if(stage.equals("Initial")) {
                    aggInitialTypes = new String[aggGroup.length];
                    aggTypesArray = aggInitialTypes;
                } else if (stage.equals("Intermediate")) {
                    aggIntermediateTypes = new String[aggGroup.length];
                    aggTypesArray = aggIntermediateTypes;
                } else  {// final
                    aggFinalTypes = new String[aggGroup.length];
                    aggTypesArray = aggFinalTypes;
                }

                for (int i = 0; i < aggTypesArray.length; i++) {
                    aggTypesArray[i] = aggGroup[i] + stage;
                }
            }
            for(int k = 0; k < aggFinalTypes.length; k++) {
                EvalFunc<?> aggInitial = evalFuncMap.get(aggInitialTypes[k]);
                Tuple tup = inputMap.get(getInputType(aggInitialTypes[k]));
                // To test this case, first <Agg>Initial is called for each input
                // value. The output from <Agg>Initial for quarter of values from
                // the inputs is put into one bag. Then 4 calls are made to Intermediate
                // with each bag going to one call. This simulates the call in the map-combine
                // boundary. The outputs from the first two calls to Intermediate above are
                // put into a bag and the output from the next two calls put into another bag.
                // These two bags are provided as inputs to two separate calls of <Agg>Intermediate.
                // This simulates the call in the combine-reduce boundary.
                // The outputs from the two calls to <Agg>Intermediate are put into a bag
                // and sent as input to <Agg>Final

                // The tuple we got above has a bag with input
                // values. Lets call <Agg>Initial with each value:
                DataBag bg = (DataBag) tup.get(0);
                DataBag[]  mapIntermediateInputBgs = new DataBag[4];
                for (int i = 0; i < mapIntermediateInputBgs.length; i++) {
                    mapIntermediateInputBgs[i] = bagFactory.newDefaultBag();
                }
                Iterator<Tuple> it = bg.iterator();
                for(int i = 0; i < 4; i++) {
                    for(int j = 0; j < bg.size()/4; j++) {
                        DataBag initialInputBg = bagFactory.newDefaultBag();
                        initialInputBg.add(it.next());
                        Tuple initialInputTuple = tupleFactory.newTuple(initialInputBg);
                        mapIntermediateInputBgs[i].add((Tuple)aggInitial.exec(initialInputTuple));
                    }
                    if(i == 3) {
                        // if the last quarter has more elements process them
                        while(it.hasNext()) {
                            DataBag initialInputBg = bagFactory.newDefaultBag();
                            initialInputBg.add(it.next());
                            Tuple initialInputTuple = tupleFactory.newTuple(initialInputBg);
                            mapIntermediateInputBgs[i].add((Tuple)aggInitial.exec(initialInputTuple));
                        }
                    }
                }

                EvalFunc<?> aggIntermediate = evalFuncMap.get(aggIntermediateTypes[k]);
                DataBag[] reduceIntermediateInputBgs = new DataBag[2];
                for (int i = 0; i < reduceIntermediateInputBgs.length; i++) {
                    reduceIntermediateInputBgs[i] = bagFactory.newDefaultBag();
                }

                // simulate call to combine after map
                for(int i = 0; i < 4; i++) {
                    Tuple intermediateInputTuple = tupleFactory.newTuple(mapIntermediateInputBgs[i]);
                    if(i < 2) {
                        reduceIntermediateInputBgs[0].add((Tuple)aggIntermediate.exec(intermediateInputTuple));
                    } else {
                        reduceIntermediateInputBgs[1].add((Tuple)aggIntermediate.exec(intermediateInputTuple));
                    }
                }

                DataBag finalInputBag = bagFactory.newDefaultBag();
                // simulate call to combine before reduce
                for(int i = 0; i < 2; i++) {
                    Tuple intermediateInputTuple = tupleFactory.newTuple(reduceIntermediateInputBgs[i]);
                    finalInputBag.add((Tuple)aggIntermediate.exec(intermediateInputTuple));
                }

                // simulate call to final (in reduce)
                Tuple finalInputTuple = tupleFactory.newTuple(finalInputBag);
                EvalFunc<?> aggFinal = evalFuncMap.get(aggFinalTypes[k]);
                String msg = "[Testing " + aggGroup[k] + " on input type: " + getInputType(aggFinalTypes[k]);
                System.err.println(msg + " for multiple combiner case]");
                Object output = aggFinal.exec(finalInputTuple);
                msg += " ( (output) " + output + " == " + getExpected(aggFinalTypes[k]) + " (expected) )]";
                // for doubles, precisions can be a problem - so check
                // if the type is double for expected result and check
                // within some precision
                if(getExpected(aggFinalTypes[k]) instanceof Double) {
                    assertEquals(msg, (Double)getExpected(aggFinalTypes[k]), (Double)output, 0.00001);
                } else {
                    assertEquals(msg, getExpected(aggFinalTypes[k]), output);
                }
            }
        }

    }

    /**
     * Test the case where an empty bag is given as input to
     * the Initial function and the output is fed to Intermediate
     * function whose output is fed to the Final function
     * @throws Exception
     */
    @Test
    public void testAggEmptyBagWithCombiner() throws Exception {

        for (String[] aggGroup : aggs) {
            String[] aggFinalTypes = null; // will contains AVGFinal, DoubleAvgFinal etc
            String[] aggInitialTypes = null; // will contains AVGInitial, DoubleAvgInitial etc
            String[] aggIntermediateTypes = null; // will contains AVGIntermediate, DoubleAvgIntermediate etc
            for (String stage: stages) {
                String[] aggTypesArray = null;
                if(stage.equals("Initial")) {
                    aggInitialTypes = new String[aggGroup.length];
                    aggTypesArray = aggInitialTypes;
                } else if (stage.equals("Intermediate")) {
                    aggIntermediateTypes = new String[aggGroup.length];
                    aggTypesArray = aggIntermediateTypes;
                } else  {// final
                    aggFinalTypes = new String[aggGroup.length];
                    aggTypesArray = aggFinalTypes;
                }

                for (int i = 0; i < aggTypesArray.length; i++) {
                    aggTypesArray[i] = aggGroup[i] + stage;
                }
            }
            for(int k = 0; k < aggFinalTypes.length; k++) {
                EvalFunc<?> aggInitial = evalFuncMap.get(aggInitialTypes[k]);
                // To test this case, first <Agg>Initial is called with an empty bag
                // as input. This is done in two ierations of 5 calls.
                // The output from <Agg>Initial for the first half of inputs is
                // put into one bag and the next half into another. Then these two
                // bags are provided as inputs to two separate calls of <Agg>Intermediate.
                // The outputs from the two calls to <Agg>Intermediate are put into a bag
                // and sent as input to <Agg>Final

                DataBag  intermediateInputBg1 = bagFactory.newDefaultBag();
                DataBag  intermediateInputBg2 = bagFactory.newDefaultBag();
                Tuple outputTuple = null;
                for(int i = 0; i < 10; i++) {
                    // create empty bag input to be provided as input
                    // argument to the "Initial" function
                    DataBag initialInputBg = bagFactory.newDefaultBag();
                    Tuple initialInputTuple = tupleFactory.newTuple(initialInputBg);

                    if(i < 5) {
                        outputTuple = (Tuple)aggInitial.exec(initialInputTuple);
                        // check that output is null for all aggs except COUNT
                        // COUNT will give an output of 0 for empty bag input
                        checkZeroOrNull(aggInitial, outputTuple.get(0));
                        intermediateInputBg1.add(outputTuple);
                    } else {
                        outputTuple = (Tuple)aggInitial.exec(initialInputTuple);
                        // check that output is null for all aggs except COUNT
                        // COUNT will give an output of 0 for empty bag input
                        checkZeroOrNull(aggInitial, outputTuple.get(0));
                        intermediateInputBg2.add(outputTuple);
                    }
                }

                EvalFunc<?> aggIntermediate = evalFuncMap.get(aggIntermediateTypes[k]);
                DataBag finalInputBg = bagFactory.newDefaultBag();
                Tuple intermediateInputTuple = tupleFactory.newTuple(intermediateInputBg1);
                outputTuple = (Tuple)aggIntermediate.exec(intermediateInputTuple);
                // check that output is null for all aggs except COUNT
                // COUNT will give an output of 0 for empty bag input
                checkZeroOrNull(aggIntermediate, outputTuple.get(0));
                finalInputBg.add(outputTuple);
                intermediateInputTuple = tupleFactory.newTuple(intermediateInputBg2);
                outputTuple = (Tuple)aggIntermediate.exec(intermediateInputTuple);
                // check that output is null for all aggs except COUNT
                // COUNT will give an output of 0 for empty bag input
                checkZeroOrNull(aggIntermediate, outputTuple.get(0));
                finalInputBg.add(outputTuple);

                Tuple finalInputTuple = tupleFactory.newTuple(finalInputBg);

                EvalFunc<?> aggFinal = evalFuncMap.get(aggFinalTypes[k]);
                Object output = aggFinal.exec(finalInputTuple);
                // check that output is null for all aggs except COUNT
                // COUNT will give an output of 0 for empty bag input
                checkZeroOrNull(aggFinal, output);
            }
        }

    }

    /**
     * Test the case where an empty bag is given as input to the non
     * combiner version of aggregate functions
     * @throws Exception if there are issues executing the aggregate function
     */
    @Test
    public void testAggEmptyBag() throws Exception {

        for (String[] aggGroup : aggs) {

            for(int k = 0; k < aggGroup.length; k++) {
                EvalFunc<?> agg = evalFuncMap.get(aggGroup[k]);

                // call agg with empty bag as input
                DataBag inputBag = bagFactory.newDefaultBag();
                Tuple inputTuple = tupleFactory.newTuple(inputBag);

                Object output = agg.exec(inputTuple);
                // check that output is null for all aggs except COUNT
                // COUNT will give an output of 0 for empty bag input
                checkZeroOrNull(agg, output);
            }
        }

    }

    private void checkZeroOrNull(EvalFunc<?> func, Object output) {
        if(func.getClass().getName().contains("COUNT")) {
            assertEquals(new Long(0), output);
        } else {
            assertEquals(null, output);
        }
    }


    // Builtin MATH Functions
    // =======================
    @Test
    public void testAVG() throws Exception {
        String[] avgTypes = {"AVG", "DoubleAvg", "LongAvg", "IntAvg", "FloatAvg"};
        for(int k = 0; k < avgTypes.length; k++) {
            EvalFunc<?> avg = evalFuncMap.get(avgTypes[k]);
            Tuple tup = inputMap.get(getInputType(avgTypes[k]));
            Object output = avg.exec(tup);
            String msg = "[Testing " + avgTypes[k] + " on input type: " + getInputType(avgTypes[k]) + " ( (output) " +
                         output + " == " + getExpected(avgTypes[k]) + " (expected) )]";
            assertEquals(msg, (Double)output, (Double)getExpected(avgTypes[k]), 0.00001);

        }
    }

    @Test
    public void testAVGIntermediate() throws Exception {
        String[] avgTypes = {"AVGIntermediate", "DoubleAvgIntermediate", "LongAvgIntermediate", "IntAvgIntermediate", "FloatAvgIntermediate"};
        for(int k = 0; k < avgTypes.length; k++) {
            EvalFunc<?> avg = evalFuncMap.get(avgTypes[k]);
            String inputType = getInputType(avgTypes[k]);
            Tuple tup = inputMap.get(inputType);
            // The tuple we got above has a bag with input
            // values. Input to the Intermediate.exec() however comes
            // from the map which would put each value and a count of
            // 1 in a tuple and send it down. So lets create a bag with
            // tuples that have two fields - the value and a count 1.
            DataBag bag = (DataBag) tup.get(0);
            DataBag  bg = bagFactory.newDefaultBag();
            for (Tuple t: bag) {
                Tuple newTuple = tupleFactory.newTuple(2);
                newTuple.set(0, t.get(0));
                newTuple.set(1, new Long(1));
                bg.add(newTuple);
            }
            Tuple intermediateInput = tupleFactory.newTuple();
            intermediateInput.append(bg);

            Object output = avg.exec(intermediateInput);

            if(inputType == "Long" || inputType == "Integer" || inputType == "IntegerAsLong") {
                Long l = (Long)((Tuple)output).get(0);
                String msg = "[Testing " + avgTypes[k] + " on input type: " + getInputType(avgTypes[k]) + " ( (output) " +
                              l + " == " + getExpected(avgTypes[k]) + " (expected) )]";
                assertEquals(msg, getExpected(avgTypes[k]), l);
            } else {
                Double f1 = (Double)((Tuple)output).get(0);
                String msg = "[Testing " + avgTypes[k] + " on input type: " + getInputType(avgTypes[k]) + " ( (output) " +
                               f1 + " == " + getExpected(avgTypes[k]) + " (expected) )]";
                assertEquals(msg, (Double)getExpected(avgTypes[k]), f1, 0.00001);
            }
            Long f2 = (Long)((Tuple)output).get(1);
            assertEquals("[Testing " + avgTypes[k] + " on input type: "+
                inputType+"]Expected count to be 11", 11, f2.longValue());
        }
    }

    @Test
    public void testAVGFinal() throws Exception {
        String[] avgTypes = {"AVGFinal", "DoubleAvgFinal", "LongAvgFinal", "IntAvgFinal", "FloatAvgFinal"};
        String[] avgIntermediateTypes = {"AVGIntermediate", "DoubleAvgIntermediate", "LongAvgIntermediate", "IntAvgIntermediate", "FloatAvgIntermediate"};
        for(int k = 0; k < avgTypes.length; k++) {
            EvalFunc<?> avg = evalFuncMap.get(avgTypes[k]);
            Tuple tup = inputMap.get(getInputType(avgTypes[k]));

            // To test AVGFinal, AVGIntermediate should first be called and
            // the output of AVGIntermediate should be supplied as input to
            // AVGFinal. To simulate this, we will call Intermediate twice
            // on the above tuple and collect the outputs and pass it to
            // Final.

            // get the right "Intermediate" EvalFunc
            EvalFunc<?> avgIntermediate = evalFuncMap.get(avgIntermediateTypes[k]);
            // The tuple we got above has a bag with input
            // values. Input to the Intermediate.exec() however comes
            // from the map which would put each value and a count of
            // 1 in a tuple and send it down. So lets create a bag with
            // tuples that have two fields - the value and a count 1.
            // The input has 10 values - lets put the first five of them
            // in the input to the first call of AVGIntermediate and the
            // remaining five in the second call.
            DataBag bg = (DataBag) tup.get(0);
            DataBag  bg1 = bagFactory.newDefaultBag();
            DataBag  bg2 = bagFactory.newDefaultBag();
            int i = 0;
            for (Tuple t: bg) {
                Tuple newTuple = tupleFactory.newTuple(2);
                newTuple.set(0, t.get(0));
                if ( t.get(0) == null)
                    newTuple.set(1, new Long(0));
                else
                    newTuple.set(1, new Long(1));
                if(i < 5) {
                    bg1.add(newTuple);
                } else {
                    bg2.add(newTuple);
                }
                i++;
            }
            Tuple intermediateInput1 = tupleFactory.newTuple();
            intermediateInput1.append(bg1);
            Object output1 = avgIntermediate.exec(intermediateInput1);
            Tuple intermediateInput2 = tupleFactory.newTuple();
            intermediateInput2.append(bg2);
            Object output2 = avgIntermediate.exec(intermediateInput2);

            DataBag bag = Util.createBag(new Tuple[]{(Tuple)output1, (Tuple)output2});

            Tuple finalTuple = TupleFactory.getInstance().newTuple(1);
            finalTuple.set(0, bag);
            Object output = avg.exec(finalTuple);
            String msg = "[Testing " + avgTypes[k] + " on input type: " + getInputType(avgTypes[k]) + " ( (output) " +
            output + " == " + getExpected(avgTypes[k]) + " (expected) )]";
            assertEquals(msg, (Double)getExpected(avgTypes[k]), (Double)output, 0.00001);
        }
    }

    @Test
    public void testCOUNT() throws Exception {
        Integer input[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, null };
        long expected = input.length - 1;

        EvalFunc<Long> count = new COUNT();
        Tuple tup = Util.loadNestTuple(TupleFactory.getInstance().newTuple(1), input);
        Long output = count.exec(tup);

        assertTrue(output == expected);
    }

    @Test
    public void testCOUNTIntermed() throws Exception {
        Integer input[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };

        DataBag intermediateInputBag = bagFactory.newDefaultBag();
        // call initial and then Intermed
        for (Integer i : input) {
            Tuple t = tupleFactory.newTuple(i);
            DataBag b = bagFactory.newDefaultBag();
            b.add(t);
            Tuple initialInput = tupleFactory.newTuple(b);
            EvalFunc<?> initial = new COUNT.Initial();
            intermediateInputBag.add((Tuple)initial.exec(initialInput));
        }

        EvalFunc<Tuple> countIntermed = new COUNT.Intermediate();
        Tuple intermediateInput = tupleFactory.newTuple(intermediateInputBag);
        Tuple output = countIntermed.exec(intermediateInput);

        Long f1 = DataType.toLong(output.get(0));
        assertEquals("Expected count to be 10", 10, f1.longValue());
    }

    @Test
    public void testCOUNTFinal() throws Exception {
        long input[] = { 23, 38, 39 };
        Tuple tup = Util.loadNestTuple(TupleFactory.getInstance().newTuple(1), input);

        EvalFunc<Long> count = new COUNT.Final();
        Long output = count.exec(tup);

        assertEquals("Expected count to be 100", 100, output.longValue());
    }

    @Test
    public void testCount_ValidNumberOfArguments_WithoutInputSchema_One() throws Exception {
         File inputFile = createCountInputFile();
         try {
             pigServer.registerQuery("A = load '" + inputFile.getAbsolutePath() + "';");
             pigServer.registerQuery("B = group A all;");
             pigServer.registerQuery("C = foreach B generate COUNT(A.$0);");
             assertValidCount();
         }catch(TypeCheckerException e) {
             Assert.fail("Query is in accordance with schema, still it failed to execute");
         }finally {
             inputFile.delete();
         }
    }

    @Test
    public void testCount_ValidNumberOfArguments_WithoutInputSchema_Two() throws Exception {
         File inputFile = createCountInputFile();
         try {
             pigServer.registerQuery("A = load '" + inputFile.getAbsolutePath() + "';");
             pigServer.registerQuery("B = group A all;");
             pigServer.registerQuery("C = foreach B generate COUNT(A);");
             assertValidCount();
         }catch(TypeCheckerException e) {
             Assert.fail("Query is in accordance with schema, still it failed to execute");
         }finally {
             inputFile.delete();
         }
    }

    @Test
    public void testCount_ValidNumberOfArguments_WithNullTuplesInInput_CaseOne() throws Exception {
        String inputFileName = "CountTest.txt";
        String[] inputData = new String[] {
                "1 2 3 4\n",
                "\n",
                "a b c d\n",
                "\n",
                "r s t u"};
        File inputFile = Util.createInputFile("tmp", inputFileName, inputData);

         try {
             pigServer.registerQuery("A = load '" + inputFile.getAbsolutePath() + "';");
             pigServer.registerQuery("B = group A all;");
             pigServer.registerQuery("C = foreach B generate COUNT(A);");
             assertValidCount();
         }catch(TypeCheckerException e) {
             Assert.fail("Query is in accordance with schema, still it failed to execute");
         }finally {
             inputFile.delete();
         }
    }

    @Test
    public void testCount_ValidNumberOfArguments_WithNullTuplesInInput_CaseTwo() throws Exception {
        String[] inputData = new String[] {
                "1 2 3 4\n",
                "\n",
                "a b c d\n",
                "\n",
                "r s t u"};
        File file = Util.createInputFile("tmp", "CountTest.txt", inputData);
        String inputFileName = file.getAbsolutePath();

         try {
             pigServer.registerQuery("A = load '" + inputFileName + "';");
             pigServer.registerQuery("B = group A all;");
             pigServer.registerQuery("C = foreach B generate COUNT(A.$0);");
             assertValidCount();
         }catch(TypeCheckerException e) {
             Assert.fail("Query is in accordance with schema, still it failed to execute");
         } finally {
           file.delete();
         }
    }


    @Test
    public void testCount_ValidNumberOfArguments_WithInputSchema_One() throws Exception {
         File inputFile = createCountInputFile();
         try {
             pigServer.registerQuery("A = load '" + inputFile.getAbsolutePath() + "' as (data:chararray);");
             pigServer.registerQuery("B = group A all;");
             pigServer.registerQuery("C = foreach B generate COUNT(A.$0);");
             assertValidCount();
         }catch(TypeCheckerException e) {
             Assert.fail("Query is in accordance with schema, still it failed to execute");
         } finally {
           inputFile.delete();
         }
    }

    @Test
    public void testCount_ValidNumberOfArguments_WithInputSchema_Two() throws Exception {
         File inputFile = createCountInputFile();
         try {
             pigServer.registerQuery("A = load '" + inputFile.getAbsolutePath() + "' as (data:chararray);");
             pigServer.registerQuery("B = group A all;");
             pigServer.registerQuery("C = foreach B generate COUNT(A);");
             assertValidCount();
         }catch(TypeCheckerException e) {
             Assert.fail("Query is in accordance with schema, still it failed to execute");
         } finally {
           inputFile.delete();
         }
    }

    /**
     * Assert the result of COUNT for valid scenarios has only one tuple in output which has only one item which is set
     * to 3.
     *
     * @throws IOException
     * @throws ExecException
     */
    private void assertValidCount() throws IOException, ExecException {
        Iterator<Tuple> it = pigServer.openIterator("C");
         int i=0;
         final int expectedOutputTupleSize = 1;
         final long expectedCount = 3;
         while (it.hasNext()) {
             Tuple t = it.next();
             assertEquals("Testing SIZE(<Tuple>): ", expectedOutputTupleSize, t.size());
             assertEquals("Testing Value within<Tuple>: ", expectedCount, t.get(0));
             i++;
         }
         assertEquals("Testing the above loop ran only once.",1,i);
    }

    @Test
    public void testCount_InvalidNumberOfArguments_WithoutInputSchema() throws Exception {
         File inputFile = createCountInputFile();
         try {
             pigServer.registerQuery("A = load '" + inputFile.getAbsolutePath() + "';");
             pigServer.registerQuery("B = group A all;");
             pigServer.registerQuery("C = foreach B generate COUNT(A.$0, A.$0);");
             pigServer.openIterator("C");
             Assert.fail("COUNT is suppose to run with one argument of type BAG, however it ran with couple of arguments.");
         }catch(FrontendException e) {
             Assert.assertTrue( e.getCause() instanceof TypeCheckerException );
         } finally {
           inputFile.delete();
         }
    }

    @Test
    public void testCount_InvalidNumberOfArguments_WithInputSchema() throws Exception {
         File inputFile = createCountInputFile();
         try {
             pigServer.registerQuery("A = load '" + inputFile.getAbsolutePath() + "' as (data:chararray);");
             pigServer.registerQuery("B = group A all;");
             pigServer.registerQuery("C = foreach B generate COUNT(A.$0, A.$0);");
             pigServer.openIterator("C");
             Assert.fail("COUNT is suppose to run with one argument of type BAG, however it ran with couple of arguments.");
         }catch(FrontendException e) {
             Assert.assertTrue( e.getCause() instanceof TypeCheckerException );
         } finally {
           inputFile.delete();
         }
    }

    @Test
    public void testCount_InvalidArgumentType() throws Exception {
         File inputFile = createCountInputFile();
         try {
             pigServer.registerQuery("A = load '" + inputFile.getAbsolutePath() + "' as (data:chararray);");
             pigServer.registerQuery("B = group A all;");
             pigServer.registerQuery("C = foreach B generate COUNT('data');");
             pigServer.openIterator("C");
             Assert.fail("COUNT is suppose to run with one argument of type BAG, however it ran with an argument of type chararray.");
         }catch(FrontendException e) {
             Assert.assertTrue( e.getCause() instanceof TypeCheckerException );
         } finally {
           inputFile.delete();
         }
    }

    /**
     * Creates a input file that will be used to test COUNT builtin function.
     *
     * @return name of the input file.
     * @throws IOException
     *             Unable to create input file.
     */
    protected File createCountInputFile() throws IOException {
        String inputFileName = "CountTest.txt";
         String[] inputData = new String[] {
                 "1 2 3 4\n",
                 "a b c d\n",
                 "r s t u"};
        return Util.createInputFile("tmp", inputFileName, inputData);
    }

    @Test
    public void testCOUNT_STAR() throws Exception {
        Integer input[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, null };
        long expected = input.length;

        EvalFunc<Long> count = new COUNT_STAR();
        Tuple tup = Util.loadNestTuple(TupleFactory.getInstance().newTuple(1), input);
        Long output = count.exec(tup);

        assertTrue(output == expected);
    }

    @Test
    public void testCOUNT_STARIntermed() throws Exception {
        Integer input[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };

        DataBag intermediateInputBag = bagFactory.newDefaultBag();
        // call initial and then Intermed
        for (Integer i : input) {
            Tuple t = tupleFactory.newTuple(i);
            DataBag b = bagFactory.newDefaultBag();
            b.add(t);
            Tuple initialInput = tupleFactory.newTuple(b);
            EvalFunc<?> initial = new COUNT_STAR.Initial();
            intermediateInputBag.add((Tuple)initial.exec(initialInput));
        }

        EvalFunc<Tuple> countIntermed = new COUNT_STAR.Intermediate();
        Tuple intermediateInput = tupleFactory.newTuple(intermediateInputBag);
        Tuple output = countIntermed.exec(intermediateInput);

        Long f1 = DataType.toLong(output.get(0));
        assertEquals("Expected count to be 10", 10, f1.longValue());
    }

    @Test
    public void testCOUNT_STARFinal() throws Exception {
        long input[] = { 23, 38, 39 };
        Tuple tup = Util.loadNestTuple(TupleFactory.getInstance().newTuple(1), input);

        EvalFunc<Long> count = new COUNT_STAR.Final();
        Long output = count.exec(tup);

        assertEquals("Expected count to be 100", 100, output.longValue());
    }

    @Test
    public void testSUM() throws Exception {
        String[] sumTypes = {"SUM", "DoubleSum", "LongSum", "IntSum", "FloatSum"};
        for(int k = 0; k < sumTypes.length; k++) {
            EvalFunc<?> sum = evalFuncMap.get(sumTypes[k]);
            String inputType = getInputType(sumTypes[k]);
            Tuple tup = inputMap.get(inputType);
            Object output = sum.exec(tup);

            String msg = "[Testing " + sumTypes[k] + " on input type: " + getInputType(sumTypes[k]) + " ( (output) " +
                          output + " == " + getExpected(sumTypes[k]) + " (expected) )]";

            if(inputType == "Integer" || inputType == "Long") {
                assertEquals(msg, (Long)output, (Long)getExpected(sumTypes[k]), 0.00001);
            } else {
                assertEquals(msg, (Double)output, (Double)getExpected(sumTypes[k]), 0.00001);
            }
        }
    }

    @Test
    public void testSUMIntermed() throws Exception {
        String[] sumTypes = {"SUMIntermediate", "DoubleSumIntermediate", "LongSumIntermediate", "IntSumIntermediate", "FloatSumIntermediate"};
        for(int k = 0; k < sumTypes.length; k++) {
            EvalFunc<?> sum = evalFuncMap.get(sumTypes[k]);
            String inputType = getInputType(sumTypes[k]);

            Tuple tup = inputMap.get(inputType);
            Object output = sum.exec(tup);

            String msg = "[Testing " + sumTypes[k] + " on input type: " + getInputType(sumTypes[k]) + " ( (output) " +
                            ((Tuple)output).get(0) + " == " + getExpected(sumTypes[k]) + " (expected) )]";
            if(inputType.equals("Integer") || inputType.equals("Long") || inputType.equals("IntegerAsLong")) {
              assertEquals(msg, (Long) ((Tuple)output).get(0), (Long)getExpected(sumTypes[k]), 0.00001);
            } else {
              assertEquals(msg, (Double) ((Tuple)output).get(0), (Double)getExpected(sumTypes[k]), 0.00001);
            }
        }
    }

    @Test
    public void testSUMFinal() throws Exception {
        String[] sumTypes = {"SUMFinal", "DoubleSumFinal", "LongSumFinal", "IntSumFinal", "FloatSumFinal"};
        for(int k = 0; k < sumTypes.length; k++) {
            EvalFunc<?> sum = evalFuncMap.get(sumTypes[k]);
            String inputType = getInputType(sumTypes[k]);
            Tuple tup = inputMap.get(inputType);
            Object output = sum.exec(tup);

            String msg = "[Testing " + sumTypes[k] + " on input type: " + getInputType(sumTypes[k]) + " ( (output) " +
            output + " == " + getExpected(sumTypes[k]) + " (expected) )]";

            if(inputType.equals("Integer") || inputType.equals("Long") || inputType.equals("IntegerAsLong")) {
              assertEquals(msg, (Long)output, (Long)getExpected(sumTypes[k]), 0.00001);
            } else {
              assertEquals(msg, (Double)output, (Double)getExpected(sumTypes[k]), 0.00001);
            }
        }
    }

    @Test
    public void testMIN() throws Exception {
        String[] minTypes = {"MIN", "LongMin", "IntMin", "FloatMin"};
        for(int k = 0; k < minTypes.length; k++) {
            EvalFunc<?> min = evalFuncMap.get(minTypes[k]);
            String inputType = getInputType(minTypes[k]);
            Tuple tup = inputMap.get(inputType);
            Object output = min.exec(tup);

            String msg = "[Testing " + minTypes[k] + " on input type: " + getInputType(minTypes[k]) + " ( (output) " +
                           output + " == " + getExpected(minTypes[k]) + " (expected) )]";

            if(inputType == "ByteArray") {
              assertEquals(msg, output, getExpected(minTypes[k]));
            } else if(inputType == "Long") {
                assertEquals(msg, output, getExpected(minTypes[k]));
            } else if(inputType == "Integer") {
                assertEquals(msg, output, getExpected(minTypes[k]));
            } else if (inputType == "Double") {
                assertEquals(msg, output, getExpected(minTypes[k]));
            } else if (inputType == "Float") {
                assertEquals(msg, output, getExpected(minTypes[k]));
            } else if (inputType == "String") {
                assertEquals(msg, output, getExpected(minTypes[k]));
            }
        }
    }


    @Test
    public void testMINIntermediate() throws Exception {

        String[] minTypes = {"MINIntermediate", "LongMinIntermediate", "IntMinIntermediate", "FloatMinIntermediate"};
        for(int k = 0; k < minTypes.length; k++) {
            EvalFunc<?> min = evalFuncMap.get(minTypes[k]);
            String inputType = getInputType(minTypes[k]);
            Tuple tup = inputMap.get(inputType);
            Object output = min.exec(tup);

            String msg = "[Testing " + minTypes[k] + " on input type: " + getInputType(minTypes[k]) + " ( (output) " +
                           ((Tuple)output).get(0) + " == " + getExpected(minTypes[k]) + " (expected) )]";

            if(inputType == "ByteArray") {
              assertEquals(msg, ((Tuple)output).get(0), getExpected(minTypes[k]));
            } else if(inputType == "Long") {
                assertEquals(msg, ((Tuple)output).get(0), getExpected(minTypes[k]));
            } else if(inputType == "Integer") {
                assertEquals(msg, ((Tuple)output).get(0), getExpected(minTypes[k]));
            } else if (inputType == "Double") {
                assertEquals(msg, ((Tuple)output).get(0), getExpected(minTypes[k]));
            } else if (inputType == "Float") {
                assertEquals(msg, ((Tuple)output).get(0), getExpected(minTypes[k]));
            } else if (inputType == "String") {
                assertEquals(msg, ((Tuple)output).get(0), getExpected(minTypes[k]));
            }
        }
    }

    @Test
    public void testMINFinal() throws Exception {
        String[] minTypes = {"MINFinal", "LongMinFinal", "IntMinFinal", "FloatMinFinal"};
        for(int k = 0; k < minTypes.length; k++) {
            EvalFunc<?> min = evalFuncMap.get(minTypes[k]);
            String inputType = getInputType(minTypes[k]);
            Tuple tup = inputMap.get(inputType);
            Object output = min.exec(tup);

            String msg = "[Testing " + minTypes[k] + " on input type: " + getInputType(minTypes[k]) + " ( (output) " +
                           output + " == " + getExpected(minTypes[k]) + " (expected) )]";

            if(inputType == "ByteArray") {
              assertEquals(msg, output, getExpected(minTypes[k]));
            } else if(inputType == "Long") {
                assertEquals(msg, output, getExpected(minTypes[k]));
            } else if(inputType == "Integer") {
                assertEquals(msg, output, getExpected(minTypes[k]));
            } else if (inputType == "Double") {
                assertEquals(msg, output, getExpected(minTypes[k]));
            } else if (inputType == "Float") {
                assertEquals(msg, output, getExpected(minTypes[k]));
            } else if (inputType == "String") {
                assertEquals(msg, output, getExpected(minTypes[k]));
            }
        }
    }

    @Test
    public void testMAX() throws Exception {

        String[] maxTypes = {"MAX", "LongMax", "IntMax", "FloatMax"};
        for(int k = 0; k < maxTypes.length; k++) {
            EvalFunc<?> max = evalFuncMap.get(maxTypes[k]);
            String inputType = getInputType(maxTypes[k]);
            Tuple tup = inputMap.get(inputType);
            Object output = max.exec(tup);

            String msg = "[Testing " + maxTypes[k] + " on input type: " + getInputType(maxTypes[k]) + " ( (output) " +
                           output + " == " + getExpected(maxTypes[k]) + " (expected) )]";

            if(inputType == "ByteArray") {
              assertEquals(msg, output, getExpected(maxTypes[k]));
            } else if(inputType == "Long") {
                assertEquals(msg, output, getExpected(maxTypes[k]));
            } else if(inputType == "Integer") {
                assertEquals(msg, output, getExpected(maxTypes[k]));
            } else if (inputType == "Double") {
                assertEquals(msg, output, getExpected(maxTypes[k]));
            } else if (inputType == "Float") {
                assertEquals(msg, output, getExpected(maxTypes[k]));
            } else if (inputType == "String") {
                assertEquals(msg, output, getExpected(maxTypes[k]));
            }
        }
    }


    @Test
    public void testMAXIntermed() throws Exception {

        String[] maxTypes = {"MAXIntermediate", "LongMaxIntermediate", "IntMaxIntermediate", "FloatMaxIntermediate"};
        for(int k = 0; k < maxTypes.length; k++) {
            EvalFunc<?> max = evalFuncMap.get(maxTypes[k]);
            String inputType = getInputType(maxTypes[k]);
            Tuple tup = inputMap.get(inputType);
            Object output = max.exec(tup);

            String msg = "[Testing " + maxTypes[k] + " on input type: " + getInputType(maxTypes[k]) + " ( (output) " +
                           ((Tuple)output).get(0) + " == " + getExpected(maxTypes[k]) + " (expected) )]";

            if(inputType == "ByteArray") {
              assertEquals(msg, ((Tuple)output).get(0), getExpected(maxTypes[k]));
            } else if(inputType == "Long") {
                assertEquals(msg, ((Tuple)output).get(0), getExpected(maxTypes[k]));
            } else if(inputType == "Integer") {
                assertEquals(msg, ((Tuple)output).get(0), getExpected(maxTypes[k]));
            } else if (inputType == "Double") {
                assertEquals(msg, ((Tuple)output).get(0), getExpected(maxTypes[k]));
            } else if (inputType == "Float") {
                assertEquals(msg, ((Tuple)output).get(0), getExpected(maxTypes[k]));
            } else if (inputType == "String") {
                assertEquals(msg, ((Tuple)output).get(0), getExpected(maxTypes[k]));
            }
        }
    }

    @Test
    public void testMAXFinal() throws Exception {

        String[] maxTypes = {"MAXFinal", "LongMaxFinal", "IntMaxFinal", "FloatMaxFinal"};
        for(int k = 0; k < maxTypes.length; k++) {
            EvalFunc<?> max = evalFuncMap.get(maxTypes[k]);
            String inputType = getInputType(maxTypes[k]);
            Tuple tup = inputMap.get(inputType);
            Object output = max.exec(tup);

            String msg = "[Testing " + maxTypes[k] + " on input type: " + getInputType(maxTypes[k]) + " ( (output) " +
                           output + " == " + getExpected(maxTypes[k]) + " (expected) )]";

            if(inputType == "ByteArray") {
              assertEquals(msg, output, getExpected(maxTypes[k]));
            } else if(inputType == "Long") {
                assertEquals(msg, output, getExpected(maxTypes[k]));
            } else if(inputType == "Integer") {
                assertEquals(msg, output, getExpected(maxTypes[k]));
            } else if (inputType == "Double") {
                assertEquals(msg, output, getExpected(maxTypes[k]));
            } else if (inputType == "Float") {
                assertEquals(msg, output, getExpected(maxTypes[k]));
            } else if (inputType == "String") {
                assertEquals(msg, output, getExpected(maxTypes[k]));
            }
        }

    }

    @Test
    public void testMathFuncs() throws Exception {
        Random generator = new Random();
        generator.setSeed(System.currentTimeMillis());
        Double delta = 0.1;
        // We assume that UDFs are stored in org.apache.pig.builtin
        // Change this test case if we add more hierarchy later\
        // Also, we assume that we have a function with math function
        // associated with these UDF with a lowercase name
        String[] mathFuncs = {
                "SIN",
                "SINH",
                "ASIN",
                "COS",
                "COSH",
                "ACOS",
                "TAN",
                "TANH",
                "ATAN",
                "LOG",
                "LOG10",
                "SQRT",
                "CEIL",
                "EXP",
                "FLOOR",
                "CBRT"
        };
        String udfPackage = "org.apache.pig.builtin.";
        //String[] mathNonStdFuncs = {};
        EvalFunc<Double> evalFunc;
        Tuple tup;
        Double input, actual, expected;
        Method mathMethod;
        String msg;
        for(String func: mathFuncs) {
            evalFunc = (EvalFunc<Double>) Class.forName(udfPackage + func).newInstance();
            tup = TupleFactory.getInstance().newTuple(1);
            // double value between 0.0 and 1.0
            input = generator.nextDouble();
            tup.set(0, input);
            mathMethod = Math.class.getDeclaredMethod(func.toLowerCase(), double.class);
            actual = evalFunc.exec(tup);
            expected = (Double)mathMethod.invoke(null, input);
            msg = "[Testing " + func + " on input: " + input + " ( (actual) " + actual + " == " + expected + " (expected) )]";
            assertEquals(msg, actual, expected, delta);
        }
    }

    @Test
    public void testStringFuncs() throws Exception {
        // Since String functions are trivial we add test on per case basis
        String inputStr = "Hello World!";
        String inputStrLower = "hello world!";
        String inputStrUpper = "HELLO WORLD!";
        String inputStrCamel = "hello World!";
        String inputStroWitha = "Hella Warld!";
        String inpuStrExtra = "Hello World!   ";

        List<Object> l = new LinkedList<Object>();
        l.add(inputStr);
        l.add("o");

        String expected = null;
        Tuple input;
        String output;
        Integer intOutput;
        EvalFunc<String> strFunc;
        EvalFunc<Integer> intFunc;

        strFunc = new LCFIRST();
        input = TupleFactory.getInstance().newTuple(inputStr);
        expected = inputStrCamel;
        output = strFunc.exec(input);
        assertTrue(output.equals(expected));

        strFunc = new UCFIRST();
        input = TupleFactory.getInstance().newTuple(inputStrCamel);
        expected = inputStr;
        output = strFunc.exec(input);
        assertTrue(output.equals(expected));

        intFunc = new LAST_INDEX_OF();
        input = TupleFactory.getInstance().newTuple(l);
        intOutput = intFunc.exec(input);
        assertTrue(intOutput.intValue()==7);

        intFunc = new INDEXOF();
        input = TupleFactory.getInstance().newTuple(l);
        intOutput = intFunc.exec(input);
        assertTrue(intOutput.intValue()==4);

        strFunc = new UPPER();
        input = TupleFactory.getInstance().newTuple(inputStr);
        expected = inputStrUpper;
        output = strFunc.exec(input);
        assertTrue(output.equals(expected));

        strFunc = new LOWER();
        input = TupleFactory.getInstance().newTuple(inputStr);
        expected = inputStrLower;
        output = strFunc.exec(input);
        assertTrue(output.equals(expected));

        strFunc = new REPLACE();
        l.clear();
        l.add(inputStr);
        l.add("o");
        l.add("a");
        input = TupleFactory.getInstance().newTuple(l);
        expected = inputStroWitha;
        output = strFunc.exec(input);
        assertTrue(output.equals(expected));

        strFunc = new SUBSTRING();
        l.clear();
        l.add(inputStr);
        l.add(1);
        l.add(5);
        input = TupleFactory.getInstance().newTuple(l);
        expected = "ello";
        output = strFunc.exec(input);
        assertTrue(output.equals(expected));

        strFunc = new TRIM();
        input = TupleFactory.getInstance().newTuple(inpuStrExtra);
        expected = inputStr;
        output = strFunc.exec(input);
        assertTrue(output.equals(expected));

        STRSPLIT splitter = new STRSPLIT();
        Tuple test1 = TupleFactory.getInstance().newTuple(1);
        Tuple test2 = TupleFactory.getInstance().newTuple(2);
        Tuple test3 = TupleFactory.getInstance().newTuple(3);

        test2.set(0, "foo");
        test2.set(1, ":");
        Tuple splits = splitter.exec(test2);
        assertEquals("no matches should return tuple with original string", 1, splits.size());
        assertEquals("no matches should return tuple with original string", "foo",
                splits.get(0));

        // test default delimiter
        test1.set(0, "f ooo bar");
        splits = splitter.exec(test1);
        assertEquals("split on default value ", 3, splits.size());
        assertEquals("f", splits.get(0));
        assertEquals("ooo", splits.get(1));
        assertEquals("bar", splits.get(2));

        // test trimming of whitespace
        test1.set(0, "foo bar  ");
        splits = splitter.exec(test1);
        assertEquals("whitespace trimmed if no length arg", 2, splits.size());

        // test forcing null matches with length param
        test3.set(0, "foo bar   ");
        test3.set(1, "\\s");
        test3.set(2, 10);
        splits = splitter.exec(test3);
        assertEquals("length forces empty string matches on end", 5, splits.size());

        // test limiting results with limit
        test3.set(0, "foo:bar:baz");
        test3.set(1, ":");
        test3.set(2, 2);
        splits = splitter.exec(test3);
        assertEquals(2, splits.size());
        assertEquals("foo", splits.get(0));
        assertEquals("bar:baz", splits.get(1));

        Tuple t1 = TupleFactory.getInstance().newTuple(3);
        t1.set(0, "/search/iy/term1/test");
        t1.set(1, "^\\/search\\/iy\\/(.*?)\\/.*");
        t1.set(2, 1);

        Tuple t2 = TupleFactory.getInstance().newTuple(3);
        t2.set(0, "/search/iy/term1/test");
        t2.set(1, "^\\/search\\/iy\\/(.*?)\\/.*");
        t2.set(2, 2);

        Tuple t3 = TupleFactory.getInstance().newTuple(3);
        t3.set(0, null);
        t3.set(1, "^\\/search\\/iy\\/(.*?)\\/.*");
        t3.set(2, 2);
        
        Tuple t4 = tupleFactory.newTuple(3);
        t4.set(0,"this is a match");
        t4.set(1, "this is a (.+?)");
        t4.set(2, 1);

        REGEX_EXTRACT func = new REGEX_EXTRACT();
        String r = func.exec(t1);
        assertTrue(r.equals("term1"));
        r = func.exec(t2);
        assertTrue(r==null);
        r = func.exec(t3);
        assertTrue(r==null);
        r = func.exec(t4);
        assertEquals("m", r);

        func = new REGEX_EXTRACT(true);
        r = func.exec(t4);
        assertEquals("match", r);

        String matchRegex = "^(.+)\\b\\s+is a\\s+\\b(.+)$";
        TupleFactory tupleFactory = TupleFactory.getInstance();
        Tuple te1 = tupleFactory.newTuple(2);
        te1.set(0,"this is a match");
        te1.set(1, matchRegex);

        Tuple te2 = tupleFactory.newTuple(2);
        te2.set(0, "no match");
        te2.set(1, matchRegex);

        Tuple te3 = tupleFactory.newTuple(2);
        te3.set(0, null);
        te3.set(1, matchRegex);

        REGEX_EXTRACT_ALL funce = new REGEX_EXTRACT_ALL();
        Tuple re = funce.exec(te1);
        assertEquals(re.size(), 2);
        assertEquals("this", re.get(0));
        assertEquals("match", re.get(1));

        re = funce.exec(te2);
        assertTrue(re==null);

        re = funce.exec(te3);
        assertTrue(re==null);

        matchRegex = "(.+?)(.+?)";
        tupleFactory = TupleFactory.getInstance();
        te1 = tupleFactory.newTuple(2);
        te1.set(0,"this is a match");
        te1.set(1, matchRegex);

        funce = new REGEX_EXTRACT_ALL();
        re = funce.exec(te1);
        assertEquals(re.size(), 2);
        assertEquals("t", re.get(0));
        assertEquals("his is a match", re.get(1));

        funce = new REGEX_EXTRACT_ALL(false);
        re = funce.exec(te1);
        assertEquals(re.size(), 2);
        assertEquals("t", re.get(0));
        assertEquals("h", re.get(1));

        re = funce.exec(te2);
        assertTrue(re==null);

        re = funce.exec(te3);
        assertTrue(re==null);
    }

    @Test
    public void testStatsFunc() throws Exception {
        COV cov = new COV("a","b");
        DataBag dBag = DefaultBagFactory.getInstance().newDefaultBag();
        Tuple tup1 = TupleFactory.getInstance().newTuple(1);
        tup1.set(0, 1.0);
        dBag.add(tup1);
        tup1 = TupleFactory.getInstance().newTuple(1);
        tup1.set(0, 4.0);
        dBag.add(tup1);
        tup1 = TupleFactory.getInstance().newTuple(1);
        tup1.set(0, 8.0);
        dBag.add(tup1);
        tup1 = TupleFactory.getInstance().newTuple(1);
        tup1.set(0, 4.0);
        dBag.add(tup1);
        tup1 = TupleFactory.getInstance().newTuple(1);
        tup1.set(0, 7.0);
        dBag.add(tup1);
        tup1 = TupleFactory.getInstance().newTuple(1);
        tup1.set(0, 8.0);
        dBag.add(tup1);
        DataBag dBag1 = DefaultBagFactory.getInstance().newDefaultBag();
        tup1 = TupleFactory.getInstance().newTuple(1);
        tup1.set(0, 2.0);
        dBag1.add(tup1);
        tup1 = TupleFactory.getInstance().newTuple(1);
        tup1.set(0, 2.0);
        dBag1.add(tup1);
        tup1 = TupleFactory.getInstance().newTuple(1);
        tup1.set(0, 3.0);
        dBag1.add(tup1);
        tup1 = TupleFactory.getInstance().newTuple(1);
        tup1.set(0, 3.0);
        dBag1.add(tup1);
        tup1 = TupleFactory.getInstance().newTuple(1);
        tup1.set(0, 2.0);
        dBag1.add(tup1);
        tup1 = TupleFactory.getInstance().newTuple(1);
        tup1.set(0, 4.0);
        dBag1.add(tup1);
        Tuple input = TupleFactory.getInstance().newTuple(2);
        input.set(0, dBag);
        input.set(1, dBag1);
        DataBag output = cov.exec(input);
        Iterator<Tuple> it = output.iterator();
        Tuple ans = it.next();
        assertEquals(ans.get(0),"a");
        assertEquals(ans.get(1),"b");
        assertEquals(1.11111, (Double)ans.get(2),0.0005);

        COR cor = new COR("a","b");
        dBag = DefaultBagFactory.getInstance().newDefaultBag();
        tup1 = TupleFactory.getInstance().newTuple(1);
        tup1.set(0, 1.0);
        dBag.add(tup1);
        tup1 = TupleFactory.getInstance().newTuple(1);
        tup1.set(0, 4.0);
        dBag.add(tup1);
        tup1 = TupleFactory.getInstance().newTuple(1);
        tup1.set(0, 8.0);
        dBag.add(tup1);
        tup1 = TupleFactory.getInstance().newTuple(1);
        tup1.set(0, 4.0);
        dBag.add(tup1);
        tup1 = TupleFactory.getInstance().newTuple(1);
        tup1.set(0, 7.0);
        dBag.add(tup1);
        tup1 = TupleFactory.getInstance().newTuple(1);
        tup1.set(0, 8.0);
        dBag.add(tup1);
        dBag1 = DefaultBagFactory.getInstance().newDefaultBag();
        tup1 = TupleFactory.getInstance().newTuple(1);
        tup1.set(0, 2.0);
        dBag1.add(tup1);
        tup1 = TupleFactory.getInstance().newTuple(1);
        tup1.set(0, 2.0);
        dBag1.add(tup1);
        tup1 = TupleFactory.getInstance().newTuple(1);
        tup1.set(0, 3.0);
        dBag1.add(tup1);
        tup1 = TupleFactory.getInstance().newTuple(1);
        tup1.set(0, 3.0);
        dBag1.add(tup1);
        tup1 = TupleFactory.getInstance().newTuple(1);
        tup1.set(0, 2.0);
        dBag1.add(tup1);
        tup1 = TupleFactory.getInstance().newTuple(1);
        tup1.set(0, 4.0);
        dBag1.add(tup1);
        input = TupleFactory.getInstance().newTuple(2);
        input.set(0, dBag);
        input.set(1, dBag1);
        output = cor.exec(input);
        it = output.iterator();
        ans = it.next();
        assertEquals(ans.get(0),"a");
        assertEquals(ans.get(1),"b");
        assertEquals(0.582222509739582, (Double)ans.get(2) ,0.0005);
    }

    private void checkItemsGT(Iterable<Tuple> tuples, int field, int limit) throws ExecException {
        for (Tuple t : tuples) {
            Long val = (Long) t.get(field);
            assertTrue("Value "+ val + " exceeded the expected limit", val > limit);
        }
    }

    @Test
    public void testToBag() throws Exception {
        //TEST TOBAG
        TOBAG tb = new TOBAG();

        //test output schema of udf
        Schema expectedSch =
            Schema.generateNestedSchema(DataType.BAG, DataType.INTEGER);

        //check schema of TOBAG when given input tuple having only integers
        Schema inputSch = new Schema();
        inputSch.add(new FieldSchema(null, DataType.INTEGER));
        assertEquals("schema of tobag when input has only ints",
                expectedSch, tb.outputSchema(inputSch));

        //add another int column
        inputSch.add(new FieldSchema(null, DataType.INTEGER));
        assertEquals("schema of tobag when input has only ints",
                expectedSch, tb.outputSchema(inputSch));

        //add a long column
        inputSch.add(new FieldSchema(null, DataType.LONG));
        //expect null inner schema
        expectedSch =
            Schema.generateNestedSchema(DataType.BAG, DataType.NULL);
        assertEquals("schema of tobag when input has ints and long",
                expectedSch, tb.outputSchema(inputSch));


        //test schema when input is a tuple with inner schema
        Schema tupInSchema = new Schema(new FieldSchema("x", DataType.CHARARRAY));
        inputSch = new Schema();
        inputSch.add(new FieldSchema("a", tupInSchema, DataType.TUPLE));
        Schema inputSchCp = new Schema(inputSch);
        inputSchCp.getField(0).alias = null;
        expectedSch = new Schema(new FieldSchema(null, inputSchCp, DataType.BAG));
        assertEquals("schema of tobag when input has cols of type tuple ",
                expectedSch, tb.outputSchema(inputSch));

        inputSch.add(new FieldSchema("b", tupInSchema, DataType.TUPLE));
        assertEquals("schema of tobag when input has cols of type tuple ",
                expectedSch, tb.outputSchema(inputSch));

        //add a column of type tuple with different inner schema
        tupInSchema = new Schema(new FieldSchema("x", DataType.BYTEARRAY));
        inputSch.add(new FieldSchema("c", tupInSchema, DataType.TUPLE));
        //expect null inner schema
        expectedSch =
            Schema.generateNestedSchema(DataType.BAG, DataType.NULL);
        assertEquals("schema of tobag when input has cols of type tuple with diff inner schema",
                expectedSch, tb.outputSchema(inputSch));



        Tuple input = TupleFactory.getInstance().newTuple();
        for (int i = 0; i < 100; ++i) {
            input.append(i);
        }
        //test null value in input
        input.append(null);

        Set<Integer> s = new HashSet<Integer>();
        DataBag db = tb.exec(input);
        for (Tuple t : db) {
            s.add((Integer) t.get(0));
        }

        // finally check the bag had everything we put in the tuple.
        assertEquals(101, s.size());
        for (int i = 0; i < 100; ++i) {
            assertTrue(s.contains(i));
        }
        assertTrue("null in tobag result", s.contains(null));
    }
        
    @Test
    public void testTOBAGSupportsTuplesInInput() throws IOException {
        String[][] expected = {
                { "a", "b" },
                { "c", "d" }
        };
        TOBAG function = new TOBAG();
        Tuple input = TupleFactory.getInstance().newTuple(); // input represents a tuple of all the params sent to TOBAG
        Tuple firstItem = TupleFactory.getInstance().newTuple(); // first item of the params is a Tuple
        firstItem.append(expected[0][0]); // containing a and b
        firstItem.append(expected[0][1]);
        Tuple secondItem = TupleFactory.getInstance().newTuple(); // second item of the params is a Tuple
        secondItem.append(expected[1][0]); // containing c and d
        secondItem.append(expected[1][1]);

        input.append(firstItem);
        input.append(secondItem);

        DataBag result = function.exec(input); // run TOBAG on ((a,b),(c,d))

        assertEquals("number of tuples in the bag", 2, result.size()); // we should have 2 tuples in the output bag
        int position = 0;
        for (Tuple t : result) {
            assertEquals("number of items in tuple " + position, 2, t.size()); // each tuple should contain 2 items
            assertEquals("first item in tuple " + position, expected[position][0], t.get(0)); // check the items
            assertEquals("second item in tuple " + position, expected[position][1], t.get(1));

            position++;
        }
    }

    @Test
    public void testMiscFunc() throws Exception {
        TOTUPLE tt = new TOTUPLE();

        Tuple input = TupleFactory.getInstance().newTuple();
        for (int i = 0; i < 100; ++i) {
            input.append(i);
        }

        Tuple output = tt.exec(input);
        assertEquals(input, output);

	// TOMAP - construct a map from input fields
	TOMAP tm = new TOMAP();
	Tuple t = TupleFactory.getInstance().newTuple(6);
	t.set(0, "k1");
	t.set(1, 1);
	t.set(2, "k2");
	t.set(3, 2.0);
	t.set(4, "k3");
	t.set(5, "foo");
	Map m = tm.exec(t);
	assertEquals("", m.get("k1"), 1);
	assertEquals("", m.get("k2"), 2.0);
	assertEquals("", m.get("k3"), "foo");


        TOP top = new TOP();
        TupleFactory tupleFactory = TupleFactory.getInstance();
        BagFactory bagFactory = DefaultBagFactory.getInstance();
        Tuple inputTuple = tupleFactory.newTuple(3);
        DataBag dBag = bagFactory.newDefaultBag();

        // set N = 10 i.e retain top 10 tuples
        inputTuple.set(0, 10);
        // compare tuples by field number 1
        inputTuple.set(1, 1);
        // set the data bag containing the tuples
        inputTuple.set(2, dBag);

        // generate tuples of the form (group-1, 1), (group-2, 2) ...
        for (long i = 0; i < 100; i++) {
            Tuple nestedTuple = tupleFactory.newTuple(2);
            nestedTuple.set(0, "group-" + i);
            nestedTuple.set(1, i);
            dBag.add(nestedTuple);
        }

        DataBag outBag = top.exec(inputTuple);
        assertEquals(outBag.size(), 10L);
        checkItemsGT(outBag, 1, 89);

        // two initial results
        Tuple init1 = (new TOP.Initial()).exec(inputTuple);
        Tuple init2 = (new TOP.Initial()).exec(inputTuple);
        // two intermediate results

        DataBag intermedBag = bagFactory.newDefaultBag();
        intermedBag.add(init1);
        intermedBag.add(init2);
        Tuple intermedInput = tupleFactory.newTuple(intermedBag);
        Tuple intermedOutput1 = (new TOP.Intermed()).exec(intermedInput);
        Tuple intermedOutput2 = (new TOP.Intermed()).exec(intermedInput);
        checkItemsGT((DataBag)intermedOutput1.get(2), 1, 94);

        // final result
        DataBag finalInputBag = bagFactory.newDefaultBag();
        finalInputBag.add(intermedOutput1);
        finalInputBag.add(intermedOutput2);
        Tuple finalInput = tupleFactory.newTuple(finalInputBag);
        outBag = (new TOP.Final()).exec(finalInput);
        assertEquals(outBag.size(), 10L);
        checkItemsGT(outBag, 1, 96);
    }

    @Test
    public void testDistinct() throws Exception {

        Integer[] inp = new Integer[] { 1, 2 , 3, 1 ,4, 5, 3};
        DataBag inputBag = Util.createBagOfOneColumn(inp);
        EvalFunc<Tuple> initial = new Distinct.Initial();
        DataBag intermedInputBg1 = bagFactory.newDefaultBag();
        DataBag intermedInputBg2 = bagFactory.newDefaultBag();
        int i = 0;
        for (Tuple t : inputBag) {
            Tuple initialOutput = initial.exec(tupleFactory.newTuple(t));
            if(i < inp.length/2 ) {
                intermedInputBg1.add(initialOutput);
            } else {
                intermedInputBg2.add(initialOutput);
            }
            i++;
        }

        EvalFunc<Tuple> intermed = new Distinct.Intermediate();

        DataBag finalInputBg = bagFactory.newDefaultBag();
        finalInputBg.add(intermed.exec(tupleFactory.newTuple(intermedInputBg1)));
        finalInputBg.add(intermed.exec(tupleFactory.newTuple(intermedInputBg2)));
        EvalFunc<DataBag> fin = new Distinct.Final();
        DataBag result = fin.exec(tupleFactory.newTuple(finalInputBg));

        Integer[] exp = new Integer[] { 1, 2, 3, 4, 5};
        DataBag expectedBag = Util.createBagOfOneColumn(exp);
        assertEquals(expectedBag, result);

    }

    @Test
    public void testDistinctProgressNonAlgebraic() throws Exception {

        //This test is for the exec method in Distinct which is not
        //called currently.

        int inputSize = 2002;
        Integer[] inp = new Integer[inputSize];
        for(int i = 0; i < inputSize; i+=2) {
            inp[i] = i/2;
            inp[i+1] = i/2;
        }

        DataBag inputBag = Util.createBagOfOneColumn(inp);
        EvalFunc<DataBag> distinct = new Distinct();
        DataBag result = distinct.exec(tupleFactory.newTuple(inputBag));

        Integer[] exp = new Integer[inputSize/2];
        for(int j = 0; j < inputSize/2; ++j) {
            exp[j] = j;
        }

        DataBag expectedBag = Util.createBagOfOneColumn(exp);
        assertEquals(expectedBag, result);

    }

    @Test
    public void testCONCAT() throws Exception {

        // DataByteArray concat
        byte[] a = {1,2,3};
        byte[] b = {4,5,6};
        byte[] expected = {1,2,3,4,5,6};
        DataByteArray dbaExpected = new DataByteArray(expected);

        DataByteArray dbaA = new DataByteArray(a);
        DataByteArray dbaB = new DataByteArray(b);
        EvalFunc<DataByteArray> concat = new CONCAT();

        Tuple t = TupleFactory.getInstance().newTuple(2);
        t.set(0, dbaA);
        t.set(1, dbaB);
        DataByteArray result = concat.exec(t);
        String msg = "[Testing CONCAT on input type: bytearray]";
        assertTrue(msg, result.equals(dbaExpected));

        // String concat
        String s1 = "unit ";
        String s2 = "test";
        String exp = "unit test";
        EvalFunc<String> sConcat = new StringConcat();
        Tuple ts = TupleFactory.getInstance().newTuple(2);
        ts.set(0, s1);
        ts.set(1, s2);
        String res = sConcat.exec(ts);
        msg = "[Testing StringConcat on input type: String]";
        assertTrue(msg, res.equals(exp));

    }

    @Test
    public void testMultiCONCAT() throws Exception {

        // DataByteArray concat
        byte[] a = {1,2,3};
        byte[] b = {4,5,6};
        byte[] c = {7,8,9};
        byte[] d = {10,11,12};
        byte[] e = {13,14,15};
        byte[] expected = {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15};
        DataByteArray dbaExpected = new DataByteArray(expected);

        DataByteArray dbaA = new DataByteArray(a);
        DataByteArray dbaB = new DataByteArray(b);
        DataByteArray dbaC = new DataByteArray(c);
        DataByteArray dbaD = new DataByteArray(d);
        DataByteArray dbaE = new DataByteArray(e);

        EvalFunc<DataByteArray> concat = new CONCAT();

        Tuple t = TupleFactory.getInstance().newTuple(5);
        t.set(0, dbaA);
        t.set(1, dbaB);
        t.set(2, dbaC);
        t.set(3, dbaD);
        t.set(4, dbaE);

        DataByteArray result = concat.exec(t);
        String msg = "[Testing CONCAT on >2 tuples for input type: bytearray]";
        assertTrue(msg, result.equals(dbaExpected));

        // String concat
        String s1 = "high ";
        String s2 = "fives ";
        String s3 = "kick ";
        String s4 = "ass ";
        String s5 = "yo";
        String exp = "high fives kick ass yo";
        EvalFunc<String> sConcat = new StringConcat();
        Tuple ts = TupleFactory.getInstance().newTuple(5);
        ts.set(0, s1);
        ts.set(1, s2);
        ts.set(2, s3);
        ts.set(3, s4);
        ts.set(4, s5);
        String res = sConcat.exec(ts);
        msg = "[Testing StringConcat on >2 tuples input type: String]";
        assertTrue(msg, res.equals(exp));

    }

    @Test
    public void testSIZE() throws Exception {

        // DataByteArray size
        byte[] a = {1,2,3};
        DataByteArray dba = new DataByteArray(a);
        Long expected = new Long(3);
        Tuple t = TupleFactory.getInstance().newTuple(1);
        t.set(0, dba);
        EvalFunc<Long> size = new SIZE();
        String msg = "[Testing SIZE on input type: bytearray]";
        assertTrue(msg, expected.equals(size.exec(t)));

        // String size
        String s = "Unit test case";
        expected = new Long(14);
        t.set(0, s);
        size = new StringSize();
        msg = "[Testing StringSize on input type: String]";
        assertTrue(msg, expected.equals(size.exec(t)));

        // Map size
        String[] mapContents = new String[]{"key1", "value1", "key2", "value2"};
        Map<String, Object> map = Util.createMap(mapContents);
        expected = new Long(2);
        t.set(0, map);
        size = new MapSize();
        msg = "[Testing MapSize on input type: Map]";
        assertTrue(msg, expected.equals(size.exec(t)));

        // Bag size
        Tuple t1 = Util.createTuple(new String[]{"a", "b", "c"});
        Tuple t2 = Util.createTuple(new String[]{"d", "e", "f"});
        Tuple t3 = Util.createTuple(new String[]{"g", "h", "i"});
        Tuple t4 = Util.createTuple(new String[]{"j", "k", "l"});
        DataBag b = Util.createBag(new Tuple[]{t1, t2, t3, t4});
        expected = new Long(4);
        t.set(0, b);
        size = new BagSize();
        msg = "[Testing BagSize on input type: Bag]";
        assertTrue(msg, expected.equals(size.exec(t)));


        // Tuple size
        Tuple suspect = Util.createTuple(new String[]{"key1", "str1"});
        size = new TupleSize();
        msg = "[Testing TupleSize on input type: Tuple]";
        expected = new Long(2);
        Tuple suspectStuffer = TupleFactory.getInstance().newTuple(1);
        suspectStuffer.set(0, suspect);
        assertTrue(msg, expected.equals(size.exec(suspectStuffer)));

        // Tuple size again
        int expectedSize = 4;
        Tuple suspect2 = TupleFactory.getInstance().newTuple(expectedSize);
        suspect2.set(0, a);
        suspect2.set(1, s);
        suspect2.set(2, b);
        suspect2.set(3, suspect);
        expected = new Long(expectedSize);
        size = new TupleSize();
        msg = "[Testing TupleSize on input type: Tuple]";
        suspectStuffer.set(0, suspect2);
        assertTrue(msg, expected.equals(size.exec(suspectStuffer)));


        // Test for ARITY function.
        // It is depricated but we still need to make sure it works
        ARITY arrity = new ARITY();
        expected = new Long(3);
        msg = "[Testing ARRITY on input type: Tuple]";
        assertTrue(msg, expected.equals(new Long(arrity.exec(t1))));
    }


    /* End-to-end testing of the SIZE() builtin function for Tuple
    */
    @Test
    public void testTupleSize() throws Exception {
    	String inputFileName = "TupleSizeIn.txt";
    	String[] inputData = new String[] {
    			"Don't call my name, don't call my name, Alejandro",
    			"I'm not your babe, I'm not your babe, Fernando",
    			"Don't wanna kiss, don't wanna touch",
    			"Just smoke my cigarette and hush",
    			"Don't call my name, don't call my name, Roberto"};
        File inputFile = Util.createInputFile("tmp", inputFileName, inputData);

        // test typed data
        pigServer.registerQuery("A = load '" + inputFile.getAbsolutePath() + "' AS value:chararray;");
        pigServer.registerQuery("B = foreach A generate STRSPLIT(value, ' ') AS values;");
        pigServer.registerQuery("C = foreach B generate values, SIZE(values) as cnt;");

        Iterator<Tuple> it = pigServer.openIterator("C");
        int i=0;
        while (it.hasNext()) {
        	Tuple t = it.next();
        	assertEquals("Testing SIZE(<Tuple>): ", new Long(new StringTokenizer(inputData[i]).countTokens()), t.get(1));
        	i++;
        }
        inputFile.delete();
        assertTrue(!it.hasNext());
        assertTrue(i==inputData.length);
    }


    // Builtin APPLY Functions
    // ========================


    // Builtin LOAD Functions
    // =======================
    @Test
    public void testLFPig() throws Exception {
        Util.createInputFile(cluster, "input.txt", new String[]
            {"this:is:delimited:by:a:colon\n"});
        int arity1 = 6;
        LoadFunc lf = new PigStorage(":");
        LoadFunc p1 = new ReadToEndLoader(lf, ConfigurationUtil.
            toConfiguration(cluster.getProperties()), "input.txt", 0);
        Tuple f1 = p1.getNext();
        assertTrue(f1.size() == arity1);
        Util.deleteFile(cluster, "input.txt");

        int LOOP_COUNT = 100;
        String[] input = new String[LOOP_COUNT * LOOP_COUNT];
        int n = 0;
        for (int i = 0; i < LOOP_COUNT; i++) {
            for (int j = 0; j < LOOP_COUNT; j++) {
                input[n++] = (i + "\t" + i + "\t" + j % 2);
            }
        }
        Util.createInputFile(cluster, "input.txt", input);

        LoadFunc p15 = new ReadToEndLoader(new PigStorage(), ConfigurationUtil.
            toConfiguration(cluster.getProperties()), "input.txt", 0);

        int count = 0;
        while (true) {
            Tuple f15 = p15.getNext();
            if (f15 == null)
                break;
            count++;
            assertEquals(3, f15.size());
        }
        assertEquals(LOOP_COUNT * LOOP_COUNT, count);
        Util.deleteFile(cluster, "input.txt");

        String input2 = ":this:has:a:leading:colon\n";
        int arity2 = 6;
        Util.createInputFile(cluster, "input.txt", new String[] {input2});
        LoadFunc p2 = new ReadToEndLoader(new PigStorage(":"), ConfigurationUtil.
            toConfiguration(cluster.getProperties()), "input.txt", 0);
        Tuple f2 = p2.getNext();
        assertTrue(f2.size() == arity2);
        Util.deleteFile(cluster, "input.txt");

        String input3 = "this:has:a:trailing:colon:\n";
        int arity3 = 6;
        Util.createInputFile(cluster, "input.txt", new String[] {input2});
        LoadFunc p3 = new ReadToEndLoader(new PigStorage(":"), ConfigurationUtil.
            toConfiguration(cluster.getProperties()), "input.txt", 0);
        Tuple f3 = p3.getNext();
        assertTrue(f3.size() == arity3);
        Util.deleteFile(cluster, "input.txt");
    }

    /**
     * test {@link TextLoader} - this also tests that {@link TextLoader} is capable
     * of reading data a couple of dirs deep when the input specified is the top
     * level directory
     */
    @Test
    public void testLFText() throws Exception {
        String input1 = "This is some text.\nWith a newline in it.\n";
        String expected1 = "This is some text.";
        String expected2 = "With a newline in it.";
        Util.createInputFile(cluster,
                "testLFTest-input1.txt",
                new String[] {input1});
        // check that loading the top level dir still reading the file a couple
        // of subdirs below
        LoadFunc text1 = new ReadToEndLoader(new TextLoader(), ConfigurationUtil.
            toConfiguration(cluster.getProperties()), "testLFTest-input1.txt", 0);
        Tuple f1 = text1.getNext();
        Tuple f2 = text1.getNext();
        Util.deleteFile(cluster, "testLFTest-input1.txt");
        assertTrue(expected1.equals(f1.get(0).toString()) &&
            expected2.equals(f2.get(0).toString()));
        Util.createInputFile(cluster, "testLFTest-input2.txt", new String[] {});
        LoadFunc text2 = new ReadToEndLoader(new TextLoader(), ConfigurationUtil.
            toConfiguration(cluster.getProperties()), "testLFTest-input2.txt", 0);
        Tuple f3 = text2.getNext();
        Util.deleteFile(cluster, "testLFTest-input2.txt");
        assertTrue(f3 == null);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSFPig() throws Exception {
        PigServer mrPigServer = new PigServer(ExecType.MAPREDUCE);
        String inputStr = "amy\tbob\tcharlene\tdavid\terin\tfrank";
        Util.createInputFile(cluster, "testSFPig-input.txt", new String[]
                                                                    {inputStr});
        DataByteArray[] input = { new DataByteArray("amy"),
            new DataByteArray("bob"), new DataByteArray("charlene"),
            new DataByteArray("david"), new DataByteArray("erin"),
            new DataByteArray("frank") };
        Tuple f1 = Util.loadTuple(TupleFactory.getInstance().
                newTuple(input.length), input);
        String outputLocation = "testSFPig-output.txt";
        String query = "a = load 'testSFPig-input.txt';" +
        		"store a into '" + outputLocation + "';";
        mrPigServer.setBatchOn();
        Util.registerMultiLineQuery(mrPigServer, query);
        mrPigServer.executeBatch();
        LoadFunc lfunc = new ReadToEndLoader(new PigStorage(), ConfigurationUtil.
            toConfiguration(cluster.getProperties()), outputLocation, 0);
        Tuple f2 = lfunc.getNext();
        Util.deleteFile(cluster, "testSFPig-input.txt");

        Util.deleteFile(cluster, outputLocation);
        assertEquals(f1, f2);
    }

    /* This are e2e tests to make sure that function that maps
     * arguments to class is properly setup. More comprehansive
     * unit tests are done in TestStringUDFs
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testStringUDFs() throws Exception {
        String inputStr = "amy smith ";
        File inputFile = Util.createInputFile("tmp", "testStrUDFsIn.txt", new String[] {inputStr});

        // test typed data
        pigServer.registerQuery("A = load '" + inputFile.getAbsolutePath() + "' as (name: chararray);");
        pigServer.registerQuery("B = foreach A generate SUBSTRING(name, 0, 3), " +
            "INDEXOF(name, 'a'), INDEXOF(name, 'a', 3), LAST_INDEX_OF(name, 'a'), REPLACE(name, 'a', 'b'), " +
            "STRSPLIT(name), STRSPLIT(name, ' '), STRSPLIT(name, ' ', 0), TRIM(name);");

        Iterator<Tuple> it = pigServer.openIterator("B");
        assertTrue(it.hasNext());
        Tuple t = it.next();
        Tuple expected = Util.buildTuple("amy", "smith");
        assertTrue(!it.hasNext());
        assertEquals(9, t.size());
        assertEquals("amy", t.get(0));
        assertEquals(0, t.get(1));
        assertEquals(-1, t.get(2));
        assertEquals(0, t.get(3));
        assertEquals("bmy smith ", t.get(4));
        assertEquals(expected, t.get(5));
        assertEquals(expected, t.get(6));
        assertEquals(expected, t.get(7));
        assertEquals("amy smith", t.get(8));

        // test untyped data
        pigServer.registerQuery("A = load '" + inputFile.getAbsolutePath() + "' as (name);");
        pigServer.registerQuery("B = foreach A generate SUBSTRING(name, 0, 3), " +
            "LAST_INDEX_OF(name, 'a'), REPLACE(name, 'a', 'b'), TRIM(name);");

        it = pigServer.openIterator("B");
        assertTrue(it.hasNext());
        t = it.next();
        assertTrue(!it.hasNext());
        assertEquals(4, t.size());
        assertEquals("amy", t.get(0));
        assertEquals(0, t.get(1));
        assertEquals("bmy smith ", t.get(2));
        assertEquals("amy smith", t.get(3));
    }

    @Test
    public void testTOKENIZE() throws Exception {
        TupleFactory tf = TupleFactory.getInstance();
        Tuple t1 = tf.newTuple(1);
        t1.set(0, "123 456\"789");
        Tuple t2 = tf.newTuple(1);
        t2.set(0, null);
        Tuple t3 = tf.newTuple(0);

        TOKENIZE f = new TOKENIZE();
        DataBag b = f.exec(t1);
        assertTrue(b.size()==3);
        Iterator<Tuple> i = b.iterator();
        Tuple rt = i.next();
        assertTrue(rt.get(0).equals("123"));
        rt = i.next();
        assertTrue(rt.get(0).equals("456"));
        rt = i.next();
        assertTrue(rt.get(0).equals("789"));
        
        // Check when delim specified
        Tuple t4 = tf.newTuple(2);
        t4.set(0, "123|456|78\"9");
        t4.set(1, "|");        
        b = f.exec(t4);
        assertTrue(b.size()==3);
        i = b.iterator();
        rt = i.next();
        assertTrue(rt.get(0).equals("123"));
        rt = i.next();
        assertTrue(rt.get(0).equals("456"));
        rt = i.next();
        assertTrue(rt.get(0).equals("78\"9"));

        b = f.exec(t2);
        assertTrue(b==null);
        
        b = f.exec(t3);
        assertTrue(b==null);
    }

    @Test
    public void testDIFF() throws Exception {
        // Test it in the case with two bags.
        BagFactory bf = BagFactory.getInstance();
        TupleFactory tf = TupleFactory.getInstance();

        DataBag b1 = bf.newDefaultBag();
        DataBag b2 = bf.newDefaultBag();
        for (int i = 0; i < 10; i++) b1.add(tf.newTuple(new Integer(i)));
        for (int i = 0; i < 10; i += 2) b2.add(tf.newTuple(new Integer(i)));
        Tuple t = tf.newTuple(2);
        t.set(0, b1);
        t.set(1, b2);
        DIFF d = new DIFF();
        DataBag result = d.exec(t);

        assertEquals(5, result.size());
        Iterator<Tuple> i = result.iterator();
        int[] values = new int[5];
        for (int j = 0; j < 5; j++) values[j] = (Integer)i.next().get(0);
        Arrays.sort(values);
        for (int j = 1; j < 10; j += 2) assertEquals(j, values[j/2]);

        // Test it in the case of two objects that are equals
        t = tf.newTuple(2);
        t.set(0, new Integer(1));
        t.set(1, new Integer(1));
        result = d.exec(t);
        assertEquals(0, result.size());

        // Test it in the case of two objects that are not equal
        t = tf.newTuple(2);
        t.set(0, new Integer(1));
        t.set(1, new Integer(2));
        result = d.exec(t);
        assertEquals(2, result.size());
    }
    
    //see PIG-2331
    @Test
    public void testURIwithCurlyBrace() throws Exception {
        String inputFileName = "input.txt";
        String inputFileName1 ="part-1";
        String[] inputData = new String[] {
                "1",
                "a",
                "r"};
        Util.createInputFile(cluster, inputFileName, inputData);

         try {
             pigServer.registerQuery("a = load '" + inputFileName + "'AS (s:chararray);");
             pigServer.store("a", inputFileName1, "BinStorage");
             pigServer.registerQuery("b = load 'part-{1,2}' using BinStorage() AS (s:chararray);");
             Iterator<Tuple> it = pigServer.openIterator("b");
             int i=0;
             while(it.hasNext())
             {
            	 assertTrue(it.next().get(0).equals(inputData[i]));
            	 i++;
             }
             assertTrue(i==3);
         }catch(Exception e) {
         }finally {
             Util.deleteFile(cluster, inputFileName);
         }
    }
    private static String getInputType(String typeFor) {
        return allowedInput.get(typeFor);
    }

    /**
     * @param expectedFor functionName for which expected result is sought
     * @return Object appropriate expected result
     */
    private Object getExpected(String expectedFor) {
        return expectedMap.get(expectedFor);
    }

    @Test
    public void testKeySet() throws Exception {
        Map<String, Object> m = new HashMap<String, Object>();
        m.put("open", "apache");
        m.put("1", "hadoop");
        m.put("source", "code");
        Tuple input = TupleFactory.getInstance().newTuple(m);

        KEYSET keySet = new KEYSET();
        DataBag result = keySet.exec(input);
        Iterator<Tuple> i = result.iterator();
        assertEquals(result.size(), m.size());

        while(i.hasNext()) {
            Tuple t = i.next();
            assertTrue(m.containsKey((String)t.get(0)));
        }

    }

    @SuppressWarnings("unchecked")
    @Test
    public void testValueSet() throws Exception {
        Map<String, Object> m = new HashMap<String, Object>();
        m.put("open", "apache");
        m.put("1", "hadoop");
        m.put("source", "apache");
        Tuple input = TupleFactory.getInstance().newTuple(m);

        VALUESET valueSet = new VALUESET();
        DataBag result = valueSet.exec(input);

        //Value set should only contain 2 elements
        assertEquals(result.size(), 2);
        Iterator<Tuple> i = result.iterator();
        List resultList = new ArrayList<String>();
        while(i.hasNext()) {
            resultList.add(i.next().get(0));
        }

        //Value set should only contain "apache" and "hadoop"
        Collections.sort(resultList);
        assertEquals(resultList.get(0), "apache");
        assertEquals(resultList.get(1), "hadoop");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testValueList() throws Exception {
        Map<String, Object> m = new HashMap<String, Object>();
        m.put("open", "apache");
        m.put("1", "hadoop");
        m.put("source", "apache");
        Tuple input = TupleFactory.getInstance().newTuple(m);

        VALUELIST valueList = new VALUELIST();
        DataBag result = valueList.exec(input);
        //Bag must contain all values, not just unique ones
        assertEquals(result.size(), 3);
        Iterator<Tuple> i = result.iterator();
        List resultList = new ArrayList();
        while(i.hasNext()) {
            Tuple t = i.next();
            resultList.add(t.get(0));
        }
        Collections.sort(resultList);
        assertEquals((String)resultList.get(0), "apache");
        assertEquals((String)resultList.get(1), "apache");
        assertEquals((String)resultList.get(2), "hadoop");
    }


    @SuppressWarnings("unchecked")
    @Test
    public void testInverseMap() throws Exception {
        Map<String, Object> m = new HashMap<String, Object>();
        m.put("open", "apache");
        m.put("1", "hadoop");
        m.put("source", "pig");
        m.put("integer", new Integer(100));
        m.put("floating", new Float(100.012));
        m.put("long", new Long(10000000000000l));
        m.put("boolean", true);
        Tuple input = TupleFactory.getInstance().newTuple(m);

        INVERSEMAP inverseMap = new INVERSEMAP();
        Map inverse  = inverseMap.exec(input);
        assertEquals(inverse.size(), 7);
        assertTrue(inverse.containsKey("apache"));
        assertTrue(inverse.containsKey("hadoop"));
        assertTrue(inverse.containsKey("pig"));
        assertTrue(inverse.containsKey("100"));
        assertTrue(inverse.containsKey("true"));
        assertTrue(inverse.containsKey("100.012"));

        //Test when values are non-unique
        m.clear();
        m.put("open", "apache");
        m.put("1", "hadoop");
        m.put("source", "apache");
        input.set(0, m);
        inverse = inverseMap.exec(input);
        assertEquals(inverse.size(), 2);
        assertTrue(inverse.containsKey("apache"));
        assertTrue(inverse.containsKey("hadoop"));

        DataBag bag = (DataBag)inverse.get("apache");
        List resultList = new ArrayList<String>();
        Iterator<Tuple> i = bag.iterator();
        while(i.hasNext()) {
          Tuple t = i.next();
          resultList.add(t.get(0));
        }
        Collections.sort(resultList);
        assertEquals((String)resultList.get(0), "open");
        assertEquals((String)resultList.get(1), "source");
    }

}
