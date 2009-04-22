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

import java.io.File;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import junit.framework.TestCase;

import org.junit.Test;

import org.apache.pig.Algebraic;
import org.apache.pig.FilterFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigServer;
import org.apache.pig.ExecType;
import org.apache.pig.EvalFunc;
import org.apache.pig.StoreFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.builtin.*;
import org.apache.pig.data.*;
import org.apache.pig.data.DefaultAbstractBag.BagDelimiterTuple;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.PigContext;

public class TestBuiltin extends TestCase {
    
    private String initString = "mapreduce";
    //private String initString = "local";
    MiniCluster cluster = MiniCluster.buildCluster();

    TupleFactory tupleFactory = DefaultTupleFactory.getInstance();
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
    
    @Override
    public void setUp() {
       
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

        expectedMap.put("AVG", new Double(5.0));
        expectedMap.put("DoubleAvg", new Double(15.506126530417545));
        expectedMap.put("LongAvg", new Double(1.3252451333305637E13));
        expectedMap.put("IntAvg", new Double(5.0));
        expectedMap.put("FloatAvg", new Double(5.104904507723722));
        
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
        
        expectedMap.put("COUNT", new Long(11));

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
                assertEquals(msg, (Long)getExpected(avgTypes[k]), l);
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
        int input[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        long expected = input.length;

        EvalFunc<Long> count = new COUNT();
        Tuple tup = Util.loadNestTuple(TupleFactory.getInstance().newTuple(1), input);
        Long output = count.exec(tup);

        assertTrue(output == expected);
    }

    @Test
    public void testCOUNTIntermed() throws Exception {
        int input[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        
        DataBag intermediateInputBag = bagFactory.newDefaultBag();
        // call initial and then Intermed
        for (int i : input) {
            Tuple t = tupleFactory.newTuple(new Integer(i));
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
              assertEquals(msg, (Double)output, (Double)getExpected(minTypes[k]));
            } else if(inputType == "Long") {
                assertEquals(msg, (Long)output, (Long)getExpected(minTypes[k]));
            } else if(inputType == "Integer") {
                assertEquals(msg, (Integer)output, (Integer)getExpected(minTypes[k]));
            } else if (inputType == "Double") {
                assertEquals(msg, (Double)output, (Double)getExpected(minTypes[k]));
            } else if (inputType == "Float") {
                assertEquals(msg, (Float)output, (Float)getExpected(minTypes[k]));
            } else if (inputType == "String") {
                assertEquals(msg, (String)output, (String)getExpected(minTypes[k]));
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
              assertEquals(msg, (Double)((Tuple)output).get(0), (Double)getExpected(minTypes[k]));
            } else if(inputType == "Long") {
                assertEquals(msg, (Long)((Tuple)output).get(0), (Long)getExpected(minTypes[k]));
            } else if(inputType == "Integer") {
                assertEquals(msg, (Integer)((Tuple)output).get(0), (Integer)getExpected(minTypes[k]));
            } else if (inputType == "Double") {
                assertEquals(msg, (Double)((Tuple)output).get(0), (Double)getExpected(minTypes[k]));
            } else if (inputType == "Float") {
                assertEquals(msg, (Float)((Tuple)output).get(0), (Float)getExpected(minTypes[k]));
            } else if (inputType == "String") {
                assertEquals(msg, (String)((Tuple)output).get(0), (String)getExpected(minTypes[k]));
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
              assertEquals(msg, (Double)output, (Double)getExpected(minTypes[k]));
            } else if(inputType == "Long") {
                assertEquals(msg, (Long)output, (Long)getExpected(minTypes[k]));
            } else if(inputType == "Integer") {
                assertEquals(msg, (Integer)output, (Integer)getExpected(minTypes[k]));
            } else if (inputType == "Double") {
                assertEquals(msg, (Double)output, (Double)getExpected(minTypes[k]));
            } else if (inputType == "Float") {
                assertEquals(msg, (Float)output, (Float)getExpected(minTypes[k]));
            } else if (inputType == "String") {
                assertEquals(msg, (String)output, (String)getExpected(minTypes[k]));
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
              assertEquals(msg, (Double)output, (Double)getExpected(maxTypes[k]));
            } else if(inputType == "Long") {
                assertEquals(msg, (Long)output, (Long)getExpected(maxTypes[k]));
            } else if(inputType == "Integer") {
                assertEquals(msg, (Integer)output, (Integer)getExpected(maxTypes[k]));
            } else if (inputType == "Double") {
                assertEquals(msg, (Double)output, (Double)getExpected(maxTypes[k]));
            } else if (inputType == "Float") {
                assertEquals(msg, (Float)output, (Float)getExpected(maxTypes[k]));
            } else if (inputType == "String") {
                assertEquals(msg, (String)output, (String)getExpected(maxTypes[k]));
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
              assertEquals(msg, (Double)((Tuple)output).get(0), (Double)getExpected(maxTypes[k]));
            } else if(inputType == "Long") {
                assertEquals(msg, (Long)((Tuple)output).get(0), (Long)getExpected(maxTypes[k]));
            } else if(inputType == "Integer") {
                assertEquals(msg, (Integer)((Tuple)output).get(0), (Integer)getExpected(maxTypes[k]));
            } else if (inputType == "Double") {
                assertEquals(msg, (Double)((Tuple)output).get(0), (Double)getExpected(maxTypes[k]));
            } else if (inputType == "Float") {
                assertEquals(msg, (Float)((Tuple)output).get(0), (Float)getExpected(maxTypes[k]));
            } else if (inputType == "String") {
                assertEquals(msg, (String)((Tuple)output).get(0), (String)getExpected(maxTypes[k]));
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
              assertEquals(msg, (Double)output, (Double)getExpected(maxTypes[k]));
            } else if(inputType == "Long") {
                assertEquals(msg, (Long)output, (Long)getExpected(maxTypes[k]));
            } else if(inputType == "Integer") {
                assertEquals(msg, (Integer)output, (Integer)getExpected(maxTypes[k]));
            } else if (inputType == "Double") {
                assertEquals(msg, (Double)output, (Double)getExpected(maxTypes[k]));
            } else if (inputType == "Float") {
                assertEquals(msg, (Float)output, (Float)getExpected(maxTypes[k]));
            } else if (inputType == "String") {
                assertEquals(msg, (String)output, (String)getExpected(maxTypes[k]));
            }
        }

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
        Map<Object, Object> map = Util.createMap(mapContents);
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
        expected = new Long(3);
        size = new TupleSize();
        msg = "[Testing TupleSize on input type: Tuple]";
        assertTrue(msg, expected.equals(size.exec(t1)));
        
        // Test for ARITY function.
        // It is depricated but we still need to make sure it works
        ARITY arrity = new ARITY();
        msg = "[Testing ARRITY on input type: Tuple]";
        assertTrue(msg, expected.equals(new Long(arrity.exec(t1))));
    }

    // Builtin APPLY Functions
    // ========================

    


    // Builtin LOAD Functions
    // =======================
    @Test
    public void testLFPig() throws Exception {
        String input1 = "this:is:delimited:by:a:colon\n";
        int arity1 = 6;

        LoadFunc p1 = new PigStorage(":");
        FakeFSInputStream ffis1 = new FakeFSInputStream(input1.getBytes());
        p1.bindTo(null, new BufferedPositionedInputStream(ffis1), 0, input1.getBytes().length);
        Tuple f1 = p1.getNext();
        assertTrue(f1.size() == arity1);

        LoadFunc p15 = new PigStorage();
        StringBuilder sb = new StringBuilder();
        int LOOP_COUNT = 100;
        for (int i = 0; i < LOOP_COUNT; i++) {
            for (int j = 0; j < LOOP_COUNT; j++) {
                sb.append(i + "\t" + i + "\t" + j % 2 + "\n");
            }
        }
        byte bytes[] = sb.toString().getBytes();
        FakeFSInputStream ffis15 = new FakeFSInputStream(bytes);
        p15.bindTo(null, new BufferedPositionedInputStream(ffis15), 0, bytes.length);
        int count = 0;
        while (true) {
            Tuple f15 = p15.getNext();
            if (f15 == null)
                break;
            count++;
            assertEquals(3, f15.size());
        }
        assertEquals(LOOP_COUNT * LOOP_COUNT, count);

        String input2 = ":this:has:a:leading:colon\n";
        int arity2 = 6;

        LoadFunc p2 = new PigStorage(":");
        FakeFSInputStream ffis2 = new FakeFSInputStream(input2.getBytes());
        p2.bindTo(null, new BufferedPositionedInputStream(ffis2), 0, input2.getBytes().length);
        Tuple f2 = p2.getNext();
        assertTrue(f2.size() == arity2);

        String input3 = "this:has:a:trailing:colon:\n";
        int arity3 = 6;

        LoadFunc p3 = new PigStorage(":");
        FakeFSInputStream ffis3 = new FakeFSInputStream(input3.getBytes());
        p3.bindTo(null, new BufferedPositionedInputStream(ffis3), 0, input1.getBytes().length);
        Tuple f3 = p3.getNext();
        assertTrue(f3.size() == arity3);
    }

    /*
    @Test
    public void testLFBin() throws Exception {

        BagFactory.init(new File("/tmp"));
        
        
        Tuple t1 = new Tuple(4);
        DataAtom a = new DataAtom("a");
        DataAtom b = new DataAtom("b");
        Tuple t2 = new Tuple(1);
        t2.setField(0,a);
        Tuple t3 = new Tuple(1);
        t3.setField(0, b);
        DataBag bag = BagFactory.getInstance().getNewBigBag();
        bag.add(t2);
        bag.add(t3);
        Tuple t4 = new Tuple(2);
        t4.setField(0, t2);
        t4.setField(1, t3);
        
        t1.setField(0, a);
        t1.setField(1, t2);
        t1.setField(2, bag);
        t1.setField(3, t4);
        
        Tuple t5 = new Tuple(4);
        DataAtom c = new DataAtom("the quick brown fox");
        DataAtom d = new DataAtom("jumps over the lazy dog");
        Tuple t6 = new Tuple(1);
        t6.setField(0,c);
        Tuple t7 = new Tuple(1);
        t7.setField(0, d);
        DataBag bag2 = BagFactory.getInstance().getNewBigBag();    
        for(int i = 0; i < 10; i ++) {
            bag2.add(t6);
            bag2.add(t7);
        }
        Tuple t8 = new Tuple(2);
        t8.setField(0, t6);
        t8.setField(1, t7);
        
        t5.setField(0, c);
        t5.setField(1, t6);
        t5.setField(2, bag2);
        t5.setField(3, t8);
        
        
        OutputStream os = new FileOutputStream("/tmp/bintest.bin");
        StoreFunc s = new BinStorage();
        s.bindTo(os);
        s.putNext(t1);
        s.putNext(t5);
        s.finish();
        
        LoadFunc l = new BinStorage();
        InputStream is = FileLocalizer.open("/tmp/bintest.bin", new PigContext(ExecType.LOCAL));
        l.bindTo("/tmp/bintest.bin", new BufferedPositionedInputStream(is), 0, Long.MAX_VALUE);
        Tuple r1 = l.getNext();
        Tuple r2 = l.getNext();
        
        assertTrue(r1.equals(t1));
        assertTrue(r2.equals(t5));
    }
    */

    
    @Test
    public void testLFText() throws Exception {
        String input1 = "This is some text.\nWith a newline in it.\n";
        String expected1 = "This is some text.";
        String expected2 = "With a newline in it.";
        FakeFSInputStream ffis1 = new FakeFSInputStream(input1.getBytes());
        LoadFunc text1 = new TextLoader();
        text1.bindTo(null, new BufferedPositionedInputStream(ffis1), 0, input1.getBytes().length);
        Tuple f1 = text1.getNext();
        Tuple f2 = text1.getNext();
        assertTrue(expected1.equals(f1.get(0).toString()) &&
            expected2.equals(f2.get(0).toString()));

        String input2 = "";
        FakeFSInputStream ffis2 = new FakeFSInputStream(input2.getBytes());
        LoadFunc text2 = new TextLoader();
        text2.bindTo(null, new BufferedPositionedInputStream(ffis2), 0, input2.getBytes().length);
        Tuple f3 = text2.getNext();
        assertTrue(f3 == null);
    }

    @Test
    public void testSFPig() throws Exception {
        byte[] buf = new byte[1024];
        FakeFSOutputStream os = new FakeFSOutputStream(buf);
        StoreFunc sfunc = new PigStorage("\t");
        sfunc.bindTo(os);

        DataByteArray[] input = { new DataByteArray("amy"),
            new DataByteArray("bob"), new DataByteArray("charlene"),
            new DataByteArray("david"), new DataByteArray("erin"),
            new DataByteArray("frank") };
        Tuple f1 = Util.loadTuple(TupleFactory.getInstance().newTuple(input.length), input);

        sfunc.putNext(f1);
        sfunc.finish();
        
        FakeFSInputStream is = new FakeFSInputStream(buf);
        LoadFunc lfunc = new PigStorage();
        lfunc.bindTo(null, new BufferedPositionedInputStream(is), 0, buf.length);
        Tuple f2 = lfunc.getNext();
        
        assertTrue(f1.equals(f2));        
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



}
