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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import junit.framework.TestCase;

import org.junit.Test;

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
import org.apache.pig.impl.builtin.ShellBagEvalFunc;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.PigContext;

public class TestBuiltin extends TestCase {
    
    private String initString = "mapreduce";
    //private String initString = "local";
    MiniCluster cluster = MiniCluster.buildCluster();

    
    // some inputs
    private static Integer[] IntInput = { 3, 1, 2, 4, 5, 7, null, 6, 8, 9, 10 };
    private static Long[] LongInput = { 145769183483345L, null, 4345639849L, 3435543121L, 2L, 5L, 9L, 7L, 8L, 6L, 10L };
    private static Float[] FloatInput = { 10.4f, 2.35f, 3.099f, null, 4.08495f, 5.350f, 6.78f, 7.0f, 8.0f, 9.0f, 0.09f };
    private static Double[] DoubleInput = { 5.5673910, 121.0, 3.0, 0.000000834593, 1.0, 6.0, 7.0, 8.0, 9.0, 10.0, null };
    private static String[] ba = { "7", "2", "3", null, "4", "5", "6", "1", "8", "9", "10"};
    private static String[] StringInput = {"unit", "test", null, "input", "string"};
    private static DataByteArray[] ByteArrayInput = Util.toDataByteArrays(ba);

    // The HashMaps below are used to set up the appropriate EvalFunc,
    // the allowed input and expected output for the different aggregate functions
    // which have different implementations for different input types
    // This way rather than quickly exploding the test cases (one per input type
    // per aggregate), all cases for a given aggregate stage are handled
    // in one test case in a loop 
    
    private static HashMap<String, EvalFunc<?>> evalFuncMap = new HashMap<String, EvalFunc<?>>();
    private static HashMap<String, Tuple> inputMap = new HashMap<String, Tuple>();
    private static HashMap<String, String> allowedInput = new HashMap<String, String>();
    private static HashMap<String, Object> expectedMap = new HashMap<String, Object>();
    
    @Override
    public void setUp() {
       
        // First set up data structs for "base" SUM, MIN and MAX
        // The allowed input and expected output data structs for 
        // the "Initial" and "Final" stages can be based on the 
        // "base" case
        evalFuncMap.put("SUM", new SUM());
        allowedInput.put("SUM", "ByteArray");
        expectedMap.put("SUM", new Double(55));
        evalFuncMap.put("DoubleSum", new DoubleSum());
        allowedInput.put("DoubleSum", "Double");
        expectedMap.put("DoubleSum", new Double(170.567391834593));
        evalFuncMap.put("IntSum", new IntSum());
        allowedInput.put("IntSum", "Integer");
        expectedMap.put("IntSum", new Long(55));
        evalFuncMap.put("LongSum", new LongSum());
        allowedInput.put("LongSum", "Long");
        expectedMap.put("LongSum", new Long(145776964666362L));
        evalFuncMap.put("FloatSum", new FloatSum());        
        allowedInput.put("FloatSum", "Float");
        expectedMap.put("FloatSum", new Double(56.15395));


        evalFuncMap.put("MIN", new MIN());
        allowedInput.put("MIN", "ByteArray");
        expectedMap.put("MIN", new Double(1));
        evalFuncMap.put("IntMin", new IntMin());
        allowedInput.put("IntMin", "Integer");
        expectedMap.put("IntMin", new Integer(1));
        evalFuncMap.put("LongMin", new LongMin());
        allowedInput.put("LongMin", "Long");
        expectedMap.put("LongMin", new Long(2));
        evalFuncMap.put("FloatMin", new FloatMin());
        allowedInput.put("FloatMin", "Float");
        expectedMap.put("FloatMin", new Float(0.09f));
        evalFuncMap.put("DoubleMin", new DoubleMin());
        allowedInput.put("DoubleMin", "Double");
        expectedMap.put("DoubleMin", new Double(0.000000834593));
        evalFuncMap.put("StringMin", new StringMin());
        allowedInput.put("StringMin", "String");
        expectedMap.put("StringMin", "input");
        
        evalFuncMap.put("MAX", new MAX());
        allowedInput.put("MAX", "ByteArray");
        expectedMap.put("MAX", new Double(10));
        evalFuncMap.put("IntMax", new IntMax());
        allowedInput.put("IntMax", "Integer");
        expectedMap.put("IntMax", new Integer(10));
        evalFuncMap.put("LongMax", new LongMax());
        allowedInput.put("LongMax", "Long");
        expectedMap.put("LongMax", new Long(145769183483345L));
        evalFuncMap.put("FloatMax", new FloatMax());
        allowedInput.put("FloatMax", "Float");
        expectedMap.put("FloatMax", new Float(10.4f));
        evalFuncMap.put("DoubleMax", new DoubleMax());
        allowedInput.put("DoubleMax", "Double");
        expectedMap.put("DoubleMax", new Double(121.0));
        evalFuncMap.put("StringMax", new StringMax());
        allowedInput.put("StringMax", "String");
        expectedMap.put("String", "unit");

        // The idea here is that we can reuse the same input
        // and expected output of the algebraic functions
        // for their Initial and Final Stages 
        String[] stages = {"Initial", "Final"};
        String[] aggs = {"SUM", "DoubleSum", "IntSum", "LongSum", "FloatSum",
                        "MIN", "IntMin", "LongMin", "FloatMin", "StringMin",
                        "MAX", "IntMax", "LongMax", "FloatMax", "StringMax",};
        for (String agg : aggs) {
            for (String stage : stages) {
                // For Int Sum Final and Float Sum Final, the input is expected
                // be Long and Double respectively
                if((agg + stage).equals("IntSumFinal")) {
                    allowedInput.put(agg + stage, allowedInput.get("LongSum"));
                    expectedMap.put(agg + stage, expectedMap.get("LongSum"));
                } else if ((agg + stage).equals("FloatSumFinal")) {
                    allowedInput.put(agg + stage, allowedInput.get("DoubleSum"));
                    expectedMap.put(agg + stage, expectedMap.get("DoubleSum"));
                } else {
                    // In all other cases, the input and expected output
                    // for "Initial" and "Final" stages should match the input
                    // and expected output for the aggregate function itself
                    allowedInput.put(agg + stage, allowedInput.get(agg));
                    expectedMap.put(agg + stage, expectedMap.get(agg));
                }
            }
            
        }
        // Set up the EvalFunc data structure for "Initial" and
        // "Final" stages of SUM, MIN and MAX
        evalFuncMap.put("SUMInitial", new SUM.Initial());
        evalFuncMap.put("DoubleSumInitial", new DoubleSum.Initial());
        evalFuncMap.put("IntSumInitial", new IntSum.Initial());
        evalFuncMap.put("LongSumInitial", new LongSum.Initial());
        evalFuncMap.put("FloatSumInitial", new FloatSum.Initial());
        
        evalFuncMap.put("SUMFinal", new SUM.Final());
        evalFuncMap.put("DoubleSumFinal", new DoubleSum.Final());
        evalFuncMap.put("IntSumFinal", new IntSum.Final());
        evalFuncMap.put("LongSumFinal", new LongSum.Final());
        evalFuncMap.put("FloatSumFinal", new FloatSum.Final());

        evalFuncMap.put("MINInitial", new MIN.Initial());
        evalFuncMap.put("IntMinInitial", new IntMin.Initial());
        evalFuncMap.put("LongMinInitial", new LongMin.Initial());
        evalFuncMap.put("FloatMinInitial", new FloatMin.Initial());
        evalFuncMap.put("DoubleMinInitial", new DoubleMin.Initial());
        evalFuncMap.put("StringMinInitial", new StringMin.Initial());
        
        evalFuncMap.put("MINFinal", new MIN.Final());
        evalFuncMap.put("IntMinFinal", new IntMin.Final());
        evalFuncMap.put("LongMinFinal", new LongMin.Final());
        evalFuncMap.put("FloatMinFinal", new FloatMin.Final());
        evalFuncMap.put("DoubleMinFinal", new DoubleMin.Final());
        evalFuncMap.put("StringMinFinal", new StringMin.Final());


        evalFuncMap.put("MAXInitial", new MAX.Initial());
        evalFuncMap.put("IntMaxInitial", new IntMax.Initial());
        evalFuncMap.put("LongMaxInitial", new LongMax.Initial());
        evalFuncMap.put("FloatMaxInitial", new FloatMax.Initial());
        evalFuncMap.put("DoubleMaxInitial", new DoubleMax.Initial());
        evalFuncMap.put("StringMaxInitial", new StringMax.Initial());
        
        evalFuncMap.put("MAXFinal", new MAX.Final());
        evalFuncMap.put("IntMaxFinal", new IntMax.Final());
        evalFuncMap.put("LongMaxFinal", new LongMax.Final());
        evalFuncMap.put("FloatMaxFinal", new FloatMax.Final());
        evalFuncMap.put("DoubleMaxFinal", new DoubleMax.Final());
        evalFuncMap.put("StringMaxFinal", new StringMax.Final());

        // For Avg, the expected output (for the sum part) for Initial are the 
        // same as SUM - so handled a little differently accordingly
        evalFuncMap.put("AVG", new AVG());
        allowedInput.put("AVG", "ByteArray");
        expectedMap.put("AVG", new Double(5.0));
        evalFuncMap.put("DoubleAvg", new DoubleAvg());
        allowedInput.put("DoubleAvg", "Double");
        expectedMap.put("DoubleAvg", new Double(15.506126530417545));
        evalFuncMap.put("LongAvg", new LongAvg());
        allowedInput.put("LongAvg", "Long");
        expectedMap.put("LongAvg", new Double(1.3252451333305637E13));
        evalFuncMap.put("IntAvg", new IntAvg());
        allowedInput.put("IntAvg", "Integer");
        expectedMap.put("IntAvg", new Double(5.0));
        evalFuncMap.put("FloatAvg", new FloatAvg());
        allowedInput.put("FloatAvg", "Float");
        expectedMap.put("FloatAvg", new Double(5.104904507723722));

        String[] avgs = {"AVG", "LongAvg", "DoubleAvg", "IntAvg", "FloatAvg"};
        for (String avg : avgs) {
            for (String stage : stages) {
                allowedInput.put(avg + stage, allowedInput.get(avg));
                if(stage.equals("Final"))
                    expectedMap.put(avg + stage, expectedMap.get(avg));
            }
            
        }

        
        evalFuncMap.put("AVGInitial", new AVG.Initial());
        expectedMap.put("AVGInitial", expectedMap.get("SUM"));
        evalFuncMap.put("DoubleAvgInitial", new DoubleAvg.Initial());
        expectedMap.put("DoubleAvgInitial", expectedMap.get("DoubleSum"));
        evalFuncMap.put("LongAvgInitial", new LongAvg.Initial());
        expectedMap.put("LongAvgInitial", expectedMap.get("LongSum"));
        evalFuncMap.put("IntAvgInitial", new IntAvg.Initial());
        expectedMap.put("IntAvgInitial", expectedMap.get("IntSum"));
        evalFuncMap.put("FloatAvgInitial", new FloatAvg.Initial());
        expectedMap.put("FloatAvgInitial", expectedMap.get("FloatSum"));
        
        evalFuncMap.put("AVGFinal", new AVG.Final());
        evalFuncMap.put("DoubleAvgFinal", new DoubleAvg.Final());
        evalFuncMap.put("LongAvgFinal", new LongAvg.Final());
        evalFuncMap.put("IntAvgFinal", new IntAvg.Final());
        evalFuncMap.put("FloatAvgFinal", new FloatAvg.Final());

        
        // set up input hash
        try{
            inputMap.put("Integer", Util.loadNestTuple(TupleFactory.getInstance().newTuple(1), IntInput));
            inputMap.put("Long", Util.loadNestTuple(TupleFactory.getInstance().newTuple(1), LongInput));
            inputMap.put("Float", Util.loadNestTuple(TupleFactory.getInstance().newTuple(1), FloatInput));
            inputMap.put("Double", Util.loadNestTuple(TupleFactory.getInstance().newTuple(1), DoubleInput));
            inputMap.put("ByteArray", Util.loadNestTuple(TupleFactory.getInstance().newTuple(1), ByteArrayInput));
            inputMap.put("String", Util.loadNestTuple(TupleFactory.getInstance().newTuple(1), StringInput));
            
        }catch(ExecException e) {
            e.printStackTrace();
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
    public void testAVGInitial() throws Exception {
        String[] avgTypes = {"AVGInitial", "DoubleAvgInitial", "LongAvgInitial", "IntAvgInitial", "FloatAvgInitial"};
        for(int k = 0; k < avgTypes.length; k++) {
            EvalFunc<?> avg = evalFuncMap.get(avgTypes[k]);
            String inputType = getInputType(avgTypes[k]);
            Tuple tup = inputMap.get(inputType);
            Object output = avg.exec(tup);
            
            if(inputType == "Long" || inputType == "Integer") {
                Long l = (Long)((Tuple)output).get(0);
                String msg = "[Testing " + avgTypes[k] + " on input type: " + getInputType(avgTypes[k]) + " ( (output) " +
                              l + " == " + getExpected(avgTypes[k]) + " (expected) )]";
                assertEquals(msg, l, (Long)getExpected(avgTypes[k]));
            } else {
                Double f1 = (Double)((Tuple)output).get(0);
                String msg = "[Testing " + avgTypes[k] + " on input type: " + getInputType(avgTypes[k]) + " ( (output) " +
                               f1 + " == " + getExpected(avgTypes[k]) + " (expected) )]";
                assertEquals(msg, f1, (Double)getExpected(avgTypes[k]), 0.00001);
            }
            Long f2 = (Long)((Tuple)output).get(1);
            assertEquals("[Testing " + avgTypes[k] + " on input type: "+ 
                inputType+"]Expected count to be 11", 11, f2.longValue());
        }
    }

    @Test
    public void testAVGFinal() throws Exception {
        String[] avgTypes = {"AVGFinal", "DoubleAvgFinal", "LongAvgFinal", "IntAvgFinal", "FloatAvgFinal"};
        String[] avgInitialTypes = {"AVGInitial", "DoubleAvgInitial", "LongAvgInitial", "IntAvgInitial", "FloatAvgInitial"};
        for(int k = 0; k < avgTypes.length; k++) {
            EvalFunc<?> avg = evalFuncMap.get(avgTypes[k]);
            Tuple tup = inputMap.get(getInputType(avgTypes[k]));
            
            // To test AVGFinal, AVGInitial should first be called and
            // the output of AVGInitial should be supplied as input to
            // AVGFinal. To simulate this, we will call Initial twice
            // on the above tuple and collect the outputs and pass it to
            // Final.
            
            // get the right "Initial" EvalFunc
            EvalFunc<?> avgInitial = evalFuncMap.get(avgInitialTypes[k]);
            Object output1 = avgInitial.exec(tup);
            Object output2 = avgInitial.exec(tup);
            
            DataBag bag = Util.createBag(new Tuple[]{(Tuple)output1, (Tuple)output2});
            
            Tuple finalTuple = TupleFactory.getInstance().newTuple(1);
            finalTuple.set(0, bag);
            Object output = avg.exec(finalTuple);
            String msg = "[Testing " + avgTypes[k] + " on input type: " + getInputType(avgTypes[k]) + " ( (output) " +
            output + " == " + getExpected(avgTypes[k]) + " (expected) )]";
            assertEquals(msg, (Double)output, (Double)getExpected(avgTypes[k]), 0.00001);
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
    public void testCOUNTMap() throws Exception {
        Map<Object, Object> map = new HashMap<Object, Object>();
        
        Tuple tup = TupleFactory.getInstance().newTuple();
        tup.append(map);
        
        EvalFunc<Long> count = new COUNT();
        FilterFunc isEmpty = new IsEmpty();
        
        assertTrue(isEmpty.exec(tup));
        Long output = count.exec(tup);
        assertTrue(output == 0);
        
        map.put("a", "a");

        assertFalse(isEmpty.exec(tup));
        output = count.exec(tup);
        assertTrue(output == 1);

        
        map.put("b", TupleFactory.getInstance().newTuple());

        assertFalse(isEmpty.exec(tup));
        output = count.exec(tup);
        assertTrue(output == 2);
        
    }

    @Test
    public void testCOUNTInitial() throws Exception {
        int input[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };

        EvalFunc<Tuple> count = new COUNT.Initial();
        Tuple tup = Util.loadNestTuple(TupleFactory.getInstance().newTuple(1), input);
        Tuple output = count.exec(tup);

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
    public void testSUMInitial() throws Exception {
        String[] sumTypes = {"SUMInitial", "DoubleSumInitial", "LongSumInitial", "IntSumInitial", "FloatSumInitial"};
        for(int k = 0; k < sumTypes.length; k++) {
            EvalFunc<?> sum = evalFuncMap.get(sumTypes[k]);
            String inputType = getInputType(sumTypes[k]);
            
            Tuple tup = inputMap.get(inputType);
            Object output = sum.exec(tup);
            
            String msg = "[Testing " + sumTypes[k] + " on input type: " + getInputType(sumTypes[k]) + " ( (output) " +
                            ((Tuple)output).get(0) + " == " + getExpected(sumTypes[k]) + " (expected) )]";

            if(inputType == "Integer" || inputType == "Long") {
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

            if(inputType == "Integer" || inputType == "Long") {
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
    public void testMINInitial() throws Exception {
        
        String[] minTypes = {"MINInitial", "LongMinInitial", "IntMinInitial", "FloatMinInitial"};
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
    public void testMAXInitial() throws Exception {
        
        String[] maxTypes = {"MAXInitial", "LongMaxInitial", "IntMaxInitial", "FloatMaxInitial"};
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
    
    /*@Test
    public void testShellFuncSingle() throws Throwable {
        //ShellBagEvalFunc func = new ShellBagEvalFunc("tr o 0");
        PigServer pig = new PigServer(initString);
        
        File tempFile = File.createTempFile("tmp", ".dat");
        PrintWriter writer = new PrintWriter(tempFile);
        writer.println("foo");
        writer.println("boo");
        writer.close();
        
        pig.registerFunction("myTr", new FuncSpec(ShellBagEvalFunc.class.getName() + "('tr o 0')"));
        pig.registerQuery("a = load 'file:" + tempFile + "';");
        pig.registerQuery("b = foreach a generate myTr(*);");
        Iterator<Tuple> iter = pig.openIterator("b");
                
        Tuple t;
        
        assertTrue(iter.hasNext());
        t = iter.next();
        assertEquals("{(f00)}", t.get(0).toString());
        assertTrue(iter.hasNext());
        t = iter.next();
        assertEquals("{(b00)}", t.get(0).toString());
        assertFalse(iter.hasNext());
        tempFile.delete();
    }
    
    @Test
    public void testShellFuncMultiple() throws Throwable {

        PigServer pig = new PigServer(initString);
        final int numTimes = 100;
        
        File tempFile = File.createTempFile("tmp", ".dat");
        PrintWriter writer = new PrintWriter(tempFile);
        for (int i=0; i< numTimes; i++){
            writer.println(i+"oo");
        }
        writer.close();
        
        pig.registerFunction("tr1",
            new FuncSpec(ShellBagEvalFunc.class.getName() + "('tr o A')"));
        pig.registerFunction("tr2",
            new FuncSpec(ShellBagEvalFunc.class.getName() + "('tr o B')"));
        pig.registerQuery("a = load 'file:" + tempFile + "';");
        pig.registerQuery("b = foreach a generate tr1(*),tr2(*);");
        Iterator<Tuple> iter = pig.openIterator("b");
        
        for (int i=0; i< numTimes; i++){
            Tuple t = iter.next();
            DataBag b = DataType.toBag(t.get(0));
            Tuple t1 = b.iterator().next();
            assertEquals(i+"AA", t1.get(0).toString());
            b = DataType.toBag(t.get(1));
            t1 = b.iterator().next();
            assertEquals(i+"BB", t1.get(0).toString());
        }
        
        assertFalse(iter.hasNext());
        tempFile.delete();
    }*/
           
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
