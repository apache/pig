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

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestStreamingLocal {

    private TupleFactory tf = TupleFactory.getInstance();
    PigServer pigServer;

    private static final String simpleEchoStreamingCommand;
    static {
        String quote = "'";
        if (Util.WINDOWS) {
           quote= "\"";
        }

        simpleEchoStreamingCommand = "perl -ne "+quote+"print $_"+quote;
    }

    @Before
    public void setUp() throws Exception {
        pigServer = new PigServer(ExecType.LOCAL);
    }

    @After
    public void tearDown() throws Exception {
        pigServer.shutdown();
    }

    private Tuple[] setupExpectedResults(Object[] firstField,
            Object[] secondField) throws ExecException {
        Assert.assertEquals(firstField.length, secondField.length);

        Tuple[] expectedResults = new Tuple[firstField.length];
        for (int i=0; i < expectedResults.length; ++i) {
            expectedResults[i] = tf.newTuple(2);
            expectedResults[i].set(0, firstField[i]);
            expectedResults[i].set(1, secondField[i]);
        }

        return expectedResults;
    }

    @Test
    public void testSimpleMapSideStreaming()
    throws Exception {
        File input = Util.createInputFile("tmp", "",
                new String[] {"A,1", "B,2", "C,3", "D,2",
                "A,5", "B,5", "C,8", "A,8",
                "D,8", "A,9"});

        // Expected results
        String[] expectedFirstFields = new String[] {"A", "B", "C", "A", "D", "A"};
        Integer[] expectedSecondFields = new Integer[] {5, 5, 8, 8, 8, 9};
        boolean[] withTypes = {true, false};
        for (int i = 0; i < withTypes.length; i++) {
            Tuple[] expectedResults = null;
            if(withTypes[i] == true) {
                expectedResults =
                    setupExpectedResults(expectedFirstFields, expectedSecondFields);
            } else {
                expectedResults = setupExpectedResults(Util
                        .toDataByteArrays(expectedFirstFields), Util
                        .toDataByteArrays(expectedSecondFields));
            }

            // Pig query to run
            pigServer.registerQuery("IP = load '" +
                    Util.generateURI(Util.encodeEscape(input.toString()), pigServer.getPigContext()) +
                    "' using " + PigStorage.class.getName() + "(',');");
            pigServer.registerQuery("FILTERED_DATA = filter IP by $1 > '3';");
            pigServer.registerQuery("S1 = stream FILTERED_DATA through `" +
                    simpleEchoStreamingCommand + "`;");
            if(withTypes[i] == true) {
                pigServer.registerQuery("OP = stream S1 through `" +
                        simpleEchoStreamingCommand + "` as (f0:chararray, f1:int);");
            } else {
                pigServer.registerQuery("OP = stream S1 through `" +
                        simpleEchoStreamingCommand + "`;");
            }

            // Run the query and check the results
            Util.checkQueryOutputs(pigServer.openIterator("OP"), expectedResults);
        }
    }

    @Test
    public void testSimpleMapSideStreamingWithOutputSchema()
    throws Exception {
        File input = Util.createInputFile("tmp", "",
                new String[] {"A,1", "B,2", "C,3", "D,2",
                "A,5", "B,5", "C,8", "A,8",
                "D,8", "A,9"});

        // Expected results
        Object[] expectedFirstFields = new String[] {"C", "A", "D", "A"};
        Object[] expectedSecondFields = new Integer[] {8, 8, 8, 9};

        boolean[] withTypes = {true, false};
        for (int i = 0; i < withTypes.length; i++) {
            Tuple[] expectedResults = null;
            if(withTypes[i] == true) {
                expectedResults =
                    setupExpectedResults(expectedFirstFields, expectedSecondFields);
            } else {
                expectedResults = setupExpectedResults(Util
                        .toDataByteArrays(expectedFirstFields), Util
                        .toDataByteArrays(expectedSecondFields));
            }
            // Pig query to run
            pigServer.registerQuery("IP = load '" +
                    Util.generateURI(Util.encodeEscape(input.toString()), pigServer.getPigContext()) +
                    "' using " + PigStorage.class.getName() + "(',');");
            pigServer.registerQuery("FILTERED_DATA = filter IP by $1 > '3';");
            if(withTypes[i] == true) {
                pigServer.registerQuery("STREAMED_DATA = stream FILTERED_DATA through `" +
                        simpleEchoStreamingCommand + "` as (f0:chararray, f1:int);");
            } else {
                pigServer.registerQuery("STREAMED_DATA = stream FILTERED_DATA through `" +
                        simpleEchoStreamingCommand + "` as (f0, f1);");
            }
            pigServer.registerQuery("OP = filter STREAMED_DATA by f1 > 6;");

            // Run the query and check the results
            Util.checkQueryOutputs(pigServer.openIterator("OP"), expectedResults);
        }
    }

    @Test
    public void testSimpleReduceSideStreamingAfterFlatten()
    throws Exception {
        File input = Util.createInputFile("tmp", "",
                new String[] {"A,1", "B,2", "C,3", "D,2",
                "A,5", "B,5", "C,8", "A,8",
                "D,8", "A,9"});

        // Expected results
        String[] expectedFirstFields = new String[] {"A", "A", "A", "B", "C", "D"};
        Integer[] expectedSecondFields = new Integer[] {5, 8, 9, 5, 8, 8};
        boolean[] withTypes = {true, false};
        for (int i = 0; i < withTypes.length; i++) {
            Tuple[] expectedResults = null;
            if(withTypes[i] == true) {
                expectedResults =
                    setupExpectedResults(expectedFirstFields, expectedSecondFields);
            } else {
                expectedResults = setupExpectedResults(Util
                        .toDataByteArrays(expectedFirstFields), Util
                        .toDataByteArrays(expectedSecondFields));
            }

            // Pig query to run
            pigServer.registerQuery("IP = load '" +
                    Util.generateURI(Util.encodeEscape(input.toString()), pigServer.getPigContext()) +
                    "' using " + PigStorage.class.getName() + "(',');");
            pigServer.registerQuery("FILTERED_DATA = filter IP by $1 > '3';");
            pigServer.registerQuery("GROUPED_DATA = group FILTERED_DATA by $0;");
            pigServer.registerQuery("FLATTENED_GROUPED_DATA = foreach GROUPED_DATA " +
            "generate flatten($1);");
            pigServer.registerQuery("S1 = stream FLATTENED_GROUPED_DATA through `" +
                    simpleEchoStreamingCommand + "`;");
            if(withTypes[i] == true) {
                pigServer.registerQuery("OP = stream S1 through `" +
                        simpleEchoStreamingCommand + "` as (f0:chararray, f1:int);");
            } else {
                pigServer.registerQuery("OP = stream S1 through `" +
                        simpleEchoStreamingCommand + "`;");
            }

            // Run the query and check the results
            Util.checkQueryOutputsAfterSort(pigServer.openIterator("OP"), expectedResults);
        }
    }

    @Test
    public void testSimpleOrderedReduceSideStreamingAfterFlatten() throws Exception {
        File input = Util.createInputFile("tmp", "",
                new String[] {"A,1,2,3", "B,2,4,5",
                "C,3,1,2", "D,2,5,2",
                "A,5,5,1", "B,5,7,4",
                "C,8,9,2", "A,8,4,5",
                "D,8,8,3", "A,9,2,5"}
        );

        // Expected results
        String[] expectedFirstFields =
            new String[] {"A", "A", "A", "A", "B", "B", "C", "C", "D", "D"};
        Integer[] expectedSecondFields = new Integer[] {1, 9, 8, 5, 2, 5, 3, 8, 2, 8};
        Integer[] expectedThirdFields = new Integer[] {2, 2, 4, 5, 4, 7, 1, 9, 5, 8};
        Integer[] expectedFourthFields = new Integer[] {3, 5, 5, 1, 5, 4, 2, 2, 2, 3};
        Tuple[] expectedResults = new Tuple[10];
        for (int i = 0; i < expectedResults.length; ++i) {
            expectedResults[i] = tf.newTuple(4);
            expectedResults[i].set(0, expectedFirstFields[i]);
            expectedResults[i].set(1, expectedSecondFields[i]);
            expectedResults[i].set(2, expectedThirdFields[i]);
            expectedResults[i].set(3, expectedFourthFields[i]);
        }
        //setupExpectedResults(expectedFirstFields, expectedSecondFields);

        // Pig query to run
        pigServer.registerQuery("IP = load '" +
                Util.generateURI(Util.encodeEscape(input.toString()), pigServer.getPigContext()) +
                "' using " + PigStorage.class.getName() + "(',');");
        pigServer.registerQuery("FILTERED_DATA = filter IP by $1 > '3';");
        pigServer.registerQuery("S1 = stream FILTERED_DATA through `" +
                simpleEchoStreamingCommand + "`;");
        pigServer.registerQuery("S2 = stream S1 through `" +
                simpleEchoStreamingCommand + "`;");
        pigServer.registerQuery("GROUPED_DATA = group IP by $0;");
        pigServer.registerQuery("ORDERED_DATA = foreach GROUPED_DATA { " +
                "  D = order IP BY $2, $3;" +
                "  generate flatten(D);" +
        "};");
        pigServer.registerQuery("S3 = stream ORDERED_DATA through `" +
                simpleEchoStreamingCommand + "`;");
        pigServer.registerQuery("OP = stream S3 through `" +
                simpleEchoStreamingCommand + "` as (f0:chararray, f1:int, f2:int, f3:int);");

        // Run the query and check the results
        Util.checkQueryOutputs(pigServer.openIterator("OP"), expectedResults);
    }

    @Test
    public void testSimpleMapSideStreamingWithUnixPipes()
    throws Exception {
        File input = Util.createInputFile("tmp", "",
                new String[] {"A,1", "B,2", "C,3", "D,2",
                "A,5", "B,5", "C,8", "A,8",
                "D,8", "A,9"});

        // Expected results
        String[] expectedFirstFields =
            new String[] {"A", "B", "C", "D", "A", "B", "C", "A", "D", "A"};
        Integer[] expectedSecondFields = new Integer[] {1, 2, 3, 2, 5, 5, 8, 8, 8, 9};
        boolean[] withTypes = {true, false};
        for (int i = 0; i < withTypes.length; i++) {
            Tuple[] expectedResults = null;
            if(withTypes[i] == true) {
                expectedResults =
                    setupExpectedResults(expectedFirstFields, expectedSecondFields);
            } else {
                expectedResults =
                    setupExpectedResults(Util.toDataByteArrays(expectedFirstFields), Util.toDataByteArrays(expectedSecondFields));
            }

            // Pig query to run
            pigServer.registerQuery("define CMD `" + simpleEchoStreamingCommand +
                    " | " + simpleEchoStreamingCommand + "`;");
            pigServer.registerQuery("IP = load '" +
                    Util.generateURI(Util.encodeEscape(input.toString()), pigServer.getPigContext()) +
                    "' using " + PigStorage.class.getName() + "(',');");
            if(withTypes[i] == true) {
                pigServer.registerQuery("OP = stream IP through CMD as (f0:chararray, f1:int);");
            } else {
                pigServer.registerQuery("OP = stream IP through CMD;");
            }

            // Run the query and check the results
            Util.checkQueryOutputs(pigServer.openIterator("OP"), expectedResults);
        }
    }

    @Test
    public void testJoinTwoStreamingRelations()
    throws Exception {
        ArrayList<String> list = new ArrayList<String>();
        for (int i=0; i<10000; i++) {
            list.add("A," + i);
        }
        File input = Util.createInputFile("tmp", "", list.toArray(new String[0]));

        // Expected results
        Tuple expected = TupleFactory.getInstance().newTuple(4);
        expected.set(0, "A");
        expected.set(1, 0);
        expected.set(2, "A");
        expected.set(3, 0);

        pigServer.registerQuery("A = load '" +
                Util.generateURI(Util.encodeEscape(input.toString()), pigServer.getPigContext()) +
                "' using " + PigStorage.class.getName() + "(',') as (a0, a1);");
        pigServer.registerQuery("B = stream A through `head -1` as (a0, a1);");
        pigServer.registerQuery("C = load '" +
                Util.generateURI(Util.encodeEscape(input.toString()), pigServer.getPigContext()) +
                "' using " + PigStorage.class.getName() + "(',') as (a0, a1);");
        pigServer.registerQuery("D = stream C through `head -1` as (a0, a1);");
        pigServer.registerQuery("E = join B by a0, D by a0;");

        Iterator<Tuple> iter = pigServer.openIterator("E");
        int count = 0;
        while (iter.hasNext()) {
            Assert.assertEquals(expected.toString(), iter.next().toString());
            count++;
        }
        Assert.assertTrue(count == 1);
    }

    @Test
    public void testLocalNegativeLoadStoreOptimization() throws Exception {
        testNegativeLoadStoreOptimization(ExecType.LOCAL);
    }

    private void testNegativeLoadStoreOptimization(ExecType execType)
    throws Exception {
        File input = Util.createInputFile("tmp", "",
                new String[] {"A,1", "B,2", "C,3", "D,2",
                "A,5", "B,5", "C,8", "A,8",
                "D,8", "A,9"});

        // Expected results
        String[] expectedFirstFields = new String[] {"A", "B", "C", "A", "D", "A"};
        Integer[] expectedSecondFields = new Integer[] {5, 5, 8, 8, 8, 9};
        boolean[] withTypes = {true, false};
        for (int i = 0; i < withTypes.length; i++) {
            Tuple[] expectedResults = null;
            if(withTypes[i] == true) {
                expectedResults =
                    setupExpectedResults(expectedFirstFields, expectedSecondFields);
            } else {
                expectedResults = setupExpectedResults(Util
                        .toDataByteArrays(expectedFirstFields), Util
                        .toDataByteArrays(expectedSecondFields));
            }

            // Pig query to run
            pigServer.registerQuery("define CMD `"+ simpleEchoStreamingCommand +
            "` input(stdin);");
            pigServer.registerQuery("IP = load '" +
                    Util.generateURI(Util.encodeEscape(input.toString()), pigServer.getPigContext()) +
                    "' using " + PigStorage.class.getName() + "(',');");
            pigServer.registerQuery("FILTERED_DATA = filter IP by $1 > '3';");
            if(withTypes[i] == true) {
                pigServer.registerQuery("OP = stream FILTERED_DATA through `" +
                        simpleEchoStreamingCommand + "` as (f0:chararray, f1:int);");
            } else {
                pigServer.registerQuery("OP = stream FILTERED_DATA through `" +
                        simpleEchoStreamingCommand + "`;");
            }

            // Run the query and check the results
            Util.checkQueryOutputs(pigServer.openIterator("OP"), expectedResults);
        }
    }
}
