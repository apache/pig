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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;

import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.builtin.mock.Storage;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;
import org.junit.Before;
import org.junit.Test;

public class TestFlatten {

    private PigServer pig ;

    @Before
    public void setUp() throws Exception{
        pig = new PigServer(Util.getLocalTestMode()) ;
    }

    @Test
    public void testTwoBagFlatten() throws Exception {
        Storage.Data data = Storage.resetData(pig);
        data.set("input",
            Storage.tuple(
                Storage.bag(
                    Storage.tuple("a","b"),
                    Storage.tuple("c","d")),
                Storage.bag(
                    Storage.tuple("1","2"),
                    Storage.tuple("3","4"))
            )
        );
        pig.setBatchOn();
        pig.registerQuery("A = load 'input' using mock.Storage() as (bag1:bag {(a1_1:chararray, a1_2:chararray)}, bag2:bag{(a2_1:chararray, a2_2:chararray)});");
        pig.registerQuery("B = foreach A GENERATE FLATTEN(bag1), FLATTEN(bag2);");
        pig.registerQuery("store B into 'output' using mock.Storage();");
        List<ExecJob> execJobs = pig.executeBatch();
        for( ExecJob execJob : execJobs ) {
          assertTrue(execJob.getStatus() == ExecJob.JOB_STATUS.COMPLETED );
        }
        Schema expectedSch = Utils.getSchemaFromString("bag1::a1_1: chararray,bag1::a1_2: chararray,bag2::a2_1: chararray,bag2::a2_2: chararray");
        assertEquals(expectedSch, data.getSchema("output"));
        List<Tuple> actualResults = data.get("output");
        List<Tuple> expectedResults = Util.getTuplesFromConstantTupleStrings(
                new String[] {
                "('a', 'b', '1', '2')", "('a', 'b', '3', '4')", "('c', 'd', '1', '2')", "('c', 'd', '3', '4')" });
        Util.checkQueryOutputs(actualResults.iterator(), expectedResults);
    }

    @Test
    public void testTwoMapFlatten() throws Exception {
        Storage.Data data = Storage.resetData(pig);
        data.set("input",
            Storage.tuple(
                Storage.map("a","b",
                            "c","d"),
                Storage.map("1","2",
                            "3","4")
            )
        );
        pig.setBatchOn();
        pig.registerQuery("A = load 'input' using mock.Storage() as (map1:map [chararray], map2:map [chararray]);");
        pig.registerQuery("B = foreach A GENERATE FLATTEN(map1), FLATTEN(map2);");
        pig.registerQuery("store B into 'output' using mock.Storage();");
        List<ExecJob> execJobs = pig.executeBatch();
        for( ExecJob execJob : execJobs ) {
          assertTrue(execJob.getStatus() == ExecJob.JOB_STATUS.COMPLETED );
        }
        Schema expectedSch = Utils.getSchemaFromString("map1::key: chararray,map1::value: chararray,map2::key: chararray,map2::value: chararray");
        assertEquals(expectedSch, data.getSchema("output"));
        List<Tuple> actualResults = data.get("output");
        List<Tuple> expectedResults = Util.getTuplesFromConstantTupleStrings(
                new String[] {
                "('a', 'b', '1', '2')", "('a', 'b', '3', '4')", "('c', 'd', '1', '2')", "('c', 'd', '3', '4')" });
        Util.checkQueryOutputsAfterSort(actualResults.iterator(), expectedResults);
    }

    @Test
    public void testFlattenOnNullBag() throws Exception {
        Storage.Data data = Storage.resetData(pig);
        data.set("input",
            Storage.tuple(
                Storage.bag(
                    Storage.tuple("a","b"),
                    Storage.tuple("c","d")),
                Storage.bag(
                    Storage.tuple("1","2"),
                    Storage.tuple("3","4"))
            ),
            Storage.tuple(
                null,
                Storage.bag(
                    Storage.tuple("11","12"),
                    Storage.tuple("13","14"))
            ),
            Storage.tuple(
                Storage.bag(
                    Storage.tuple("k","l"),
                    Storage.tuple("m","n")),
                null
            ),
            Storage.tuple(
                Storage.bag(
                    null,
                    Storage.tuple("e","f")),
                Storage.bag(
                    Storage.tuple("5","6"),
                    Storage.tuple("7","8"))
            ),
            Storage.tuple(
                Storage.bag(
                    Storage.tuple("g","h"),
                    Storage.tuple("i","j")),
                Storage.bag(
                    Storage.tuple("9","10"),
                    null
                    )
            )
        );

        pig.setBatchOn();
        pig.registerQuery("A = load 'input' using mock.Storage() as (bag1:bag {(a1_1:chararray, a1_2:chararray)}, bag2:bag{(a2_1:chararray, a2_2:chararray)});");
        pig.registerQuery("B = foreach A GENERATE FLATTEN(bag1), FLATTEN(bag2);");
        pig.registerQuery("store B into 'output' using mock.Storage();");
        List<ExecJob> execJobs = pig.executeBatch();
        for( ExecJob execJob : execJobs ) {
          assertTrue(execJob.getStatus() == ExecJob.JOB_STATUS.COMPLETED );
        }
        List<Tuple> actualResults = data.get("output");
        List<Tuple> expectedResults = Util.getTuplesFromConstantTupleStrings(
                new String[] {
                "('a', 'b', '1', '2')", "('a', 'b', '3', '4')", "('c', 'd', '1', '2')", "('c', 'd', '3', '4')",

                //flatten(null-bag) on schema {(a1_1:int, a1_2:chararray)} expands to (null, null)
                "(null, null, '11', '12')", "(null, null, '13', '14')",
                "('k', 'l', null, null)", "('m', 'n', null, null)",

                //flatten(null-tuple-from-bag) on schema {(a1_1:int, a1_2:chararray)} also expands to (null, null)
                "(null, null, '5', '6')", "(null, null, '7', '8')", "('e', 'f', '5', '6')", "('e', 'f', '7', '8')",
                "('g', 'h', '9', '10')", "('g', 'h', null, null)", "('i', 'j', '9', '10')", "('i', 'j', null, null)" });
        Util.checkQueryOutputs(actualResults.iterator(), expectedResults);

        //instead of passing the bag with a correct inner schema to flatten, passing one
        //with empty inner schema but later specifying the inner schema by 'as' clause.
        pig.registerQuery("A = load 'input' using mock.Storage() as (bag1:bag {}, bag2:bag{});");
        pig.registerQuery("B = foreach A GENERATE FLATTEN(bag1) as (a1_1:chararray, a1_2:chararray), FLATTEN(bag2) as (a2_1:chararray, a2_2:chararray);");
        pig.registerQuery("store B into 'output2' using mock.Storage();");
        execJobs = pig.executeBatch();
        for( ExecJob execJob : execJobs ) {
            assertTrue(execJob.getStatus() == ExecJob.JOB_STATUS.COMPLETED );
        }
        actualResults = data.get("output2");
        Util.checkQueryOutputs(actualResults.iterator(), expectedResults);
    }

    @Test
    public void testFlattenOnNullMap() throws Exception {
        Storage.Data data = Storage.resetData(pig);
        data.set("input",
            Storage.tuple(
                Storage.map("a","b",
                            "c","d"),
                Storage.map("1","2",
                            "3","4")
            )
            ,
            Storage.tuple(
                null,
                Storage.map("11","12",
                            "13","14")
            ),
            Storage.tuple(
                Storage.map("k","l",
                            "m","n"),
                null
            )
        );
        pig.setBatchOn();
        pig.registerQuery("A = load 'input' using mock.Storage() as (map1:map [chararray], map2:map [chararray]);");
        pig.registerQuery("B = foreach A GENERATE FLATTEN(map1), FLATTEN(map2);");
        pig.registerQuery("store B into 'output' using mock.Storage();");
        List<ExecJob> execJobs = pig.executeBatch();
        for( ExecJob execJob : execJobs ) {
          assertTrue(execJob.getStatus() == ExecJob.JOB_STATUS.COMPLETED );
        }
        List<Tuple> actualResults = data.get("output");
        List<Tuple> expectedResults = Util.getTuplesFromConstantTupleStrings(
                new String[] {
                "('a', 'b', '1', '2')", "('a', 'b', '3', '4')", "('c', 'd', '1', '2')", "('c', 'd', '3', '4')",
                // flatten(null-map) should expand to (null, null)
                "(null, null, '11', '12')", "(null, null, '13', '14')", "('k', 'l', null, null)", "('m', 'n', null, null)"
                });
        Util.checkQueryOutputsAfterSort(actualResults.iterator(), expectedResults);
    }

    @Test
    public void testFlattenOnNullTuple() throws Exception {
        Storage.Data data = Storage.resetData(pig);
        data.set("input",
            Storage.tuple(
                Storage.tuple("a","b"),
                Storage.tuple("1","2")
            ),
            Storage.tuple(
                null,
                Storage.tuple("3","4")
            ),
            Storage.tuple(
                Storage.tuple("c","d"),
                null
            ),
            Storage.tuple(
                Storage.tuple("e", null),
                Storage.tuple(null,"5")
            )
        );
        pig.setBatchOn();
        pig.registerQuery("A = load 'input' using mock.Storage() as (tuple1:tuple (a1:chararray, a2:chararray), tuple2:tuple (a3:chararray, a4:chararray));");
        pig.registerQuery("B = foreach A GENERATE FLATTEN(tuple1), FLATTEN(tuple2);");
        pig.registerQuery("store B into 'output' using mock.Storage();");
        List<ExecJob> execJobs = pig.executeBatch();
        for( ExecJob execJob : execJobs ) {
          assertTrue(execJob.getStatus() == ExecJob.JOB_STATUS.COMPLETED );
        }
        List<Tuple> actualResults = data.get("output");
        List<Tuple> expectedResults = Util.getTuplesFromConstantTupleStrings(
                new String[] {
                "('a', 'b', '1', '2')", "(null, null, '3', '4')", "('c', 'd', null, null)", "('e', null, null, '5')" });
        Util.checkQueryOutputs(actualResults.iterator(), expectedResults);

        //instead of passing the tuple with correct inner schema to flatten, passing one
        //with empty inner schema but later specifying the inner schema by 'as' clause.
        pig.registerQuery("A = load 'input' using mock.Storage() as (tuple1:tuple(), tuple2:tuple());");
        pig.registerQuery("B = foreach A GENERATE FLATTEN(tuple1) as (a1:chararray, a2:chararray), FLATTEN(tuple2) as (a3:chararray, a4:chararray);");
        pig.registerQuery("store B into 'output2' using mock.Storage();");
        execJobs = pig.executeBatch();
        for( ExecJob execJob : execJobs ) {
            assertTrue(execJob.getStatus() == ExecJob.JOB_STATUS.COMPLETED );
        }
        actualResults = data.get("output2");

        Util.checkQueryOutputs(actualResults.iterator(), expectedResults);
    }

    @Test
    public void testFlattenOnNullWithNoSchema() throws Exception {
        Storage.Data data = Storage.resetData(pig);
        data.set("input",
            Storage.tuple(
                null,
                Storage.bag(
                    Storage.tuple("1","2"),
                    Storage.tuple("3","4"))
            ),

            Storage.tuple(
                Storage.bag(
                    null,
                    Storage.tuple("e","f")),
                Storage.bag(
                    Storage.tuple("5","6"),
                    Storage.tuple("7","8"))
            ),

            Storage.tuple(
                null,
                Storage.map("9","10")
            ),

            Storage.tuple(
                Storage.tuple("g","h"),
                null
            ),

            Storage.tuple(
                Storage.tuple("13", null),
                Storage.tuple(null,"16")
            )
        );
        pig.setBatchOn();
        pig.registerQuery("A = load 'input' using mock.Storage() as (a1, a2);");
        pig.registerQuery("B = foreach A GENERATE FLATTEN(a1), FLATTEN(a2);");
        pig.registerQuery("store B into 'output' using mock.Storage();");
        List<ExecJob> execJobs = pig.executeBatch();
        for( ExecJob execJob : execJobs ) {
          assertTrue(execJob.getStatus() == ExecJob.JOB_STATUS.COMPLETED );
        }
        List<Tuple> actualResults = data.get("output");
        List<Tuple> expectedResults = Util.getTuplesFromConstantTupleStrings(
                new String[] {
                "(null, '1', '2')", "(null, '3', '4')",  //since no schema, flatten(null) ==> one null
                "(null, '5', '6')", "(null, '7', '8')", "('e', 'f', '5', '6')", "('e', 'f', '7', '8')",
                "(null, '9', '10')",
                "('g', 'h', null)",
                "('13', null, null, '16')"});

        Util.checkQueryOutputs(actualResults.iterator(), expectedResults);
    }

    @Test
    public void testFlattenOnNullBagWithColumnPrune() throws Exception {
        Storage.Data data = Storage.resetData(pig);
        data.set("input",
            Storage.tuple(
                1,
                Storage.bag(
                    Storage.tuple("a","b"),
                    Storage.tuple("c","d")),
                Storage.bag(
                    Storage.tuple("1","2"),
                    Storage.tuple("3","4"))
            ),
            Storage.tuple(
                2,
                null,
                Storage.bag(
                    Storage.tuple("11","12"),
                    Storage.tuple("13","14"))
            ),
            Storage.tuple(
                3,
                Storage.bag(
                    Storage.tuple("k","l"),
                    Storage.tuple("m","n")),
                null
            ),
            Storage.tuple(
                4,
                Storage.bag(
                    null,
                    Storage.tuple("e","f")),
                Storage.bag(
                    Storage.tuple("5","6"),
                    Storage.tuple("7","8"))
            ),
            Storage.tuple(
                5,
                Storage.bag(
                    Storage.tuple("g","h"),
                    Storage.tuple("i","j")),
                Storage.bag(
                    Storage.tuple("9","10"),
                    null
                    )
            )
        );
        pig.setBatchOn();
        pig.registerQuery("A = load 'input' using mock.Storage() as (a0:int, bag1:bag {(a1_1:int, a1_2:chararray)}, bag2:bag{(a2_1:chararray, a2_2:chararray)});");
        pig.registerQuery("B = foreach A GENERATE FLATTEN(bag1), FLATTEN(bag2);");
        pig.registerQuery("store B into 'output' using mock.Storage();");
        List<ExecJob> execJobs = pig.executeBatch();
        for( ExecJob execJob : execJobs ) {
          assertTrue(execJob.getStatus() == ExecJob.JOB_STATUS.COMPLETED );
        }
        List<Tuple> actualResults = data.get("output");
        List<Tuple> expectedResults = Util.getTuplesFromConstantTupleStrings(
                new String[] {
                "('a', 'b', '1', '2')", "('a', 'b', '3', '4')", "('c', 'd', '1', '2')", "('c', 'd', '3', '4')",
                "(null, null, '11', '12')", "(null, null, '13', '14')",
                "('k', 'l', null, null)", "('m', 'n', null, null)",
                "(null, null, '5', '6')", "(null, null, '7', '8')", "('e', 'f', '5', '6')", "('e', 'f', '7', '8')",
                "('g', 'h', '9', '10')", "('g', 'h', null, null)", "('i', 'j', '9', '10')", "('i', 'j', null, null)" });

        Util.checkQueryOutputs(actualResults.iterator(), expectedResults);
    }
}
