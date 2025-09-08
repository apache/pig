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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.builtin.mock.Storage;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
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

    @Test
    public void testFlattenOnUDFWithNoSchemaAndAsClauseCastError() throws Exception {
        /*
        This is a regression caused by PIG-2315.
        The extra POCast added by ForEachUserSchemaVisitor when AS clause has schema, encounters below error
        as CastLineageSetter was unable to determine and set a LoadFunc on it causing caster to be null

        ERROR 1075: Received a bytearray from the UDF or Union from two different Loaders. Cannot determine how to convert the bytearray to string for [x1[-1,-1]]
        at org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POCast.getNextString(POCast.java:1125)
         */
        Storage.Data data = Storage.resetData(pig);
        data.set("input",
                Storage.tuple(1, 1.1, "quick"),
                Storage.tuple(2, 2.2, "brown"),
                Storage.tuple(3, 3.3, "fox"),
                Storage.tuple(4, 4.4, "jumps"),
                Storage.tuple(5, null, "over"),
                Storage.tuple(6, null, null)
        );
        data.set("input1",
                Storage.tuple(1, new DataByteArray("the")),
                Storage.tuple(2, new DataByteArray("lazy")),
                Storage.tuple(3, new DataByteArray("dog")),
                Storage.tuple(4, null));

        pig.setBatchOn();
        pig.registerQuery("A = load 'input' using mock.Storage() as (a0:int, a1:double, a2: chararray);");
        // This FOREACH after LOAD is required to reproduce issue of CastLineageSetter not being able to resolve LoadFunc for casting
        // Without that the issue is not reproducible
        pig.registerQuery("A = FOREACH A GENERATE a0, a1, SUBSTRING(a2,0,4) as a2:chararray;");
        pig.registerQuery("B = load 'input1' using mock.Storage() as (b0:int, b1:bytearray);");
        pig.registerQuery("C = JOIN A BY a0 LEFT OUTER, B by b0;");
        pig.registerQuery("D = FOREACH C GENERATE A::a0 as a0, A::a1 as a1, B::b1 as b1, TOTUPLE(a1, a2, b1) as tup1;");
        pig.registerQuery("E = GROUP D BY (a0, a1, b1);");
        pig.registerQuery("F = FOREACH E GENERATE group.$0 as a0, group.$1 as a1, group.$2 as b1, $1.tup1 as bag1;");
        pig.registerQuery("G = FOREACH F GENERATE a0, org.apache.pig.test.TestFlatten$UDFWithNoOutputSchema(a0, a1, b1, bag1) as bag2;");
        pig.registerQuery("H = FOREACH G GENERATE a0, FLATTEN(bag2) as (x1:chararray, x2:double, x3:chararray, x4:long);");
        pig.registerQuery("STORE H INTO 'output' USING mock.Storage();");

        List<ExecJob> execJobs = pig.executeBatch();
        for( ExecJob execJob : execJobs ) {
            assertTrue(execJob.getStatus() == ExecJob.JOB_STATUS.COMPLETED );
        }
        List<Tuple> actualResults = data.get("output");
        List<Tuple> expectedResults = Util.getTuplesFromConstantTupleStrings(
                new String[] {
                        "(1, 'the', 1.1, 'the', 3L)",
                        "(2, 'lazy', 2.2, 'lazy', 3L)",
                        "(3, 'dog', 3.3, 'dog', 3L)",
                        "(4, null, 4.4, null, 4L)",
                        "(5, null, null, null, 5L)",
                        "(6, null, null, null, 6L)"
                });
        Util.checkQueryOutputsAfterSort(actualResults.iterator(), expectedResults);
    }

    @Test
    public void testFlattenOnUDFWithNoSchemaAndAsClauseWrongOutput() throws Exception {
        /*
            This is a regression caused by PIG-2315.
            Casting fields of the bag returned by UDF with no outputSchema() throws error (Refer testFlattenOnUDFWithNoSchemaAndAsClauseCastError),
            But when AS clause is defined directly on the bag returned by UDF, the bag output becomes empty.
            This is because the whole bag is being casted using POCast.getNextDataBag compared to FLATTEN(bag) which casts
            individual fields. It swallows the IOException for caster not being set and discards the row with the warning message
            PigWarning.FIELD_DISCARDED_TYPE_CONVERSION_FAILED leading to empty output.

                |   Cast[bag:{(chararray,double,chararray,long)}] - scope-84
                |   |
                |   |---Project[bag][1] - scope-83
         */
        Storage.Data data = Storage.resetData(pig);
        data.set("input",
                Storage.tuple(1, 1.1, "quick"),
                Storage.tuple(2, 2.2, "brown"),
                Storage.tuple(3, 3.3, "fox"),
                Storage.tuple(4, 4.4, "jumps"),
                Storage.tuple(5, null, "over"),
                Storage.tuple(6, null, null)
        );
        data.set("input1",
                Storage.tuple(1, new DataByteArray("the")),
                Storage.tuple(2, new DataByteArray("lazy")),
                Storage.tuple(3, new DataByteArray("dog")),
                Storage.tuple(4, null));

        pig.setBatchOn();
        pig.registerQuery("A = load 'input' using mock.Storage() as (a0:int, a1:double, a2: chararray);");
        pig.registerQuery("A = FOREACH A GENERATE a0, a1, SUBSTRING(a2,0,4) as a2:chararray;");
        pig.registerQuery("B = load 'input1' using mock.Storage() as (b0:int, b1:bytearray);");
        pig.registerQuery("C = JOIN A BY a0 LEFT OUTER, B by b0;");
        pig.registerQuery("D = FOREACH C GENERATE A::a0 as a0, A::a1 as a1, B::b1 as b1, TOTUPLE(a1, a2, b1) as tup1;");
        pig.registerQuery("E = GROUP D BY (a0, a1, b1);");
        pig.registerQuery("F = FOREACH E GENERATE group.$0 as a0, group.$1 as a1, group.$2 as b1, $1.tup1 as bag1;");
        // Below two lines differ from testFlattenOnUDFWithNoSchemaAndAsClauseCastError
        // and is something users tried when they encountered cast error
        pig.registerQuery("G = FOREACH F GENERATE a0, org.apache.pig.test.TestFlatten$UDFWithNoOutputSchema(a0, a1, b1, bag1) as " +
                "bag2:{t:(a1:chararray, a2:double, a3:chararray, a4:long)};");
        pig.registerQuery("H = FOREACH G GENERATE a0, FLATTEN(bag2);");
        pig.registerQuery("STORE H INTO 'output' USING mock.Storage();");

        List<ExecJob> execJobs = pig.executeBatch();
        for( ExecJob execJob : execJobs ) {
            assertTrue(execJob.getStatus() == ExecJob.JOB_STATUS.COMPLETED );
        }
        List<Tuple> actualResults = data.get("output");
        List<Tuple> expectedResults = Util.getTuplesFromConstantTupleStrings(
                new String[] {
                        "(1, 'the', 1.1, 'the', 3L)",
                        "(2, 'lazy', 2.2, 'lazy', 3L)",
                        "(3, 'dog', 3.3, 'dog', 3L)",
                        "(4, null, 4.4, null, 4L)",
                        "(5, null, null, null, 5L)",
                        "(6, null, null, null, 6L)"
                });
        Util.checkQueryOutputsAfterSort(actualResults.iterator(), expectedResults);
    }

    public static class UDFWithNoOutputSchema extends EvalFunc<DataBag> {

        private final TupleFactory tupleFactory;

        public UDFWithNoOutputSchema() {
            tupleFactory = TupleFactory.getInstance();
        }

        public DataBag exec(Tuple input) throws IOException {
            Integer f0 = (Integer)input.get(0);
            DataBag f3 = (DataBag)input.get(3);

            DataBag result = new DefaultDataBag();
            Tuple tuple = tupleFactory.newTuple(4);
            tuple.set(1, input.get(1));
            tuple.set(2, input.get(2));
            if(f3 == null || f3.size() == 0) {
                tuple.set(0, null);
                tuple.set(3, null);
            } else {
                int size_nullcount = 0;
                for (Tuple t: f3) {
                    Tuple inner = ((Tuple)t.get(0));
                    tuple.set(0, inner.get(2));
                    size_nullcount = inner.size();
                    // System.out.println("input fields = " + f0 + "," + input.get(1) + "," + input.get(2) + "," + t + "," + inner.size());
                    for (Object o: inner) {
                        size_nullcount = size_nullcount + (o == null ? 1 : 0);
                        // System.out.println ("size_nullcount =" + size_nullcount + ", o=" + o);
                    }
                    break;
                }
                tuple.set(3, size_nullcount);
            }
            result.add(tuple);
            return result;
        }
    }

}
