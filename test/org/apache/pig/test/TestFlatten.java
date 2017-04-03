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
        Util.checkQueryOutputs(actualResults.iterator(), expectedResults);
    }
}
