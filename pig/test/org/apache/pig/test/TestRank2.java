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

import static org.apache.pig.builtin.mock.Storage.resetData;
import static org.apache.pig.builtin.mock.Storage.tuple;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.pig.PigServer;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class TestRank2 {
    private static PigServer pigServer;
    private static TupleFactory tf = TupleFactory.getInstance();
    private Data data;

    @Before
    public void setUp() throws Exception {
        pigServer = new PigServer("local");

        data = resetData(pigServer);
        data.set("test01", tuple("A", 1, "N"), tuple("B", 2, "N"),
                tuple("C", 3, "M"), tuple("D", 4, "P"), tuple("E", 4, "Q"),
                tuple("E", 4, "Q"), tuple("F", 8, "Q"), tuple("F", 7, "Q"),
                tuple("F", 8, "T"), tuple("F", 8, "Q"), tuple("G", 10, "V"));

        data.set(
                "test02",
                tuple("Michael", "Blythe", 1, 1, 1, 1, 4557045.046, 98027),
                tuple("Linda", "Mitchell", 2, 1, 1, 1, 5200475.231, 98027),
                tuple("Jillian", "Carson", 3, 1, 1, 1, 3857163.633, 98027),
                tuple("Garrett", "Vargas", 4, 1, 1, 1, 1764938.986, 98027),
                tuple("Tsvi", "Reiter", 5, 1, 1, 2, 2811012.715, 98027),
                tuple("Shu", "Ito", 6, 6, 2, 2, 3018725.486, 98055),
                tuple("Jose", "Saraiva", 7, 6, 2, 2, 3189356.247, 98055),
                tuple("David", "Campbell", 8, 6, 2, 3, 3587378.426, 98055),
                tuple("Tete", "Mensa-Annan", 9, 6, 2, 3, 1931620.184, 98055),
                tuple("Lynn", "Tsoflias", 10, 6, 2, 3, 1758385.926, 98055),
                tuple("Rachel", "Valdez", 11, 6, 2, 4, 2241204.042, 98055),
                tuple("Jae", "Pak", 12, 6, 2, 4, 5015682.375, 98055),
                tuple("Ranjit", "Varkey Chudukatil", 13, 6, 2, 4,
                        3827950.238, 98055));
    }

    @Test
    public void testRank01RankByDense() throws IOException {
        String query = "A = LOAD 'test01' USING mock.Storage() AS (f1:chararray,f2:int,f3:chararray);"
            + "C = rank A by f3 DENSE;"
            + "store C into 'result' using mock.Storage();";

        Util.registerMultiLineQuery(pigServer, query);

        Set<Tuple> expected = ImmutableSet.of(
                tf.newTuple(ImmutableList.of((long) 1, "C", 3, "M")),
                tf.newTuple(ImmutableList.of((long) 2, "A", 1, "N")),
                tf.newTuple(ImmutableList.of((long) 2, "B", 2, "N")),
                tf.newTuple(ImmutableList.of((long) 3, "D", 4, "P")),
                tf.newTuple(ImmutableList.of((long) 4, "E", 4, "Q")),
                tf.newTuple(ImmutableList.of((long) 4, "E", 4, "Q")),
                tf.newTuple(ImmutableList.of((long) 4, "F", 8, "Q")),
                tf.newTuple(ImmutableList.of((long) 4, "F", 7, "Q")),
                tf.newTuple(ImmutableList.of((long) 4, "F", 8, "Q")),
                tf.newTuple(ImmutableList.of((long) 5, "F", 8, "T")),
                tf.newTuple(ImmutableList.of((long) 6, "G", 10, "V")));

        verifyExpected(data.get("result"), expected);
    }

    @Test
    public void testRank02RankByDense() throws IOException {
        String query = "A = LOAD 'test01' USING mock.Storage() AS (f1:chararray,f2:int,f3:chararray);"
            + "C = rank A by f2 ASC DENSE;"
            + "store C into 'result' using mock.Storage();";

        Util.registerMultiLineQuery(pigServer, query);

        Set<Tuple> expected = ImmutableSet.of(
                tf.newTuple(ImmutableList.of((long) 1, "A", 1, "N")),
                tf.newTuple(ImmutableList.of((long) 2, "B", 2, "N")),
                tf.newTuple(ImmutableList.of((long) 3, "C", 3, "M")),
                tf.newTuple(ImmutableList.of((long) 4, "D", 4, "P")),
                tf.newTuple(ImmutableList.of((long) 4, "E", 4, "Q")),
                tf.newTuple(ImmutableList.of((long) 4, "E", 4, "Q")),
                tf.newTuple(ImmutableList.of((long) 5, "F", 7, "Q")),
                tf.newTuple(ImmutableList.of((long) 6, "F", 8, "Q")),
                tf.newTuple(ImmutableList.of((long) 6, "F", 8, "Q")),
                tf.newTuple(ImmutableList.of((long) 6, "F", 8, "T")),
                tf.newTuple(ImmutableList.of((long) 7, "G", 10, "V")));

        verifyExpected(data.get("result"), expected);
    }

    @Test
    public void testRank03RankByDense() throws IOException {
        String query = "A = LOAD 'test01' USING mock.Storage() AS (f1:chararray,f2:int,f3:chararray);"
            + "C = rank A by f1 DESC DENSE;"
            + "store C into 'result' using mock.Storage();";

        Util.registerMultiLineQuery(pigServer, query);

        Set<Tuple> expected = ImmutableSet.of(
                tf.newTuple(ImmutableList.of((long) 1, "G", 10, "V")),
                tf.newTuple(ImmutableList.of((long) 2, "F", 8, "T")),
                tf.newTuple(ImmutableList.of((long) 2, "F", 8, "Q")),
                tf.newTuple(ImmutableList.of((long) 2, "F", 8, "Q")),
                tf.newTuple(ImmutableList.of((long) 2, "F", 7, "Q")),
                tf.newTuple(ImmutableList.of((long) 3, "E", 4, "Q")),
                tf.newTuple(ImmutableList.of((long) 3, "E", 4, "Q")),
                tf.newTuple(ImmutableList.of((long) 4, "D", 4, "P")),
                tf.newTuple(ImmutableList.of((long) 5, "C", 3, "M")),
                tf.newTuple(ImmutableList.of((long) 6, "B", 2, "N")),
                tf.newTuple(ImmutableList.of((long) 7, "A", 1, "N")));

        verifyExpected(data.get("result"), expected);
    }

    @Test
    public void testRank04RankByDense() throws IOException {
        String query = "A = LOAD 'test01' USING mock.Storage() AS (f1:chararray,f2:int,f3:chararray);"
            + "C = rank A by f1, f2 DESC DENSE;"
            + "store C into 'result' using mock.Storage();";

        Util.registerMultiLineQuery(pigServer, query);

        Set<Tuple> expected = ImmutableSet.of(
                tf.newTuple(ImmutableList.of((long) 1, "A", 1, "N")),
                tf.newTuple(ImmutableList.of((long) 2, "B", 2, "N")),
                tf.newTuple(ImmutableList.of((long) 3, "C", 3, "M")),
                tf.newTuple(ImmutableList.of((long) 4, "D", 4, "P")),
                tf.newTuple(ImmutableList.of((long) 5, "E", 4, "Q")),
                tf.newTuple(ImmutableList.of((long) 5, "E", 4, "Q")),
                tf.newTuple(ImmutableList.of((long) 6, "F", 8, "Q")),
                tf.newTuple(ImmutableList.of((long) 6, "F", 8, "Q")),
                tf.newTuple(ImmutableList.of((long) 6, "F", 8, "T")),
                tf.newTuple(ImmutableList.of((long) 7, "F", 7, "Q")),
                tf.newTuple(ImmutableList.of((long) 8, "G", 10, "V")));

        verifyExpected(data.get("result"), expected);
    }

    public void verifyExpected(List<Tuple> out, Set<Tuple> expected) {
        for (Tuple tup : out) {
            assertTrue(expected + " contains " + tup, expected.contains(tup));
        }
    }
}