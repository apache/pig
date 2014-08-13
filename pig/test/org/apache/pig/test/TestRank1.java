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

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class TestRank1 {
    private static TupleFactory tf = TupleFactory.getInstance();
    private static PigServer pigServer;
    private Data data;

    @Before
    public void setUp() throws Exception {
        pigServer = new PigServer(ExecType.LOCAL);

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
    public void testRank01RowNumber() throws IOException {
        String query = "A = LOAD 'test01' USING mock.Storage() AS (f1:chararray,f2:int,f3:chararray);"
            + "C = rank A;"
            + "store C into 'result' using mock.Storage();";

        Util.registerMultiLineQuery(pigServer, query);

        Set<Tuple> expected = ImmutableSet.of(
                tf.newTuple(ImmutableList.of((long) 1, "A", 1, "N")),
                tf.newTuple(ImmutableList.of((long) 2, "B", 2, "N")),
                tf.newTuple(ImmutableList.of((long) 3, "C", 3, "M")),
                tf.newTuple(ImmutableList.of((long) 4, "D", 4, "P")),
                tf.newTuple(ImmutableList.of((long) 5, "E", 4, "Q")),
                tf.newTuple(ImmutableList.of((long) 6, "E", 4, "Q")),
                tf.newTuple(ImmutableList.of((long) 7, "F", 8, "Q")),
                tf.newTuple(ImmutableList.of((long) 8, "F", 7, "Q")),
                tf.newTuple(ImmutableList.of((long) 9, "F", 8, "T")),
                tf.newTuple(ImmutableList.of((long) 10, "F", 8, "Q")),
                tf.newTuple(ImmutableList.of((long) 11, "G", 10, "V")));

        verifyExpected(data.get("result"), expected);
    }

    @Test
    public void testRank02RowNumber() throws IOException {
        String query = "A = LOAD 'test02' USING mock.Storage() AS (firstname:chararray,lastname:chararray,rownumberPrev:int,rankPrev:int,denserankPrev:int,quartilePrev:int,sales:double,postalcode:int);"
            + "B = rank A;"
            + "store B into 'result' using mock.Storage();";

        Util.registerMultiLineQuery(pigServer, query);

        Set<Tuple> expected = ImmutableSet.of(
                tf.newTuple(ImmutableList.of((long) 1, "Michael", "Blythe", 1,1, 1, 1, 4557045.046, 98027)),
                tf.newTuple(ImmutableList.of((long) 2, "Linda","Mitchell", 2, 1, 1, 1, 5200475.231, 98027)),
                tf.newTuple(ImmutableList.of((long) 3, "Jillian", "Carson", 3,1, 1, 1, 3857163.633, 98027)),
                tf.newTuple(ImmutableList.of((long) 4, "Garrett","Vargas", 4, 1, 1, 1, 1764938.986, 98027)),
                tf.newTuple(ImmutableList.of((long) 5, "Tsvi", "Reiter",5, 1, 1, 2, 2811012.715, 98027)),
                tf.newTuple(ImmutableList.of((long) 6, "Shu", "Ito", 6,6, 2, 2, 3018725.486, 98055)),
                tf.newTuple(ImmutableList.of((long) 7, "Jose", "Saraiva",7, 6, 2, 2, 3189356.247, 98055)),
                tf.newTuple(ImmutableList.of((long) 8, "David","Campbell", 8, 6, 2, 3, 3587378.426, 98055)),
                tf.newTuple(ImmutableList.of((long) 9, "Tete", "Mensa-Annan",9, 6, 2, 3, 1931620.184, 98055)),
                tf.newTuple(ImmutableList.of((long) 10, "Lynn","Tsoflias", 10, 6, 2, 3, 1758385.926, 98055)),
                tf.newTuple(ImmutableList.of((long) 11, "Rachel", "Valdez", 11,6, 2, 4, 2241204.042, 98055)),
                tf.newTuple(ImmutableList.of((long) 12, "Jae", "Pak", 12,6, 2, 4, 5015682.375, 98055)),
                tf.newTuple(ImmutableList.of((long) 13, "Ranjit","Varkey Chudukatil", 13, 6, 2, 4, 3827950.238,98055)));

        verifyExpected(data.get("result"), expected);
    }

    @Test
    public void testRank01RankBy() throws IOException {
        String query = "A = LOAD 'test01' USING mock.Storage() AS (f1:chararray,f2:int,f3:chararray);"
            + "C = rank A by f3;"
            + "store C into 'result' using mock.Storage();";

        Util.registerMultiLineQuery(pigServer, query);

        Set<Tuple> expected = ImmutableSet.of(
                tf.newTuple(ImmutableList.of((long) 1, "C", 3, "M")),
                tf.newTuple(ImmutableList.of((long) 2, "A", 1, "N")),
                tf.newTuple(ImmutableList.of((long) 2, "B", 2, "N")),
                tf.newTuple(ImmutableList.of((long) 4, "D", 4, "P")),
                tf.newTuple(ImmutableList.of((long) 5, "E", 4, "Q")),
                tf.newTuple(ImmutableList.of((long) 5, "E", 4, "Q")),
                tf.newTuple(ImmutableList.of((long) 5, "F", 8, "Q")),
                tf.newTuple(ImmutableList.of((long) 5, "F", 7, "Q")),
                tf.newTuple(ImmutableList.of((long) 5, "F", 8, "Q")),
                tf.newTuple(ImmutableList.of((long) 10, "F", 8, "T")),
                tf.newTuple(ImmutableList.of((long) 11, "G", 10, "V")));

        verifyExpected(data.get("result"), expected);
    }

    @Test
    public void testRank02RankBy() throws IOException {
        String query = "A = LOAD 'test01' USING mock.Storage() AS (f1:chararray,f2:int,f3:chararray);"
            + "C = rank A by f2 ASC;"
            + "store C into 'result' using mock.Storage();";

        Util.registerMultiLineQuery(pigServer, query);

        Set<Tuple> expected = ImmutableSet.of(
                tf.newTuple(ImmutableList.of((long) 1, "A", 1, "N")),
                tf.newTuple(ImmutableList.of((long) 2, "B", 2, "N")),
                tf.newTuple(ImmutableList.of((long) 3, "C", 3, "M")),
                tf.newTuple(ImmutableList.of((long) 4, "D", 4, "P")),
                tf.newTuple(ImmutableList.of((long) 4, "E", 4, "Q")),
                tf.newTuple(ImmutableList.of((long) 4, "E", 4, "Q")),
                tf.newTuple(ImmutableList.of((long) 7, "F", 7, "Q")),
                tf.newTuple(ImmutableList.of((long) 8, "F", 8, "Q")),
                tf.newTuple(ImmutableList.of((long) 8, "F", 8, "Q")),
                tf.newTuple(ImmutableList.of((long) 8, "F", 8, "T")),
                tf.newTuple(ImmutableList.of((long) 11, "G", 10, "V")));

        verifyExpected(data.get("result"), expected);
    }

    @Test
    public void testRank03RankBy() throws IOException {
        String query = "A = LOAD 'test01' USING mock.Storage() AS (f1:chararray,f2:int,f3:chararray);"
            + "C = rank A by f1 DESC;"
            + "store C into 'result' using mock.Storage();";

        Util.registerMultiLineQuery(pigServer, query);

        Set<Tuple> expected = ImmutableSet.of(
                tf.newTuple(ImmutableList.of((long) 1, "G", 10, "V")),
                tf.newTuple(ImmutableList.of((long) 2, "F", 8, "T")),
                tf.newTuple(ImmutableList.of((long) 2, "F", 8, "Q")),
                tf.newTuple(ImmutableList.of((long) 2, "F", 8, "Q")),
                tf.newTuple(ImmutableList.of((long) 2, "F", 7, "Q")),
                tf.newTuple(ImmutableList.of((long) 6, "E", 4, "Q")),
                tf.newTuple(ImmutableList.of((long) 6, "E", 4, "Q")),
                tf.newTuple(ImmutableList.of((long) 8, "D", 4, "P")),
                tf.newTuple(ImmutableList.of((long) 9, "C", 3, "M")),
                tf.newTuple(ImmutableList.of((long) 10, "B", 2, "N")),
                tf.newTuple(ImmutableList.of((long) 11, "A", 1, "N")));

        verifyExpected(data.get("result"), expected);
    }

    @Test
    public void testRank04RankBy() throws IOException {
        String query = "A = LOAD 'test02' USING mock.Storage() AS (firstname:chararray,lastname:chararray,rownumberPrev:int,rankPrev:int,denserankPrev:int,quartilePrev:int,sales:double,postalcode:int);"
            + "C = rank A by postalcode;"
            + "store C into 'result' using mock.Storage();";

        Util.registerMultiLineQuery(pigServer, query);

        Set<Tuple> expected = ImmutableSet.of(
                tf.newTuple(ImmutableList.of((long) 1, "Michael", "Blythe", 1,1, 1, 1, 4557045.046, 98027)),
                tf.newTuple(ImmutableList.of((long) 1, "Linda","Mitchell", 2, 1, 1, 1, 5200475.231, 98027)),
                tf.newTuple(ImmutableList.of((long) 1, "Jillian", "Carson", 3,1, 1, 1, 3857163.633, 98027)),
                tf.newTuple(ImmutableList.of((long) 1, "Garrett","Vargas", 4, 1, 1, 1, 1764938.986, 98027)),
                tf.newTuple(ImmutableList.of((long) 1, "Tsvi", "Reiter",5, 1, 1, 2, 2811012.715, 98027)),
                tf.newTuple(ImmutableList.of((long) 6, "Shu", "Ito", 6,6, 2, 2, 3018725.486, 98055)),
                tf.newTuple(ImmutableList.of((long) 6, "Jose", "Saraiva",7, 6, 2, 2, 3189356.247, 98055)),
                tf.newTuple(ImmutableList.of((long) 6, "David","Campbell", 8, 6, 2, 3, 3587378.426, 98055)),
                tf.newTuple(ImmutableList.of((long) 6, "Tete", "Mensa-Annan",9, 6, 2, 3, 1931620.184, 98055)),
                tf.newTuple(ImmutableList.of((long) 6, "Lynn","Tsoflias", 10, 6, 2, 3, 1758385.926, 98055)),
                tf.newTuple(ImmutableList.of((long) 6, "Rachel", "Valdez", 11,6, 2, 4, 2241204.042, 98055)),
                tf.newTuple(ImmutableList.of((long) 6, "Jae", "Pak", 12,6, 2, 4, 5015682.375, 98055)),
                tf.newTuple(ImmutableList.of((long) 6, "Ranjit","Varkey Chudukatil", 13, 6, 2, 4, 3827950.238,98055)));

        verifyExpected(data.get("result"), expected);
    }

    @Test
    public void testRank05RankBy() throws IOException {
        String query = "A = LOAD 'test02' USING mock.Storage() AS (firstname:chararray,lastname:chararray,rownumberPrev:int,rankPrev:int,denserankPrev:int,quartilePrev:int,sales:double,postalcode:int);"
            + "C = rank A by *;"
            + "store C into 'result' using mock.Storage();";

        Util.registerMultiLineQuery(pigServer, query);

        Set<Tuple> expected = ImmutableSet.of(
                tf.newTuple(ImmutableList.of((long) 1, "David", "Campbell", 8,6, 2, 3, 3587378.426, 98055)),
                tf.newTuple(ImmutableList.of((long) 2, "Garrett","Vargas", 4, 1, 1, 1, 1764938.986, 98027)),
                tf.newTuple(ImmutableList.of((long) 3, "Jae", "Pak", 12,6, 2, 4, 5015682.375, 98055)),
                tf.newTuple(ImmutableList.of((long) 4, "Jillian","Carson", 3, 1, 1, 1, 3857163.633, 98027)),
                tf.newTuple(ImmutableList.of((long) 5, "Jose", "Saraiva",7, 6, 2, 2, 3189356.247, 98055)),
                tf.newTuple(ImmutableList.of((long) 6, "Linda","Mitchell", 2, 1, 1, 1, 5200475.231, 98027)),
                tf.newTuple(ImmutableList.of((long) 7, "Lynn", "Tsoflias", 10,6, 2, 3, 1758385.926, 98055)),
                tf.newTuple(ImmutableList.of((long) 8, "Michael","Blythe", 1, 1, 1, 1, 4557045.046, 98027)),
                tf.newTuple(ImmutableList.of((long) 9, "Rachel","Valdez", 11, 6, 2, 4, 2241204.042, 98055)),
                tf.newTuple(ImmutableList.of((long) 10, "Ranjit","Varkey Chudukatil", 13, 6, 2, 4, 3827950.238, 98055)),
                tf.newTuple(ImmutableList.of((long) 11, "Shu", "Ito", 6, 6, 2, 2, 3018725.486,98055)),
                tf.newTuple(ImmutableList.of((long) 12, "Tete", "Mensa-Annan", 9, 6, 2, 3,1931620.184, 98055)),
                tf.newTuple(ImmutableList.of((long) 13, "Tsvi", "Reiter", 5, 1, 1, 2, 2811012.715,98027)));

        verifyExpected(data.get("result"), expected);
    }

    @Test
    public void testRank06RankBy() throws IOException {
        String query = "A = LOAD 'test02' USING mock.Storage() AS (firstname:chararray,lastname:chararray,rownumberPrev:int,rankPrev:int,denserankPrev:int,quartilePrev:int,sales:double,postalcode:int);"
            + "C = rank A by $0..$2;"
            + "store C into 'result' using mock.Storage();";

        Util.registerMultiLineQuery(pigServer, query);

        Set<Tuple> expected = ImmutableSet.of(
                tf.newTuple(ImmutableList.of((long) 1, "David", "Campbell", 8, 6, 2, 3, 3587378.426, 98055)),
                tf.newTuple(ImmutableList.of((long) 2, "Garrett","Vargas", 4, 1, 1, 1, 1764938.986, 98027)),
                tf.newTuple(ImmutableList.of((long) 3, "Jae", "Pak", 12,6, 2, 4, 5015682.375, 98055)),
                tf.newTuple(ImmutableList.of((long) 4, "Jillian","Carson", 3, 1, 1, 1, 3857163.633, 98027)),
                tf.newTuple(ImmutableList.of((long) 5, "Jose", "Saraiva",7, 6, 2, 2, 3189356.247, 98055)),
                tf.newTuple(ImmutableList.of((long) 6, "Linda","Mitchell", 2, 1, 1, 1, 5200475.231, 98027)),
                tf.newTuple(ImmutableList.of((long) 7, "Lynn", "Tsoflias", 10,6, 2, 3, 1758385.926, 98055)),
                tf.newTuple(ImmutableList.of((long) 8, "Michael","Blythe", 1, 1, 1, 1, 4557045.046, 98027)),
                tf.newTuple(ImmutableList.of((long) 9, "Rachel","Valdez", 11, 6, 2, 4, 2241204.042, 98055)),
                tf.newTuple(ImmutableList.of((long) 10, "Ranjit","Varkey Chudukatil", 13, 6, 2, 4, 3827950.238, 98055)),
                tf.newTuple(ImmutableList.of((long) 11, "Shu", "Ito", 6, 6, 2, 2, 3018725.486,98055)),
                tf.newTuple(ImmutableList.of((long) 12, "Tete", "Mensa-Annan", 9, 6, 2, 3,1931620.184, 98055)),
                tf.newTuple(ImmutableList.of((long) 13, "Tsvi", "Reiter", 5, 1, 1, 2, 2811012.715,98027)));

        verifyExpected(data.get("result"), expected);
    }

    @Test
    public void testRank07RankBy() throws IOException {
        String query = "A = LOAD 'test01' USING mock.Storage() AS (f1:chararray,f2:int,f3:chararray);"
            + "C = rank A by f1..f3;"
            + "store C into 'result' using mock.Storage();";

        Util.registerMultiLineQuery(pigServer, query);

        Set<Tuple> expected = ImmutableSet.of(
                tf.newTuple(ImmutableList.of((long) 1, "A", 1, "N")),
                tf.newTuple(ImmutableList.of((long) 2, "B", 2, "N")),
                tf.newTuple(ImmutableList.of((long) 3, "C", 3, "M")),
                tf.newTuple(ImmutableList.of((long) 4, "D", 4, "P")),
                tf.newTuple(ImmutableList.of((long) 5, "E", 4, "Q")),
                tf.newTuple(ImmutableList.of((long) 5, "E", 4, "Q")),
                tf.newTuple(ImmutableList.of((long) 7, "F", 7, "Q")),
                tf.newTuple(ImmutableList.of((long) 8, "F", 8, "Q")),
                tf.newTuple(ImmutableList.of((long) 8, "F", 8, "Q")),
                tf.newTuple(ImmutableList.of((long) 10, "F", 8, "T")),
                tf.newTuple(ImmutableList.of((long) 11, "G", 10, "V")));

        verifyExpected(data.get("result"), expected);
    }

    public void verifyExpected(List<Tuple> out, Set<Tuple> expected) {
        for (Tuple tup : out) {
            assertTrue(expected + " contains " + tup, expected.contains(tup));
        }
    }

}
