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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;

import org.apache.pig.PigServer;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.newplan.Operator;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

public class TestCubeOperator {
    private static PigServer pigServer;
    private static TupleFactory tf = TupleFactory.getInstance();
    private Data data;

    @BeforeClass
    public static void oneTimeSetUp() throws Exception {
        pigServer = new PigServer(Util.getLocalTestMode());
    }

    @Before
    public void setUp() throws Exception {

        data = resetData(pigServer);
        data.set("input", tuple("dog", "miami", 12), tuple("cat", "miami", 18),
                tuple("turtle", "tampa", 4), tuple("dog", "tampa", 14), tuple("cat", "naples", 9),
                tuple("dog", "naples", 5), tuple("turtle", "naples", 1));

        data.set("input1", tuple("u1,men,green,mango"), tuple("u2,men,red,mango"),
                tuple("u3,men,green,apple"), tuple("u4,women,red,mango"),
                tuple("u6,women,green,mango"), tuple("u7,men,red,apple"),
                tuple("u8,men,green,mango"), tuple("u9,women,red,apple"),
                tuple("u10,women,green,apple"), tuple("u11,men,red,apple"),
                tuple("u12,women,green,mango"));

        data.set("input2", tuple("dog", "miami", "white", "pet", 5));

        data.set("input3", tuple("dog", "miami", 12), tuple(null, "miami", 18));

    }

    @AfterClass
    public static void oneTimeTearDown() throws IOException {
    }

    @Test
    public void testCubeBasic() throws IOException {
        // basic correctness test
        String query = "a = load 'input' USING mock.Storage() as (x:chararray,y:chararray,z:long);"
                    + "b = cube a by cube(x,y);"
                + "c = foreach b generate flatten(group) as (type,location), COUNT_STAR(cube) as count, SUM(cube.z) as total;\n"
                + "store c into 'output' using mock.Storage();";
        Util.registerMultiLineQuery(pigServer, query);

        Set<Tuple> expected = ImmutableSet.of(
                tf.newTuple(Lists.newArrayList("cat", "miami", (long) 1, (long) 18)),
                tf.newTuple(Lists.newArrayList("cat", "naples", (long) 1, (long) 9)),
                tf.newTuple(Lists.newArrayList("cat", null, (long) 2, (long) 27)),
                tf.newTuple(Lists.newArrayList("dog", "miami", (long) 1, (long) 12)),
                tf.newTuple(Lists.newArrayList("dog", "tampa", (long) 1, (long) 14)),
                tf.newTuple(Lists.newArrayList("dog", "naples", (long) 1, (long) 5)),
                tf.newTuple(Lists.newArrayList("dog", null, (long) 3, (long) 31)),
                tf.newTuple(Lists.newArrayList("turtle", "tampa", (long) 1, (long) 4)),
                tf.newTuple(Lists.newArrayList("turtle", "naples", (long) 1, (long) 1)),
                tf.newTuple(Lists.newArrayList("turtle", null, (long) 2, (long) 5)),
                tf.newTuple(Lists.newArrayList(null, "miami", (long) 2, (long) 30)),
                tf.newTuple(Lists.newArrayList(null, "tampa", (long) 2, (long) 18)),
                tf.newTuple(Lists.newArrayList(null, "naples", (long) 3, (long) 15)),
                tf.newTuple(Lists.newArrayList(null, null, (long) 7, (long) 63)));

        List<Tuple> out = data.get("output");
        for (Tuple tup : out) {
            assertTrue(expected + " contains " + tup, expected.contains(tup));
        }

    }

    @Test
    public void testRollupBasic() throws IOException {
        // basic correctness test
        String query = "a = load 'input' USING mock.Storage() as (x:chararray,y:chararray,z:long);"
                + "b = cube a by rollup(x,y);"
                + "c = foreach b generate flatten(group) as (type,location), COUNT_STAR(cube) as count, SUM(cube.z) as total;"
                + "store c into 'output' using mock.Storage();";
        Util.registerMultiLineQuery(pigServer, query);

        Set<Tuple> expected = ImmutableSet.of(
                tf.newTuple(Lists.newArrayList("cat", "miami", (long) 1, (long) 18)),
                tf.newTuple(Lists.newArrayList("cat", "naples", (long) 1, (long) 9)),
                tf.newTuple(Lists.newArrayList("cat", null, (long) 2, (long) 27)),
                tf.newTuple(Lists.newArrayList("dog", "miami", (long) 1, (long) 12)),
                tf.newTuple(Lists.newArrayList("dog", "tampa", (long) 1, (long) 14)),
                tf.newTuple(Lists.newArrayList("dog", "naples", (long) 1, (long) 5)),
                tf.newTuple(Lists.newArrayList("dog", null, (long) 3, (long) 31)),
                tf.newTuple(Lists.newArrayList("turtle", "tampa", (long) 1, (long) 4)),
                tf.newTuple(Lists.newArrayList("turtle", "naples", (long) 1, (long) 1)),
                tf.newTuple(Lists.newArrayList("turtle", null, (long) 2, (long) 5)),
                tf.newTuple(Lists.newArrayList(null, null, (long) 7, (long) 63)));

        List<Tuple> out = data.get("output");
        for (Tuple tup : out) {
            assertTrue(expected + " contains " + tup, expected.contains(tup));
        }
    }

    @Test
    public void testCubeAndRollup() throws IOException {
        // basic correctness test
        String query = "a = load 'input2' USING mock.Storage() as (v:chararray,w:chararray,x:chararray,y:chararray,z:long);"
                + "b = cube a by cube(v,w), rollup(x,y);"
                + "c = foreach b generate flatten(group) as (type,location,color,category), COUNT_STAR(cube) as count, SUM(cube.z) as total;"
                + "store c into 'output' using mock.Storage();";
        Util.registerMultiLineQuery(pigServer, query);

        Set<Tuple> expected = ImmutableSet
                .of(tf.newTuple(Lists.newArrayList("dog", "miami", "white", "pet", (long) 1,
                        (long) 5)), tf.newTuple(Lists.newArrayList("dog", null, "white", "pet",
                        (long) 1, (long) 5)), tf.newTuple(Lists.newArrayList(null, "miami",
                        "white", "pet", (long) 1, (long) 5)), tf.newTuple(Lists.newArrayList(null,
                        null, "white", "pet", (long) 1, (long) 5)), tf.newTuple(Lists.newArrayList(
                        "dog", "miami", "white", null, (long) 1, (long) 5)), tf.newTuple(Lists
                        .newArrayList("dog", null, "white", null, (long) 1, (long) 5)), tf
                        .newTuple(Lists.newArrayList(null, "miami", "white", null, (long) 1,
                                (long) 5)), tf.newTuple(Lists.newArrayList(null, null, "white",
                        null, (long) 1, (long) 5)), tf.newTuple(Lists.newArrayList("dog", "miami",
                        null, null, (long) 1, (long) 5)), tf.newTuple(Lists.newArrayList("dog",
                        null, null, null, (long) 1, (long) 5)), tf.newTuple(Lists.newArrayList(
                        null, "miami", null, null, (long) 1, (long) 5)), tf.newTuple(Lists
                        .newArrayList(null, null, null, null, (long) 1, (long) 5)));

        List<Tuple> out = data.get("output");
        for (Tuple tup : out) {
            assertTrue(expected + " contains " + tup, expected.contains(tup));
        }

    }

    @Test
    public void testCubeMultipleIAliases() throws IOException {
        // test for input alias to cube being assigned multiple times
        String query = "a = load 'input' USING mock.Storage() as (x:chararray,y:chararray,z:long);"
                + "a = load 'input' USING mock.Storage() as (x,y:chararray,z:long);"
                + "a = load 'input' USING mock.Storage() as (x:chararray,y:chararray,z:long);"
                + "b = cube a by cube(x,y);"
                + "c = foreach b generate flatten(group) as (type,location), COUNT_STAR(cube) as count, SUM(cube.z) as total;"
                + "store c into 'output' using mock.Storage();";

        Util.registerMultiLineQuery(pigServer, query);

        Set<Tuple> expected = ImmutableSet.of(
                tf.newTuple(Lists.newArrayList("cat", "miami", (long) 1, (long) 18)),
                tf.newTuple(Lists.newArrayList("cat", "naples", (long) 1, (long) 9)),
                tf.newTuple(Lists.newArrayList("cat", null, (long) 2, (long) 27)),
                tf.newTuple(Lists.newArrayList("dog", "miami", (long) 1, (long) 12)),
                tf.newTuple(Lists.newArrayList("dog", "tampa", (long) 1, (long) 14)),
                tf.newTuple(Lists.newArrayList("dog", "naples", (long) 1, (long) 5)),
                tf.newTuple(Lists.newArrayList("dog", null, (long) 3, (long) 31)),
                tf.newTuple(Lists.newArrayList("turtle", "tampa", (long) 1, (long) 4)),
                tf.newTuple(Lists.newArrayList("turtle", "naples", (long) 1, (long) 1)),
                tf.newTuple(Lists.newArrayList("turtle", null, (long) 2, (long) 5)),
                tf.newTuple(Lists.newArrayList(null, "miami", (long) 2, (long) 30)),
                tf.newTuple(Lists.newArrayList(null, "tampa", (long) 2, (long) 18)),
                tf.newTuple(Lists.newArrayList(null, "naples", (long) 3, (long) 15)),
                tf.newTuple(Lists.newArrayList(null, null, (long) 7, (long) 63)));

        List<Tuple> out = data.get("output");
        for (Tuple tup : out) {
            assertTrue(expected + " contains " + tup, expected.contains(tup));
        }

    }

    @Test
    public void testCubeAfterForeach() throws IOException {
        // test for foreach projection before cube operator
        String query = "a = load 'input' USING mock.Storage() as (x:chararray,y:chararray,z:long);"
                + "b = foreach a generate x as type,y as location,z as number;"
                + "c = cube b by cube(type,location);"
                + "d = foreach c generate flatten(group) as (type,location), COUNT_STAR(cube) as count, SUM(cube.number) as total;"
                + "store d into 'output' using mock.Storage();";

        Util.registerMultiLineQuery(pigServer, query);

        Set<Tuple> expected = ImmutableSet.of(
                tf.newTuple(Lists.newArrayList("cat", "miami", (long) 1, (long) 18)),
                tf.newTuple(Lists.newArrayList("cat", "naples", (long) 1, (long) 9)),
                tf.newTuple(Lists.newArrayList("cat", null, (long) 2, (long) 27)),
                tf.newTuple(Lists.newArrayList("dog", "miami", (long) 1, (long) 12)),
                tf.newTuple(Lists.newArrayList("dog", "tampa", (long) 1, (long) 14)),
                tf.newTuple(Lists.newArrayList("dog", "naples", (long) 1, (long) 5)),
                tf.newTuple(Lists.newArrayList("dog", null, (long) 3, (long) 31)),
                tf.newTuple(Lists.newArrayList("turtle", "tampa", (long) 1, (long) 4)),
                tf.newTuple(Lists.newArrayList("turtle", "naples", (long) 1, (long) 1)),
                tf.newTuple(Lists.newArrayList("turtle", null, (long) 2, (long) 5)),
                tf.newTuple(Lists.newArrayList(null, "miami", (long) 2, (long) 30)),
                tf.newTuple(Lists.newArrayList(null, "tampa", (long) 2, (long) 18)),
                tf.newTuple(Lists.newArrayList(null, "naples", (long) 3, (long) 15)),
                tf.newTuple(Lists.newArrayList(null, null, (long) 7, (long) 63)));

        List<Tuple> out = data.get("output");
        for (Tuple tup : out) {
            assertTrue(expected + " contains " + tup, expected.contains(tup));
        }

    }

    @Test
    public void testCubeAfterLimit() throws IOException {
        // test for limit operator before cube operator
        String query = "a = load 'input' USING mock.Storage() as (x:chararray,y:chararray,z:long);"
                + "b = limit a 2;" + "c = cube b by cube(x,y);"
                + "d = foreach c generate flatten(group) as (x,y), SUM(cube.z) as total;"
                + "store d into 'output' using mock.Storage();";

        Util.registerMultiLineQuery(pigServer, query);

        Set<Tuple> expected = ImmutableSet.of(
                tf.newTuple(Lists.newArrayList("cat", "miami", (long) 18)),
                tf.newTuple(Lists.newArrayList("cat", null, (long) 18)),
                tf.newTuple(Lists.newArrayList("dog", "miami", (long) 12)),
                tf.newTuple(Lists.newArrayList("dog", null, (long) 12)),
                tf.newTuple(Lists.newArrayList(null, "miami", (long) 30)),
                tf.newTuple(Lists.newArrayList(null, null, (long) 30)));

        List<Tuple> out = data.get("output");
        for (Tuple tup : out) {
            assertTrue(expected + " contains " + tup, expected.contains(tup));
        }

    }

    @Test
    public void testCubeWithStar() throws IOException {
        // test for * (all) dimensions in cube operator
        String query = "a = load 'input' USING mock.Storage() as (x:chararray,y:chararray);"
                + "b = foreach a generate x as type,y as location;"
                + "c = cube b by cube(*);"
                + "d = foreach c generate flatten(group) as (type,location), COUNT_STAR(cube) as count;"
                + "store d into 'output' using mock.Storage();";

        Util.registerMultiLineQuery(pigServer, query);

        Set<Tuple> expected = ImmutableSet.of(
                tf.newTuple(Lists.newArrayList("cat", "miami", (long) 1)),
                tf.newTuple(Lists.newArrayList("cat", "naples", (long) 1)),
                tf.newTuple(Lists.newArrayList("cat", null, (long) 2)),
                tf.newTuple(Lists.newArrayList("dog", "miami", (long) 1)),
                tf.newTuple(Lists.newArrayList("dog", "tampa", (long) 1)),
                tf.newTuple(Lists.newArrayList("dog", "naples", (long) 1)),
                tf.newTuple(Lists.newArrayList("dog", null, (long) 3)),
                tf.newTuple(Lists.newArrayList("turtle", "tampa", (long) 1)),
                tf.newTuple(Lists.newArrayList("turtle", "naples", (long) 1)),
                tf.newTuple(Lists.newArrayList("turtle", null, (long) 2)),
                tf.newTuple(Lists.newArrayList(null, "miami", (long) 2)),
                tf.newTuple(Lists.newArrayList(null, "tampa", (long) 2)),
                tf.newTuple(Lists.newArrayList(null, "naples", (long) 3)),
                tf.newTuple(Lists.newArrayList(null, null, (long) 7)));

        List<Tuple> out = data.get("output");
        for (Tuple tup : out) {
            assertTrue(expected + " contains " + tup, expected.contains(tup));
        }

    }

    @Test
    public void testCubeWithRange() throws IOException {
        // test for range projection of dimensions in cube operator
        String query = "a = load 'input' USING mock.Storage() as (x:chararray,y:chararray,z:long);"
                + "b = foreach a generate x as type,y as location, z as number;"
                + "c = cube b by cube($0..$1);"
                + "d = foreach c generate flatten(group) as (type,location), COUNT_STAR(cube) as count, SUM(cube.number) as total;"
                + "store d into 'output' using mock.Storage();";

        Util.registerMultiLineQuery(pigServer, query);

        Set<Tuple> expected = ImmutableSet.of(
                tf.newTuple(Lists.newArrayList("cat", "miami", (long) 1, (long) 18)),
                tf.newTuple(Lists.newArrayList("cat", "naples", (long) 1, (long) 9)),
                tf.newTuple(Lists.newArrayList("cat", null, (long) 2, (long) 27)),
                tf.newTuple(Lists.newArrayList("dog", "miami", (long) 1, (long) 12)),
                tf.newTuple(Lists.newArrayList("dog", "tampa", (long) 1, (long) 14)),
                tf.newTuple(Lists.newArrayList("dog", "naples", (long) 1, (long) 5)),
                tf.newTuple(Lists.newArrayList("dog", null, (long) 3, (long) 31)),
                tf.newTuple(Lists.newArrayList("turtle", "tampa", (long) 1, (long) 4)),
                tf.newTuple(Lists.newArrayList("turtle", "naples", (long) 1, (long) 1)),
                tf.newTuple(Lists.newArrayList("turtle", null, (long) 2, (long) 5)),
                tf.newTuple(Lists.newArrayList(null, "miami", (long) 2, (long) 30)),
                tf.newTuple(Lists.newArrayList(null, "tampa", (long) 2, (long) 18)),
                tf.newTuple(Lists.newArrayList(null, "naples", (long) 3, (long) 15)),
                tf.newTuple(Lists.newArrayList(null, null, (long) 7, (long) 63)));

        List<Tuple> out = data.get("output");
        for (Tuple tup : out) {
            assertTrue(expected + " contains " + tup, expected.contains(tup));
        }

    }

    @Test
    public void testCubeDuplicateDimensions() throws IOException {
        // test for cube operator with duplicate dimensions
        String query = "a = load 'input' USING mock.Storage() as (x:chararray,y:chararray,z:long);"
                + "b = foreach a generate x as type,y as location, z as number;"
                + "c = cube b by cube($0..$1,$0..$1);"
                + "d = foreach c generate flatten(group), COUNT_STAR(cube) as count, SUM(cube.number) as total;"
                + "store d into 'output' using mock.Storage();";

        try {
            Util.registerMultiLineQuery(pigServer, query);
            pigServer.openIterator("d");
        } catch (FrontendException e) {
            // FEException with 'duplicate dimensions detected' message is throw
            return;
        }

        Assert.fail("Expected to throw an exception when duplicate dimensions are detected!");

    }

    @Test
    public void testCubeAfterFilter() throws IOException {
        // test for filtering before cube operator
        String query = "a = load 'input' USING mock.Storage() as (x:chararray,y:chararray,z:long);"
                + "b = filter a by x == 'dog';"
                + "c = cube b by cube(x,y);"
                + "d = foreach c generate flatten(group), COUNT_STAR(cube) as count, SUM(cube.z) as total;"
                + "store d into 'output' using mock.Storage();";

        Util.registerMultiLineQuery(pigServer, query);
        // Iterator<Tuple> it = pigServer.openIterator("d");

        Set<Tuple> expected = ImmutableSet.of(
                tf.newTuple(Lists.newArrayList("dog", "miami", (long) 1, (long) 12)),
                tf.newTuple(Lists.newArrayList("dog", "tampa", (long) 1, (long) 14)),
                tf.newTuple(Lists.newArrayList("dog", "naples", (long) 1, (long) 5)),
                tf.newTuple(Lists.newArrayList("dog", null, (long) 3, (long) 31)),
                tf.newTuple(Lists.newArrayList(null, "miami", (long) 1, (long) 12)),
                tf.newTuple(Lists.newArrayList(null, "tampa", (long) 1, (long) 14)),
                tf.newTuple(Lists.newArrayList(null, "naples", (long) 1, (long) 5)),
                tf.newTuple(Lists.newArrayList(null, null, (long) 3, (long) 31)));

        List<Tuple> out = data.get("output");
        for (Tuple tup : out) {
            assertTrue(expected + " contains " + tup, expected.contains(tup));
        }

    }

    @Test
    public void testCubeAfterOrder() throws IOException {
        // test for ordering before cube operator
        String query = "a = load 'input' USING mock.Storage() as (x:chararray,y:chararray,z:long);"
                + "b = order a by $2;"
                + "c = cube b by cube(x,y);"
                + "d = foreach c generate flatten(group), COUNT_STAR(cube) as count, SUM(cube.z) as total;"
                + "store d into 'output' using mock.Storage();";

        Util.registerMultiLineQuery(pigServer, query);

        Set<Tuple> expected = ImmutableSet.of(
                tf.newTuple(Lists.newArrayList("cat", "miami", (long) 1, (long) 18)),
                tf.newTuple(Lists.newArrayList("cat", "naples", (long) 1, (long) 9)),
                tf.newTuple(Lists.newArrayList("cat", null, (long) 2, (long) 27)),
                tf.newTuple(Lists.newArrayList("dog", "miami", (long) 1, (long) 12)),
                tf.newTuple(Lists.newArrayList("dog", "tampa", (long) 1, (long) 14)),
                tf.newTuple(Lists.newArrayList("dog", "naples", (long) 1, (long) 5)),
                tf.newTuple(Lists.newArrayList("dog", null, (long) 3, (long) 31)),
                tf.newTuple(Lists.newArrayList("turtle", "tampa", (long) 1, (long) 4)),
                tf.newTuple(Lists.newArrayList("turtle", "naples", (long) 1, (long) 1)),
                tf.newTuple(Lists.newArrayList("turtle", null, (long) 2, (long) 5)),
                tf.newTuple(Lists.newArrayList(null, "miami", (long) 2, (long) 30)),
                tf.newTuple(Lists.newArrayList(null, "tampa", (long) 2, (long) 18)),
                tf.newTuple(Lists.newArrayList(null, "naples", (long) 3, (long) 15)),
                tf.newTuple(Lists.newArrayList(null, null, (long) 7, (long) 63)));

        List<Tuple> out = data.get("output");
        for (Tuple tup : out) {
            assertTrue(expected + " contains " + tup, expected.contains(tup));
        }
    }

    @Test
    public void testCubeAfterJoin() throws IOException {
        // test for cubing on joined relations
        String query = "a = load 'input1' USING mock.Storage() as (a1:chararray,b1,c1,d1); "
                + "b = load 'input' USING mock.Storage() as (a2,b2,c2:long,d2:chararray);"
                + "c = join a by a1, b by d2;"
                + "d = cube c by cube($4,$5);"
                + "e = foreach d generate flatten(group), COUNT_STAR(cube) as count, SUM(cube.c2) as total;"
                + "store e into 'output' using mock.Storage();";

        Util.registerMultiLineQuery(pigServer, query);

        Set<Tuple> expected = ImmutableSet.of(
                tf.newTuple(Lists.newArrayList("cat", "miami", (long) 1, (long) 18)),
                tf.newTuple(Lists.newArrayList("cat", null, (long) 1, (long) 18)),
                tf.newTuple(Lists.newArrayList("dog", "miami", (long) 1, (long) 12)),
                tf.newTuple(Lists.newArrayList("dog", "tampa", (long) 1, (long) 14)),
                tf.newTuple(Lists.newArrayList("dog", null, (long) 2, (long) 26)),
                tf.newTuple(Lists.newArrayList("turtle", "tampa", (long) 1, (long) 4)),
                tf.newTuple(Lists.newArrayList("turtle", "naples", (long) 1, (long) 1)),
                tf.newTuple(Lists.newArrayList("turtle", null, (long) 2, (long) 5)),
                tf.newTuple(Lists.newArrayList(null, "miami", (long) 2, (long) 30)),
                tf.newTuple(Lists.newArrayList(null, "tampa", (long) 2, (long) 18)),
                tf.newTuple(Lists.newArrayList(null, "naples", (long) 1, (long) 1)),
                tf.newTuple(Lists.newArrayList(null, null, (long) 5, (long) 49)));

        List<Tuple> out = data.get("output");
        for (Tuple tup : out) {
            assertTrue(expected + " contains " + tup, expected.contains(tup));
        }
    }

    @Test
    public void testCubeAfterCogroup() throws IOException {
        // test for cubing on co-grouped relation
        String query = "a = load 'input1' USING mock.Storage() as (a1:chararray,b1,c1,d1); "
                + "b = load 'input' USING mock.Storage() as (a2,b2,c2:long,d2:chararray);"
                + "c = cogroup a by a1, b by d2;"
                + "d = foreach c generate flatten(a), flatten(b);"
                + "e = cube d by cube(a2,b2);"
                + "f = foreach e generate flatten(group), COUNT_STAR(cube) as count, SUM(cube.c2) as total;"
                + "store f into 'output' using mock.Storage();";

        Util.registerMultiLineQuery(pigServer, query);

        Set<Tuple> expected = ImmutableSet.of(
                tf.newTuple(Lists.newArrayList("cat", "miami", (long) 1, (long) 18)),
                tf.newTuple(Lists.newArrayList("cat", null, (long) 1, (long) 18)),
                tf.newTuple(Lists.newArrayList("dog", "miami", (long) 1, (long) 12)),
                tf.newTuple(Lists.newArrayList("dog", "tampa", (long) 1, (long) 14)),
                tf.newTuple(Lists.newArrayList("dog", null, (long) 2, (long) 26)),
                tf.newTuple(Lists.newArrayList("turtle", "tampa", (long) 1, (long) 4)),
                tf.newTuple(Lists.newArrayList("turtle", "naples", (long) 1, (long) 1)),
                tf.newTuple(Lists.newArrayList("turtle", null, (long) 2, (long) 5)),
                tf.newTuple(Lists.newArrayList(null, "miami", (long) 2, (long) 30)),
                tf.newTuple(Lists.newArrayList(null, "tampa", (long) 2, (long) 18)),
                tf.newTuple(Lists.newArrayList(null, "naples", (long) 1, (long) 1)),
                tf.newTuple(Lists.newArrayList(null, null, (long) 5, (long) 49)));

        List<Tuple> out = data.get("output");
        for (Tuple tup : out) {
            assertTrue(expected + " contains " + tup, expected.contains(tup));
        }
    }

    @Test
    public void testCubeWithNULLs() throws IOException {
        // test for dimension values with legitimate null values
        String query = "a = load 'input3' USING mock.Storage() as (x:chararray,y:chararray,z:long);"
                + "b = cube a by cube(x,y);"
                + "c = foreach b generate flatten(group) as (type,location), SUM(cube.z) as total;"
                + "store c into 'output' using mock.Storage();";

        Util.registerMultiLineQuery(pigServer, query);

        Set<Tuple> expected = ImmutableSet.of(
                tf.newTuple(Lists.newArrayList("dog", "miami", (long) 12)),
                tf.newTuple(Lists.newArrayList("dog", null, (long) 12)),
                tf.newTuple(Lists.newArrayList(null, "miami", (long) 30)),
                tf.newTuple(Lists.newArrayList(null, null, (long) 30)),
                tf.newTuple(Lists.newArrayList("unknown", "miami", (long) 18)),
                tf.newTuple(Lists.newArrayList("unknown", null, (long) 18)));

        List<Tuple> out = data.get("output");
        for (Tuple tup : out) {
            assertTrue(expected + " contains " + tup, expected.contains(tup));
        }

    }

    @Test
    public void testCubeWithNULLAndFilter() throws IOException {
        // test for dimension values with legitimate null values
        // followed by filter
        String query = "a = load 'input3' USING mock.Storage() as (x:chararray,y:chararray,z:long);"
                + "b = cube a by cube(x,y);"
                + "c = foreach b generate flatten(group) as (type,location), SUM(cube.z) as total;"
                + "d = filter c by type!='unknown';"
                + "store d into 'output' using mock.Storage();";

        Util.registerMultiLineQuery(pigServer, query);

        Set<Tuple> expected = ImmutableSet.of(
                tf.newTuple(Lists.newArrayList("dog", "miami", (long) 12)),
                tf.newTuple(Lists.newArrayList("dog", null, (long) 12)));

        List<Tuple> out = data.get("output");
        for (Tuple tup : out) {
            assertTrue(expected + " contains " + tup, expected.contains(tup));
        }

    }

    @Test
    public void testRollupAfterCogroup() throws IOException {
        // test for cubing on co-grouped relation
        String query = "a = load 'input1' USING mock.Storage() as (a1:chararray,b1,c1,d1); "
                + "b = load 'input' USING mock.Storage() as (a2,b2,c2:long,d2:chararray);"
                + "c = cogroup a by a1, b by d2;"
                + "d = foreach c generate flatten(a), flatten(b);"
                + "e = cube d by rollup(a2,b2);"
                + "f = foreach e generate flatten(group), COUNT(cube) as count, SUM(cube.c2) as total;"
                + "store f into 'output' using mock.Storage();";

        Util.registerMultiLineQuery(pigServer, query);

        Set<Tuple> expected = ImmutableSet.of(
                tf.newTuple(Lists.newArrayList("cat", "miami", (long) 1, (long) 18)),
                tf.newTuple(Lists.newArrayList("cat", null, (long) 1, (long) 18)),
                tf.newTuple(Lists.newArrayList("dog", "miami", (long) 1, (long) 12)),
                tf.newTuple(Lists.newArrayList("dog", "tampa", (long) 1, (long) 14)),
                tf.newTuple(Lists.newArrayList("dog", null, (long) 2, (long) 26)),
                tf.newTuple(Lists.newArrayList("turtle", "tampa", (long) 1, (long) 4)),
                tf.newTuple(Lists.newArrayList("turtle", "naples", (long) 1, (long) 1)),
                tf.newTuple(Lists.newArrayList("turtle", null, (long) 2, (long) 5)),
                tf.newTuple(Lists.newArrayList(null, null, (long) 5, (long) 49)));

        List<Tuple> out = data.get("output");
        for (Tuple tup : out) {
            assertTrue(expected + " contains " + tup, expected.contains(tup));
        }
    }

    @Test
    public void testIllustrate() throws Exception {
	// test for illustrate
        Assume.assumeTrue("illustrate does not work in tez (PIG-3993)", !Util.getLocalTestMode().toString().startsWith("TEZ"));
	String query = "a = load 'input' USING mock.Storage() as (a1:chararray,b1:chararray,c1:long); "
	        + "b = cube a by cube(a1,b1);";

        Util.registerMultiLineQuery(pigServer, query);
        Map<Operator, DataBag> examples = pigServer.getExamples("b");
        assertTrue(examples != null);
    }

    @Test
    public void testExplainCube() throws IOException {
        // test for explain
        String query = "a = load 'input' USING mock.Storage() as (a1:chararray,b1:chararray,c1:long); "
                + "b = cube a by cube(a1,b1);";

        Util.registerMultiLineQuery(pigServer, query);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        pigServer.explain("b", ps);
        assertTrue(baos.toString().contains("CubeDimensions"));
    }

    @Test
    public void testExplainRollup() throws IOException {
        // test for explain
        String query = "a = load 'input' USING mock.Storage() as (a1:chararray,b1:chararray,c1:long); "
                + "b = cube a by rollup(a1,b1);";

        Util.registerMultiLineQuery(pigServer, query);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        pigServer.explain("b", ps);
        assertTrue(baos.toString().contains("RollupDimensions"));
    }

    @Test
    public void testDescribe() throws IOException {
        // test for describe
        String query = "a = load 'input' USING mock.Storage() as (a1:chararray,b1:chararray,c1:long); "
                + "b = cube a by cube(a1,b1);";

        Util.registerMultiLineQuery(pigServer, query);
        Schema sch = pigServer.dumpSchema("b");
        for (String alias : sch.getAliases()) {
            if (alias.compareTo("cube") == 0) {
            assertTrue(alias.contains("cube"));
            }
        }
    }
}
