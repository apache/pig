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
package org.apache.pig.impl.builtin;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.test.MiniGenericCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Lists;

import static org.apache.pig.builtin.mock.Storage.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestHiveUDTF {
    private static PigServer pigServer = null;
    private static MiniGenericCluster cluster = MiniGenericCluster.buildCluster();

    @BeforeClass
    public static void oneTimeSetup() throws ExecException {
        pigServer = new PigServer(ExecType.LOCAL);
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        cluster.shutDown();
    }

    @Test
    public void testHiveUDTFOnBagInput() throws IOException {
        Data data = resetData(pigServer);

        Tuple tuple = tuple(bag(tuple("a"), tuple("b"), tuple("c")));

        data.set("TestHiveUDTF", tuple);

        pigServer.registerQuery("define posexplode HiveUDTF('posexplode');");
        pigServer.registerQuery("A = load 'TestHiveUDTF' USING mock.Storage() as (a0:{(b0:chararray)});");
        pigServer.registerQuery("B = foreach A generate posexplode(a0);");

        Iterator<Tuple> result = pigServer.openIterator("B");
        List<Tuple> out = Lists.newArrayList(result);

        assertEquals(2, out.size());
        assertTrue("Result doesn't contain the HiveUDTF output",
                out.contains(tuple(bag(tuple(0, "a"), tuple(1, "b"), tuple(2, "c")))));
        assertTrue("Result doesn't contain an empty bag",
                out.contains(tuple(bag())));
    }

    @Test
    public void testHiveUDTFOnBagInputWithTwoProjection() throws IOException {
        Data data = resetData(pigServer);

        Tuple tuple = tuple(bag(tuple("a"), tuple("b"), tuple("c")));

        data.set("TestHiveUDTF", tuple);

        pigServer.registerQuery("define posexplode HiveUDTF('posexplode');");
        pigServer.registerQuery("A = load 'TestHiveUDTF' USING mock.Storage() as (a0:{(b0:chararray)});");
        pigServer.registerQuery("B = foreach A generate a0, posexplode(a0);");

        Iterator<Tuple> result = pigServer.openIterator("B");
        List<Tuple> out = Lists.newArrayList(result);

        assertEquals(2, out.size());
        assertTrue("Result doesn't contain the HiveUDTF output",
                out.contains(tuple(bag(tuple("a"), tuple("b"), tuple("c")), bag(tuple(0, "a"), tuple(1, "b"), tuple(2, "c")))));
        assertTrue("Result doesn't contain an empty bag",
                out.contains(tuple(null, bag())));
    }

    @Test
    public void testHiveUDTFOnClose() throws IOException {
        Data data = resetData(pigServer);

        List<Tuple> tuples = Arrays.asList(tuple("a", 1), tuple("a", 2), tuple("a", 3));

        data.set("TestHiveUDTF", tuples);

        pigServer.registerQuery("define COUNT2 HiveUDTF('org.apache.hadoop.hive.contrib.udtf.example.GenericUDTFCount2');");
        pigServer.registerQuery("a = load 'TestHiveUDTF' USING mock.Storage() as (name:chararray, id:int);");
        pigServer.registerQuery("b = foreach a generate flatten(COUNT2(name));");

        Iterator<Tuple> result = pigServer.openIterator("b");
        List<Tuple> out = Lists.newArrayList(result);

        assertEquals(2, out.size());
        assertEquals(tuple(3), out.get(0));
        assertEquals(tuple(3), out.get(1));
    }

}
