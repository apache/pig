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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.Utils;
import org.junit.Before;
import org.junit.Test;
import org.python.google.common.collect.Sets;

import com.google.common.collect.ImmutableSet;

public class TestBuiltinInvoker {
    private static PigServer pigServer;
    private static Data data;
    private static Set<Tuple> chardata = ImmutableSet.of(tuple("a"),tuple("b"),tuple("c"));
    private static Set<Tuple> intdata = ImmutableSet.of(tuple(1),tuple(2),tuple(3));
    private static Set<Tuple> charintdata = ImmutableSet.of(tuple("1"),tuple("2"),tuple("3"));
    private static TupleFactory mTupleFactory = TupleFactory.getInstance();
    private static Random r;

    @Before
    public void setUp() throws Exception {
        pigServer = new PigServer(ExecType.LOCAL);

        data = resetData(pigServer);

        data.set("chardata", Utils.getSchemaFromString("x:chararray"), chardata);
        data.set("charintdata", Utils.getSchemaFromString("x:chararray"), charintdata);

        r = new Random(42L);
    }

    @Test
    public void testConcat() throws Exception {
        Set<Tuple> expected = Sets.newHashSet();
        for (Tuple t : chardata) {
            String str = (String)t.get(0);
            expected.add(mTupleFactory.newTuple(str + str));
        }

        pigServer.registerQuery("a = load 'chardata' using mock.Storage();");
        pigServer.registerQuery("b = foreach @ generate invoke(x)concat(x);");
        pigServer.registerQuery("store b into 'res' using mock.Storage();");
        List<Tuple> results = data.get("res");
        dataIsEqual(expected, results);
    }

    @Test
    public void testValueOf() throws Exception {
        Set<Tuple> expected = Sets.newHashSet();
        for (Tuple t : charintdata) {
            String str = (String)t.get(0);
            expected.add(mTupleFactory.newTuple(Integer.valueOf(str)));
        }

        pigServer.registerQuery("a = load 'charintdata' using mock.Storage();");
        pigServer.registerQuery("b = foreach @ generate invoke&Integer.valueOf(x);");
        pigServer.registerQuery("store b into 'res' using mock.Storage();");
        List<Tuple> results = data.get("res");
        dataIsEqual(expected, results);
    }

    @Test
    public void testManyConcat() throws Exception {
        Set<Tuple> expected = Sets.newHashSet();
        for (Tuple t : chardata) {
            String str = (String)t.get(0);
            expected.add(mTupleFactory.newTuple(str + str + str + str));
        }

        pigServer.registerQuery("a = load 'chardata' using mock.Storage();");
        pigServer.registerQuery("b = foreach @ generate invoke(invoke(invoke(x)concat(x))concat(x))concat(x);");
        pigServer.registerQuery("store b into 'res' using mock.Storage();");
        List<Tuple> results = data.get("res");
        dataIsEqual(expected, results);
    }

    @Test
    public void testTupleSize() throws Exception {
        pigServer.registerQuery("a = load 'chardata' using mock.Storage();");
        pigServer.registerQuery("b = foreach @ generate invoke(TOTUPLE(x))size();");
        pigServer.registerQuery("store b into 'res' using mock.Storage();");
        List<Tuple> results = data.get("res");
        assertEquals(3, results.size());
        for (Tuple t : results) {
            assertEquals(Integer.valueOf(1), (Integer)t.get(0));
        }
    }

    @Test
    public void testStringSize() throws Exception {
        Set<Tuple> input = Sets.newHashSet();
        Set<Tuple> expected = Sets.newHashSet();
        for (int i = 0; i < 1000; i++) {
            String val = Integer.toString(r.nextInt());
            input.add(tuple(val));
            expected.add(tuple(val, val.length()));
        }
        data.set("foo", Utils.getSchemaFromString("x:chararray"), input);

        pigServer.registerQuery("a = load 'foo' using mock.Storage();");
        pigServer.registerQuery("b = foreach @ generate $0, invoke($0)length();");
        pigServer.registerQuery("store b into 'bar' using mock.Storage();");

        dataIsEqual(expected, data.get("bar"));
    }

    private void dataIsEqual(Set<Tuple> expected, Collection<Tuple> results) {
        assertEquals(expected.size(), results.size());
        for (Tuple t : results) {
            assertTrue(expected.remove(t));
        }
        assertEquals(0, expected.size());
    }
}
