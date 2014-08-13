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
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.junit.Before;
import org.junit.Test;


public class TestNullConstant {
    private PigServer pigServer;

    @Before
    public void setUp() throws Exception{
        pigServer = new PigServer(ExecType.LOCAL);
    }

    @Test
    public void testArithExpressions() throws IOException, ExecException {
        Data data = resetData(pigServer);
        data.set("foo", tuple(10, 11.0));

        pigServer.registerQuery("a = load 'foo' using mock.Storage() as (x:int, y:double);");
        pigServer.registerQuery("b = foreach a generate x + null, x * null, x / null, x - null, null % x, " +
                "y + null, y * null, y / null, y - null;");
        Iterator<Tuple> it = pigServer.openIterator("b");
        Tuple t = it.next();
        for (int i = 0; i < 9; i++) {
            assertNull(t.get(i));
        }
    }

    @Test
    public void testBinCond() throws IOException, ExecException {
        Data data = resetData(pigServer);
        data.set("foo", tuple(10, 11.0));

        pigServer.registerQuery("a = load 'foo' using mock.Storage() as (x:int, y:double);");
        pigServer.registerQuery("b = foreach a generate (2 > 1? null : 1), ( 2 < 1 ? null : 1), (2 > 1 ? 1 : null), ( 2 < 1 ? 1 : null);");
        Iterator<Tuple> it = pigServer.openIterator("b");
        Tuple t = it.next();
        Object[] result = new Object[] { null, 1, 1, null};
        for (int i = 0; i < 4; i++) {
            assertEquals(result[i], t.get(i));
        }

        // is null and is not null test
        pigServer.registerQuery("b = foreach a generate (null is null ? 1 : 2), ( null is not null ? 2 : 1);");
        it = pigServer.openIterator("b");
        t = it.next();
        for (int i = 0; i < 2; i++) {
            assertEquals(1, t.get(i));
        }
    }

    @Test
    public void testForeachGenerate() throws ExecException, IOException {
        Data data = resetData(pigServer);
        data.set("foo", tuple(10, 11.0));

        pigServer.registerQuery("a = load 'foo' using mock.Storage() as (x:int, y:double);");
        pigServer.registerQuery("b = foreach a generate x, null, y, null;");
        Iterator<Tuple> it = pigServer.openIterator("b");
        Tuple t = it.next();
        Object[] result = new Object[] { 10, null, 11.0, null};
        for (int i = 0; i < 4; i++) {
            assertEquals(result[i], t.get(i));
        }
    }

    @Test
    public void testOuterJoin() throws IOException, ExecException {
        Data data = resetData(pigServer);
        data.set("foo_left", tuple(10, "will_join"), tuple(11, "will_not_join"));
        data.set("foo_right", tuple(10, "will_join"), tuple(12, "will_not_join"));
        pigServer.registerQuery("a = load 'foo_left' using mock.Storage() as (x:int, y:chararray);");
        pigServer.registerQuery("b = load 'foo_right' using mock.Storage() as (u:int, v:chararray);");
        pigServer.registerQuery("c = cogroup a by x, b by u;");
        pigServer.registerQuery("d = foreach c generate flatten((SIZE(a) == 0 ? null: a)), flatten((SIZE(b) == 0 ? null : b));");
        Iterator<Tuple> it = pigServer.openIterator("d");
        Object[][] results = new Object[][]{{10, "will_join", 10, "will_join"}, {11, "will_not_join", null}, {null, 12, "will_not_join"}};
        int i = 0;
        while(it.hasNext()) {

            Tuple t = it.next();
            Object[] result = results[i++];
            assertEquals(result.length, t.size());
            for (int j = 0; j < result.length; j++) {
                assertEquals(result[j], t.get(j));
            }
        }
    }

    @Test
    public void testConcatAndSize() throws IOException, ExecException {
        Data data = resetData(pigServer);
        data.set("foo", tuple(10, 11.0, "will_join"));

        pigServer.registerQuery("a = load 'foo' using mock.Storage() as (x:int, y:double, str:chararray);");
        pigServer.registerQuery("b = foreach a generate SIZE(null), CONCAT(str, null), " +
                "CONCAT(null, str);");
        Iterator<Tuple> it = pigServer.openIterator("b");
        Tuple t = it.next();
        for (int i = 0; i < 3; i++) {
            assertNull(t.get(i));
        }
    }

    @Test
    public void testExplicitCast() throws IOException, ExecException {
        Data data = resetData(pigServer);
        data.set("foo", tuple(10, 11.0, "will_join"));

        pigServer.registerQuery("a = load 'foo' using mock.Storage() as (x:int, y:double, str:chararray);");
        pigServer.registerQuery("b = foreach a generate (int)null, (double)null, (chararray)null, (map[])null;");
        Iterator<Tuple> it = pigServer.openIterator("b");
        Tuple t = it.next();
        for (int i = 0; i < 3; i++) {
            assertNull(t.get(i));
        }
    }

    @Test
    public void testComplexNullConstants() throws IOException, ExecException {
        Data data = resetData(pigServer);
        data.set("foo", tuple(10, 11.0, "will_join"));

        pigServer.registerQuery("a = load 'foo' using mock.Storage() as (x:int, y:double, str:chararray);");
        pigServer.registerQuery("b = foreach a generate {(null)}, ['2'#null];");
        Iterator<Tuple> it = pigServer.openIterator("b");
        Tuple t = it.next();
        assertNull(((DataBag)t.get(0)).iterator().next().get(0));
        assertNull(((Map<String, Object>)t.get(1)).get("2"));
    }

    @Test(expected = FrontendException.class)
    public void testMapNullKeyFailure() throws IOException {
        Data data = resetData(pigServer);
        data.set("foo", tuple(10, 11.0, "will_join"));

        pigServer.registerQuery("a = load 'foo' using mock.Storage() as (x:int, y:double, str:chararray);");
        pigServer.registerQuery("b = foreach a generate [null#'2'];");
    }
}
