/**
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

package org.apache.pig.builtin;

import static org.apache.pig.builtin.mock.Storage.resetData;
import static org.apache.pig.builtin.mock.Storage.tuple;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;
import org.junit.Before;
import org.junit.Test;

public class TestPluckTuple {
    private PigServer pigServer;

    @Before
    public void setUp() throws Exception {
        pigServer = new PigServer(ExecType.LOCAL);
    }

    @Test
    public void testStartsWith() throws Exception {
        String query = "a = load 'a' as (x:int,y:chararray,z:long);" +
                       "b = load 'b' as (x:int,y:chararray,z:long);" +
                       "c = join a by x, b by x;" +
                       "define pluck PluckTuple('a::');" +
                       "d = foreach c generate flatten(pluck(*));";
        pigServer.registerQuery(query);
        assertTrue(Schema.equals(pigServer.dumpSchema("a"), pigServer.dumpSchema("d"), false, true));
    }

    @Test
    public void testPatternMatches() throws Exception {
        String query = "a1 = load 'a1' as (x:int,y:chararray,z:long);" +
                "a2 = load 'a2' as (x:int,y:chararray,z:long);" +
                "b = join a1 by x, a2 by x;" +
                "define pluck PluckTuple('a[2|3]::.*');" +
                "c = foreach b generate flatten(pluck(*));";
        pigServer.registerQuery(query);
        assertTrue(Schema.equals(pigServer.dumpSchema("a2"), pigServer.dumpSchema("c"), false, true));
    }

    @Test
    public void testOutput() throws Exception {
        Data data = resetData(pigServer);

        Tuple exp1 = tuple(1, "hey", 2L);
        Tuple exp2 = tuple(2, "woah", 3L);

        data.set("a",
            Utils.getSchemaFromString("x:int,y:chararray,z:long"),
            exp1,
            exp2,
            tuple(3, "c", 4L)
            );
        data.set("b",
            Utils.getSchemaFromString("x:int,y:chararray,z:long"),
            tuple(1, "sasf", 5L),
            tuple(2, "woah", 6L),
            tuple(4, "c", 7L)
            );

        String query = "a = load 'a' using mock.Storage();" +
            "b = load 'b' using mock.Storage();" +
            "c = join a by x, b by x;" +
            "define pluck PluckTuple('a::');" +
            "d = foreach c generate flatten(pluck(*));";
        pigServer.registerQuery(query);
        Iterator<Tuple> it = pigServer.openIterator("d");
        assertTrue(it.hasNext());
        assertEquals(exp1, it.next());
        assertTrue(it.hasNext());
        assertEquals(exp2, it.next());
        assertFalse(it.hasNext());
    }

    @Test
    public void testExcludeColumns() throws Exception {
        String query = "a1 = load 'a1' as (x:int,y:chararray,z:long);" +
                "a2 = load 'a2' as (x:int,y:chararray,z:long);" +
                "a3 = foreach a2 generate x, y;" +
                "b = join a1 by x, a2 by x;" +
                "define pluck PluckTuple('a[2|3]::.*','z');" +
                "c = foreach b generate flatten(pluck(*));";
        pigServer.registerQuery(query);
        assertTrue(Schema.equals(pigServer.dumpSchema("a3"), pigServer.dumpSchema("c"), false, true));
    }

    @Test
    public void testExcludeColumnsOutput() throws Exception {
        Data data = resetData(pigServer);

        Tuple exp1 = tuple(1, "hey", 2L);
        Tuple exp2 = tuple(2, "woah", 3L);

        Tuple exp1_excl = tuple(1, 2L);
        Tuple exp2_excl = tuple(2, 3L);

        data.set("a",
            Utils.getSchemaFromString("x:int,y:chararray,z:long"),
            exp1,
            exp2,
            tuple(3, "c", 4L)
            );
        data.set("b",
            Utils.getSchemaFromString("x:int,y:chararray,z:long"),
            tuple(1, "sasf", 5L),
            tuple(2, "woah", 6L),
            tuple(4, "c", 7L)
            );

        String query = "a = load 'a' using mock.Storage();" +
            "b = load 'b' using mock.Storage();" +
            "c = join a by x, b by x;" +
            "define pluck PluckTuple('a::','y');" +
            "d = foreach c generate flatten(pluck(*));";
        pigServer.registerQuery(query);
        Iterator<Tuple> it = pigServer.openIterator("d");
        assertTrue(it.hasNext());
        assertEquals(exp1_excl, it.next());
        assertTrue(it.hasNext());
        assertEquals(exp2_excl, it.next());
        assertFalse(it.hasNext());
    }
}