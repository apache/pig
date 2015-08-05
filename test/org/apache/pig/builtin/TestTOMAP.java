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

package org.apache.pig.builtin;

import org.apache.pig.PigServer;
import org.apache.pig.builtin.mock.Storage;
import org.apache.pig.data.Tuple;
import org.apache.pig.test.Util;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.apache.pig.builtin.mock.Storage.map;
import static org.apache.pig.builtin.mock.Storage.tuple;
import static org.apache.pig.builtin.mock.Storage.resetData;

public class TestTOMAP {

    static PigServer pigServer;

    @BeforeClass
    public static void setUp() throws Exception {
        pigServer = new PigServer(Util.getLocalTestMode());
    }

    @Test
    public void testTOMAP_Tuple() throws Exception {
        Storage.Data data = resetData(pigServer);

        data.set("foo",
                tuple("a", "b", "c", "d")
        );

        pigServer.registerQuery("A = LOAD 'foo' USING mock.Storage();");
        pigServer.registerQuery("B = FOREACH A GENERATE TOMAP($0, $1, $2, $3);");
        pigServer.registerQuery("STORE B INTO 'bar' USING mock.Storage();");

        List<Tuple> out = data.get("bar");
        assertEquals(tuple(map("c", "d", "a", "b")), out.get(0));
    }

    @Test
    public void testTOMAP_Bad_Tuple() throws Exception {
        Storage.Data data = resetData(pigServer);

        data.set("foo",
                tuple("a", "b", "c")
        );

        pigServer.registerQuery("A = LOAD 'foo' USING mock.Storage();");
        pigServer.registerQuery("B = FOREACH A GENERATE TOMAP($0, $1, $2);");
        pigServer.registerQuery("STORE B INTO 'bar' USING mock.Storage();");

        List<Tuple> out = data.get("bar");
        assertEquals(0, out.size()); // Error should occur --> no results
    }

    @Test
    public void testTOMAP_BagOfPairs() throws Exception {
        Storage.Data data = resetData(pigServer);

        data.set("foo",
                tuple("a", "b"),
                tuple("c", "d")
        );

        pigServer.registerQuery("A = LOAD 'foo' USING mock.Storage();");
        pigServer.registerQuery("B = GROUP A ALL;");
        pigServer.registerQuery("C = FOREACH B GENERATE TOMAP(A);");
        pigServer.registerQuery("STORE C INTO 'bar' USING mock.Storage();");

        List<Tuple> out = data.get("bar");
        assertEquals(tuple(map("a", "b", "c", "d")), out.get(0));
    }

    @Test
    public void testTOMAP_Bad_BagOfPairs() throws Exception {
        Storage.Data data = resetData(pigServer);

        data.set("foo",
                tuple("a", "b"),
                tuple("c")
        );

        pigServer.registerQuery("A = LOAD 'foo' USING mock.Storage();");
        pigServer.registerQuery("B = GROUP A ALL;");
        pigServer.registerQuery("C = FOREACH B GENERATE TOMAP(A);");
        pigServer.registerQuery("STORE C INTO 'bar' USING mock.Storage();");

        List<Tuple> out = data.get("bar");
        assertEquals(0, out.size()); // Error should occur --> no results
    }

}
