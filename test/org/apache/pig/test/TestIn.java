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

import static org.junit.Assert.assertEquals;
import static org.apache.pig.builtin.mock.Storage.bag;
import static org.apache.pig.builtin.mock.Storage.resetData;
import static org.apache.pig.builtin.mock.Storage.tuple;
import static org.junit.Assert.fail;

import java.util.List;

import org.apache.pig.PigServer;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.junit.Test;

public class TestIn {

    /**
     * Verify that IN operator works with FILTER BY.
     * @throws Exception
     */
    @Test
    public void testWithFilter() throws Exception {
        PigServer pigServer = new PigServer(Util.getLocalTestMode());
        Data data = resetData(pigServer);

        data.set("foo",
                tuple(1),
                tuple(2),
                tuple(3),
                tuple(4),
                tuple(5)
                );

        pigServer.registerQuery("A = LOAD 'foo' USING mock.Storage() AS (i:int);");
        pigServer.registerQuery("B = FILTER A BY i IN (1, 2, 3);");
        pigServer.registerQuery("STORE B INTO 'bar' USING mock.Storage();");

        List<Tuple> out = data.get("bar");
        assertEquals(3, out.size());
        assertEquals(tuple(1), out.get(0));
        assertEquals(tuple(2), out.get(1));
        assertEquals(tuple(3), out.get(2));
    }

    /**
     * Verify that IN operator works with ? operator.
     * @throws Exception
     */
    @Test
    public void testWithBincond() throws Exception {
        PigServer pigServer = new PigServer(Util.getLocalTestMode());
        Data data = resetData(pigServer);

        data.set("foo",
                tuple(1),
                tuple(2),
                tuple(3),
                tuple(4),
                tuple(5)
                );

        pigServer.registerQuery("A = LOAD 'foo' USING mock.Storage() AS (i:int);");
        pigServer.registerQuery("B = FOREACH A GENERATE (i IN (1, 3, 5) ? 'ODD' : 'EVEN');");
        pigServer.registerQuery("STORE B INTO 'bar' USING mock.Storage();");

        List<Tuple> out = data.get("bar");
        assertEquals(5, out.size());
        assertEquals(tuple("ODD"), out.get(0));
        assertEquals(tuple("EVEN"), out.get(1));
        assertEquals(tuple("ODD"), out.get(2));
        assertEquals(tuple("EVEN"), out.get(3));
        assertEquals(tuple("ODD"), out.get(4));
    }

    /**
     * Verify that IN operator works with SPLIT.
     * @throws Exception
     */
    @Test
    public void testWithSplit() throws Exception {
        PigServer pigServer = new PigServer(Util.getLocalTestMode());
        Data data = resetData(pigServer);

        data.set("foo",
                tuple(1),
                tuple(2),
                tuple(3),
                tuple(4),
                tuple(5)
                );

        pigServer.registerQuery("A = LOAD 'foo' USING mock.Storage() AS (i:int);");
        pigServer.registerQuery("SPLIT A INTO B IF i IN (1, 3, 5), C OTHERWISE;");
        pigServer.registerQuery("STORE B INTO 'odd' USING mock.Storage();");
        pigServer.registerQuery("STORE C INTO 'even' USING mock.Storage();");

        List<Tuple> out = data.get("odd");
        assertEquals(3, out.size());
        assertEquals(tuple(1), out.get(0));
        assertEquals(tuple(3), out.get(1));
        assertEquals(tuple(5), out.get(2));

        out = data.get("even");
        assertEquals(2, out.size());
        assertEquals(tuple(2), out.get(0));
        assertEquals(tuple(4), out.get(1));
    }

    /**
     * Verify that IN operator works when expressions contain dereference operators.
     * @throws Exception
     */
    @Test
    public void testWithDereferenceOperator() throws Exception {
        PigServer pigServer = new PigServer(Util.getLocalTestMode());
        Data data = resetData(pigServer);

        data.set("foo",
                tuple("a","x",1),
                tuple("a","y",2),
                tuple("b","x",3),
                tuple("b","y",4),
                tuple("c","x",5),
                tuple("c","y",6)
                );

        pigServer.registerQuery("A = LOAD 'foo' USING mock.Storage() AS (k1:chararray, k2:chararray, i:int);");
        pigServer.registerQuery("B = GROUP A BY (k1, k2);");
        pigServer.registerQuery("C = FILTER B BY group.k1 IN ('a', 'b') AND group.k2 IN ('x');");
        pigServer.registerQuery("STORE C INTO 'bar' USING mock.Storage();");

        List<Tuple> out = data.get("bar");
        assertEquals(2, out.size());
        assertEquals(tuple(tuple("a","x"),bag(tuple("a","x",1))), out.get(0));
        assertEquals(tuple(tuple("b","x"),bag(tuple("b","x",3))), out.get(1));
    }

    /**
     * Verify that IN operator throws FrontendException when no operand is given.
     * @throws Exception
     */
    @Test(expected = FrontendException.class)
    public void testMissingRhsOperand() throws Exception {
        PigServer pigServer = new PigServer(Util.getLocalTestMode());
        Data data = resetData(pigServer);

        data.set("foo",
                tuple(1),
                tuple(2),
                tuple(3),
                tuple(4),
                tuple(5)
                );

        pigServer.registerQuery("A = LOAD 'foo' USING mock.Storage() AS (i:int);");
        pigServer.registerQuery("B = FILTER A BY i IN ();"); // No operand
        pigServer.registerQuery("STORE B INTO 'bar' USING mock.Storage();");
        fail("FrontendException must be thrown since no rhs operand is given to IN.");
    }
}
