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

public class TestCase {

    /**
     * Verify that CASE statement without else branch works correctly.
     * @throws Exception
     */
    @Test
    public void testNoElseBranch() throws Exception {
        PigServer pigServer = new PigServer(Util.getLocalTestMode());
        Data data = resetData(pigServer);

        data.set("foo",
                tuple(1),
                tuple(2),
                tuple(3),
                tuple(4),
                tuple(5),
                tuple(6),
                tuple(7)
                );

        pigServer.registerQuery("A = LOAD 'foo' USING mock.Storage() AS (i:int);");
        pigServer.registerQuery("B = FOREACH A GENERATE i, (" +
                "  CASE i % 5" +
                "    WHEN 0 THEN '5n'" +
                "    WHEN 1 THEN '5n+1'" +
                "    WHEN 2 THEN '5n+2'" +
                "    WHEN 3 THEN '5n+3'" +
                "  END" +
                ") AS s;");
        pigServer.registerQuery("C = FILTER B BY s IS NOT NULL;");
        pigServer.registerQuery("STORE C INTO 'bar' USING mock.Storage();");

        List<Tuple> out = data.get("bar");
        assertEquals(6, out.size());
        assertEquals(tuple(1,"5n+1"), out.get(0));
        assertEquals(tuple(2,"5n+2"), out.get(1));
        assertEquals(tuple(3,"5n+3"), out.get(2));
        assertEquals(tuple(5,"5n"),   out.get(3));
        assertEquals(tuple(6,"5n+1"), out.get(4));
        assertEquals(tuple(7,"5n+2"), out.get(5));
    }

    /**
     * Verify that CASE statement with else branch works correctly.
     * @throws Exception
     */
    @Test
    public void testWithElseBranch() throws Exception {
        PigServer pigServer = new PigServer(Util.getLocalTestMode());
        Data data = resetData(pigServer);

        data.set("foo",
                tuple(1),
                tuple(2),
                tuple(3),
                tuple(4),
                tuple(5),
                tuple(6),
                tuple(7)
                );

        pigServer.registerQuery("A = LOAD 'foo' USING mock.Storage() AS (i:int);");
        pigServer.registerQuery("B = FOREACH A GENERATE i, (" +
                "  CASE i % 4" +
                "    WHEN 0 THEN '4n'" +
                "    WHEN 1 THEN '4n+1'" +
                "    WHEN 2 THEN '4n+2'" +
                "    ELSE        '4n+3'" +
                "  END" +
                ");");
        pigServer.registerQuery("STORE B INTO 'bar' USING mock.Storage();");

        List<Tuple> out = data.get("bar");
        assertEquals(7, out.size());
        assertEquals(tuple(1,"4n+1"), out.get(0));
        assertEquals(tuple(2,"4n+2"), out.get(1));
        assertEquals(tuple(3,"4n+3"), out.get(2));
        assertEquals(tuple(4,"4n"),   out.get(3));
        assertEquals(tuple(5,"4n+1"), out.get(4));
        assertEquals(tuple(6,"4n+2"), out.get(5));
        assertEquals(tuple(7,"4n+3"), out.get(6));
    }

    /**
     * Verify that conditional CASE statement without else branch works correctly.
     * @throws Exception
     */
    @Test
    public void testConditionalWithNoElse() throws Exception {
        PigServer pigServer = new PigServer(Util.getLocalTestMode());
        Data data = resetData(pigServer);

        data.set("foo",
                tuple(1),
                tuple(2),
                tuple(3),
                tuple(4),
                tuple(5),
                tuple(6),
                tuple(7)
                );

        pigServer.registerQuery("A = LOAD 'foo' USING mock.Storage() AS (i:int);");
        pigServer.registerQuery("B = FOREACH A GENERATE i, (" +
                "  CASE " +
                "    WHEN i % 5 == 0 THEN '5n'" + // Conditional expression in when branch
                "    WHEN i % 5 == 1 THEN '5n+1'" +
                "    WHEN i % 5 == 2 THEN '5n+2'" +
                "    WHEN i % 5 == 3 THEN '5n+3'" +
                "  END" +
                ") AS s;");
        pigServer.registerQuery("C = FILTER B BY s IS NOT NULL;");
        pigServer.registerQuery("STORE C INTO 'bar' USING mock.Storage();");

        List<Tuple> out = data.get("bar");
        assertEquals(6, out.size());
        assertEquals(tuple(1,"5n+1"), out.get(0));
        assertEquals(tuple(2,"5n+2"), out.get(1));
        assertEquals(tuple(3,"5n+3"), out.get(2));
        assertEquals(tuple(5,"5n"),   out.get(3));
        assertEquals(tuple(6,"5n+1"), out.get(4));
        assertEquals(tuple(7,"5n+2"), out.get(5));
    }

    /**
     * Verify that conditional CASE statement with else branch works correctly.
     * @throws Exception
     */
    @Test
    public void testConditionalWithElse() throws Exception {
        PigServer pigServer = new PigServer(Util.getLocalTestMode());
        Data data = resetData(pigServer);

        data.set("foo",
                tuple(1),
                tuple(2),
                tuple(3),
                tuple(4),
                tuple(5),
                tuple(6),
                tuple(7)
                );

        pigServer.registerQuery("A = LOAD 'foo' USING mock.Storage() AS (i:int);");
        pigServer.registerQuery("B = FOREACH A GENERATE i, (" +
                "  CASE " +
                "    WHEN i % 4 == 0 THEN '4n'" + // Conditional expression in when branch
                "    WHEN i % 4 == 1 THEN '4n+1'" +
                "    WHEN i % 4 == 2 THEN '4n+2'" +
                "    ELSE                 '4n+3'" +
                "  END" +
                ");");
        pigServer.registerQuery("STORE B INTO 'bar' USING mock.Storage();");

        List<Tuple> out = data.get("bar");
        assertEquals(7, out.size());
        assertEquals(tuple(1,"4n+1"), out.get(0));
        assertEquals(tuple(2,"4n+2"), out.get(1));
        assertEquals(tuple(3,"4n+3"), out.get(2));
        assertEquals(tuple(4,"4n"),   out.get(3));
        assertEquals(tuple(5,"4n+1"), out.get(4));
        assertEquals(tuple(6,"4n+2"), out.get(5));
        assertEquals(tuple(7,"4n+3"), out.get(6));
    }

    /**
     * Verify that CASE statement preserves the order of conditions.
     * @throws Exception
     */
    @Test
    public void testOrderOfConditions() throws Exception {
        PigServer pigServer = new PigServer(Util.getLocalTestMode());
        Data data = resetData(pigServer);

        data.set("foo",
                tuple(1),
                tuple(5),
                tuple(10),
                tuple(15),
                tuple(20),
                tuple(25),
                tuple(30)
                );

        pigServer.registerQuery("A = LOAD 'foo' USING mock.Storage() AS (i:int);");
        pigServer.registerQuery("B = FOREACH A GENERATE i, (" +
                "  CASE " +
                "    WHEN i > 20 THEN '> 20'" + // Conditions are not mutually exclusive
                "    WHEN i > 10 THEN '> 10'" +
                "    ELSE             '> 0'" +
                "  END" +
                ");");
        pigServer.registerQuery("STORE B INTO 'bar' USING mock.Storage();");

        List<Tuple> out = data.get("bar");
        assertEquals(7, out.size());
        assertEquals(tuple(1,"> 0"),   out.get(0));
        assertEquals(tuple(5,"> 0"),   out.get(1));
        assertEquals(tuple(10,"> 0"),  out.get(2));
        assertEquals(tuple(15,"> 10"), out.get(3));
        assertEquals(tuple(20,"> 10"), out.get(4));
        assertEquals(tuple(25,"> 20"), out.get(5));
        assertEquals(tuple(30,"> 20"), out.get(6));
    }

    /**
     * Verify that CASE statement works when expressions contain dereference operators.
     * @throws Exception
     */
    @Test
    public void testWithDereferenceOperator() throws Exception {
        PigServer pigServer = new PigServer(Util.getLocalTestMode());
        Data data = resetData(pigServer);

        data.set("foo",
                tuple("a","x",1),
                tuple("a","y",1),
                tuple("b","x",2),
                tuple("b","y",2),
                tuple("c","x",3),
                tuple("c","y",3)
                );

        pigServer.registerQuery("A = LOAD 'foo' USING mock.Storage() AS (c1:chararray, c2:chararray, i:int);");
        pigServer.registerQuery("B = GROUP A BY (c1, i);");
        pigServer.registerQuery("C = FOREACH B GENERATE group.i, (" +
                "  CASE group.i % 3" +
                "    WHEN 0 THEN '3n'" +
                "    WHEN 1 THEN '3n+1'" +
                "    ELSE        '3n+2'" +
                "  END" +
                "), A.(c1, c2);");
        pigServer.registerQuery("STORE C INTO 'bar' USING mock.Storage();");

        List<Tuple> out = data.get("bar");
        assertEquals(3, out.size());
        assertEquals(tuple(1, "3n+1", bag(tuple("a","x"), tuple("a","y"))), out.get(0));
        assertEquals(tuple(2, "3n+2", bag(tuple("b","x"), tuple("b","y"))), out.get(1));
        assertEquals(tuple(3, "3n",   bag(tuple("c","x"), tuple("c","y"))), out.get(2));
    }

    /**
     * Verify that FrontendException is thrown when case expression is missing,
     * and when branches do not contain conditional expressions.
     * @throws Exception
     */
    @Test(expected = FrontendException.class)
    public void testMissingCaseExpression() throws Exception {
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
        pigServer.registerQuery("B = FOREACH A GENERATE (" +
                "  CASE " + // No case expression
                "    WHEN 0 THEN '3n'" + // When expression is not conditional
                "    WHEN 1 THEN '3n+1'" +
                "    ELSE        '3n+2'" +
                "  END" +
                ");");
        pigServer.registerQuery("STORE B INTO 'bar' USING mock.Storage();");
        fail("FrontendException must be thrown for invalid case statement");
    }

    /**
     * Verify that FrontendException is thrown when when expression is missing.
     * @throws Exception
     */
    @Test(expected = FrontendException.class)
    public void testMissingWhenExpression() throws Exception {
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
        pigServer.registerQuery("B = FOREACH A GENERATE (" +
                "  CASE i % 3" +
                "    WHEN   THEN '3n'" + // No when expression
                "    WHEN 1 THEN '3n+1'" +
                "    ELSE        '3n+2'" +
                "  END" +
                ");");
        pigServer.registerQuery("STORE B INTO 'bar' USING mock.Storage();");
        fail("FrontendException must be thrown for invalid case statement");
    }

    /**
     * Verify that FrontendException is thrown when when expression is missing.
     * @throws Exception
     */
    @Test(expected = FrontendException.class)
    public void testMissingThenExpression() throws Exception {
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
        pigServer.registerQuery("B = FOREACH A GENERATE (" +
                "  CASE i % 3" +
                "    WHEN 0 THEN " + // No then expression
                "    WHEN 1 THEN '3n+1'" +
                "    ELSE        '3n+2'" +
                "  END" +
                ");");
        pigServer.registerQuery("STORE B INTO 'bar' USING mock.Storage();");
        fail("FrontendException must be thrown for invalid case statement");
    }

    /**
     * Verify that FrontendException is thrown when when expression is missing.
     * @throws Exception
     */
    @Test(expected = FrontendException.class)
    public void testMissingElseExpression() throws Exception {
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
        pigServer.registerQuery("B = FOREACH A GENERATE (" +
                "  CASE i % 3" +
                "    WHEN 0 THEN '3n'" +
                "    WHEN 1 THEN '3n+1'" +
                "    ELSE " + // No else expression
                "  END" +
                ");");
        pigServer.registerQuery("STORE B INTO 'bar' USING mock.Storage();");
        fail("FrontendException must be thrown for invalid case statement");
    }
}

