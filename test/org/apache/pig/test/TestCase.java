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

import static junit.framework.Assert.assertEquals;
import static org.apache.pig.builtin.mock.Storage.resetData;
import static org.apache.pig.builtin.mock.Storage.tuple;
import static org.junit.Assert.fail;

import java.util.List;

import org.apache.pig.ExecType;
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
        PigServer pigServer = new PigServer(ExecType.LOCAL);
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
        PigServer pigServer = new PigServer(ExecType.LOCAL);
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
                "    ELSE        '5n+4'" +
                "  END" +
                ");");
        pigServer.registerQuery("STORE B INTO 'bar' USING mock.Storage();");

        List<Tuple> out = data.get("bar");
        assertEquals(7, out.size());
        assertEquals(tuple(1,"5n+1"), out.get(0));
        assertEquals(tuple(2,"5n+2"), out.get(1));
        assertEquals(tuple(3,"5n+3"), out.get(2));
        assertEquals(tuple(4,"5n+4"), out.get(3));
        assertEquals(tuple(5,"5n"),   out.get(4));
        assertEquals(tuple(6,"5n+1"), out.get(5));
        assertEquals(tuple(7,"5n+2"), out.get(6));
    }

    /**
     * Verify that conditional CASE statement without else branch works correctly.
     * @throws Exception
     */
    @Test
    public void testConditionalWithNoElse() throws Exception {
        PigServer pigServer = new PigServer(ExecType.LOCAL);
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
        PigServer pigServer = new PigServer(ExecType.LOCAL);
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
                "    ELSE                 '5n+4'" +
                "  END" +
                ");");
        pigServer.registerQuery("STORE B INTO 'bar' USING mock.Storage();");

        List<Tuple> out = data.get("bar");
        assertEquals(7, out.size());
        assertEquals(tuple(1,"5n+1"), out.get(0));
        assertEquals(tuple(2,"5n+2"), out.get(1));
        assertEquals(tuple(3,"5n+3"), out.get(2));
        assertEquals(tuple(4,"5n+4"), out.get(3));
        assertEquals(tuple(5,"5n"),   out.get(4));
        assertEquals(tuple(6,"5n+1"), out.get(5));
        assertEquals(tuple(7,"5n+2"), out.get(6));
    }

    /**
     * Verify that FrontendException is thrown when case expression is missing,
     * and when branches do not contain conditional expressions.
     * @throws Exception
     */
    @Test(expected = FrontendException.class)
    public void testMissingCaseExpression() throws Exception {
        PigServer pigServer = new PigServer(ExecType.LOCAL);
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
        PigServer pigServer = new PigServer(ExecType.LOCAL);
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
        PigServer pigServer = new PigServer(ExecType.LOCAL);
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
        PigServer pigServer = new PigServer(ExecType.LOCAL);
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

