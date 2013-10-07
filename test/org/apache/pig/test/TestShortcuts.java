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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.Set;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.tools.grunt.Grunt;
import org.apache.pig.tools.pigscript.parser.ParseException;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;

public class TestShortcuts {
    private String basedir = "test/org/apache/pig/test/data";
    private Data data;
    private PigServer pigServer;
    private PigContext context;

    @Before
    public void setup() throws ExecException, IOException {
        pigServer = new PigServer(ExecType.LOCAL);
        context = pigServer.getPigContext();
        data = resetData(pigServer);
        data.set("input", tuple("dog", "miami", 12), tuple("cat", "miami", 18), tuple("turtle", "tampa", 4),
                tuple("dog", "tampa", 14), tuple("cat", "naples", 9), tuple("dog", "naples", 5),
                tuple("turtle", "naples", 1));
    }

    @Test
    public void testExplainShortcut() throws Throwable {
        String cmd = "a = load 'input' USING mock.Storage() as (x:chararray,y:chararray,z:long);"
                + "b = foreach a generate $0 as foo;" + "\\e b;";

        ByteArrayInputStream cmdstream = new ByteArrayInputStream(cmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmdstream);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);
        grunt.exec();
    }

    /**
     * When no alias is passed, entire script should be explained
     *
     * @throws Throwable
     */
    @Test
    public void testExplainShortcutNoAlias() throws Throwable {
        String cmd = "a = load 'input' USING mock.Storage() as (x:chararray,y:chararray,z:long);"
                + "b = foreach a generate $0 as foo;" + "\\e;";

        ByteArrayInputStream cmdstream = new ByteArrayInputStream(cmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmdstream);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);
        grunt.exec();
    }

    /**
     * When no alias is defined earlier, empty output is expected. Note that
     * ParseException is thrown in interactive mode.
     *
     * @throws Throwable
     */
    @Test
    public void testExplainShortcutNoAliasDefined() throws Throwable {
        String cmd = "\\e";

        ByteArrayInputStream cmdstream = new ByteArrayInputStream(cmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmdstream);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);
        grunt.exec(); // Interactive is set to false.
    }

    @Test
    public void testExplainShortcutScript() throws Throwable {
        String cmd = "\\e -script " + basedir + "/explainScript.pig;";

        ByteArrayInputStream cmdstream = new ByteArrayInputStream(cmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmdstream);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);
        grunt.exec();
    }

    @Test
    public void testDescribeShortcut() throws Throwable {
        String cmd = "a = load 'input' USING mock.Storage() as (x:chararray,y:chararray,z:long);"
                + "b = foreach a generate $0 as foo;" + "\\de b;";

        ByteArrayInputStream cmdstream = new ByteArrayInputStream(cmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmdstream);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);
        grunt.exec();
    }

    /**
     * When no alias is passed, last alias should be described
     *
     * @throws Throwable
     */
    @Test
    public void testDescribeShortcutNoAlias() throws Throwable {
        String cmd = "a = load 'input' USING mock.Storage() as (x:chararray,y:chararray,z:long);"
                + "b = foreach a generate $0 as foo;" + "\\de;";

        ByteArrayInputStream cmdstream = new ByteArrayInputStream(cmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmdstream);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);
        grunt.exec();
    }

    /**
     * When no alias is define, then exception is expected
     *
     * @throws Throwable
     */
    @Test(expected = IOException.class)
    public void testDescribeShortcutNoAliasDefined() throws Throwable {
        String cmd = "\\de";

        ByteArrayInputStream cmdstream = new ByteArrayInputStream(cmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmdstream);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);
        grunt.exec();
    }

    @Test
    public void testDumpShortcut() throws Throwable {
        String cmd = "a = load 'input' USING mock.Storage() as (x:chararray,y:chararray,z:long);"
                + "b = foreach a generate $0 as foo;" + "\\d b;";

        ByteArrayInputStream cmdstream = new ByteArrayInputStream(cmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmdstream);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);
        grunt.exec();
    }

    /**
     * When no alias is passed, last alias should be dumped
     *
     * @throws Throwable
     */
    @Test
    public void testDumpShortcutNoAlias() throws Throwable {
        String cmd = "a = load 'input' USING mock.Storage() as (x:chararray,y:chararray,z:long);"
                + "b = foreach a generate $0 as foo;" + "\\d;";

        ByteArrayInputStream cmdstream = new ByteArrayInputStream(cmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmdstream);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);
        grunt.exec();
    }

    /**
     * When no alias is defined, exception is expected
     *
     * @throws Throwable
     */
    @Test(expected = IOException.class)
    public void testDumpShortcutNoAliasDefined() throws Throwable {
        String cmd = "\\d";

        ByteArrayInputStream cmdstream = new ByteArrayInputStream(cmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmdstream);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);
        grunt.exec();
    }

    @Test
    public void testIllustrateShortcut() throws Throwable {
        String cmd = "a = load 'input' USING mock.Storage() as (x:chararray,y:chararray,z:long);"
                + "b = group a by $0;" + "\\i b;";

        ByteArrayInputStream cmdstream = new ByteArrayInputStream(cmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmdstream);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);
        grunt.exec();
    }

    /**
     * When no alias is passed, last alias should be illustrated
     *
     * @throws Throwable
     */
    @Test
    public void testIllustrateShortcutNoAlias() throws Throwable {
        String cmd = "a = load 'input' USING mock.Storage() as (x:chararray,y:chararray,z:long);"
                + "b = group a by $0;" + "\\i";

        ByteArrayInputStream cmdstream = new ByteArrayInputStream(cmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmdstream);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);
        grunt.exec();
    }

    /**
     * When no alias is defined, exception is expected
     *
     * @throws Throwable
     */
    @Test(expected = ParseException.class)
    public void testIllustrateShortcutNoAliasDefined() throws Throwable {
        String cmd = "\\i";

        ByteArrayInputStream cmdstream = new ByteArrayInputStream(cmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmdstream);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);
        grunt.exec();
    }

    @Test
    public void testIllustrateShortcutScript() throws Throwable {
        String cmd = "\\i -script " + basedir + "/illustrate.pig;";

        ByteArrayInputStream cmdstream = new ByteArrayInputStream(cmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmdstream);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);
        grunt.exec();
    }

    @Test
    public void testQuit() throws Throwable {
        String cmd = "a = load 'input' USING mock.Storage() as (x:chararray,y:chararray,z:long);"
                + "b = group a by $0;" + "\\q";

        ByteArrayInputStream cmdstream = new ByteArrayInputStream(cmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmdstream);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);
        grunt.exec();
    }

    @Test
    public void testDumpWithPreviousRelation() throws Exception {
        Data data = resetData(pigServer);
        Set<Tuple> expected = Sets.newHashSet(tuple("a"), tuple("b"), tuple("c"));

        data.set("foo", Utils.getSchemaFromString("x:chararray"), expected);
        pigServer.registerQuery("=> load 'foo' using mock.Storage();");
        Iterator<Tuple> op = pigServer.openIterator("@");
        while (op.hasNext()) {
            assertTrue(expected.remove(op.next()));
        }
        assertFalse(op.hasNext());
        assertTrue(expected.isEmpty());
    }
    
    @Test
    public void testDescribeWithPreviousRelation() throws Exception {
        Data data = resetData(pigServer);
        Set<Tuple> expected = Sets.newHashSet(tuple("a"), tuple("b"), tuple("c"));

        Schema s = Utils.getSchemaFromString("x:chararray");
        data.set("foo", s, expected);
        pigServer.registerQuery("=> load 'foo' using mock.Storage();");
        Schema s2 = pigServer.dumpSchema("@");
        assertEquals(s,s2);
    }
    
    @Test
    public void testExplainWithPreviousRelation() throws Throwable {
        String cmd = "=> load 'input' USING mock.Storage() as (x:chararray,y:chararray,z:long);"
                + "explain @;";

        ByteArrayInputStream cmdstream = new ByteArrayInputStream(cmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmdstream);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);
        grunt.exec();
    }
    
    @Test
    public void testIllustrateWithPreviousRelation() throws Throwable {
        String cmd = "=> load 'input' USING mock.Storage() as (x:chararray,y:chararray,z:long);"
                + "illustrate @;";

        ByteArrayInputStream cmdstream = new ByteArrayInputStream(cmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmdstream);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);
        grunt.exec();
    }
}
