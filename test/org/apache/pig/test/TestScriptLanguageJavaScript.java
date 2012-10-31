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
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.scripting.ScriptEngine;
import org.apache.pig.tools.pigstats.PigStats;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestScriptLanguageJavaScript {

    static MiniCluster cluster = MiniCluster.buildCluster();
    private PigServer pigServer;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {

    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        cluster.shutDown();
    }

    @Before
    public void setUp() throws Exception {
        pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
//        pigServer = new PigServer(ExecType.LOCAL);
    }

    @After
    public void tearDown() throws Exception {
        Util.deleteFile(pigServer.getPigContext(), "simple_out");
    }

    @Test
    public void firstTest() throws Exception {
        String[] script = {
                "importPackage(Packages.org.apache.pig.scripting.js)",
                "pig = org.apache.pig.scripting.js.JSPig;",
                "function main() {",
                "  pig.fs(\"-rmr simple_out\")",
                "  input = 'simple_table'",
                "  output = 'simple_out'",
                "  P = pig.compile(\"a = load '$input';store a into '$output';\")",
                "  Q = P.bind({'input':input, 'output':output})",
                "  stats = Q.runSingle()",
                "  if (stats.isSuccessful()) {",
                "    print(\"success!\")",
                "  } else {",
                "    print(\"failed\")",
                "  }",
                "}"
        };
        String[] input = {
                "1\t3",
                "2\t4",
                "3\t5"
        };

        Util.deleteFile(pigServer.getPigContext(), "simple_table");
        Util.createInputFile(pigServer.getPigContext(), "simple_table", input);
        Util.createLocalInputFile( "testScript.js", script);

        ScriptEngine scriptEngine = ScriptEngine.getInstance("javascript");
        Map<String, List<PigStats>> statsMap = scriptEngine.run(pigServer.getPigContext(), "testScript.js");
        assertEquals(1, statsMap.size());
        Iterator<List<PigStats>> it = statsMap.values().iterator();
        PigStats stats = it.next().get(0);
        assertTrue("job should succeed", stats.isSuccessful());
        assertEquals(1, stats.getNumberJobs());
        String name = stats.getOutputNames().get(0);
        assertEquals("simple_out", name);

        String[] output = Util.readOutput(pigServer.getPigContext(), "simple_out");
        assertTrue(Arrays.toString(input)+" equals "+Arrays.toString(output), Arrays.equals(input, output));

    }


    @Test
    public void testTC() throws Exception {
        String[] input = {
                "(id0,[name#a])\t(id1,[name#b])\tMATCH",
                "(id1,[name#a])\t(id2,[name#b])\tMATCH",
                "(id2,[name#b])\t(id3,[name#c])\tMATCH",
                "(id3,[name#c])\t(id4,[name#d])\tMATCH",
                "(id4,[name#e])\t(id5,[name#f])\tMATCH",
                "(id5,[name#f])\t(id6,[name#e])\tMATCH",
                "(id6,[name#e])\t(id7,[name#g])\tMATCH",
                "(id7,[name#g])\t(id8,[name#e])\tMATCH",
                "(id8,[name#g])\t(id9,[name#f])\tMATCH",
                "(id9,[name#f])\t(id10,[name#g])\tMATCH",
                "(id10,[name#h])\t(id11,[name#i])\tMATCH",
                "(id11,[name#a])\t(id12,[name#b])\tMATCH",
                "(id12,[name#b])\t(id13,[name#c])\tMATCH",
                "(id13,[name#c])\t(id14,[name#d])\tMATCH",
                "(id14,[name#e])\t(id15,[name#f])\tMATCH",
                "(id15,[name#f])\t(id16,[name#e])\tMATCH",
                "(id16,[name#e])\t(id17,[name#g])\tMATCH",
                "(id17,[name#g])\t(id18,[name#e])\tMATCH",
                "(id18,[name#g])\t(id19,[name#f])\tMATCH",
                "(id19,[name#f])\t(id20,[name#g])\tMATCH",
                "(id20,[name#a])\t(id21,[name#b])\tMATCH",
                "(id21,[name#a])\t(id22,[name#b])\tMATCH",
                "(id22,[name#b])\t(id23,[name#c])\tMATCH",
                "(id23,[name#c])\t(id24,[name#d])\tMATCH",
                "(id24,[name#e])\t(id25,[name#f])\tMATCH",
                "(id25,[name#f])\t(id26,[name#e])\tMATCH",
                "(id26,[name#e])\t(id27,[name#g])\tMATCH",
                "(id27,[name#g])\t(id28,[name#e])\tMATCH",
                "(id28,[name#g])\t(id29,[name#f])\tMATCH",
                "(id29,[name#f])\t(id30,[name#g])\tMATCH",
                "(id30,[name#a])\t(id31,[name#b])\tMATCH",
                "(id32,[name#a])"
        };

        Util.deleteFile(pigServer.getPigContext(), "simple_table");
        Util.createInputFile(pigServer.getPigContext(), "simple_table", input);

        ScriptEngine scriptEngine = ScriptEngine.getInstance("javascript");
        Map<String, List<PigStats>> statsMap = scriptEngine.run(pigServer.getPigContext(), "test/org/apache/pig/test/data/tc.js");
        for (List<PigStats> pigStatsList : statsMap.values()) {
            for (PigStats pigStats : pigStatsList) {
                assertTrue(pigStats.getScriptId()+" succesful", pigStats.isSuccessful());
            }
        }

    }

}
