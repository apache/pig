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

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.pig.ExecType;
import org.apache.pig.PigRunner;
import org.apache.pig.PigServer;
import org.apache.pig.scripting.ScriptEngine;
import org.apache.pig.tools.pigstats.PigStats;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestScriptLanguage {

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
    }

    @After
    public void tearDown() throws Exception {
        Util.deleteFile(cluster, "simple_out");
    }
    
    @Test
    public void firstTest() throws Exception {
        String[] script = {
                "#!/usr/bin/python",
                "from org.apache.pig.scripting import *",
                "Pig.fs(\"-rmr simple_out\")",
                "input = 'simple_table'",
                "output = 'simple_out'",
                "P = Pig.compile(\"\"\"a = load '$input';store a into '$output';\"\"\")",
                "Q = P.bind({'input':input, 'output':output})",
                "stats = Q.runSingle()",
                "if stats.isSuccessful():",
                "\tprint 'success!'",
                "else:",
                "\traise 'failed'"
        };
        String[] input = {
                "1\t3",
                "2\t4",
                "3\t5"
        };
        
        Util.createInputFile(cluster, "simple_table", input);
        Util.createLocalInputFile( "testScript.py", script);
        
        ScriptEngine scriptEngine = ScriptEngine.getInstance("jython");
        Map<String, List<PigStats>> statsMap = scriptEngine.run(pigServer.getPigContext(), "testScript.py");
        assertEquals(1, statsMap.size());        
        Iterator<List<PigStats>> it = statsMap.values().iterator();      
        PigStats stats = it.next().get(0);
        assertTrue(stats.isSuccessful());
        assertEquals(1, stats.getNumberJobs());
        String name = stats.getOutputNames().get(0);
        assertEquals("simple_out", name);
        assertEquals(12, stats.getBytesWritten());
        assertEquals(3, stats.getRecordWritten());     
    }
    
    @Test
    public void secondTest() throws Exception {
        String[] script = {
                "#!/usr/bin/python",
                "from org.apache.pig.scripting import *",
                "Pig.fs(\"-rmr simple_out\")",
                "input = 'simple_table_6'",
                "output = 'simple_out'",
                "P = Pig.compileFromFile(\"\"\"testScript.pig\"\"\")",
                "Q = P.bind({'input':input, 'output':output})",
                "stats = Q.runSingle()",
                "if stats.isSuccessful():",
                "\tprint 'success!'",
                "else:",
                "\traise 'failed'"
        };
        String[] input = {
                "1\t3",
                "2\t4",
                "3\t5"
        };
        
        String[] pigLatin = {
                "a = load '$input';",
                "store a into '$output';"
        };
        
        Util.createInputFile(cluster, "simple_table_6", input);
        Util.createLocalInputFile( "testScript.py", script);
        Util.createLocalInputFile( "testScript.pig", pigLatin);
        
        ScriptEngine scriptEngine = ScriptEngine.getInstance("jython");
        Map<String, List<PigStats>> statsMap = scriptEngine.run(pigServer.getPigContext(), "testScript.py");
        assertEquals(1, statsMap.size());        
        Iterator<List<PigStats>> it = statsMap.values().iterator();      
        PigStats stats = it.next().get(0);
        assertTrue(stats.isSuccessful());
        assertEquals(1, stats.getNumberJobs());
        String name = stats.getOutputNames().get(0);
        assertEquals("simple_out", name);
        assertEquals(12, stats.getBytesWritten());
        assertEquals(3, stats.getRecordWritten());     
    }
    
    @Test
    public void firstParallelTest() throws Exception {
        String[] script = {
                "#!/usr/bin/python",
                "from org.apache.pig.scripting import *",
                "Pig.fs(\"-rmr simple_out\")",
                "Pig.fs(\"-rmr simple_out2\")",
                "input = 'simple_table_1'",
                "output1 = 'simple_out'",
                "output2 = 'simple_out'",
                "P = Pig.compile(\"mypipeline\", \"\"\"a = load '$input';store a into '$output';\"\"\")",
                "Q = P.bind([{'input':input, 'output':output1}, {'input':input, 'output':output2}])",
                "stats = Q.run()"
        };
        String[] input = {
                "1\t3",
                "2\t4",
                "3\t5"
        };
        
        Util.createInputFile(cluster, "simple_table_1", input);
        Util.createLocalInputFile( "testScript.py", script);
        
        ScriptEngine scriptEngine = ScriptEngine.getInstance("jython");
        Map<String, List<PigStats>> statsMap = scriptEngine.run(pigServer.getPigContext(), "testScript.py");
        assertEquals(1, statsMap.size());
        assertEquals("mypipeline", statsMap.keySet().iterator().next());
        List<PigStats> lst = statsMap.get("mypipeline");
        assertEquals(2, lst.size());
        for (PigStats stats : lst) {
            assertTrue(stats.isSuccessful());
            assertEquals(1, stats.getNumberJobs());
            assertEquals(12, stats.getBytesWritten());
            assertEquals(3, stats.getRecordWritten());     
        }
    }
    
    @Test
    public void pigRunnerTest() throws Exception {
        String[] script = {
                "from org.apache.pig.scripting import *",
                "Pig.fs(\"-rmr simple_out\")",
                "input = 'simple_table_2'",
                "output = 'simple_out'",
                "P = Pig.compile(\"\"\"a = load '$input';store a into '$output';\"\"\")",
                "Q = P.bind({'input':input, 'output':output})",
                "stats = Q.runSingle()",
                "if stats.isSuccessful():",
                "\tprint 'success!'",
                "else:",
                "\traise 'failed'"
        };
        String[] input = {
                "1\t3",
                "2\t4",
                "3\t5"
        };
        
        Util.createInputFile(cluster, "simple_table_2", input);
        Util.createLocalInputFile( "testScript.py", script);
          
        String[] args = { "-g", "jython", "testScript.py" };
        
        PigStats mainStats = PigRunner.run(args, new TestPigRunner.TestNotificationListener());
        assertTrue(mainStats.isEmbedded());
        assertTrue(mainStats.isSuccessful());
        Map<String, List<PigStats>> statsMap = mainStats.getAllStats();
        assertEquals(1, statsMap.size());        
        Iterator<List<PigStats>> it = statsMap.values().iterator();      
        PigStats stats = it.next().get(0);
        assertTrue(stats.isSuccessful());
        assertEquals(1, stats.getNumberJobs());
        String name = stats.getOutputNames().get(0);
        assertEquals("simple_out", name);
        assertEquals(12, stats.getBytesWritten());
        assertEquals(3, stats.getRecordWritten());     
    }
    
    @Test
    public void runParallelTest() throws Exception {
        String[] script = {
                "#!/usr/bin/python",
                "from org.apache.pig.scripting import *",
                "input = 'simple_table_3'",
                "Pig.fs(\"-rmr simple_out\")",
                "Pig.fs(\"-rmr simple_out2\")",
                "output1 = 'simple_out'",
                "output2 = 'simple_out2'",
                "P = Pig.compile(\"mypipeline\", \"\"\"a = load '$input';store a into '$output';\"\"\")",
                "Q = P.bind([{'input':input, 'output':output1}, {'input':input, 'output':output2}])",
                "stats = Q.run()"
        };
        String[] input = {
                "1\t3",
                "2\t4",
                "3\t5"
        };
        
        Util.createInputFile(cluster, "simple_table_3", input);
        Util.createLocalInputFile( "testScript.py", script);
        
        String[] args = { "-g", "jython", "testScript.py" };
        PigStats mainStats = PigRunner.run(args, new TestPigRunner.TestNotificationListener());
        assertTrue(mainStats.isEmbedded());
        assertTrue(mainStats.isSuccessful());
        Map<String, List<PigStats>> statsMap = mainStats.getAllStats();
        assertEquals(1, statsMap.size());
        assertEquals("mypipeline", statsMap.keySet().iterator().next());
        List<PigStats> lst = statsMap.get("mypipeline");
        assertEquals(2, lst.size());
        for (PigStats stats : lst) {
            assertTrue(stats.isSuccessful());
            assertEquals(1, stats.getNumberJobs());
            assertEquals(12, stats.getBytesWritten());
            assertEquals(3, stats.getRecordWritten());     
        }
    }
    
    @Test
    public void runLoopTest() throws Exception {
        String[] script = {
                "#!/usr/bin/python",
                "from org.apache.pig.scripting import *",
                "Pig.fs(\"-rmr simple_out\")",
                "Pig.fs(\"-rmr simple_out2\")",
                "input = 'simple_table_4'",
                "P = Pig.compile(\"mypipeline\", \"\"\"a = load '$input';store a into '$output';\"\"\")",
                "for x in [\"simple_out\", \"simple_out2\"]:",
                "\tQ = P.bind({'input':input, 'output':x}).run()"
        };
        String[] input = {
                "1\t3",
                "2\t4",
                "3\t5"
        };
        
        Util.createInputFile(cluster, "simple_table_4", input);
        Util.createLocalInputFile( "testScript.py", script);
        
        String[] args = { "-g", "jython", "testScript.py" };
        PigStats mainStats = PigRunner.run(args, new TestPigRunner.TestNotificationListener());
        assertTrue(mainStats.isEmbedded());
        assertTrue(mainStats.isSuccessful());
        Map<String, List<PigStats>> statsMap = mainStats.getAllStats();
        assertEquals(1, statsMap.size());
        assertEquals("mypipeline", statsMap.keySet().iterator().next());
        List<PigStats> lst = statsMap.get("mypipeline");
        assertEquals(2, lst.size());
        for (PigStats stats : lst) {
            assertTrue(stats.isSuccessful());
            assertEquals(1, stats.getNumberJobs());
            assertEquals(12, stats.getBytesWritten());
            assertEquals(3, stats.getRecordWritten());     
        }
    }
    
    @Test
    public void bindLocalVariableTest() throws Exception {
        String[] script = {
                "#!/usr/bin/python",
                "from org.apache.pig.scripting import *",
                "Pig.fs(\"-rmr simple_out\")",
                "input = 'simple_table_5'",
                "output = 'simple_out'",
                "P = Pig.compile(\"\"\"a = load '$input';store a into '$output';\"\"\")",
                "Q = P.bind()",
                "stats = Q.runSingle()",
                "if stats.isSuccessful():",
                "\tprint 'success!'",
                "else:",
                "\traise 'failed'"
        };
        String[] input = {
                "1\t3",
                "2\t4",
                "3\t5"
        };
        
        Util.createInputFile(cluster, "simple_table_5", input);
        Util.createLocalInputFile( "testScript.py", script);
        
        ScriptEngine scriptEngine = ScriptEngine.getInstance("jython");
        Map<String, List<PigStats>> statsMap = scriptEngine.run(pigServer.getPigContext(), "testScript.py");
        assertEquals(1, statsMap.size());        
        Iterator<List<PigStats>> it = statsMap.values().iterator();      
        PigStats stats = it.next().get(0);
        assertTrue(stats.isSuccessful());
        assertEquals(1, stats.getNumberJobs());
        String name = stats.getOutputNames().get(0);
        assertEquals("simple_out", name);
        assertEquals(12, stats.getBytesWritten());
        assertEquals(3, stats.getRecordWritten());     
    }

}
