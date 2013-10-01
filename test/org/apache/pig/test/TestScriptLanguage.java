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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.pig.ExecType;
import org.apache.pig.PigRunner;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
import org.apache.pig.scripting.ScriptEngine;
import org.apache.pig.tools.pigstats.OutputStats;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.PigStatsUtil;
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
        Util.deleteFile(cluster, "simple_out2");
    }

    private String createScript(String name, String[] script) throws Exception {
        String scriptName = name + "_testScript.py";
        Util.createLocalInputFile(scriptName, script);
        return scriptName;
    }

    private Map<String, List<PigStats>> runScript(String name, String[] script) throws Exception {
        String scriptName = name + "_testScript.py";
        Util.createLocalInputFile(scriptName, script);
        ScriptEngine scriptEngine = ScriptEngine.getInstance("jython");
        Map<String, List<PigStats>> statsMap = scriptEngine.run(pigServer.getPigContext(), scriptName);
        return statsMap;
    }

    private PigStats runSimpleScript(String name, String[] script) throws Exception {
        Map<String, List<PigStats>> statsMap = runScript(name, script);
        assertEquals(1, statsMap.size());
        Iterator<List<PigStats>> it = statsMap.values().iterator();
        PigStats stats = it.next().get(0);
        assertTrue("job should succeed", stats.isSuccessful());
        return stats;
    }

    private PigStats runPigRunner(String name, String[] script, boolean shouldSucceed) throws Exception {
        String scriptName = createScript(name, script);

        String[] args = { "-g", "jython", scriptName };

        PigStats mainStats = PigRunner.run(args, new TestPigRunner.TestNotificationListener());
        if (shouldSucceed) {
            assertTrue(mainStats.isEmbedded());
            assertTrue("job should succeed", mainStats.isSuccessful());
        } else {
            assertFalse("job should fail", mainStats.isSuccessful());
        }
        return mainStats;
    }


    private PigStats runPigRunner(String name, String[] script) throws Exception {
        return runPigRunner(name, script, true);
    }

    @Test
    public void firstTest() throws Exception {
        String[] script = {
                "#!/usr/bin/python",
                "from org.apache.pig.scripting import *",
                "Pig.fs(\"rmr simple_out\")",
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

        Util.deleteFile(cluster, "simple_table");
        Util.createInputFile(cluster, "simple_table", input);

        PigStats stats = runSimpleScript("firstTest", script);

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
                "Pig.fs(\"rmr simple_out\")",
                "input = 'simple_table_6'",
                "output = 'simple_out'",
                "P = Pig.compileFromFile(\"\"\"secondTest.pig\"\"\")",
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
                "-- ensure comment parsed correctly",
                "a = load '$input';",
                "store a into '$output';"
        };

        Util.createInputFile(cluster, "simple_table_6", input);
        Util.createLocalInputFile( "secondTest.pig", pigLatin);

        PigStats stats = runSimpleScript("secondTest", script);
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
                "Pig.fs(\"rmr simple_out\")",
                "Pig.fs(\"rmr simple_out2\")",
                "input = 'simple_table_1'",
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

        Util.createInputFile(cluster, "simple_table_1", input);

        Map<String, List<PigStats>> statsMap = runScript("firstParallelTest", script);
        assertEquals(1, statsMap.size());
        assertEquals("mypipeline", statsMap.keySet().iterator().next());
        List<PigStats> lst = statsMap.get("mypipeline");
        assertEquals(2, lst.size());
        for (PigStats stats : lst) {
            assertTrue("job should succeed", stats.isSuccessful());
            assertEquals(1, stats.getNumberJobs());
            assertEquals(12, stats.getBytesWritten());
            assertEquals(3, stats.getRecordWritten());
        }
    }

    @Test
    public void pigRunnerTest() throws Exception {
        String[] script = {
                "from org.apache.pig.scripting import *",
                "Pig.fs(\"rmr simple_out\")",
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

        PigStats mainStats = runPigRunner("pigRunnerTest", script);

        Map<String, List<PigStats>> statsMap = mainStats.getAllStats();
        assertEquals(1, statsMap.size());
        Iterator<List<PigStats>> it = statsMap.values().iterator();
        PigStats stats = it.next().get(0);
        assertTrue("job should succeed", stats.isSuccessful());
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
                "Pig.fs(\"rmr simple_out\")",
                "Pig.fs(\"rmr simple_out2\")",
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

        PigStats mainStats = runPigRunner("runParallelTest", script);

        Map<String, List<PigStats>> statsMap = mainStats.getAllStats();
        assertEquals(1, statsMap.size());
        assertEquals("mypipeline", statsMap.keySet().iterator().next());
        List<PigStats> lst = statsMap.get("mypipeline");
        assertEquals(2, lst.size());
        for (PigStats stats : lst) {
            assertTrue("job should succeed", stats.isSuccessful());
            assertEquals(1, stats.getNumberJobs());
            assertEquals(12, stats.getBytesWritten());
            assertEquals(3, stats.getRecordWritten());
        }
    }

    @Test
    public void runParallelTest2() throws Exception {
        String[] script = {
                "#!/usr/bin/python",
                "from org.apache.pig.scripting import *",
                "input = 'simple_table_7'",
                "Pig.fs(\"rmr simple_out\")",
                "Pig.fs(\"rmr simple_out2\")",
                "output1 = 'simple_out'",
                "output2 = 'simple_out2'",
                "P = Pig.compile(\"mypipeline\", \"\"\"a = load '$input';",
                "b = foreach a generate $0, org.apache.pig.test.utils.UDFContextTestEvalFunc3($0);",
                "store b into '$output';\"\"\")",
                "Q = P.bind([{'input':input, 'output':output1}, {'input':input, 'output':output2}])",
                "stats = Q.run()"
        };
        String[] input = {
                "1\t3"
        };

        Util.createInputFile(cluster, "simple_table_7", input);
        PigStats mainStats = runPigRunner("runParallelTest2", script);

        Map<String, List<PigStats>> statsMap = mainStats.getAllStats();
        assertEquals(1, statsMap.size());
        assertEquals("mypipeline", statsMap.keySet().iterator().next());
        List<PigStats> lst = statsMap.get("mypipeline");
        assertEquals(2, lst.size());
        String[] results = new String[2];
        int i = 0;
        for (PigStats stats : lst) {
            assertTrue("job should succeed", stats.isSuccessful());
            assertEquals(1, stats.getNumberJobs());
            OutputStats os = stats.getOutputStats().get(0);
            Tuple t = os.iterator().next();
            results[i++] = t.get(1).toString();
        }
        assertTrue(results[0] != null);
        assertTrue(results[1] != null);
        assertTrue(!results[0].equals(results[1]));
    }

    @Test
    public void runLoopTest() throws Exception {
        String[] script = {
                "#!/usr/bin/python",
                "from org.apache.pig.scripting import *",
                "Pig.fs(\"rmr simple_out\")",
                "Pig.fs(\"rmr simple_out2\")",
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

        PigStats mainStats = runPigRunner("runLoopTest", script);

        Map<String, List<PigStats>> statsMap = mainStats.getAllStats();
        assertEquals(1, statsMap.size());
        assertEquals("mypipeline", statsMap.keySet().iterator().next());
        List<PigStats> lst = statsMap.get("mypipeline");
        assertEquals(2, lst.size());
        for (PigStats stats : lst) {
            assertTrue("job should succeed", stats.isSuccessful());
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
                "Pig.fs(\"rmr simple_out\")",
                "input = 'simple_table_5'",
                "output = 'simple_out'",
                "testvar = 'abcd$py'",
                "testvar2 = '$'",
                "testvar3 = '\\\\\\\\$'",
                "testvar4 = 'abcd\\$py$'",
                "testvar5 = 'abcd\\$py'",
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

        PigStats stats = runSimpleScript("bindLocalVariableTest", script);

        assertEquals(1, stats.getNumberJobs());
        String name = stats.getOutputNames().get(0);
        assertEquals("simple_out", name);
        assertEquals(12, stats.getBytesWritten());
        assertEquals(3, stats.getRecordWritten());
    }

    @Test
    public void bindLocalVariableTest2() throws Exception {
        String[] script = {
                "#!/usr/bin/python",
                "from org.apache.pig.scripting import *",
                "Pig.fs(\"rmr simple_out\")",
                "input = 'bindLocalVariableTest2'",
                "output = 'simple_out'",
                "separator = '$'",
                "P = Pig.compile(\"\"\"a = load '$input' using PigStorage('$separator');store a into '$output';\"\"\")",
                "Q = P.bind()",
                "stats = Q.runSingle()",
                "if stats.isSuccessful():",
                "\tprint 'success!'",
                "else:",
                "\traise 'failed'"
        };
        String[] input = {
                "1$3",
                "2$4",
                "3$5"
        };

        Util.createInputFile(cluster, "bindLocalVariableTest2", input);

        PigStats stats = runSimpleScript("bindLocalVariableTest2", script);

        assertEquals(1, stats.getNumberJobs());
        String name = stats.getOutputNames().get(0);
        assertEquals("simple_out", name);
        assertEquals(12, stats.getBytesWritten());
        assertEquals(3, stats.getRecordWritten());
    }

    @Test
    public void bindNonStringVariableTest() throws Exception {
        String[] script = {
                "#!/usr/bin/python",
                "from org.apache.pig.scripting import *",
                "Pig.fs(\"-rmr simple_out\")",
                "input = 'simple_table'",
                "output = 'simple_out'",
                "max = 2",
                "P = Pig.compile(\"\"\"a = load '$in' as (a0:int, a1:int);" +
                "   b = filter a by a0 > $max;" +
                "   store b into '$out';\"\"\")",
                "Q = P.bind({'in':input, 'out':output, 'max':max})",
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

        Util.deleteFile(cluster, "simple_table");
        Util.createInputFile(cluster, "simple_table", input);

        PigStats stats = runSimpleScript("bindNonStringVariableTest", script);

        assertEquals(1, stats.getNumberJobs());
        String name = stats.getOutputNames().get(0);
        assertEquals("simple_out", name);
        assertEquals(4, stats.getBytesWritten());
        assertEquals(1, stats.getRecordWritten());
    }

    @Test
    public void fsTest() throws Exception {
        String[] script = {
                "#!/usr/bin/python",
                "from org.apache.pig.scripting import *",
                "ret = Pig.fs(\"-rmr simple_out\")",
                "if ret == 0:",
                "\tprint 'success!'",
                "else:",
                "\traise 'fs command failed'"
        };

        runPigRunner("fsTest", script, false);

    }

    @Test
    public void NegativeTest() throws Exception {
        String[] script = {
                "#!/usr/bin/python",
                " from org.apache.pig.scripting import *",
                "Pig.fs(\"rmr simple_out\")"
        };

        PigStats stats = runPigRunner("NegativeTest", script, false);

        assertTrue(stats.getErrorCode() == 1121);
        assertTrue(stats.getReturnCode() == PigRunner.ReturnCode.PIG_EXCEPTION);
    }

    @Test
    public void NegativeTest2() throws Exception {
        String[] script = {
                "#!/usr/bin/python",
                " from org.apache.pig.scripting import *",
                "raise 'This is a test'"
        };

        PigStats stats = runPigRunner("NegativeTest2", script, false);
        assertTrue(stats.getErrorCode() == 1121);
        assertTrue(stats.getReturnCode() == PigRunner.ReturnCode.PIG_EXCEPTION);
    }

    @Test // PIG-2056
    public void NegativeTest3() throws Exception {
        String[] script = {
                "#!/usr/bin/python",
                "from org.apache.pig.scripting import Pig",
                "P = Pig.compile(\"\"\"",
                "    #TEST PIG COMMENTS",
                "    A = load 'studenttab10k' as (name, age, gpa);",
                "    store A into 'CompileBindRun_2.out';\"\"\")",
                "result = P.bind().runSingle()"
        };

        PigStats stats = runPigRunner("NegativeTest3", script, false);
        assertTrue(stats.getErrorCode() == 1121);
        assertTrue(stats.getReturnCode() == PigRunner.ReturnCode.PIG_EXCEPTION);

        String expected = "Python Error. Traceback (most recent call last):";

        String msg = stats.getErrorMessage();
        Util.checkErrorMessageContainsExpected(msg, expected);

    }

    @Test
    public void testFixNonEscapedDollarSign() throws Exception {
        java.lang.reflect.Method fixNonEscapedDollarSign = Class.forName(
                "org.apache.pig.scripting.Pig").getDeclaredMethod(
                "fixNonEscapedDollarSign", new Class[] { String.class });

        fixNonEscapedDollarSign.setAccessible(true);

        String s = (String)fixNonEscapedDollarSign.invoke(null, "abc$py$");
        assertEquals("abc\\\\$py\\\\$", s);

        s = (String)fixNonEscapedDollarSign.invoke(null, "$abc$py");
        assertEquals("\\\\$abc\\\\$py", s);

        s = (String)fixNonEscapedDollarSign.invoke(null, "$");
        assertEquals("\\\\$", s);

        s = (String)fixNonEscapedDollarSign.invoke(null, "$$abc");
        assertEquals("\\\\$\\\\$abc", s);
    }

    // See PIG-2291
    @Test
    public void testDumpInScript() throws Exception{

    	  String[] script = {
                  "#!/usr/bin/python",
                  "from org.apache.pig.scripting import *",
                  "Pig.fs(\"rmr simple_out\")",
                  "input = 'testDumpInScript_table'",
                  "output = 'simple_out'",
                  "P = Pig.compileFromFile(\"\"\"testDumpInScript.pig\"\"\")",
                  "Q = P.bind({'input':input, 'output':output})",
                  "stats = Q.runSingle()",
                  "if stats.isSuccessful():",
                  "\tprint 'success!'",
                  "else:",
                  "\tprint 'failed'"
          };
        String[] input = {
                "1\t3",
                "2\t4",
                "3\t5"
        };

        String[] pigLatin = {
                "a = load '$input' as (a0:int,a1:int);",
                "store a into '$output';",
                "dump a"
        };

        Util.createInputFile(cluster, "testDumpInScript_table", input);
        Util.createLocalInputFile( "testDumpInScript.pig", pigLatin);
        runSimpleScript("testDumpInScript", script);

    }

    // See PIG-2291
    @Test
    public void testIllustrateInScript() throws Exception {

        String[] script = { "#!/usr/bin/python",
                "from org.apache.pig.scripting import *",
                "Pig.fs(\"rmr simple_out\")",
                "input = 'testIllustrateInScript_table'",
                "output = 'simple_out'",
                "P = Pig.compileFromFile(\"\"\"testIllustrateInScript.pig\"\"\")",
                "Q = P.bind({'input':input, 'output':output})",
                "stats = Q.runSingle()", "if stats.isSuccessful():",
                "\tprint 'success!'", "else:", "\tprint 'failed'" };
        String[] input = { "1\t3", "2\t4", "3\t5" };

        String[] pigLatin = { "a = load '$input' as (a0:int,a1:int);",
                "store a into '$output';", "illustrate a" };

        Util.createInputFile(cluster, "testIllustrateInScript_table", input);
        Util.createLocalInputFile("testIllustrateInScript.pig", pigLatin);
        runSimpleScript("testIllustrateInScript", script);
    }

    @Test
    public void testPyShouldNotFailScriptIfExitCodeIs0() throws Exception {
        String[] script = {
                "#!/usr/bin/python",
                "from org.apache.pig.scripting import *",
                "import sys",
                "if 1 == 2:",
                "   sys.exit(1)",
                "else: sys.exit(0)"
         };

        Map<String, List<PigStats>> statsMap = runScript("testPyShouldNotFailScriptIfExitCodeIs0", script);
        assertEquals(0, statsMap.size());

   }
    
    @Test
    public void testSysArguments() throws Exception {
        String[] script = {
                "#!/usr/bin/python",
                "from org.apache.pig.scripting import Pig",
                "import sys",
                "P = Pig.compile(\"\"\"rmf $file;\"\"\")",
                "files = sys.argv[1].strip().split(',')",
                "for file in files:",
                "    P.bind({'file':file}).runSingle()"
         };
        String file1 = "/tmp/sysarg_1";
        String file2 = "/tmp/sysarg_2";
        createEmptyFiles(file1, file2);
        
        File scriptFile = Util.createInputFile("sysarg", ".py", script);
        // ExecMode.STRING
        PigStats stats = PigRunner.run(new String[] { scriptFile.getAbsolutePath(),
                file1 + "," + file2 }, null);
        assertEquals(null, stats.getErrorMessage());
        assertFileNotExists(file1, file2);
        
        createEmptyFiles(file1, file2);
        // Clear stats from previous execution
        PigStatsUtil.getEmptyPigStats();
        
        // ExecMode.FILE
        stats = PigRunner.run(new String[] { "-f", scriptFile.getAbsolutePath(), "arg0", 
                file1 + "," + file2 }, null);
        assertEquals(null, stats.getErrorMessage());
        assertFileNotExists(file1, file2);
    }
    
    private void createEmptyFiles(String... filenames) throws IOException {
        for (String file : filenames) {
            Util.createInputFile(cluster, file, new String[]{""});
            assertTrue(cluster.getFileSystem().exists(new Path(file)));
        }
    }
    
    private void assertFileNotExists(String... filenames) throws IOException {
        for (String file : filenames) {
            assertFalse(cluster.getFileSystem().exists(new Path(file)));
        }
    }

}
