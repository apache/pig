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
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Appender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.PatternLayout;
import org.apache.pig.ExecType;
import org.apache.pig.PigException;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.backend.executionengine.ExecJob.JOB_STATUS;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.test.Util.ProcessReturnInfo;
import org.apache.pig.tools.grunt.Grunt;
import org.apache.pig.tools.pigscript.parser.ParseException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestGrunt {

    static MiniCluster cluster = MiniCluster.buildCluster();
    private String basedir = "test/org/apache/pig/test/data";

    @BeforeClass
    public static void oneTimeSetup() throws Exception {
        cluster.setProperty("opt.multiquery","true");
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        cluster.shutDown();
    }


    @Test
    public void testCopyFromLocal() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();

        String strCmd = "copyFromLocal README.txt sh_copy ;";

        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);

        grunt.exec();
    }


    @Test
    public void testDefine() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();

        String strCmd = "define myudf org.apache.pig.builtin.AVG();\n";

        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);

        try {
            grunt.exec();
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Encountered \"define\""));
        }
        assertTrue(null != context.getFuncSpecFromAlias("myudf"));
    }

    @Test
    public void testBagSchema() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();

        String strCmd = "a = load 'input1' as (b: bag{t:(i: int, c:chararray, f: float)});\n";

        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);

        grunt.exec();
    }

    @Test
    public void testBagSchemaFail() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();

        String strCmd = "a = load 'input1'as (b: bag{t:(i: int, c:chararray, f: float)});\n";

        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);

        try {
            grunt.exec();
            fail( "Test case is supposed to fail." );
        } catch (Exception e) {
            assertTrue(e.getMessage().contains(
                    "<line 1, column 62>  mismatched input ';' expecting RIGHT_PAREN"));
        }
    }

    @Test
    public void testBagConstant() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();

        String strCmd = "a = load 'input1'; b = foreach a generate {(1, '1', 0.4f),(2, '2', 0.45)};\n";

        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);

        grunt.exec();
    }

    @Test
    public void testBagConstantWithSchema() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();

        String strCmd = "a = load 'input1'; b = foreach a generate "
                + "{(1, '1', 0.4f),(2, '2', 0.45)} as "
                + "b: bag{t:(i: int, c:chararray, d: double)};\n";

        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);

        grunt.exec();
    }

    @Test
    public void testBagConstantInForeachBlock() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();

        String strCmd = "a = load 'input1'; "
                + "b = foreach a {generate {(1, '1', 0.4f),(2, '2', 0.45)};};\n";

        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);

        grunt.exec();
    }

    @Test
    public void testBagConstantWithSchemaInForeachBlock() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();

        String strCmd = "a = load 'input1'; "
                + "b = foreach a {generate {(1, '1', 0.4f),(2, '2', 0.45)} "
                + "as b: bag{t:(i: int, c:chararray, d: double)};};\n";

        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);

        grunt.exec();
    }

    @Test
    public void testParsingAsInForeachBlock() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();

        String strCmd = "a = load 'foo' as (foo, fast); "
                + "b = group a by foo; c = foreach b "
                + "{generate SUM(a.fast) as fast;};\n";

        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);

        grunt.exec();
    }

    @Test
    public void testParsingAsInForeachWithOutBlock() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();

        String strCmd = "a = load 'foo' as (foo, fast); "
                + "b = group a by foo; c = foreach b generate SUM(a.fast) as fast;\n";

        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);

        grunt.exec();
    }

    @Test
    public void testParsingWordWithAsInForeachBlock() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();

        String strCmd = "a = load 'foo' as (foo, fast); "
                + "b = group a by foo; c = foreach b {generate SUM(a.fast);};\n";

        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);

        grunt.exec();
    }

    @Test
    public void testParsingWordWithAsInForeachWithOutBlock() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();

        String strCmd = "a = load 'foo' as (foo, fast); "
                + "b = group a by foo; c = foreach b generate SUM(a.fast);\n";

        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);

        grunt.exec();
    }

    @Test
    public void testParsingWordWithAsInForeachWithOutBlock2() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();

        String strCmd = "cash = load 'foo' as (foo, fast); "
                + "b = foreach cash generate fast * 2.0;\n";

        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);

        grunt.exec();
    }


    @Test
    public void testParsingGenerateInForeachBlock() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();

        String strCmd = "a = load 'foo' as (foo, fast, regenerate); "
                + "b = group a by foo; c = foreach b {generate a.regenerate;};\n";

        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);

        grunt.exec();
    }

    @Test
    public void testParsingGenerateInForeachWithOutBlock() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();

        String strCmd = "a = load 'foo' as (foo, fast, regenerate); "
                + "b = group a by foo; c = foreach b generate a.regenerate;\n";

        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);

        grunt.exec();
    }

    @Test
    public void testParsingAsGenerateInForeachBlock() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();

        String strCmd = "a = load 'foo' as (foo, fast, regenerate); "
                + "b = group a by foo; c = foreach b {generate "
                + "{(1, '1', 0.4f),(2, '2', 0.45)} "
                + "as b: bag{t:(i: int, cease:chararray, degenerate: double)}, "
                + "SUM(a.fast) as fast, a.regenerate as degenerated;};\n";

        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);

        grunt.exec();
    }

    @Test
    public void testParsingAsGenerateInForeachWithOutBlock() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();

        String strCmd = "a = load 'foo' as (foo, fast, regenerate); "
                + "b = group a by foo; c = foreach b generate "
                + "{(1, '1', 0.4f),(2, '2', 0.45)} "
                + "as b: bag{t:(i: int, cease:chararray, degenerate: double)}, "
                + "SUM(a.fast) as fast, a.regenerate as degenerated;\n";

        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);

        grunt.exec();
    }

    @Test
    public void testRunStatment() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();

        String strCmd = "a = load 'foo' as (foo, fast, regenerate);"
                + " run -param LIMIT=5 -param_file " + basedir
                + "/test_broken.ppf " + basedir + "/testsub.pig; explain bar";

        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);

        grunt.exec();
    }

    @Test
    public void testExecStatment() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();
        boolean caught = false;

        String strCmd = "a = load 'foo' as (foo, fast, regenerate);"
                + " exec -param LIMIT=5 -param FUNCTION=COUNT "
                + "-param FILE=foo " + basedir + "/testsub.pig; explain bar;";

        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);

        try {
            grunt.exec();
        } catch (Exception e) {
            caught = true;
            assertTrue(e.getMessage().contains("alias bar"));
        }
        assertTrue(caught);
    }

    @Test
    public void testRunStatmentNested() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();

        String strCmd = "a = load 'foo' as (foo, fast, regenerate); run "
                + basedir + "/testsubnested_run.pig; explain bar";

        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);

        grunt.exec();
    }

    @Test
    public void testExecStatmentNested() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();
        boolean caught = false;

        String strCmd = "a = load 'foo' as (foo, fast, regenerate); exec "
                + basedir + "/testsubnested_exec.pig; explain bar";

        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);

        try {
            grunt.exec();
        } catch (Exception e) {
            caught = true;
            assertTrue(e.getMessage().contains("alias bar"));
        }
        assertTrue(caught);
    }

    @Test
    public void testErrorLineNumber() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();

        String strCmd = "A = load 'x' as ( u:int, v:chararray );\n" +
                        "sh ls\n" +
                        "B = foreach A generate u , v; C = distinct 'xxx';\n" +
                        "store C into 'y';";

        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);

        try {
            grunt.exec();
        } catch (Exception e) {
            assertTrue(e.getMessage().contains(
                    "<line 3, column 42>  Syntax error, unexpected symbol at or near ''xxx''"));
            return;
        }
        fail( "Test case is supposed to fail." );
    }

    @Test
    public void testExplainEmpty() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();

        String strCmd = "a = load 'foo' as (foo, fast, regenerate); run "
                + basedir + "/testsubnested_run.pig; explain";

        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);

        grunt.exec();
    }

    @Test
    public void testExplainScript() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();

        String strCmd = "a = load 'foo' as (foo, fast, regenerate); explain -script "
                + basedir + "/testsubnested_run.pig;";

        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);

        grunt.exec();
    }

    /**
     * PIG-2084
     * Check if only statements used in query are validated, in non-interactive
     * /non-check mode. There is an  'unused' statement in query that would otherise
     * fail the validation.
     * Primary purpose of test is to verify that check not happening for
     *  every statement.
     * @throws Throwable
     */
    @Test
    public void testExplainScriptIsEachStatementValidated() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();

        String strCmd = "a = load 'foo' as (foo, fast, regenerate);" +
                "b = foreach a generate foo + 'x' + 1;" +
                "c = foreach a generate foo, fast;" +
                "explain c; ";

        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);

        grunt.exec();
    }

    @Test
    public void testIllustrateScript() throws Throwable {
        PigServer server = new PigServer(ExecType.LOCAL, new Properties());
        PigContext context = server.getPigContext();

        String strCmd = "illustrate -script "
                + basedir + "/illustrate.pig;";

        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);

        grunt.exec();
    }

    @Test
    public void testIllustrateScript2() throws Throwable {
        PigServer server = new PigServer(ExecType.LOCAL, new Properties());
        PigContext context = server.getPigContext();

        String strCmd = "illustrate -script "
                + basedir + "/illustrate2.pig;";

        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);

        grunt.exec();
    }

    @Test
    public void testIllustrateScript3() throws Throwable {
        PigServer server = new PigServer(ExecType.LOCAL, new Properties());
        PigContext context = server.getPigContext();

        String strCmd = "illustrate -script "
                + basedir + "/illustrate3.pig;";

        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);

        grunt.exec();
    }

    @Test
    public void testIllustrateScript4() throws Throwable {
        // empty line/field test
        PigServer server = new PigServer(ExecType.LOCAL, new Properties());
        PigContext context = server.getPigContext();

        String strCmd = "illustrate -script "
                + basedir + "/illustrate4.pig;";

        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);

        grunt.exec();
    }

    @Test
    public void testIllustrateScript5() throws Throwable {
        // empty line/field test
        PigServer server = new PigServer(ExecType.LOCAL, new Properties());
        PigContext context = server.getPigContext();

        String strCmd = "illustrate -script "
                + basedir + "/illustrate5.pig;";

        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);

        grunt.exec();
    }

    @Test
    public void testIllustrateScript6() throws Throwable {
        // empty line/field test
        PigServer server = new PigServer(ExecType.LOCAL, new Properties());
        PigContext context = server.getPigContext();

        String strCmd = "illustrate -script "
                + basedir + "/illustrate6.pig;";

        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);

        grunt.exec();
    }

    @Test
    public void testIllustrateScript7() throws Throwable {
        // empty line/field test
        PigServer server = new PigServer(ExecType.LOCAL, new Properties());
        PigContext context = server.getPigContext();

        String strCmd = "illustrate -script "
                + basedir + "/illustrate7.pig;";

        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);

        grunt.exec();
    }

    /**
     * verify that grunt commands are ignored in explain -script mode
     */
    @Test
    public void testExplainScript2() throws Throwable {

        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();

        String strCmd = "explain -script "
                + basedir + "/explainScript.pig;";

        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);

        String logMessagesFile = "TestGrunt-testExplainScript2-stderr.txt";
        // add a file based appender to the root logger so we can parse the
        // messages logged by grunt and verify that grunt commands are ignored
        // in explain -script mode
        Appender fileAppender = new FileAppender(new PatternLayout(), logMessagesFile);

        try {
            org.apache.log4j.LogManager.getRootLogger().addAppender(fileAppender);
            Grunt grunt = new Grunt(new BufferedReader(reader), context);
            grunt.exec();
            BufferedReader in = new BufferedReader(new FileReader(logMessagesFile));
            String gruntLoggingContents = "";
            //read file into a string
            String line;
            while ( (line = in.readLine()) != null) {
                 gruntLoggingContents += line + "\n";
            }
            in.close();
            String[] cmds = new String[] { "'rm/rmf'", "'cp'", "'cat'", "'cd'", "'pwd'",
                    "'copyFromLocal'", "'copyToLocal'", "'describe'", "'ls'",
                    "'mkdir'", "'illustrate'", "'run/exec'", "'fs'", "'aliases'",
                    "'mv'", "'dump'" };
            for (String c : cmds) {
                String expected = c + " statement is ignored while processing " +
                        "'explain -script' or '-check'";
                assertTrue("Checking if " + gruntLoggingContents + " contains " +
                        expected, gruntLoggingContents.contains(expected));
            }
        } finally {
            org.apache.log4j.LogManager.getRootLogger().removeAppender(fileAppender);
            new File(logMessagesFile).delete();
        }
    }

    @Test
    public void testExplainBrief() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();

        String strCmd = "a = load 'foo' as (foo, fast, regenerate); explain -brief -script "
                + basedir + "/testsubnested_run.pig;";

        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);

        grunt.exec();
    }

    @Test
    public void testExplainDot() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();

        String strCmd = "a = load 'foo' as (foo, fast, regenerate); explain -dot -script "
                + basedir + "/testsubnested_run.pig;";

        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);

        grunt.exec();
    }

    @Test
    public void testExplainOut() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();

        String strCmd = "a = load 'foo' as (foo, fast, regenerate); explain -out /tmp -script "
                + basedir + "/testsubnested_run.pig;";

        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);

        grunt.exec();
    }

    @Test
    public void testPartialExecution() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();
        FileLocalizer.setInitialized(false);

        String strCmd = "rmf bar; rmf baz; "
                + "a = load '"
                + Util.generateURI("file:test/org/apache/pig/test/data/passwd",
                        context)
                + "';"
                + "store a into 'bar'; exec; a = load 'bar'; store a into 'baz';\n";

        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);

        grunt.exec();
    }

    @Test
    public void testFileCmds() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();

        String strCmd =
            "rmf bar; rmf baz;"
            +"a = load '"
            + Util.generateURI("file:test/org/apache/pig/test/data/passwd", context) + "';"
            +"store a into 'bar';"
            +"cp bar baz;"
            +"rm bar; rm baz;"
            +"store a into 'baz';"
            +"store a into 'bar';"
            +"rm baz; rm bar;"
            +"store a into 'baz';"
            +"mv baz bar;"
            +"b = load 'bar';"
            +"store b into 'baz';"
            +"cat baz;"
            +"rm baz;"
            +"rm bar;\n";

        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);

        grunt.exec();
    }

    @Test
    public void testCD() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();

        String strCmd =
            "mkdir /tmp;"
            +"mkdir /tmp/foo;"
            +"cd /tmp;"
            +"rmf bar; rmf foo/baz;"
            +"copyFromLocal test/org/apache/pig/test/data/passwd bar;"
            +"a = load 'bar';"
            +"cd foo;"
            +"store a into 'baz';"
            +"cd /;"
            +"rm /tmp/bar; rm /tmp/foo/baz;";

        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);

        grunt.exec();
    }

    @Test
    public void testDump() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();

        String strCmd =
            "rmf bla;"
            +"a = load '"
            + Util.generateURI("file:test/org/apache/pig/test/data/passwd", context) + "';"
            +"e = group a by $0;"
            +"f = foreach e generate group, COUNT($1);"
            +"store f into 'bla';"
            +"f1 = load 'bla';"
            +"g = order f1 by $1;"
            +"dump g;";

        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);

        grunt.exec();
    }

    @Test
    public void testIllustrate() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();

        String strCmd =
            "rmf bla;"
            +"a = load '"
            + Util.generateURI("file:test/org/apache/pig/test/data/passwd", context) + "';"
            +"e = group a by $0;"
            +"f = foreach e generate group, COUNT($1);"
            +"store f into 'bla';"
            +"f1 = load 'bla' as (f:chararray);"
            +"g = order f1 by $0;"
            +"illustrate g;";

        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);

        grunt.exec();
    }

    @Test
    public void testKeepGoing() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());

        PigContext context = server.getPigContext();

        String filename =
            Util.generateURI("file:test/org/apache/pig/test/data/passwd", context);
        String strCmd =
            "rmf bar;"
            +"rmf foo;"
            +"rmf baz;"
            +"A = load '" + filename + "';"
            +"B = foreach A generate 1;"
            +"C = foreach A generate 0/0;"
            +"store B into 'foo';"
            +"store C into 'bar';"
            +"A = load '" + filename + "';"
            +"B = stream A through `false`;"
            +"store B into 'baz';"
            +"cat bar;";

        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);

        grunt.exec();
    }

    @Test
    public void testKeepGoigFailed() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();
        Util.copyFromLocalToCluster(cluster, "test/org/apache/pig/test/data/passwd", "passwd");
        String strCmd =
            "rmf bar;"
            +"rmf foo;"
            +"rmf baz;"
            +"A = load 'passwd';"
            +"B = foreach A generate 1;"
            +"C = foreach A generate 0/0;"
            +"store B into 'foo';"
            +"store C into 'bar';"
            +"A = load 'passwd';"
            +"B = stream A through `false`;"
            +"store B into 'baz';"
            +"cat baz;";

        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);

        boolean caught = false;
        try {
            grunt.exec();
        } catch (Exception e) {
            caught = true;
            assertTrue(e.getMessage().contains("baz does not exist"));
        }
        assertTrue(caught);
    }

    @Test
    public void testInvalidParam() throws Throwable {
        PigServer server = new PigServer(ExecType.LOCAL, cluster.getProperties());
        PigContext context = server.getPigContext();

        String strCmd =
            "run -param -param;";

        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);

        boolean caught = false;
        try {
            grunt.exec();
        } catch (ParseException e) {
            caught = true;
            assertTrue(e.getMessage().contains("Encountered"));
        }
        assertTrue(caught);
    }

    @Test
    public void testStopOnFailure() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();
        context.getProperties().setProperty("stop.on.failure", ""+true);

        String strCmd =
            "rmf bar;\n"
            +"rmf foo;\n"
            +"rmf baz;\n"
            +"copyFromLocal test/org/apache/pig/test/data/passwd pre;\n"
            +"A = load '"
            + Util.generateURI("file:test/org/apache/pig/test/data/passwd", context) + "';\n"
            +"B = stream A through `false`;\n"
            +"store B into 'bar' using BinStorage();\n"
            +"A = load 'bar';\n"
            +"store A into 'foo';\n"
            +"cp pre done;\n";

        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);

        boolean caught = false;
        try {
            grunt.exec();
        } catch (PigException e) {
            caught = true;
            assertTrue(e.getErrorCode() == 6017);
        }

        assertFalse(server.existsFile("done"));
        assertTrue(caught);
    }

    @Test
    public void testFsCommand() throws Throwable {

        PigServer server = new PigServer(ExecType.MAPREDUCE,cluster.getProperties());
        PigContext context = server.getPigContext();

        String strCmd =
                "fs -ls /;"
                        +"fs -mkdir /fstmp;"
                        +"fs -mkdir /fstmp/foo;"
                        +"cd /fstmp;"
                        +"fs -copyFromLocal test/org/apache/pig/test/data/passwd bar;"
                        +"a = load 'bar';"
                        +"cd foo;"
                        +"store a into 'baz';"
                        +"cd /;"
                        +"fs -ls .;"
                        +"fs -rmr /fstmp/foo/baz;";

        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);
        grunt.exec();

    }

    @Test
    public void testShellCommand(){

        try {
            PigServer server = new PigServer(ExecType.MAPREDUCE,cluster.getProperties());
            PigContext context = server.getPigContext();

            String strRemoveFile = "rm";
            String strRemoveDir = "rmdir";

            if (Util.WINDOWS)
            {
               strRemoveFile = "del";
               strRemoveDir  = "rd";
            }

            String strCmd = "sh mkdir test_shell_tmp;";

            // Create a temp directory via command and make sure it exists
            ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
            InputStreamReader reader = new InputStreamReader(cmd);
            Grunt grunt = new Grunt(new BufferedReader(reader), context);
            grunt.exec();
            assertTrue(new File("test_shell_tmp").exists());

            // Remove the temp directory via shell and make sure it is gone
            strCmd = "sh " + strRemoveDir + " test_shell_tmp;";

            cmd = new ByteArrayInputStream(strCmd.getBytes());
            reader = new InputStreamReader(cmd);
            grunt = new Grunt(new BufferedReader(reader), context);
            grunt.exec();
            assertFalse(new File("test_shell_tmp").exists());

            // Verify pipes are usable in the command context by piping data to a file
            strCmd = "sh echo hello world > tempShFileToTestShCommand";

            cmd = new ByteArrayInputStream(strCmd.getBytes());
            reader = new InputStreamReader(cmd);
            grunt = new Grunt(new BufferedReader(reader), context);
            grunt.exec();
            BufferedReader fileReader = null;
            fileReader = new BufferedReader(new FileReader("tempShFileToTestShCommand"));
            assertTrue(fileReader.readLine().trim().equals("hello world"));

            fileReader.close();

            // Remove the file via cmd and make sure it is gone
            strCmd = "sh " + strRemoveFile + " tempShFileToTestShCommand";
            cmd = new ByteArrayInputStream(strCmd.getBytes());
            reader = new InputStreamReader(cmd);
            grunt = new Grunt(new BufferedReader(reader), context);
            grunt.exec();
            assertFalse(new File("tempShFileToTestShCommand").exists());

            if (Util.WINDOWS) {
               //FIXME
               // We need to fix this because there is a race condition with pipes.
               // dir command can potentially run before the TouchedFileInsideGrunt_61 is written
               // Solved for linux/unix below using xargs
               strCmd = "sh echo foo > TouchedFileInsideGrunt_61 | dir /B | findstr TouchedFileInsideGrunt_61 > fileContainingTouchedFileInsideGruntShell_71";
            }
            else {
               strCmd = "sh touch TouchedFileInsideGrunt_61 | ls | grep TouchedFileInsideGrunt_61 > fileContainingTouchedFileInsideGruntShell_71";
            }

            cmd = new ByteArrayInputStream(strCmd.getBytes());
            reader = new InputStreamReader(cmd);
            grunt = new Grunt(new BufferedReader(reader), context);
            grunt.exec();
            fileReader = new BufferedReader(new FileReader("fileContainingTouchedFileInsideGruntShell_71"));
            assertTrue(fileReader.readLine().trim().equals("TouchedFileInsideGrunt_61"));

            fileReader.close();
            strCmd = "sh " + strRemoveFile+" fileContainingTouchedFileInsideGruntShell_71";

            cmd = new ByteArrayInputStream(strCmd.getBytes());
            reader = new InputStreamReader(cmd);
            grunt = new Grunt(new BufferedReader(reader), context);
            grunt.exec();
            assertFalse(new File("fileContainingTouchedFileInsideGruntShell_71").exists());
            strCmd = "sh " + strRemoveFile + " TouchedFileInsideGrunt_61";
            cmd = new ByteArrayInputStream(strCmd.getBytes());
            reader = new InputStreamReader(cmd);
            grunt = new Grunt(new BufferedReader(reader), context);
            grunt.exec();
            assertFalse(new File("TouchedFileInsideGrunt_61").exists());


        } catch (ExecException e) {
            e.printStackTrace();
            fail();
        } catch (Throwable e) {
            e.printStackTrace();
            fail();
        }
    }

    // See PIG-2497
    @Test
    public void testShellCommandOrder() throws Throwable {
        PigServer server = new PigServer(ExecType.LOCAL, new Properties());

        String strRemove = "rm";

        if (Util.WINDOWS)
        {
            strRemove = "del";
        }

        File inputFile = File.createTempFile("testInputFile", ".txt");
        PrintWriter pwInput = new PrintWriter(new FileWriter(inputFile));
        pwInput.println("1");
        pwInput.close();

        File inputScript = File.createTempFile("testInputScript", "");
        File outputFile = File.createTempFile("testOutputFile", ".txt");
        outputFile.delete();
        PrintWriter pwScript = new PrintWriter(new FileWriter(inputScript));
        pwScript.println("a = load '" + Util.encodeEscape(inputFile.getAbsolutePath()) + "';");
        pwScript.println("store a into '" + Util.encodeEscape(outputFile.getAbsolutePath()) + "';");
        pwScript.println("sh " + strRemove + " " + Util.encodeEscape(inputFile.getAbsolutePath()));
        pwScript.close();

        InputStream inputStream = new FileInputStream(inputScript.getAbsoluteFile());
        server.setBatchOn();
        server.registerScript(inputStream);
        List<ExecJob> execJobs = server.executeBatch();
        assertTrue(execJobs.get(0).getStatus() == JOB_STATUS.COMPLETED);
    }

    @Test
    public void testSetPriority() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();

        String strCmd = "set job.priority high\n";

        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);

        grunt.exec();
        assertEquals("high", context.getProperties().getProperty(PigContext.JOB_PRIORITY));
    }

    @Test
    public void testSetWithQuotes() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();

        String strCmd = "set job.priority 'high'\n";

        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);

        grunt.exec();
        assertEquals("high", context.getProperties().getProperty(PigContext.JOB_PRIORITY));
    }

    @Test
    public void testRegisterWithQuotes() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();

        String strCmd = "register 'pig-withouthadoop.jar'\n";

        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);

        grunt.exec();
        assertEquals(context.extraJars+ " of size 1", 1, context.extraJars.size());
        assertTrue(context.extraJars.get(0)+" ends with /pig-withouthadoop.jar", context.extraJars.get(0).toString().endsWith("/pig-withouthadoop.jar"));
    }

    @Test
    public void testRegisterWithoutQuotes() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();

        String strCmd = "register pig-withouthadoop.jar\n";

        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);

        grunt.exec();
        assertEquals(context.extraJars+ " of size 1", 1, context.extraJars.size());
        assertTrue(context.extraJars.get(0)+" ends with /pig-withouthadoop.jar", context.extraJars.get(0).toString().endsWith("/pig-withouthadoop.jar"));
    }

    @Test
    public void testRegisterScripts() throws Throwable {
        String[] script = {
                "#!/usr/bin/python",
                "@outputSchema(\"x:{t:(num:long)}\")",
                "def square(number):" ,
                "\treturn (number * number)"
        };

        Util.createLocalInputFile( "testRegisterScripts.py", script);

        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();

        String strCmd = "register testRegisterScripts.py using jython as pig\n";

        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);

        grunt.exec();
        assertTrue(context.getFuncSpecFromAlias("pig.square") != null);

    }

    @Test
    public void testScriptMissingLastNewLine() throws Throwable {
        PigServer server = new PigServer(ExecType.LOCAL);
        PigContext context = server.getPigContext();

        String strCmd = "A = load 'bar';\nB = foreach A generate $0;";

        BufferedReader reader = new BufferedReader(new StringReader(strCmd));
        String substituted = context.doParamSubstitution(reader, null, null);
        BufferedReader pigInput = new BufferedReader(new StringReader(substituted));

        Grunt grunt = new Grunt(pigInput, context);
        int results[] = grunt.exec();
        for (int i=0; i<results.length; i++) {
            assertEquals(0, results[i]);
        }
    }


    // Test case for PIG-740 to report an error near the double quotes rather
    // than an unrelated EOF error message
    @Test
    public void testBlockErrMessage() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();

        String script = "A = load 'inputdata' using PigStorage() as ( curr_searchQuery );\n" +
                "B = foreach A { domain = CONCAT(curr_searchQuery,\"^www\\.\");\n" +
                "        generate domain; };\n";
        ByteArrayInputStream cmd = new ByteArrayInputStream(script.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);

        try {
            grunt.exec();
        } catch(FrontendException e) {
            e.printStackTrace();
            assertTrue(e.getMessage().contains("Error during parsing. <line 2, column 49>  Unexpected character '\"'"));
        }
    }

    @Test
    public void testCheckScript() throws Throwable {
        // a query which has grunt commands intermixed with pig statements - this
        // should pass through successfully with the check and all the grunt commands
        // should be ignored during the check.
        String query = "rmf input-copy.txt; cat 'foo'; a = load '1.txt' ; " +
                "aliases;illustrate a; copyFromLocal foo bar; copyToLocal foo bar; " +
                "describe a; mkdir foo; run bar.pig; exec bar.pig; cp foo bar; " +
                "explain a;cd 'bar'; pwd; ls ; fs -ls ; fs -rmr foo; mv foo bar; " +
                "dump a;store a into 'input-copy.txt' ; a = load '2.txt' as (b);" +
                "explain a; rm foo; store a into 'bar';";

        String[] cmds = new String[] { "'rm/rmf'", "'cp'", "'cat'", "'cd'", "'pwd'",
                "'copyFromLocal'", "'copyToLocal'", "'describe'", "'ls'",
                "'mkdir'", "'illustrate'", "'run/exec'", "'fs'", "'aliases'",
                "'mv'", "'dump'" };
        ArrayList<String> msgs = new ArrayList<String>();
        for (String c : cmds) {
            msgs.add(c + " statement is ignored while processing " +
                    "'explain -script' or '-check'");
        }
        validate(query, true, msgs.toArray(new String[0]));
    }

    @Test
    public void testCheckScriptSyntaxErr() throws Throwable {
        // a query which has grunt commands intermixed with pig statements - this
        // should fail with the -check option with a syntax error

        // the query has a typo - chararay instead of chararray
        String query = "a = load '1.txt' ;  fs -rmr foo; mv foo bar; dump a;" +
                "store a into 'input-copy.txt' ; dump a; a = load '2.txt' as " +
                "(b:chararay);explain a; rm foo; store a into 'bar';";

        String[] cmds = new String[] { "'fs'", "'mv'", "'dump'" };
        ArrayList<String> msgs = new ArrayList<String>();
        for (String c : cmds) {
            msgs.add(c + " statement is ignored while processing " +
                    "'explain -script' or '-check'");
        }
        msgs.add("Syntax error");
        validate(query, false, msgs.toArray(new String[0]));
    }

    @Test
    public void testCheckScriptSyntaxWithSemiColonUDFErr() throws Throwable {
        // Should able to handle semicolons in udf
        String query = "a = load 'i1' as (f1:chararray);" +
                       "c = foreach a generate REGEX_EXTRACT(f1, '.;' ,1); dump c;";

        ArrayList<String> msgs = new ArrayList<String>();                //
        validate(query, true, msgs.toArray(new String[0]));
    }

    @Test
    public void testCheckScriptTypeCheckErr() throws Throwable {
        // a query which has grunt commands intermixed with pig statements - this
        // should fail with the -check option with a type checking error

        // the query has incompatible types in bincond
        String query = "a = load 'foo.pig' as (s:chararray); dump a; explain a; " +
                "store a into 'foobar'; b = foreach a generate " +
                "(s == 2 ? 1 : 2.0); store b into 'bar';";

        String[] cmds = new String[] { "'dump'" };
        ArrayList<String> msgs = new ArrayList<String>();
        for (String c : cmds) {
            msgs.add(c + " statement is ignored while processing " +
                    "'explain -script' or '-check'");
        }
        msgs.add("incompatible types in Equal Operator");
        validate(query, false, msgs.toArray(new String[0]));
    }



    private void validate(String query, boolean syntaxOk,
            String[] logMessagesToCheck) throws Throwable {
        File scriptFile = Util.createFile(new String[] { query});
        String scriptFileName = scriptFile.getAbsolutePath();
        String cmd = "java -cp " + System.getProperty("java.class.path") +
        " org.apache.pig.Main -x local -c " + scriptFileName;

        ProcessReturnInfo  pri  = Util.executeJavaCommandAndReturnInfo(cmd);
        for (String msg : logMessagesToCheck) {
            assertTrue("Checking if " + pri.stderrContents + " contains " +
                    msg, pri.stderrContents.contains(msg));
        }
        if(syntaxOk) {
            assertTrue("Checking that the syntax OK message was printed on " +
                    "stderr <" + pri.stderrContents + ">",
                    pri.stderrContents.contains("syntax OK"));
        } else {
            assertFalse("Checking that the syntax OK message was NOT printed on " +
                    "stderr <" + pri.stderrContents + ">",
                    pri.stderrContents.contains("syntax OK"));
        }
    }

    @Test
    public void testSet() throws Throwable {

        String strCmd = "set my.arbitrary.key my.arbitrary.value\n";
        PigContext pc = new PigServer(ExecType.LOCAL).getPigContext();
        InputStreamReader reader = new InputStreamReader(new ByteArrayInputStream(strCmd.getBytes()));
        new Grunt(new BufferedReader(reader), pc).exec();

        assertEquals("my.arbitrary.value",  pc.getProperties().getProperty("my.arbitrary.key"));
    }

    @Test
    public void testCheckScriptTypeCheckErrNoStoreDump() throws Throwable {
        //the query has not store or dump, but in when -check is used
        // all statements should be validated
        String query = "a = load 'foo.pig' as (s:chararray); " +
        "b = foreach a generate $1;";

        String msg = "Trying to access non-existent column";
        validateGruntCheckFail(query, msg);
    }

    private void validateGruntCheckFail(String piglatin, String errMsg) throws Throwable{
        String scriptFile = "myscript.pig";
        try {
            BufferedReader br = new BufferedReader(new StringReader(piglatin));
            Grunt grunt = new Grunt(br, new PigContext(ExecType.LOCAL, new Properties()));
            String [] inp = {piglatin};
            Util.createLocalInputFile(scriptFile, inp);

            grunt.checkScript(scriptFile);

            fail("Expected exception isn't thrown");
        } catch (FrontendException e) {
            Util.checkMessageInException(e, errMsg);
        }
    }

    @Test
    public void testDebugOn() throws Throwable {

        String strCmd = "set debug on\n";
        PigContext pc = new PigServer(ExecType.LOCAL).getPigContext();
        InputStreamReader reader = new InputStreamReader(new ByteArrayInputStream(strCmd.getBytes()));
        new Grunt(new BufferedReader(reader), pc).exec();

        assertEquals(Level.DEBUG.toString(),  pc.getLog4jProperties().getProperty("log4j.logger.org.apache.pig"));
    }

    @Test
    public void testDebugOff() throws Throwable {

        String strCmd = "set debug off\n";
        PigContext pc = new PigServer(ExecType.LOCAL).getPigContext();
        InputStreamReader reader = new InputStreamReader(new ByteArrayInputStream(strCmd.getBytes()));
        new Grunt(new BufferedReader(reader), pc).exec();

        assertEquals(Level.INFO.toString(),  pc.getLog4jProperties().getProperty("log4j.logger.org.apache.pig"));
    }
}
