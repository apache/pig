/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.test.pigunit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.pig.ExecType;
import org.apache.pig.pigunit.Cluster;
import org.apache.pig.pigunit.PigTest;
import org.apache.pig.pigunit.pig.PigServer;
import org.apache.pig.tools.parameters.ParseException;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * <p>
 * Various examples about how to use PigUnit.
 *
 * <p>
 * Requires in CLASSPATH:
 * <ul>
 * <li>pig.jar</li>
 * <li>pigunit.jar</li>
 * <li>$HADOOP_CONF_DIR to current/future cluster if not using LOCAL mode</li>
 * </ul>
 */
public class TestPigTest {
    private PigTest test;
    private static Cluster cluster;
    private static final String PIG_SCRIPT = "test/data/pigunit/top_queries.pig";
    private static final String PIG_SCRIPT_MACRO = "test/data/pigunit/top_queries_macro.pig";
    private static final Log LOG = LogFactory.getLog(TestPigTest.class);

    @BeforeClass
    public static void setUpOnce() throws IOException {
        cluster = PigTest.getCluster();

        cluster.update(
                new Path("test/data/pigunit/top_queries_input_data.txt"),
                new Path("top_queries_input_data.txt"));
    }

    @Test
    public void testNtoN() throws ParseException, IOException {
        String[] args = {
                        "n=3",
                        "reducers=1",
                        "input=top_queries_input_data.txt",
                        "output=top_3_queries",
        };
        test = new PigTest(PIG_SCRIPT, args);

        String[] output = {
                        "(yahoo,25)",
                        "(facebook,15)",
                        "(twitter,7)",
        };

        test.assertOutput("queries_limit", output);
    }

    @Test
    public void testImplicitNtoN() throws ParseException, IOException {
        String[] args = {
                        "n=3",
                        "reducers=1",
                        "input=top_queries_input_data.txt",
                        "output=top_3_queries",
        };
        test = new PigTest(PIG_SCRIPT, args);

        String[] output = {
                        "(yahoo,25)",
                        "(facebook,15)",
                        "(twitter,7)",
        };

        test.assertOutput(output);
    }

    @Test
    public void testTextInput() throws ParseException, IOException {
        String[] args = {
                        "n=3",
                        "reducers=1",
                        "input=top_queries_input_data.txt",
                        "output=top_3_queries",
        };
        test = new PigTest(PIG_SCRIPT, args);

        String[] input = {
                        "yahoo\t10",
                        "twitter\t7",
                        "facebook\t10",
                        "yahoo\t15",
                        "facebook\t5",
                        "a\t1",
                        "b\t2",
                        "c\t3",
                        "d\t4",
                        "e\t5",
        };

        String[] output = {
                        "(yahoo,25)",
                        "(facebook,15)",
                        "(twitter,7)",
        };

        test.assertOutput("data", input, "queries_limit", output);
    }

    @Test
    public void testDelimiter() throws ParseException, IOException {
        String[] args = {
                        "n=3",
                        "reducers=1",
                        "input=top_queries_input_data.txt",
                        "output=top_3_queries",
        };
        test = new PigTest(PIG_SCRIPT, args);

        String[] input = {
                        "yahoo,10",
                        "twitter,7",
                        "facebook,10",
                        "yahoo,15",
                        "facebook,5",
                        "a,1",
                        "b,2",
                        "c,3",
                        "d,4",
                        "e,5",
        };

        String[] output = {
                        "(yahoo,25)",
                        "(facebook,15)",
                        "(twitter,7)",
        };

        test.assertOutput("data", input, "queries_limit", output, ",");
    }

    @Test
    public void testSubset() throws ParseException, IOException {
        String[] args = {
                        "n=3",
                        "reducers=1",
                        "input=top_queries_input_data.txt",
                        "output=top_3_queries",
        };
        test = new PigTest(PIG_SCRIPT, args);

        String[] input = {
                        "yahoo\t10",
                        "twitter\t7",
                        "facebook\t10",
                        "yahoo\t15",
                        "facebook\t5",
                        "a\t1",
                        "b\t2",
                        "c\t3",
                        "d\t4",
                        "e\t5",
        };

        String[] output = {
                        "(yahoo,25)",
                        "(facebook,15)",
                        "(twitter,7)",
        };

        test.assertOutput("data", input, "queries_limit", output);
    }

    @Test
    public void testOverride() throws ParseException, IOException {
        String[] args = {
                        "n=3",
                        "reducers=1",
                        "input=top_queries_input_data.txt",
                        "output=top_3_queries",
        };
        test = new PigTest(PIG_SCRIPT, args);

        test.override("queries_limit", "queries_limit = LIMIT queries_ordered 2;");

        String[] output = {
                        "(yahoo,25)",
                        "(facebook,15)",
        };

        test.assertOutput(output);
    }

    @Test
    public void testInlinePigScript() throws ParseException, IOException {
        String[] script = {
                        "data = LOAD 'top_queries_input_data.txt' AS (query:CHARARRAY, count:INT);",
                        "queries_group = GROUP data BY query PARALLEL 1;",
                        "queries_sum = FOREACH queries_group GENERATE group AS query, SUM(data.count) AS count;",
                        "queries_ordered = ORDER queries_sum BY count DESC PARALLEL 1;",
                        "queries_limit = LIMIT queries_ordered 3;",
                        "STORE queries_limit INTO 'top_3_queries';",
        };

        test = new PigTest(script);

        String[] output = {
                        "(yahoo,25)",
                        "(facebook,15)",
                        "(twitter,7)",
        };

        test.assertOutput(output);
    }

    @Test
    public void testFileOutput() throws ParseException, IOException {
        String[] args = {
                        "n=3",
                        "reducers=1",
                        "input=top_queries_input_data.txt",
                        "output=top_3_queries",
        };
        test = new PigTest(PIG_SCRIPT, args);

        test.assertOutput(new File("test/data/pigunit/top_queries_expected_top_3.txt"));
    }

    @Test
    public void testArgFiles() throws ParseException, IOException {
        String[] argsFile = {
                        "test/data/pigunit/top_queries_params.txt"
        };

        test = new PigTest(PIG_SCRIPT, null, argsFile);

        test.assertOutput(new File("test/data/pigunit/top_queries_expected_top_3.txt"));
    }

    @Test
    public void testGetLastAlias() throws ParseException, IOException {
        String[] script = {
                        "data = LOAD 'top_queries_input_data.txt' AS (query:CHARARRAY, count:INT);",
                        "queries_group = GROUP data BY query PARALLEL 1;",
                        "queries_sum = FOREACH queries_group GENERATE group AS query, SUM(data.count) AS count;",
                        "queries_ordered = ORDER queries_sum BY count DESC PARALLEL 1;",
                        "queries_limit = LIMIT queries_ordered 3;",
                        "STORE queries_limit INTO 'top_3_queries';",
        };

        test = new PigTest(script);

        String expected =
                "(yahoo,25)\n" +
                        "(facebook,15)\n" +
                        "(twitter,7)";

        assertEquals(expected, StringUtils.join(test.getAlias("queries_limit"), "\n"));
    }

    @Test
    public void testWithUdf() throws ParseException, IOException {
        String[] script = {
                        // "REGISTER myIfNeeded.jar;",
                        "DEFINE TOKENIZE TOKENIZE();",
                        "data = LOAD 'top_queries_input_data.txt' AS (query:CHARARRAY, count:INT);",
                        "queries = FOREACH data GENERATE query, TOKENIZE(query) AS query_tokens;",
                        "queries_ordered = ORDER queries BY query DESC PARALLEL 1;",
                        "queries_limit = LIMIT queries_ordered 3;",
                        "STORE queries_limit INTO 'top_3_queries';",
        };

        test = new PigTest(script);

        String[] output = {
                        "(yahoo,{(yahoo)})",
                        "(yahoo,{(yahoo)})",
                        "(twitter,{(twitter)})",
        };

        test.assertOutput(output);
    }

    @Test
    public void testStore() throws ParseException, IOException {
        String[] args = {
                        "n=3",
                        "reducers=1",
                        "input=top_queries_input_data.txt",
                        "output=top_3_queries",
        };
        test = new PigTest(PIG_SCRIPT, args);

        // By default PigUnit removes all the STORE and DUMP
        test.unoverride("STORE");

        test.runScript();

        assertTrue(cluster.delete(new Path("top_3_queries")));
    }
    
    @Test
    // Script should only be registered once, otherwise Pig will complain
    // Macro defined twice. See PIG-3114
    public void testMacro() throws ParseException, IOException {
        String[] args = {
                        "n=3",
                        "reducers=1",
                        "input=top_queries_input_data.txt",
                        "output=top_3_queries",
        };
        test = new PigTest(PIG_SCRIPT_MACRO, args);

        // By default PigUnit removes all the STORE and DUMP
        test.unoverride("STORE");

        test.runScript();

        assertTrue(cluster.delete(new Path("top_3_queries")));
    }

    @Ignore("Not ready yet")
    @Test
    public void testWithMock() throws ParseException, IOException {
        String[] args = {
                        "n=3",
                        "reducers=1",
                        "input=top_queries_input_data.txt",
                        "output=top_3_queries",
        };

        PigServer mockServer = null;
        Cluster mockCluster = null;

        test = new PigTest(PIG_SCRIPT, args, mockServer, mockCluster);

        test.assertOutput(new File("data/top_queries_expected_top_3.txt"));
    }

    /**
     * This is a test for default bootup. PIG-2456
     *
     * @throws IOException
     */

    @Test
    public void testDefaultBootup() throws ParseException, IOException {
        // Test with properties file
        String pigProps = "pig.properties";
        String bootupPath = "/tmp/.temppigbootup";
        File propertyFile = new File(pigProps);
        PrintWriter out = new PrintWriter(new FileWriter(propertyFile));
        out.println("pig.load.default.statements=" + bootupPath);
        out.close();

        File bootupFile = new File(bootupPath);
        out = new PrintWriter(new FileWriter(bootupFile));
        out.println("data = LOAD 'top_queries_input_data.txt' AS (query:CHARARRAY, count:INT);");
        out.close();

        String[] script = {
                        // The following line is commented as the test creates a bootup file which
                        // contains it instead. PigTests (and Pig scripts in general) will read the
                        // bootup file to load default statements
                        // "data = LOAD 'top_queries_input_data.txt' AS (query:CHARARRAY, count:INT);",
                        "queries_group = GROUP data BY query PARALLEL 1;",
                        "queries_sum = FOREACH queries_group GENERATE group AS query, SUM(data.count) AS count;",
                        "queries_ordered = ORDER queries_sum BY count DESC PARALLEL 1;",
                        "queries_limit = LIMIT queries_ordered 3;",
                        "STORE queries_limit INTO 'top_3_queries';",
        };

        String scriptPath = "/tmp/tempScript";
        File scriptFile = new File(scriptPath);
        out = new PrintWriter(new FileWriter(scriptFile));
        for (String line : script) {
            out.println(line);
        }
        out.close();

        String[] args = {
                        "n=3",
                        "reducers=1",
                        "input=top_queries_input_data.txt",
                        "output=top_3_queries",
        };

        // Create a pigunit.pig.PigServer and Cluster to run this test.
        PigServer pig = null;
        if (System.getProperties().containsKey("pigunit.exectype.cluster")) {
            LOG.info("Using cluster mode");
            pig = new PigServer(ExecType.MAPREDUCE);
        } else {
            LOG.info("Using default local mode");
            pig = new PigServer(ExecType.LOCAL);
        }

        final Cluster cluster = new Cluster(pig.getPigContext());

        test = new PigTest(scriptPath, args, pig, cluster);

        String[] output = {
                        "(yahoo,25)",
                        "(facebook,15)",
                        "(twitter,7)",
        };

        test.assertOutput("queries_limit", output);

        propertyFile.delete();
        scriptFile.delete();
        bootupFile.delete();
    }
}
