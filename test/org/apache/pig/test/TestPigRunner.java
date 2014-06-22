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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Counters;
import org.apache.pig.ExecType;
import org.apache.pig.PigConfiguration;
import org.apache.pig.PigRunner;
import org.apache.pig.PigRunner.ReturnCode;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.plan.OperatorPlan;
import org.apache.pig.newplan.Operator;
import org.apache.pig.tools.pigstats.InputStats;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.OutputStats;
import org.apache.pig.tools.pigstats.PigProgressNotificationListener;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.PigStatsUtil;
import org.apache.pig.tools.pigstats.EmptyPigStats;
import org.apache.pig.tools.pigstats.mapreduce.MRJobStats;
import org.apache.pig.tools.pigstats.mapreduce.MRPigStatsUtil;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestPigRunner {

    private static MiniCluster cluster;

    private static final String INPUT_FILE = "input";
    private static final String OUTPUT_FILE = "output";
    private static final String PIG_FILE = "test.pig";

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        cluster = MiniCluster.buildCluster();
        PrintWriter w = new PrintWriter(new FileWriter(INPUT_FILE));
        w.println("1\t2\t3");
        w.println("5\t3\t4");
        w.println("3\t4\t5");
        w.println("5\t6\t7");
        w.println("3\t7\t8");
        w.close();
        Util.copyFromLocalToCluster(cluster, INPUT_FILE, INPUT_FILE);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        new File(INPUT_FILE).delete();
        cluster.shutDown();
    }

    @Before
    public void setUp() {
        deleteAll(new File(OUTPUT_FILE));
    }

    @Test
    public void testErrorLogFile() throws Exception {
        PrintWriter w = new PrintWriter(new FileWriter(PIG_FILE));
        w.println("A = load '" + INPUT_FILE + "' as (a0:int, a1:int, a2:int);");
        w.println("B = foreach A generate StringSize(a0);");
        w.println("store B into '" + OUTPUT_FILE + "';");
        w.close();

        try {
            String[] args = { "-x", "local", PIG_FILE };
            PigStats stats = PigRunner.run(args, null);

            assertTrue(!stats.isSuccessful());

            Properties props = stats.getPigProperties();
            String logfile = props.getProperty("pig.logfile");
            File f = new File(logfile);
            assertTrue(f.exists());
        } finally {
            new File(PIG_FILE).delete();
        }
    }

    @Test
    public void testErrorLogFile2() throws Exception {
        PrintWriter w = new PrintWriter(new FileWriter(PIG_FILE));
        w.println("A = load '" + INPUT_FILE + "' as (a0:int, a1:int, a2:int);");
        w.println("B = foreach A generate StringSize(a0);");
        w.println("store B into '" + OUTPUT_FILE + "';");
        w.close();

        try {
            String[] args = { "-M", "-x", "local", PIG_FILE };
            PigStats stats = PigRunner.run(args, null);

            assertTrue(!stats.isSuccessful());

            Properties props = stats.getPigProperties();
            // If test on nfs, the pig script complaining "output" exists
            // and does not actually launch the job. This could due to a mapreduce
            // bug which removing file before closing it.
            // If this happens, props is null because we only set pigContext before
            // launching job.
            if (props!=null) {
                String logfile = props.getProperty("pig.logfile");
                File f = new File(logfile);
                assertTrue(f.exists());
            }
        } finally {
            new File(PIG_FILE).delete();
        }
    }

    @Test
    public void simpleTest() throws Exception {
        PrintWriter w = new PrintWriter(new FileWriter(PIG_FILE));
        w.println("A = load '" + INPUT_FILE + "' as (a0:int, a1:int, a2:int);");
        w.println("B = group A by a0;");
        w.println("C = foreach B generate group, COUNT(A);");
        w.println("store C into '" + OUTPUT_FILE + "';");
        w.close();

        try {
            String[] args = { "-Dstop.on.failure=true", "-Dopt.multiquery=false", "-Dopt.fetch=false", "-Daggregate.warning=false", PIG_FILE };
            PigStats stats = PigRunner.run(args, new TestNotificationListener());

            assertTrue(stats.isSuccessful());

            assertEquals(1, stats.getNumberJobs());
            String name = stats.getOutputNames().get(0);
            assertEquals(OUTPUT_FILE, name);
            assertEquals(12, stats.getBytesWritten());
            assertEquals(3, stats.getRecordWritten());

            assertEquals("A,B,C",
                    ((JobStats)stats.getJobGraph().getSinks().get(0)).getAlias());

            Configuration conf = ConfigurationUtil.toConfiguration(stats.getPigProperties());
            assertTrue(conf.getBoolean("stop.on.failure", false));
            assertTrue(!conf.getBoolean("aggregate.warning", true));
            assertTrue(!conf.getBoolean(PigConfiguration.OPT_MULTIQUERY, true));
            assertTrue(!conf.getBoolean("opt.fetch", true));
        } finally {
            new File(PIG_FILE).delete();
            Util.deleteFile(cluster, OUTPUT_FILE);
        }
    }

    @Test
    public void simpleTest2() throws Exception {
        PrintWriter w = new PrintWriter(new FileWriter(PIG_FILE));
        w.println("A = load '" + INPUT_FILE + "' as (a0:int, a1:int, a2:int);");
        w.println("B = filter A by a0 == 3;");
        w.println("C = limit B 1;");
        w.println("dump C;");
        w.close();

        try {
            String[] args = { "-Dstop.on.failure=true", "-Dopt.multiquery=false", "-Daggregate.warning=false", PIG_FILE };
            PigStats stats = PigRunner.run(args, new TestNotificationListener());

            assertTrue(stats instanceof EmptyPigStats);
            assertTrue(stats.isSuccessful());
            assertEquals(0, stats.getNumberJobs());
            assertEquals(stats.getJobGraph().size(), 0);

            Configuration conf = ConfigurationUtil.toConfiguration(stats.getPigProperties());
            assertTrue(conf.getBoolean("stop.on.failure", false));
            assertTrue(!conf.getBoolean("aggregate.warning", true));
            assertTrue(!conf.getBoolean(PigConfiguration.OPT_MULTIQUERY, true));
            assertTrue(conf.getBoolean("opt.fetch", true));
        } finally {
            new File(PIG_FILE).delete();
            Util.deleteFile(cluster, OUTPUT_FILE);
        }
    }

    @Test
    public void scriptsInDfsTest() throws Exception {
        PrintWriter w = new PrintWriter(new FileWriter(PIG_FILE));
        w.println("A = load '" + INPUT_FILE + "' as (a0:int, a1:int, a2:int);");
        w.println("B = group A by a0;");
        w.println("C = foreach B generate group, COUNT(A);");
        w.println("store C into '" + OUTPUT_FILE + "';");
        w.close();
        Util.copyFromLocalToCluster(cluster, PIG_FILE, PIG_FILE);
        Path inputInDfs = new Path(cluster.getFileSystem().getHomeDirectory(), PIG_FILE);

        try {
            String[] args = { inputInDfs.toString() };
            PigStats stats = PigRunner.run(args, new TestNotificationListener());

            assertTrue(stats.isSuccessful());

            assertTrue(stats.getJobGraph().size() == 1);
            String name = stats.getOutputNames().get(0);
            assertEquals(OUTPUT_FILE, name);
            assertEquals(12, stats.getBytesWritten());
            assertEquals(3, stats.getRecordWritten());

            assertEquals("A,B,C",
                    ((JobStats)stats.getJobGraph().getSinks().get(0)).getAlias());
        } finally {
            new File(PIG_FILE).delete();
            Util.deleteFile(cluster, PIG_FILE);
            Util.deleteFile(cluster, OUTPUT_FILE);
        }
    }

    @Test
    public void orderByTest() throws Exception {
        PrintWriter w = new PrintWriter(new FileWriter(PIG_FILE));
        w.println("A = load '" + INPUT_FILE + "' as (a0:int, a1:int, a2:int);");
        w.println("B = order A by a0;");
        w.println("C = limit B 2;");
        w.println("store C into '" + OUTPUT_FILE + "';");
        w.close();
        String[] args = { PIG_FILE };
        try {
            PigStats stats = PigRunner.run(args, new TestNotificationListener());
            assertTrue(stats.isSuccessful());
            assertTrue(stats.getJobGraph().size() == 4);
            assertTrue(stats.getJobGraph().getSinks().size() == 1);
            assertTrue(stats.getJobGraph().getSources().size() == 1);
            JobStats js = (JobStats) stats.getJobGraph().getSinks().get(0);
            assertEquals(OUTPUT_FILE, js.getOutputs().get(0).getName());
            assertEquals(2, js.getOutputs().get(0).getNumberRecords());
            assertEquals(12, js.getOutputs().get(0).getBytes());
            assertEquals(OUTPUT_FILE, stats.getOutputNames().get(0));
            assertEquals(2, stats.getRecordWritten());
            assertEquals(12, stats.getBytesWritten());

            assertEquals("A", ((JobStats) stats.getJobGraph().getSources().get(
                    0)).getAlias());
            assertEquals("B", ((JobStats) stats.getJobGraph().getPredecessors(
                    js).get(0)).getAlias());
            assertEquals("B", js.getAlias());
        } finally {
            new File(PIG_FILE).delete();
            Util.deleteFile(cluster, OUTPUT_FILE);
        }
    }

    @Test
    public void simpleMultiQueryTest() throws Exception {
        final String OUTPUT_FILE_2 = "output2";

        PrintWriter w = new PrintWriter(new FileWriter(PIG_FILE));
        w.println("A = load '" + INPUT_FILE + "' as (a0:int, a1:int, a2:int);");
        w.println("B = filter A by a0 >= 4;");
        w.println("C = filter A by a0 < 4;");
        w.println("store B into '" + OUTPUT_FILE_2 + "';");
        w.println("store C into '" + OUTPUT_FILE + "';");
        w.close();

        try {
            String[] args = { PIG_FILE };
            PigStats stats = PigRunner.run(args, new TestNotificationListener());
            assertTrue(stats.isSuccessful());
            assertTrue(stats.getJobGraph().size() == 1);
            // Each output file should include the following:
            // output:
            //   1\t2\t3\n
            //   3\t4\t5\n
            //   3\t7\t8\n
            // output2:
            //   5\t3\t4\n
            //   5\t6\t7\n
            final int numOfRecords = 5;
            final int numOfCharsPerRecord = 6;
            assertEquals(numOfRecords, stats.getRecordWritten());
            assertEquals(numOfRecords * numOfCharsPerRecord, stats.getBytesWritten());
            assertTrue(stats.getOutputNames().size() == 2);
            for (String fname : stats.getOutputNames()) {
                assertTrue(fname.equals(OUTPUT_FILE) || fname.equals(OUTPUT_FILE_2));
                if (fname.equals(OUTPUT_FILE)) {
                    assertEquals(3, stats.getNumberRecords(fname));
                } else {
                    assertEquals(2, stats.getNumberRecords(fname));
                }
            }
            assertEquals("A,B,C",
                    ((JobStats)stats.getJobGraph().getSinks().get(0)).getAlias());
        } finally {
            new File(PIG_FILE).delete();
            Util.deleteFile(cluster, OUTPUT_FILE);
            Util.deleteFile(cluster, OUTPUT_FILE_2);
        }
    }

    @Test
    public void simpleMultiQueryTest2() throws Exception {
        final String OUTPUT_FILE_2 = "output2";

        PrintWriter w = new PrintWriter(new FileWriter(PIG_FILE));
        w.println("A = load '" + INPUT_FILE + "' as (a0:int, a1:int, a2:int);");
        w.println("B = filter A by a0 >= 4;");
        w.println("C = filter A by a0 < 4;");
        w.println("D = group C by a0;");
        w.println("E = foreach D generate group, COUNT(C);");
        w.println("store B into '" + OUTPUT_FILE_2 + "';");
        w.println("store E into '" + OUTPUT_FILE + "';");
        w.close();

        try {
            String[] args = { PIG_FILE };
            PigStats stats = PigRunner.run(args, new TestNotificationListener());
            assertTrue(stats.isSuccessful());
            assertTrue(stats.getJobGraph().size() == 1);
            // Each output file should include the following:
            // output:
            //   5\t3\t4\n
            //   5\t6\t7\n
            // output2:
            //   1\t1\n
            //   3\t2\n
            final int numOfRecords1 = 2;
            final int numOfRecords2 = 2;
            final int numOfCharsPerRecord1 = 6;
            final int numOfCharsPerRecord2 = 4;
            assertEquals(numOfRecords1 + numOfRecords2, stats.getRecordWritten());
            assertEquals((numOfRecords1 * numOfCharsPerRecord1) + (numOfRecords2 * numOfCharsPerRecord2),
                    stats.getBytesWritten());
            assertTrue(stats.getOutputNames().size() == 2);
            for (String fname : stats.getOutputNames()) {
                assertTrue(fname.equals(OUTPUT_FILE) || fname.equals(OUTPUT_FILE_2));
                if (fname.equals(OUTPUT_FILE)) {
                    assertEquals(2, stats.getNumberRecords(fname));
                } else {
                    assertEquals(2, stats.getNumberRecords(fname));
                }
            }
            assertEquals("A,B,C,D,E",
                    ((JobStats)stats.getJobGraph().getSinks().get(0)).getAlias());
        } finally {
            new File(PIG_FILE).delete();
            Util.deleteFile(cluster, OUTPUT_FILE);
            Util.deleteFile(cluster, OUTPUT_FILE_2);
        }
    }

    @Test
    public void MQDepJobFailedTest() throws Exception {
        final String OUTPUT_FILE_2 = "output2";
        PrintWriter w = new PrintWriter(new FileWriter(PIG_FILE));
        w.println("A = load '" + INPUT_FILE + "' as (name:chararray, a1:int, a2:int);");
        w.println("store A into '" + OUTPUT_FILE_2 + "';");
        w.println("B = FOREACH A GENERATE org.apache.pig.test.utils.UPPER(name);");
        w.println("C= order B by $0;");
        w.println("store C into '" + OUTPUT_FILE + "';");
        w.close();
        try {
            String[] args = { PIG_FILE };
            PigStats stats = PigRunner.run(args, null);
            Iterator<JobStats> iter = stats.getJobGraph().iterator();
            while (iter.hasNext()) {
                 JobStats js=iter.next();
                 if(js.getState().name().equals("FAILED")) {
                     List<Operator> ops=stats.getJobGraph().getSuccessors(js);
                     for(Operator op : ops ) {
                         assertEquals(((JobStats)op).getState().toString(), "UNKNOWN");
                     }
                 }
            }
        } finally {
            new File(PIG_FILE).delete();
            Util.deleteFile(cluster, OUTPUT_FILE);
            Util.deleteFile(cluster, OUTPUT_FILE_2);
        }
    }

    @Test
    public void simpleNegativeTest() throws Exception {
        PrintWriter w = new PrintWriter(new FileWriter(PIG_FILE));
        w.println("A = load '" + INPUT_FILE + "' as (a0:int, a1:int, a2:int);");
        w.println("B = group A by a;");
        w.println("C = foreach B generate group, COUNT(A);");
        w.println("store C into '" + OUTPUT_FILE + "';");
        w.close();
        String[] args = { "-c", PIG_FILE };
        PigStats stats = PigRunner.run(args, null);
        assertTrue(stats.getReturnCode() == ReturnCode.PIG_EXCEPTION);
        // TODO: error message has changed. Need to catch the new message generated from the
        // new parser.
//        assertTrue(stats.getErrorCode() == 1000);
//        assertEquals("Error during parsing. Invalid alias: a in {a0: int,a1: int,a2: int}",
//                stats.getErrorMessage());
    }

    @Test
    public void simpleNegativeTest2() throws Exception {
        String[] args = { "-c", "-e", "this is a test" };
        PigStats stats = PigRunner.run(args, new TestNotificationListener());
        assertTrue(stats.getReturnCode() == ReturnCode.ILLEGAL_ARGS);
    }

    @Test
    public void simpleNegativeTest3() throws Exception {
        String[] args = { "-c", "-y" };
        PigStats stats = PigRunner.run(args, new TestNotificationListener());
        assertTrue(stats.getReturnCode() == ReturnCode.PARSE_EXCEPTION);
        assertEquals("Found unknown option (-y) at position 2",
                stats.getErrorMessage());
    }

    @Test
    public void NagetiveTest() throws Exception {
        final String OUTPUT_FILE_2 = "output2";
        PrintWriter w = new PrintWriter(new FileWriter(PIG_FILE));
        w.println("A = load '" + INPUT_FILE + "' as (a0:int, a1:int, a2:int);");
        w.println("B = foreach A generate 1;");
        w.println("C = foreach A generate 0/0;");
        w.println("store B into '" + OUTPUT_FILE + "';");
        w.println("store C into '" + OUTPUT_FILE_2 + "';");
        w.println("D = load '" + OUTPUT_FILE_2 + "';");
        w.println("E = stream D through `false`;");
        w.println("store E into 'ee';");
        w.close();

        try {
            String[] args = { PIG_FILE };
            PigStats stats = PigRunner.run(args, null);
            assertTrue(!stats.isSuccessful());
            assertTrue(stats.getReturnCode() == ReturnCode.PARTIAL_FAILURE);
            assertTrue(stats.getJobGraph().size() == 2);
            JobStats job = (JobStats)stats.getJobGraph().getSources().get(0);
            assertTrue(job.isSuccessful());
            job = (JobStats)stats.getJobGraph().getSinks().get(0);
            assertTrue(!job.isSuccessful());
            assertTrue(stats.getOutputStats().size() == 3);
            for (OutputStats output : stats.getOutputStats()) {
                if (output.getName().equals("ee")) {
                    assertTrue(!output.isSuccessful());
                } else {
                    assertTrue(output.isSuccessful());
                }
            }
        } finally {
            new File(PIG_FILE).delete();
            Util.deleteFile(cluster, OUTPUT_FILE);
            Util.deleteFile(cluster, OUTPUT_FILE_2);
        }
    }

    @Test
    public void testIsTempFile() throws Exception {
        PigContext context = new PigContext(ExecType.LOCAL, new Properties());
        context.connect();
        for (int i=0; i<100; i++) {
            String file = FileLocalizer.getTemporaryPath(context).toString();
            assertTrue("not a temp file: " + file, PigStatsUtil.isTempFile(file));
        }
    }

    @Test
    public void testCounterName() throws Exception {
        String s = "jdbc:hsqldb:file:/tmp/batchtest;hsqldb.default_table_type=cached;hsqldb.cache_rows=100";
        String name = MRPigStatsUtil.getMultiInputsCounterName(s, 0);
        assertEquals(MRPigStatsUtil.MULTI_INPUTS_RECORD_COUNTER + "_0_batchtest", name);
        s = "file:///tmp/batchtest{1,2}.txt";
        name = MRPigStatsUtil.getMultiInputsCounterName(s, 1);
        assertEquals(MRPigStatsUtil.MULTI_INPUTS_RECORD_COUNTER + "_1_batchtest{1,2}.txt", name);
        s = "file:///tmp/batchtest*.txt";
        name = MRPigStatsUtil.getMultiInputsCounterName(s, 2);
        assertEquals(MRPigStatsUtil.MULTI_INPUTS_RECORD_COUNTER + "_2_batchtest*.txt", name);
    }

    @Test
    public void testLongCounterName() throws Exception {
        // Pig now restricts the string size of its counter name to less than 64 characters.
        PrintWriter w = new PrintWriter(new FileWriter("myinputfile"));
        w.println("1\t2\t3");
        w.println("5\t3\t4");
        w.println("3\t4\t5");
        w.println("5\t6\t7");
        w.println("3\t7\t8");
        w.close();
        String longfilename = "longlonglonglonglonglonglonglonglonglonglonglongfilefilefilename";
        Util.copyFromLocalToCluster(cluster, "myinputfile", longfilename);

        PrintWriter w1 = new PrintWriter(new FileWriter(PIG_FILE));
        w1.println("A = load '" + INPUT_FILE + "' as (a0:int, a1:int, a2:int);");
        w1.println("B = load '" + longfilename + "' as (a0:int, a1:int, a2:int);");
        w1.println("C = join A by a0, B by a0;");
        w1.println("store C into '" + OUTPUT_FILE + "';");
        w1.close();

        try {
            String[] args = { PIG_FILE };
            PigStats stats = PigRunner.run(args, new TestNotificationListener());

            assertTrue(stats.isSuccessful());

            assertEquals(1, stats.getNumberJobs());
            List<InputStats> inputs = stats.getInputStats();
            assertEquals(2, inputs.size());
            for (InputStats instats : inputs) {
                assertEquals(5, instats.getNumberRecords());
            }
        } finally {
            new File(PIG_FILE).delete();
            Util.deleteFile(cluster, OUTPUT_FILE);
        }
    }

    @Test
    public void testDuplicateCounterName() throws Exception {
        // Pig now restricts the string size of its counter name to less than 64 characters.
        PrintWriter w = new PrintWriter(new FileWriter("myinputfile"));
        w.println("1\t2\t3");
        w.println("5\t3\t4");
        w.close();
        String samefilename = "tmp/input";
        Util.copyFromLocalToCluster(cluster, "myinputfile", samefilename);

        PrintWriter w1 = new PrintWriter(new FileWriter(PIG_FILE));
        w1.println("A = load '" + INPUT_FILE + "' as (a0:int, a1:int, a2:int);");
        w1.println("B = load '" + samefilename + "' as (a0:int, a1:int, a2:int);");
        w1.println("C = join A by a0, B by a0;");
        w1.println("store C into '" + OUTPUT_FILE + "';");
        w1.close();

        try {
            String[] args = { PIG_FILE };
            PigStats stats = PigRunner.run(args, new TestNotificationListener());

            assertTrue(stats.isSuccessful());

            assertEquals(1, stats.getNumberJobs());
            List<InputStats> inputs = stats.getInputStats();
            assertEquals(2, inputs.size());
            for (InputStats instats : inputs) {
                if (instats.getLocation().endsWith("tmp/input")) {
                    assertEquals(2, instats.getNumberRecords());
                } else {
                    assertEquals(5, instats.getNumberRecords());
                }
            }
        } finally {
            new File(PIG_FILE).delete();
            Util.deleteFile(cluster, OUTPUT_FILE);
        }
    }

    @Test
    public void testDuplicateCounterName2() throws Exception {

        PrintWriter w1 = new PrintWriter(new FileWriter(PIG_FILE));
        w1.println("A = load '" + INPUT_FILE + "' as (a0:int, a1:int, a2:int);");
        w1.println("B = filter A by a0 > 3;");
        w1.println("store A into 'output';");
        w1.println("store B into 'tmp/output';");
        w1.close();

        try {
            String[] args = { PIG_FILE };
            PigStats stats = PigRunner.run(args, new TestNotificationListener());

            assertTrue(stats.isSuccessful());

            assertEquals(1, stats.getNumberJobs());
            List<OutputStats> outputs = stats.getOutputStats();
            assertEquals(2, outputs.size());
            for (OutputStats outstats : outputs) {
                if (outstats.getLocation().endsWith("tmp/output")) {
                    assertEquals(2, outstats.getNumberRecords());
                } else {
                    assertEquals(5, outstats.getNumberRecords());
                }
            }
        } finally {
            new File(PIG_FILE).delete();
            Util.deleteFile(cluster, OUTPUT_FILE);
            Util.deleteFile(cluster, "tmp/output");
        }
    }

    @Test
    public void testRegisterExternalJar() throws Exception {
        String jarName = "pig-withouthadoop-h2.jar";
        if (System.getProperty("hadoopversion").equals("20")) {
            jarName = "pig-withouthadoop-h1.jar";
        }
        String[] args = { "-Dpig.additional.jars=" + jarName,
                "-Dmapred.job.queue.name=default",
                "-e", "A = load '" + INPUT_FILE + "';store A into '" + OUTPUT_FILE + "';\n" };
        PigStats stats = PigRunner.run(args, new TestNotificationListener());

        Util.deleteFile(cluster, OUTPUT_FILE);
        PigContext ctx = stats.getPigContext();

        assertNotNull(ctx);

        assertTrue(ctx.extraJars.contains(ClassLoader.getSystemResource(jarName)));
        assertTrue("default", ctx.getProperties().getProperty("mapred.job.queue.name")!=null && ctx.getProperties().getProperty("mapred.job.queue.name").equals("default")||
                ctx.getProperties().getProperty("mapreduce.job.queuename")!=null && ctx.getProperties().getProperty("mapreduce.job.queuename").equals("default"));

    }

    @Test
    public void classLoaderTest() throws Exception {
        // Skip in hadoop 23 test, see PIG-2449
        if (Util.isHadoop23() || Util.isHadoop2_0())
            return;
        PrintWriter w = new PrintWriter(new FileWriter(PIG_FILE));
        w.println("register test/org/apache/pig/test/data/pigtestloader.jar");
        w.println("A = load '" + INPUT_FILE + "' using org.apache.pig.test.PigTestLoader();");
        w.println("store A into '" + OUTPUT_FILE + "';");
        w.close();

        try {
            String[] args = { PIG_FILE };
            PigStats stats = PigRunner.run(args, new TestNotificationListener());
            assertTrue(stats.isSuccessful());
        } finally {
            new File(PIG_FILE).delete();
            Util.deleteFile(cluster, OUTPUT_FILE);
        }
    }

    @Test
    public void fsCommandTest() throws Exception {
        PrintWriter w = new PrintWriter(new FileWriter(PIG_FILE));
        w.println("fs -mv nonexist.file dummy.file");
        w.close();

        try {
            String[] args = { PIG_FILE };
            PigStats stats = PigRunner.run(args, new TestNotificationListener());

            assertTrue(!stats.isSuccessful());
            assertTrue(stats.getReturnCode() == PigRunner.ReturnCode.IO_EXCEPTION);
        } finally {
            new File(PIG_FILE).delete();
        }
    }

    @Test // PIG-2006
    public void testEmptyFile() throws IOException {
        File f1 = new File( PIG_FILE );

        FileWriter fw1 = new FileWriter(f1);
        fw1.close();

        try {
           String[] args = { "-x", "local", "-c", PIG_FILE };
           PigStats stats = PigRunner.run(args, null);

           assertTrue(stats.isSuccessful());
           assertEquals( 0, stats.getReturnCode() );
        } finally {
            new File(PIG_FILE).delete();
            Util.deleteFile(cluster, OUTPUT_FILE);
        }
    }

    @Test
    public void returnCodeTest() throws Exception {
        PrintWriter w = new PrintWriter(new FileWriter(PIG_FILE));
        w.println("A = load 'non-existine.file' as (a0:int, a1:int, a2:int);");
        w.println("B = filter A by a0 > 0;;");
        w.println("C = group B by $0;");
        w.println("D = join C by $0, B by $0;");
        w.println("store D into '" + OUTPUT_FILE + "';");
        w.close();

        try {
            String[] args = { PIG_FILE };
            PigStats stats = PigRunner.run(args, null);

            assertTrue(!stats.isSuccessful());
            assertTrue(stats.getReturnCode() != 0);
            assertTrue(stats.getOutputStats().size() == 0);

        } finally {
            new File(PIG_FILE).delete();
            Util.deleteFile(cluster, OUTPUT_FILE);
        }
    }

    @Test
    public void returnCodeTest2() throws Exception {
        PrintWriter w = new PrintWriter(new FileWriter(PIG_FILE));
        w.println("A = load 'non-existine.file' as (a0, a1);");
        w.println("B = load 'data' as (b0, b1);");
        w.println("C = join B by b0, A by a0 using 'repl';");
        w.println("store C into '" + OUTPUT_FILE + "';");
        w.close();

        try {
            String[] args = { PIG_FILE };
            PigStats stats = PigRunner.run(args, null);

            assertTrue(!stats.isSuccessful());
            assertTrue(stats.getReturnCode() != 0);
            assertTrue(stats.getOutputStats().size() == 0);

        } finally {
            new File(PIG_FILE).delete();
            Util.deleteFile(cluster, OUTPUT_FILE);
        }
    }


    @Test //PIG-1893
    public void testEmptyFileCounter() throws Exception {

        PrintWriter w = new PrintWriter(new FileWriter("myinputfile"));
        w.close();

        Util.copyFromLocalToCluster(cluster, "myinputfile", "1.txt");

        PrintWriter w1 = new PrintWriter(new FileWriter(PIG_FILE));
        w1.println("A = load '" + INPUT_FILE + "' as (a0:int, a1:int, a2:int);");
        w1.println("B = load '1.txt' as (a0:int, a1:int, a2:int);");
        w1.println("C = join A by a0, B by a0;");
        w1.println("store C into '" + OUTPUT_FILE + "';");
        w1.close();

        try {
            String[] args = { PIG_FILE };
            PigStats stats = PigRunner.run(args, new TestNotificationListener());

            assertTrue(stats.isSuccessful());

            assertEquals(1, stats.getNumberJobs());
            List<InputStats> inputs = stats.getInputStats();
            assertEquals(2, inputs.size());
            for (InputStats instats : inputs) {
                if (instats.getLocation().endsWith("1.txt")) {
                    assertEquals(0, instats.getNumberRecords());
                } else {
                    assertEquals(5, instats.getNumberRecords());
                }
            }
        } finally {
            new File(PIG_FILE).delete();
            Util.deleteFile(cluster, OUTPUT_FILE);
        }
    }

    @Test //PIG-1893
    public void testEmptyFileCounter2() throws Exception {

        PrintWriter w1 = new PrintWriter(new FileWriter(PIG_FILE));
        w1.println("A = load '" + INPUT_FILE + "' as (a0:int, a1:int, a2:int);");
        w1.println("B = filter A by a0 < 0;");
        w1.println("store A into '" + OUTPUT_FILE + "';");
        w1.println("store B into 'output2';");
        w1.close();

        try {
            String[] args = { PIG_FILE };
            PigStats stats = PigRunner.run(args, new TestNotificationListener());

            assertTrue(stats.isSuccessful());

            assertEquals(1, stats.getNumberJobs());
            List<OutputStats> outputs = stats.getOutputStats();
            assertEquals(2, outputs.size());
            for (OutputStats outstats : outputs) {
                if (outstats.getLocation().endsWith("output2")) {
                    assertEquals(0, outstats.getNumberRecords());
                } else {
                    assertEquals(5, outstats.getNumberRecords());
                }
            }
        } finally {
            new File(PIG_FILE).delete();
            Util.deleteFile(cluster, OUTPUT_FILE);
            Util.deleteFile(cluster, "output2");
        }
    }

    @Test // PIG-2208: Restrict number of PIG generated Haddop counters
    public void testDisablePigCounters() throws Exception {
        PrintWriter w1 = new PrintWriter(new FileWriter(PIG_FILE));
        w1.println("A = load '" + INPUT_FILE + "' as (a0:int, a1:int, a2:int);");
        w1.println("B = load '" + INPUT_FILE + "' as (a0:int, a1:int, a2:int);");
        w1.println("C = join A by a0, B by a0;");
        w1.println("store C into '" + OUTPUT_FILE + "';");
        w1.close();

        try {
            String[] args = { "-Dpig.disable.counter=true", PIG_FILE };
            PigStats stats = PigRunner.run(args, new TestNotificationListener());

            assertTrue(stats.isSuccessful());

            assertEquals(1, stats.getNumberJobs());
            List<InputStats> inputs = stats.getInputStats();
            assertEquals(2, inputs.size());
            for (InputStats instats : inputs) {
                // the multi-input counters are disabled
                assertEquals(-1, instats.getNumberRecords());
            }

            List<OutputStats> outputs = stats.getOutputStats();
            assertEquals(1, outputs.size());
            OutputStats outstats = outputs.get(0);
            assertEquals(9, outstats.getNumberRecords());
        } finally {
            new File(PIG_FILE).delete();
            Util.deleteFile(cluster, OUTPUT_FILE);
        }
    }

    @Test             //Pig-2358
    public void testGetHadoopCounters() throws Exception {
        final String OUTPUT_FILE_2 = "output2";

        PrintWriter w = new PrintWriter(new FileWriter(PIG_FILE));
        w.println("A = load '" + INPUT_FILE + "' as (a0:int, a1:int, a2:int);");
        w.println("B = filter A by a0 >= 4;");
        w.println("C = filter A by a0 < 4;");
        w.println("D = group C by a0;");
        w.println("E = foreach D generate group, COUNT(C);");
        w.println("store B into '" + OUTPUT_FILE_2 + "';");
        w.println("store E into '" + OUTPUT_FILE + "';");
        w.close();

        try {
            String[] args = { PIG_FILE };
            PigStats stats = PigRunner.run(args, new TestNotificationListener());

            Counters counter= ((MRJobStats)stats.getJobGraph().getSinks().get(0)).getHadoopCounters();
            assertEquals(5, counter.getGroup(MRPigStatsUtil.TASK_COUNTER_GROUP).getCounterForName(
                    MRPigStatsUtil.MAP_INPUT_RECORDS).getValue());
            assertEquals(3, counter.getGroup(MRPigStatsUtil.TASK_COUNTER_GROUP).getCounterForName(
                    MRPigStatsUtil.MAP_OUTPUT_RECORDS).getValue());
            assertEquals(2, counter.getGroup(MRPigStatsUtil.TASK_COUNTER_GROUP).getCounterForName(
                    MRPigStatsUtil.REDUCE_INPUT_RECORDS).getValue());
            assertEquals(0, counter.getGroup(MRPigStatsUtil.TASK_COUNTER_GROUP).getCounterForName(
                    MRPigStatsUtil.REDUCE_OUTPUT_RECORDS).getValue());
            assertEquals(20,counter.getGroup(MRPigStatsUtil.FS_COUNTER_GROUP).getCounterForName(
                    MRPigStatsUtil.HDFS_BYTES_WRITTEN).getValue());

            // Skip for hadoop 20.203+, See PIG-2446
            if (Util.isHadoop203plus())
                return;

            assertEquals(30,counter.getGroup(MRPigStatsUtil.FS_COUNTER_GROUP).getCounterForName(
                    MRPigStatsUtil.HDFS_BYTES_READ).getValue());
        } finally {
            new File(PIG_FILE).delete();
            Util.deleteFile(cluster, OUTPUT_FILE);
            Util.deleteFile(cluster, OUTPUT_FILE_2);
        }
    }

    @Test // PIG-2208: Restrict number of PIG generated Haddop counters
    public void testDisablePigCounters2() throws Exception {

        PrintWriter w1 = new PrintWriter(new FileWriter(PIG_FILE));
        w1.println("A = load '" + INPUT_FILE + "' as (a0:int, a1:int, a2:int);");
        w1.println("B = filter A by a0 > 3;");
        w1.println("store A into 'output';");
        w1.println("store B into 'tmp/output';");
        w1.close();

        try {
            String[] args = { "-Dpig.disable.counter=true", PIG_FILE };
            PigStats stats = PigRunner.run(args, new TestNotificationListener());

            assertTrue(stats.isSuccessful());

            assertEquals(1, stats.getNumberJobs());
            List<OutputStats> outputs = stats.getOutputStats();
            assertEquals(2, outputs.size());
            for (OutputStats outstats : outputs) {
                // the multi-output counters are disabled
                assertEquals(-1, outstats.getNumberRecords());
            }

            List<InputStats> inputs = stats.getInputStats();
            assertEquals(1, inputs.size());
            InputStats instats = inputs.get(0);
            assertEquals(5, instats.getNumberRecords());
        } finally {
            new File(PIG_FILE).delete();
            Util.deleteFile(cluster, OUTPUT_FILE);
            Util.deleteFile(cluster, "tmp/output");
        }
    }

    /**
     * PIG-2780: In this test case, Pig submits three jobs at the same time and
     * one of them will fail due to nonexistent input file. If users enable
     * stop.on.failure, then Pig should immediately stop if anyone of the three
     * jobs has failed.
     */
    @Test
    public void testStopOnFailure() throws Exception {

        PrintWriter w1 = new PrintWriter(new FileWriter(PIG_FILE));
        w1.println("A1 = load '" + INPUT_FILE + "';");
        w1.println("B1 = load 'nonexist';");
        w1.println("C1 = load '" + INPUT_FILE + "';");
        w1.println("A2 = distinct A1;");
        w1.println("B2 = distinct B1;");
        w1.println("C2 = distinct C1;");
        w1.println("ret = union A2,B2,C2;");
        w1.println("store ret into 'tmp/output';");
        w1.close();

        try {
            String[] args = { "-F", PIG_FILE };
            PigStats stats = PigRunner.run(args, new TestNotificationListener());

            assertTrue(!stats.isSuccessful());

            int successfulJobs = 0;
            Iterator<Operator> it = stats.getJobGraph().getOperators();
            while (it.hasNext()){
                JobStats js = (JobStats)it.next();
                if (js.isSuccessful())
                    successfulJobs++;
            }

            // we should have less than 2 successful jobs
            assertTrue("Should have less than 2 successful jobs", successfulJobs < 2);

        } finally {
            new File(PIG_FILE).delete();
            Util.deleteFile(cluster, OUTPUT_FILE);
            Util.deleteFile(cluster, "tmp/output");
        }
    }
    public static class TestNotificationListener implements PigProgressNotificationListener {

        private Map<String, int[]> numMap = new HashMap<String, int[]>();

        private static final int JobsToLaunch = 0;
        private static final int JobsSubmitted = 1;
        private static final int JobStarted = 2;
        private static final int JobFinished = 3;

        @Override
        public void initialPlanNotification(String id, OperatorPlan<?> plan) {
            System.out.println("id: " + id + " planNodes: " + plan.getKeys().size());
            assertNotNull(plan);
        }

        @Override
        public void launchStartedNotification(String id, int numJobsToLaunch) {
            System.out.println("id: " + id + " numJobsToLaunch: " + numJobsToLaunch);
            int[] nums = new int[4];
            numMap.put(id, nums);
            nums[JobsToLaunch] = numJobsToLaunch;
        }

        @Override
        public void jobFailedNotification(String id, JobStats jobStats) {
            System.out.println("id: " + id + " job failed: " + jobStats.getJobId());
        }

        @Override
        public void jobFinishedNotification(String id, JobStats jobStats) {
            System.out.println("id: " + id + " job finished: " + jobStats.getJobId());
            int[] nums = numMap.get(id);
            nums[JobFinished]++;
        }

        @Override
        public void jobStartedNotification(String id, String assignedJobId) {
            System.out.println("id: " + id + " job started: " + assignedJobId);
            int[] nums = numMap.get(id);
            nums[JobStarted]++;
        }

        @Override
        public void jobsSubmittedNotification(String id, int numJobsSubmitted) {
            System.out.println("id: " + id + " jobs submitted: " + numJobsSubmitted);
            int[] nums = numMap.get(id);
            nums[JobsSubmitted] += numJobsSubmitted;
        }

        @Override
        public void launchCompletedNotification(String id, int numJobsSucceeded) {
            System.out.println("id: " + id + " numJobsSucceeded: " + numJobsSucceeded);
            System.out.println("");
            int[] nums = numMap.get(id);
            assertEquals(nums[JobsToLaunch], numJobsSucceeded);
            assertEquals(nums[JobsSubmitted], numJobsSucceeded);
            assertEquals(nums[JobStarted], numJobsSucceeded);
            assertEquals(nums[JobFinished], numJobsSucceeded);
        }

        @Override
        public void outputCompletedNotification(String id, OutputStats outputStats) {
            System.out.println("id: " + id + " output done: " + outputStats.getLocation());
        }

        @Override
        public void progressUpdatedNotification(String id, int progress) {
            System.out.println("id: " + id + " progress: " + progress + "%");
        }

    }

    private void deleteAll(File d) {
        if (!d.exists()) return;
        if (d.isDirectory()) {
            for (File f : d.listFiles()) {
                deleteAll(f);
            }
        }
        d.delete();
    }
}
