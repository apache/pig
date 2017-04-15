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

import java.io.File;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.Iterator;
import java.util.Random;

import org.junit.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.util.ConfigurationValidator;
import org.apache.pig.test.utils.GenPhyOp;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
abstract public class TestJobSubmission {


    static PigContext pc;
    String ldFile;
    String expFile;
    PhysicalPlan php = new PhysicalPlan();
    String stFile;
    String hadoopLdFile;
    String grpName;
    Random r = new Random();
    String curDir;
    String inpDir;
    String golDir;
    static MiniGenericCluster cluster = null;

    public static void oneTimeSetUp() throws Exception {
        cluster = MiniGenericCluster.buildCluster();
        pc = new PigContext(cluster.getExecType(), cluster.getProperties());
        try {
            pc.connect();
        } catch (ExecException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        GenPhyOp.setPc(pc);
        Util.copyFromLocalToCluster(cluster, "test/org/apache/pig/test/data/passwd", "/passwd");
    }

    @Before
    public void setUp() throws Exception{
        curDir = System.getProperty("user.dir");
        inpDir = curDir + File.separatorChar + "test/org/apache/pig/test/data/InputFiles/";
        golDir = curDir + File.separatorChar + "test/org/apache/pig/test/data/GoldenFiles/";
        if (Util.WINDOWS) {
            inpDir="/"+FileLocalizer.parseCygPath(inpDir, FileLocalizer.STYLE_WINDOWS);
            golDir="/"+FileLocalizer.parseCygPath(golDir, FileLocalizer.STYLE_WINDOWS);
        }
    }

    @After
    public void tearDown() throws Exception {
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        if (cluster!=null) {
            cluster.shutDown();
        }
    }

    @Test
    public void testJobControlCompilerErr() throws Exception {
        String query = "a = load '/passwd' as (a1:bag{(t:chararray)});" + "b = order a by a1;" + "store b into 'output';";
        PigServer pigServer = new PigServer(cluster.getExecType(), cluster.getProperties());
        PhysicalPlan pp = Util.buildPp(pigServer, query);
        checkJobControlCompilerErrResult(pp, pc);
    }

    abstract protected void checkJobControlCompilerErrResult(PhysicalPlan pp, PigContext pc) throws Exception;

    @Test
    public void testDefaultParallel() throws Throwable {
        pc.defaultParallel = 100;

        String query = "a = load '/passwd';" + "b = group a by $0;" + "store b into 'output';";
        PigServer ps = new PigServer(cluster.getExecType(), cluster.getProperties());
        PhysicalPlan pp = Util.buildPp(ps, query);
        checkDefaultParallelResult(pp, pc);

        pc.defaultParallel = -1;
    }

    abstract protected void checkDefaultParallelResult(PhysicalPlan pp, PigContext pc) throws Exception;

    @Test
    public void testDefaultParallelInSort() throws Throwable {
        // default_parallel is considered only at runtime, so here we only test requested parallel
        // more thorough tests can be found in TestNumberOfReducers.java

        String query = "a = load 'input';" + "b = order a by $0 parallel 100;" + "store b into 'output';";
        PigServer ps = new PigServer(cluster.getExecType(), cluster.getProperties());
        PhysicalPlan pp = Util.buildPp(ps, query);
        MROperPlan mrPlan = Util.buildMRPlan(pp, pc);

        // Get the sort job
        Iterator<MapReduceOper> iter = mrPlan.getKeys().values().iterator();
        int counter = 0;
        while (iter.hasNext()) {
            MapReduceOper op = iter.next();
            counter++;
            if (op.isGlobalSort()) {
                assertTrue(op.getRequestedParallelism()==100);
            }
        }
        assertEquals(3, counter);

        pc.defaultParallel = -1;
    }

    @Test
    public void testDefaultParallelInSkewJoin() throws Throwable {
        // default_parallel is considered only at runtime, so here we only test requested parallel
        // more thorough tests can be found in TestNumberOfReducers.java
        String query = "a = load 'input';" +
                "b = load 'input';" +
                "c = join a by $0, b by $0 using 'skewed' parallel 100;" +
                "store c into 'output';";
        PigServer ps = new PigServer(cluster.getExecType(), cluster.getProperties());
        PhysicalPlan pp = Util.buildPp(ps, query);
        MROperPlan mrPlan = Util.buildMRPlan(pp, pc);

        // Get the skew join job
        Iterator<MapReduceOper> iter = mrPlan.getKeys().values().iterator();
        int counter = 0;
        while (iter.hasNext()) {
            MapReduceOper op = iter.next();
            counter++;
            if (op.isSkewedJoin()) {
                assertTrue(op.getRequestedParallelism()==100);
            }
        }
        assertEquals(3, counter);

        pc.defaultParallel = -1;
    }

    @Test
    public void testReducerNumEstimation() throws Exception{
        // Skip the test for Tez. Tez use a different mechanism.
        // Equivalent test is in TestTezAutoParallelism
        Assume.assumeTrue("Skip this test for TEZ",
                Util.isMapredExecType(cluster.getExecType()));
        // use the estimation
        Configuration conf = HBaseConfiguration.create(new Configuration());
        HBaseTestingUtility util = new HBaseTestingUtility(conf);
        int clientPort = util.startMiniZKCluster().getClientPort();
        util.startMiniHBaseCluster(1, 1);

        String query = "a = load '/passwd';" +
                "b = group a by $0;" +
                "store b into 'output';";
        PigServer ps = new PigServer(cluster.getExecType(), cluster.getProperties());
        PhysicalPlan pp = Util.buildPp(ps, query);
        MROperPlan mrPlan = Util.buildMRPlan(pp, pc);

        pc.getConf().setProperty("pig.exec.reducers.bytes.per.reducer", "100");
        pc.getConf().setProperty("pig.exec.reducers.max", "10");
        pc.getConf().setProperty(HConstants.ZOOKEEPER_CLIENT_PORT, Integer.toString(clientPort));
        ConfigurationValidator.validatePigProperties(pc.getProperties());
        conf = ConfigurationUtil.toConfiguration(pc.getProperties());
        JobControlCompiler jcc = new JobControlCompiler(pc, conf);
        JobControl jc=jcc.compile(mrPlan, "Test");
        Job job = jc.getWaitingJobs().get(0);
        long reducer=Math.min((long)Math.ceil(new File("test/org/apache/pig/test/data/passwd").length()/100.0), 10);

        Util.assertParallelValues(-1, -1, reducer, reducer, job.getJobConf());

        // use the PARALLEL key word, it will override the estimated reducer number
        query = "a = load '/passwd';" +
                "b = group a by $0 PARALLEL 2;" +
                "store b into 'output';";
        pp = Util.buildPp(ps, query);
        mrPlan = Util.buildMRPlan(pp, pc);

        pc.getConf().setProperty("pig.exec.reducers.bytes.per.reducer", "100");
        pc.getConf().setProperty("pig.exec.reducers.max", "10");
        ConfigurationValidator.validatePigProperties(pc.getProperties());
        conf = ConfigurationUtil.toConfiguration(pc.getProperties());
        jcc = new JobControlCompiler(pc, conf);
        jc=jcc.compile(mrPlan, "Test");
        job = jc.getWaitingJobs().get(0);

        Util.assertParallelValues(-1, 2, -1, 2, job.getJobConf());

        final byte[] COLUMNFAMILY = Bytes.toBytes("pig");
        util.createTable(Bytes.toBytesBinary("test_table"), COLUMNFAMILY);

        // the estimation won't take effect when it apply to non-dfs or the files doesn't exist, such as hbase
        query = "a = load 'hbase://test_table' using org.apache.pig.backend.hadoop.hbase.HBaseStorage('c:f1 c:f2');" +
                "b = group a by $0 ;" +
                "store b into 'output';";
        pp = Util.buildPp(ps, query);
        mrPlan = Util.buildMRPlan(pp, pc);

        pc.getConf().setProperty("pig.exec.reducers.bytes.per.reducer", "100");
        pc.getConf().setProperty("pig.exec.reducers.max", "10");

        ConfigurationValidator.validatePigProperties(pc.getProperties());
        conf = ConfigurationUtil.toConfiguration(pc.getProperties());
        jcc = new JobControlCompiler(pc, conf);
        jc=jcc.compile(mrPlan, "Test");
        job = jc.getWaitingJobs().get(0);

        Util.assertParallelValues(-1, -1, 1, 1, job.getJobConf());

        util.deleteTable(Bytes.toBytesBinary("test_table"));
        // In HBase 0.90.1 and above we can use util.shutdownMiniHBaseCluster()
        // here instead.
        MiniHBaseCluster hbc = util.getHBaseCluster();
        if (hbc != null) {
            hbc.shutdown();
            hbc.join();
        }
        util.shutdownMiniZKCluster();
    }

    @Test
    public void testReducerNumEstimationForOrderBy() throws Exception{
        // Skip the test for Tez. Tez use a different mechanism.
        // Equivalent test is in TestTezAutoParallelism
        Assume.assumeTrue("Skip this test for TEZ",
                Util.isMapredExecType(cluster.getExecType()));
        // use the estimation
        pc.getProperties().setProperty("pig.exec.reducers.bytes.per.reducer", "100");
        pc.getProperties().setProperty("pig.exec.reducers.max", "10");

        String query = "a = load '/passwd';" +
                "b = order a by $0;" +
                "store b into 'output';";
        PigServer ps = new PigServer(cluster.getExecType(), cluster.getProperties());
        PhysicalPlan pp = Util.buildPp(ps, query);

        MROperPlan mrPlan = Util.buildMRPlanWithOptimizer(pp, pc);
        Configuration conf = ConfigurationUtil.toConfiguration(pc.getProperties());
        JobControlCompiler jcc = new JobControlCompiler(pc, conf);
        JobControl jobControl = jcc.compile(mrPlan, query);

        assertEquals(2, mrPlan.size());

        // first job uses a single reducer for the sampling
        Util.assertParallelValues(-1, 1, -1, 1, jobControl.getWaitingJobs().get(0).getJobConf());

        // Simulate the first job having run so estimation kicks in.
        MapReduceOper sort = mrPlan.getLeaves().get(0);
        jcc.updateMROpPlan(jobControl.getReadyJobs());
        FileLocalizer.create(sort.getQuantFile(), pc);
        jobControl = jcc.compile(mrPlan, query);

        sort = mrPlan.getLeaves().get(0);
        long reducer=Math.min((long)Math.ceil(new File("test/org/apache/pig/test/data/passwd").length()/100.0), 10);
        assertEquals(reducer, sort.getRequestedParallelism());

        // the second job estimates reducers
        Util.assertParallelValues(-1, -1, reducer, reducer, jobControl.getWaitingJobs().get(0).getJobConf());

        // use the PARALLEL key word, it will override the estimated reducer number
        query = "a = load '/passwd';" + "b = order a by $0 PARALLEL 2;" +
                "store b into 'output';";
        pp = Util.buildPp(ps, query);

        mrPlan = Util.buildMRPlanWithOptimizer(pp, pc);

        assertEquals(2, mrPlan.size());

        sort = mrPlan.getLeaves().get(0);
        assertEquals(2, sort.getRequestedParallelism());

        // the estimation won't take effect when it apply to non-dfs or the files doesn't exist, such as hbase
        query = "a = load 'hbase://passwd' using org.apache.pig.backend.hadoop.hbase.HBaseStorage('c:f1 c:f2');" +
                "b = order a by $0 ;" +
                "store b into 'output';";
        pp = Util.buildPp(ps, query);

        mrPlan = Util.buildMRPlanWithOptimizer(pp, pc);
        assertEquals(2, mrPlan.size());

        sort = mrPlan.getLeaves().get(0);

        // the requested parallel will be -1 if users don't set any of default_parallel, paralllel
        // and the estimation doesn't take effect. MR framework will finally set it to 1.
        assertEquals(-1, sort.getRequestedParallelism());

        // test order by with three jobs (after optimization)
        query = "a = load '/passwd';" +
                "b = foreach a generate $0, $1, $2;" +
                "c = order b by $0;" +
                "store c into 'output';";
        pp = Util.buildPp(ps, query);

        mrPlan = Util.buildMRPlanWithOptimizer(pp, pc);
        assertEquals(3, mrPlan.size());

        // Simulate the first 2 jobs having run so estimation kicks in.
        sort = mrPlan.getLeaves().get(0);
        FileLocalizer.create(sort.getQuantFile(), pc);

        jobControl = jcc.compile(mrPlan, query);
        Util.copyFromLocalToCluster(cluster, "test/org/apache/pig/test/data/passwd", ((POLoad) sort.mapPlan.getRoots().get(0)).getLFile().getFileName());

        //First job is just foreach with projection, mapper-only job, so estimate gets ignored
        Util.assertParallelValues(-1, -1, -1, 0, jobControl.getWaitingJobs().get(0).getJobConf());

        jcc.updateMROpPlan(jobControl.getReadyJobs());
        jobControl = jcc.compile(mrPlan, query);
        jcc.updateMROpPlan(jobControl.getReadyJobs());

        //Second job is a sampler, which requests and gets 1 reducer
        Util.assertParallelValues(-1, 1, -1, 1, jobControl.getWaitingJobs().get(0).getJobConf());

        jobControl = jcc.compile(mrPlan, query);
        sort = mrPlan.getLeaves().get(0);
        assertEquals(reducer, sort.getRequestedParallelism());

        //Third job is the order, which uses the estimated number of reducers
        Util.assertParallelValues(-1, -1, reducer, reducer, jobControl.getWaitingJobs().get(0).getJobConf());
    }

    @Test
    public void testToUri() throws Exception {
        Class<JobControlCompiler> jobControlCompilerClass = JobControlCompiler.class;
        Method toURIMethod = jobControlCompilerClass.getDeclaredMethod("toURI", Path.class);
        toURIMethod.setAccessible(true);

        Path p1 = new Path("/tmp/temp-1510081022/tmp-1308657145#pigsample_1889145873_1351808882314");
        URI uri1 = (URI)toURIMethod.invoke(null, p1);
        Assert.assertEquals(uri1.toString(), "/tmp/temp-1510081022/tmp-1308657145#pigsample_1889145873_1351808882314");

        Path p2 = new Path("C:/Program Files/GnuWin32/bin/head.exe#pigsample_1889145873_1351808882314");
        URI uri2 = (URI)toURIMethod.invoke(null, p2);
        Assert.assertTrue(uri2.toString().equals("C:/Program%20Files/GnuWin32/bin/head.exe#pigsample_1889145873_1351808882314")||
                uri2.toString().equals("/C:/Program%20Files/GnuWin32/bin/head.exe#pigsample_1889145873_1351808882314"));
    }
}
