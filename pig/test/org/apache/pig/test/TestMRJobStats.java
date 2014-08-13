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
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.TaskReport;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigConfiguration;
import org.apache.pig.PigServer;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.FileBasedOutputSizeReader;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigStatsOutputSizeReader;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.hbase.HBaseStorage;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.PigStats.JobGraph;
import org.apache.pig.tools.pigstats.mapreduce.MRJobStats;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

public class TestMRJobStats {

    private static final TaskReport[] mapTaskReports = new TaskReport[5];
    private static final TaskReport[] reduceTaskReports = new TaskReport[5];
    private static final JobID jobID = new JobID("jobStat", Integer.MAX_VALUE);
    private static String ASSERT_STRING;

    private static final int ONE_THOUSAND = 1000;

    private static final long[][] MAP_START_FINISH_TIME_DATA = { { 0, 100},
        { 200, 400 }, { 500, 800 }, { 900, 1300 }, { 1400, 1900 } };

    private static final long[][] REDUCE_START_FINISH_TIME_DATA = { { 0, 100 },
        { 200, 400 }, { 500, 700 }, { 700, 900 }, { 1000, 1500 } };

    @BeforeClass
    public static void oneTimeSetup() throws Exception {

        // setting up TaskReport for map tasks
        for (int i = 0; i < mapTaskReports.length; i++) {
            mapTaskReports[i] = Mockito.mock(TaskReport.class);
            Mockito.when(mapTaskReports[i].getStartTime()).thenReturn(MAP_START_FINISH_TIME_DATA[i][0] * ONE_THOUSAND);
            Mockito.when(mapTaskReports[i].getFinishTime()).thenReturn(MAP_START_FINISH_TIME_DATA[i][1] * ONE_THOUSAND);
        }

        // setting up TaskReport for reduce tasks
        for (int i = 0; i < reduceTaskReports.length; i++) {
            reduceTaskReports[i] = Mockito.mock(TaskReport.class);
            Mockito.when(reduceTaskReports[i].getStartTime()).thenReturn(REDUCE_START_FINISH_TIME_DATA[i][0] * ONE_THOUSAND);
            Mockito.when(reduceTaskReports[i].getFinishTime()).thenReturn(REDUCE_START_FINISH_TIME_DATA[i][1] * ONE_THOUSAND);
        }

        StringBuilder sb = new StringBuilder();
        sb.append(jobID.toString()).append("\t");
        sb.append(mapTaskReports.length).append("\t");
        sb.append(reduceTaskReports.length).append("\t");

        sb.append("500\t100\t300\t300\t500\t100\t240\t200");
        ASSERT_STRING = sb.toString();
    }

    MRJobStats createJobStats(String name, JobGraph plan) {
        try {
            Constructor<MRJobStats> con = MRJobStats.class.getDeclaredConstructor(String.class, JobGraph.class);
            con.setAccessible(true);
            MRJobStats jobStats = (MRJobStats) con.newInstance(name, plan);
            return jobStats;
        } catch (Exception e) {
            return null;
        }
    }

    Method getJobStatsMethod(String methodName, Class<?>... parameterTypes) throws Exception {
        Method m = MRJobStats.class.getDeclaredMethod(methodName, parameterTypes);
        m.setAccessible(true);
        return m;
    }

    @Test
    public void testMedianMapReduceTime() throws Exception {
        JobClient jobClient = Mockito.mock(JobClient.class);

        // mock methods to return the predefined map and reduce task reports
        Mockito.when(jobClient.getMapTaskReports(jobID)).thenReturn(mapTaskReports);
        Mockito.when(jobClient.getReduceTaskReports(jobID)).thenReturn(reduceTaskReports);

        PigStats.JobGraph jobGraph = new PigStats.JobGraph();
        MRJobStats jobStats = createJobStats("JobStatsTest", jobGraph);
        getJobStatsMethod("setId", JobID.class).invoke(jobStats, jobID);
        jobStats.setSuccessful(true);

        getJobStatsMethod("addMapReduceStatistics", TaskReport[].class, TaskReport[].class)
            .invoke(jobStats, mapTaskReports, reduceTaskReports);
        String msg = (String)getJobStatsMethod("getDisplayString")
            .invoke(jobStats);

        System.out.println(JobStats.SUCCESS_HEADER);
        System.out.println(msg);

        assertTrue(msg.startsWith(ASSERT_STRING));
    }

    @Test
    public void testOneTaskReport() throws Exception {
        // setting up one map task report
        TaskReport[] mapTaskReports = new TaskReport[1];
        mapTaskReports[0] = Mockito.mock(TaskReport.class);
        Mockito.when(mapTaskReports[0].getStartTime()).thenReturn(300L * ONE_THOUSAND);
        Mockito.when(mapTaskReports[0].getFinishTime()).thenReturn(400L * ONE_THOUSAND);

        // setting up one reduce task report
        TaskReport[] reduceTaskReports = new TaskReport[1];
        reduceTaskReports[0] = Mockito.mock(TaskReport.class);
        Mockito.when(reduceTaskReports[0].getStartTime()).thenReturn(500L * ONE_THOUSAND);
        Mockito.when(reduceTaskReports[0].getFinishTime()).thenReturn(700L * ONE_THOUSAND);

        PigStats.JobGraph jobGraph = new PigStats.JobGraph();
        MRJobStats jobStats = createJobStats("JobStatsTest", jobGraph);
        getJobStatsMethod("setId", JobID.class).invoke(jobStats, jobID);
        jobStats.setSuccessful(true);

        getJobStatsMethod("addMapReduceStatistics", TaskReport[].class, TaskReport[].class)
            .invoke(jobStats, mapTaskReports, reduceTaskReports);
        String msg = (String)getJobStatsMethod("getDisplayString")
            .invoke(jobStats);
        System.out.println(JobStats.SUCCESS_HEADER);
        System.out.println(msg);

        StringBuilder sb = new StringBuilder();
        sb.append(jobID.toString()).append("\t");
        sb.append(mapTaskReports.length).append("\t");
        sb.append(reduceTaskReports.length).append("\t");
        sb.append("100\t100\t100\t100\t200\t200\t200\t200");

        System.out.println("assert msg: " + sb.toString());
        assertTrue(msg.startsWith(sb.toString()));

    }

    /**
     * Dummy output size reader class for testing JobStats.getOutputSize()
     */
    public static class DummyOutputSizeReader implements PigStatsOutputSizeReader {
        public static final long SIZE = 12345;

        /**
         * Returns true always
         * @param sto POStore
         */
        @Override
        public boolean supports(POStore sto, Configuration conf) {
            return true;
        }

        /**
         * Returns a dummy constant value
         * @param sto POStore
         * @param conf configuration
         */
        @Override
        public long getOutputSize(POStore sto, Configuration conf) throws IOException {
            return SIZE;
        }
    }

    private static POStore createPOStoreForFileBasedSystem(long size, StoreFuncInterface storeFunc,
            Configuration conf) throws Exception {

        File file = File.createTempFile("tempFile", ".tmp");
        file.deleteOnExit();
        RandomAccessFile f = new RandomAccessFile(file, "rw");
        f.setLength(size);
        f.close();

        storeFunc.setStoreLocation(file.getAbsolutePath(), new Job(conf));
        FuncSpec funcSpec = new FuncSpec(storeFunc.getClass().getCanonicalName());
        POStore poStore = new POStore(new OperatorKey());
        poStore.setSFile(new FileSpec(file.getAbsolutePath(), funcSpec));
        poStore.setStoreFunc(storeFunc);
        poStore.setUp();

        return poStore;
    }

    private static POStore createPOStoreForNonFileBasedSystem(StoreFuncInterface storeFunc,
            Configuration conf) throws Exception {

        String nonFileBasedUri = "hbase://tableName";
        storeFunc.setStoreLocation(nonFileBasedUri, new Job(conf));
        FuncSpec funcSpec = new FuncSpec(storeFunc.getClass().getCanonicalName());
        POStore poStore = new POStore(new OperatorKey());
        poStore.setSFile(new FileSpec(nonFileBasedUri, funcSpec));
        poStore.setStoreFunc(storeFunc);
        poStore.setUp();

        return poStore;
    }

    @Test
    public void testGetOuputSizeUsingFileBasedStorage() throws Exception {
        // By default, FileBasedOutputSizeReader is used to compute the size of output.
        Configuration conf = new Configuration();

        long size = 2L * 1024 * 1024 * 1024;
        long outputSize = JobStats.getOutputSize(
                createPOStoreForFileBasedSystem(size, new PigStorageWithStatistics(), conf), conf);

        assertEquals("The returned output size is expected to be the same as the file size",
                size, outputSize);
    }

    @Test
    public void testGetOuputSizeUsingNonFileBasedStorage1() throws Exception {
        // By default, FileBasedOutputSizeReader is used to compute the size of output.
        Configuration conf = new Configuration();

        // ClientSystemProps is needed to instantiate HBaseStorage
        UDFContext.getUDFContext().setClientSystemProps(new Properties());
        long outputSize = JobStats.getOutputSize(
                createPOStoreForNonFileBasedSystem(new HBaseStorage("colName"), conf), conf);

        assertEquals("The default output size reader returns -1 for a non-file-based uri",
                -1, outputSize);
    }

    @Test
    public void testGetOuputSizeUsingNonFileBasedStorage2() throws Exception {
        // Register a custom output size reader in configuration
        Configuration conf = new Configuration();
        conf.set(PigStatsOutputSizeReader.OUTPUT_SIZE_READER_KEY,
                DummyOutputSizeReader.class.getName());

        // ClientSystemProps is needed to instantiate HBaseStorage
        UDFContext.getUDFContext().setClientSystemProps(new Properties());
        long outputSize = JobStats.getOutputSize(
                createPOStoreForNonFileBasedSystem(new HBaseStorage("colName"), conf), conf);

        assertEquals("The dummy output size reader always returns " + DummyOutputSizeReader.SIZE,
                DummyOutputSizeReader.SIZE, outputSize);
    }

    @Test(expected = RuntimeException.class)
    public void testGetOuputSizeUsingNonFileBasedStorage3() throws Exception {
        // Register an invalid output size reader in configuration, and verify
        // that an exception is thrown at run-time.
        Configuration conf = new Configuration();
        conf.set(PigStatsOutputSizeReader.OUTPUT_SIZE_READER_KEY, "bad_output_size_reader");

        // ClientSystemProps is needed to instantiate HBaseStorage
        UDFContext.getUDFContext().setClientSystemProps(new Properties());
        JobStats.getOutputSize(
                createPOStoreForNonFileBasedSystem(new HBaseStorage("colName"), conf), conf);
    }

    @Test
    public void testGetOuputSizeUsingNonFileBasedStorage4() throws Exception {
        // Register a comma-separated list of readers in configuration, and
        // verify that the one that supports a non-file-based uri is used.
        Configuration conf = new Configuration();
        conf.set(PigStatsOutputSizeReader.OUTPUT_SIZE_READER_KEY,
                FileBasedOutputSizeReader.class.getName() + ","
                        + DummyOutputSizeReader.class.getName());

        // ClientSystemProps needs to be initialized to instantiate HBaseStorage
        UDFContext.getUDFContext().setClientSystemProps(new Properties());
        long outputSize = JobStats.getOutputSize(
                createPOStoreForNonFileBasedSystem(new HBaseStorage("colName"), conf), conf);

        assertEquals("The dummy output size reader always returns " + DummyOutputSizeReader.SIZE,
                DummyOutputSizeReader.SIZE, outputSize);
    }

    @Test
    public void testGetOuputSizeUsingNonFileBasedStorage5() throws Exception {
        Configuration conf = new Configuration();

        long size = 2L * 1024 * 1024 * 1024;
        long outputSize = JobStats.getOutputSize(
                createPOStoreForFileBasedSystem(size, new PigStorageWithStatistics(), conf), conf);

        // By default, FileBasedOutputSizeReader is used to compute the size of output.
        assertEquals("The returned output size is expected to be the same as the file size",
                size, outputSize);

        // Now add PigStorageWithStatistics to the unsupported store funcs list, and
        // verify that JobStats.getOutputSize() returns -1.
        conf.set(PigStatsOutputSizeReader.OUTPUT_SIZE_READER_UNSUPPORTED,
                PigStorageWithStatistics.class.getName());

        outputSize = JobStats.getOutputSize(
                createPOStoreForFileBasedSystem(size, new PigStorageWithStatistics(), conf), conf);
        assertEquals("The default output size reader returns -1 for unsupported store funcs",
                -1, outputSize);
    }

    // See PIG-4043
    @Test
    public void testNoTaskReportProperty() throws IOException{
        MiniGenericCluster cluster = MiniGenericCluster.buildCluster(MiniGenericCluster.EXECTYPE_MR);
        Properties properties = cluster.getProperties();

        String inputFile = "input";
        PrintWriter pw = new PrintWriter(Util.createInputFile(cluster, inputFile));
        pw.println("100\tapple");
        pw.println("200\torange");
        pw.close();

        // Enable task reports in job statistics
        properties.setProperty(PigConfiguration.PIG_NO_TASK_REPORT, "false");
        PigServer pigServer = new PigServer(cluster.getExecType(), properties);
        pigServer.setBatchOn();

        // Launch a map-only job
        pigServer.registerQuery("A = load '" + inputFile + "' as (id:int, fruit:chararray);");
        pigServer.registerQuery("store A into 'task_reports';");
        List<ExecJob> jobs = pigServer.executeBatch();
        PigStats pigStats = jobs.get(0).getStatistics();
        MRJobStats jobStats = (MRJobStats) pigStats.getJobGraph().getJobList().get(0);

        // Make sure JobStats includes TaskReports information
        long minMapTime = jobStats.getMinMapTime();
        long maxMapTime = jobStats.getMaxMapTime();
        long avgMapTime = jobStats.getAvgMapTime();
        assertTrue("TaskReports are enabled, so minMapTime shouldn't be -1", minMapTime != -1l);
        assertTrue("TaskReports are enabled, so maxMapTime shouldn't be -1", maxMapTime != -1l);
        assertTrue("TaskReports are enabled, so avgMapTime shouldn't be -1", avgMapTime != -1l);

        // Disable task reports in job statistics
        properties.setProperty(PigConfiguration.PIG_NO_TASK_REPORT, "true");

        // Launch another map-only job
        pigServer.registerQuery("B = load '" + inputFile + "' as (id:int, fruit:chararray);");
        pigServer.registerQuery("store B into 'no_task_reports';");
        jobs = pigServer.executeBatch();
        pigStats = jobs.get(0).getStatistics();
        jobStats = (MRJobStats) pigStats.getJobGraph().getJobList().get(0);

        // Make sure JobStats doesn't include any TaskReports information
        minMapTime = jobStats.getMinMapTime();
        maxMapTime = jobStats.getMaxMapTime();
        avgMapTime = jobStats.getAvgMapTime();
        assertEquals("TaskReports are disabled, so minMapTime should be -1", -1l, minMapTime);
        assertEquals("TaskReports are disabled, so maxMapTime should be -1", -1l, maxMapTime);
        assertEquals("TaskReports are disabled, so avgMapTime should be -1", -1l, avgMapTime);

        cluster.shutDown();
    }
}
