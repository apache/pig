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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.ExecType;
import org.apache.pig.PigConfiguration;
import org.apache.pig.PigServer;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MRConfiguration;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceLauncher;
import org.apache.pig.impl.PigContext;
import org.apache.pig.test.TestStore.DummyOutputCommitter;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public abstract class TestStoreBase {
    protected ExecType mode;
    protected String inputFileName;
    protected String outputFileName;

    protected static final String TESTDIR = "/tmp/" + TestStore.class.getSimpleName();

    protected static final String DUMMY_STORE_CLASS_NAME
    = "org.apache.pig.test.TestStore\\$DummyStore";

    protected static final String FAIL_UDF_NAME
    = "org.apache.pig.test.TestStore\\$FailUDF";
    protected static final String MAP_MAX_ATTEMPTS = MRConfiguration.MAP_MAX_ATTEMPTS;

    protected PigServer ps = null;

    @Before
    public void setUp() throws Exception {
        inputFileName = TESTDIR + "/TestStore-" + new Random().nextLong() + ".txt";
        outputFileName = TESTDIR + "/TestStore-output-" + new Random().nextLong() + ".txt";
        setupPigServer();
    }

    abstract protected void setupPigServer() throws Exception;

    @Test
    public void testSetStoreSchema() throws Exception {
        Map<String, Boolean> filesToVerify = new HashMap<String, Boolean>();
        filesToVerify.put(outputFileName + "_storeSchema_test", Boolean.TRUE);
        filesToVerify.put(DummyOutputCommitter.FILE_SETUPJOB_CALLED, Boolean.TRUE);
        filesToVerify.put(DummyOutputCommitter.FILE_SETUPTASK_CALLED, Boolean.TRUE);
        filesToVerify.put(DummyOutputCommitter.FILE_COMMITTASK_CALLED, Boolean.TRUE);
        filesToVerify.put(DummyOutputCommitter.FILE_ABORTTASK_CALLED, Boolean.FALSE);
        filesToVerify.put(DummyOutputCommitter.FILE_COMMITJOB_CALLED, Boolean.TRUE);
        filesToVerify.put(DummyOutputCommitter.FILE_ABORTJOB_CALLED, Boolean.FALSE);
        filesToVerify.put(DummyOutputCommitter.FILE_CLEANUPJOB_CALLED, Boolean.FALSE);
        String[] inputData = new String[]{"hello\tworld", "bye\tworld"};

        String script = "a = load '"+ inputFileName + "' as (a0:chararray, a1:chararray);" +
                "store a into '" + outputFileName + "' using " +
                DUMMY_STORE_CLASS_NAME + "();";

        if(!mode.isLocal()) {
            filesToVerify.put(DummyOutputCommitter.FILE_SETUPJOB_CALLED, Boolean.TRUE);
            filesToVerify.put(DummyOutputCommitter.FILE_COMMITJOB_CALLED, Boolean.TRUE);
        } else {
            if (Util.isHadoop1_x()) {
                // MAPREDUCE-1447/3563 (LocalJobRunner does not call methods of mapreduce
                // OutputCommitter) is fixed only in 0.23.1
                filesToVerify.put(DummyOutputCommitter.FILE_SETUPJOB_CALLED, Boolean.FALSE);
                filesToVerify.put(DummyOutputCommitter.FILE_COMMITJOB_CALLED, Boolean.FALSE);
            }
        }
        ps.setBatchOn();
        Util.deleteFile(ps.getPigContext(), TESTDIR);
        Util.createInputFile(ps.getPigContext(),
                inputFileName, inputData);
        Util.registerMultiLineQuery(ps, script);
        ps.executeBatch();
        for (Entry<String, Boolean> entry : filesToVerify.entrySet()) {
            String condition = entry.getValue() ? "" : "not";
            assertEquals("Checking if file " + entry.getKey() +
                    " does " + condition + " exists in " + mode +
                    " mode", (boolean) entry.getValue(),
                    Util.exists(ps.getPigContext(), entry.getKey()));
        }
    }

    @Test
    public void testCleanupOnFailure() throws Exception {
        String cleanupSuccessFile = outputFileName + "_cleanupOnFailure_succeeded";
        String cleanupFailFile = outputFileName + "_cleanupOnFailure_failed";
        String[] inputData = new String[]{"hello\tworld", "bye\tworld"};

        String script = "a = load '"+ inputFileName + "';" +
                "store a into '" + outputFileName + "' using " +
                DUMMY_STORE_CLASS_NAME + "('true');";

        Util.deleteFile(ps.getPigContext(), TESTDIR);
        ps.setBatchOn();
        Util.createInputFile(ps.getPigContext(),
                inputFileName, inputData);
        Util.registerMultiLineQuery(ps, script);
        ps.executeBatch();
        assertEquals(
                "Checking if file indicating that cleanupOnFailure failed " +
                " does not exists in " + mode + " mode", false,
                Util.exists(ps.getPigContext(), cleanupFailFile));
        assertEquals(
                "Checking if file indicating that cleanupOnFailure was " +
                "successfully called exists in " + mode + " mode", true,
                Util.exists(ps.getPigContext(), cleanupSuccessFile));
    }

    @Test
    public void testCleanupOnFailureMultiStore() throws Exception {
        String outputFileName1 = TESTDIR + "/TestStore-output-" + new Random().nextLong() + ".txt";
        String outputFileName2 = TESTDIR + "/TestStore-output-" + new Random().nextLong() + ".txt";

        Map<String, Boolean> filesToVerify = new HashMap<String, Boolean>();
        if (mode.toString().startsWith("SPARK")) {
            filesToVerify.put(outputFileName1 + "_cleanupOnFailure_succeeded1", Boolean.TRUE);
            filesToVerify.put(outputFileName2 + "_cleanupOnFailure_succeeded2", Boolean.TRUE);
            filesToVerify.put(outputFileName1 + "_cleanupOnFailure_failed1", Boolean.FALSE);
            filesToVerify.put(outputFileName2 + "_cleanupOnFailure_failed2", Boolean.FALSE);
            filesToVerify.put(DummyOutputCommitter.FILE_SETUPJOB_CALLED + "1", Boolean.TRUE);
             /* A = load xx;
             store A into '1.out' using DummyStore('true','1');   -- first job should fail
             store A into '2.out' using DummyStore('false','1');  -- second job should success
             After multiQuery optimization the spark plan will be:
           Split - scope-14
            |   |
            |   a: Store(hdfs://1.out:myudfs.DummyStore('true','1')) - scope-4
            |   |
            |   a: Store(hdfs://2.out:myudfs.DummyStore('false','1')) - scope-7
            |
            |---a: Load(hdfs://zly2.sh.intel.com:8020/user/root/multiStore.txt:org.apache.pig.builtin.PigStorage) - scope-0------
            In current code base, once the first job fails, the second job will not be executed.
            the FILE_SETUPJOB_CALLED of second job will not exist.
            I explain more detail in PIG-4243
            */
            filesToVerify.put(DummyOutputCommitter.FILE_SETUPJOB_CALLED + "2", Boolean.FALSE);
            filesToVerify.put(DummyOutputCommitter.FILE_SETUPTASK_CALLED + "1", Boolean.TRUE);
                /*
            In current code base, once the first job fails, the second job will not be executed.
            the FILE_SETUPTASK_CALLED of second job will not exist.
            */
            filesToVerify.put(DummyOutputCommitter.FILE_SETUPTASK_CALLED + "2", Boolean.FALSE);
            filesToVerify.put(DummyOutputCommitter.FILE_COMMITTASK_CALLED + "1", Boolean.FALSE);
            filesToVerify.put(DummyOutputCommitter.FILE_COMMITTASK_CALLED + "2", Boolean.FALSE);
            // OutputCommitter.abortTask will not be invoked in spark mode. Detail see SPARK-7953
            filesToVerify.put(DummyOutputCommitter.FILE_ABORTTASK_CALLED + "1", Boolean.FALSE);
            filesToVerify.put(DummyOutputCommitter.FILE_ABORTTASK_CALLED + "2", Boolean.FALSE);
            filesToVerify.put(DummyOutputCommitter.FILE_COMMITJOB_CALLED + "1", Boolean.FALSE);
            filesToVerify.put(DummyOutputCommitter.FILE_COMMITJOB_CALLED + "2", Boolean.FALSE);
            // OutputCommitter.abortJob will not be invoked in spark mode. Detail see SPARK-7953
            filesToVerify.put(DummyOutputCommitter.FILE_ABORTJOB_CALLED + "1", Boolean.FALSE);
            filesToVerify.put(DummyOutputCommitter.FILE_ABORTJOB_CALLED + "2", Boolean.FALSE);
            filesToVerify.put(DummyOutputCommitter.FILE_CLEANUPJOB_CALLED + "1", Boolean.FALSE);
            filesToVerify.put(DummyOutputCommitter.FILE_CLEANUPJOB_CALLED + "2", Boolean.FALSE);
        } else {
            filesToVerify.put(outputFileName1 + "_cleanupOnFailure_succeeded1", Boolean.TRUE);
            filesToVerify.put(outputFileName2 + "_cleanupOnFailure_succeeded2", Boolean.TRUE);
            filesToVerify.put(outputFileName1 + "_cleanupOnFailure_failed1", Boolean.FALSE);
            filesToVerify.put(outputFileName2 + "_cleanupOnFailure_failed2", Boolean.FALSE);
            filesToVerify.put(DummyOutputCommitter.FILE_SETUPJOB_CALLED + "1", Boolean.TRUE);
            filesToVerify.put(DummyOutputCommitter.FILE_SETUPJOB_CALLED + "2", Boolean.TRUE);
            filesToVerify.put(DummyOutputCommitter.FILE_SETUPTASK_CALLED + "1", Boolean.TRUE);
            filesToVerify.put(DummyOutputCommitter.FILE_SETUPTASK_CALLED + "2", Boolean.TRUE);
            filesToVerify.put(DummyOutputCommitter.FILE_COMMITTASK_CALLED + "1", Boolean.FALSE);
            filesToVerify.put(DummyOutputCommitter.FILE_COMMITTASK_CALLED + "2", Boolean.FALSE);
            filesToVerify.put(DummyOutputCommitter.FILE_ABORTTASK_CALLED + "1", Boolean.TRUE);
            filesToVerify.put(DummyOutputCommitter.FILE_ABORTTASK_CALLED + "2", Boolean.TRUE);
            filesToVerify.put(DummyOutputCommitter.FILE_COMMITJOB_CALLED + "1", Boolean.FALSE);
            filesToVerify.put(DummyOutputCommitter.FILE_COMMITJOB_CALLED + "2", Boolean.FALSE);
            filesToVerify.put(DummyOutputCommitter.FILE_ABORTJOB_CALLED + "1", Boolean.TRUE);
            filesToVerify.put(DummyOutputCommitter.FILE_ABORTJOB_CALLED + "2", Boolean.TRUE);
            filesToVerify.put(DummyOutputCommitter.FILE_CLEANUPJOB_CALLED + "1", Boolean.FALSE);
            filesToVerify.put(DummyOutputCommitter.FILE_CLEANUPJOB_CALLED + "2", Boolean.FALSE);
        }

        String[] inputData = new String[]{"hello\tworld", "bye\tworld"};

        // though the second store should
        // not cause a failure, the first one does and the result should be
        // that both stores are considered to have failed
        String script = "a = load '"+ inputFileName + "';" +
                "store a into '" + outputFileName1 + "' using " +
                DUMMY_STORE_CLASS_NAME + "('true', '1');" +
                "store a into '" + outputFileName2 + "' using " +
                DUMMY_STORE_CLASS_NAME + "('false', '2');";

        if(mode.isLocal()) {
            // MR LocalJobRunner does not call abortTask
            if (!mode.toString().startsWith("TEZ")) {
                filesToVerify.put(DummyOutputCommitter.FILE_ABORTTASK_CALLED + "1", Boolean.FALSE);
                filesToVerify.put(DummyOutputCommitter.FILE_ABORTTASK_CALLED + "2", Boolean.FALSE);
            }
            if (Util.isHadoop1_x()) {
                // MAPREDUCE-1447/3563 (LocalJobRunner does not call methods of mapreduce
                // OutputCommitter) is fixed only in 0.23.1
                filesToVerify.put(DummyOutputCommitter.FILE_SETUPJOB_CALLED + "1", Boolean.FALSE);
                filesToVerify.put(DummyOutputCommitter.FILE_SETUPJOB_CALLED + "2", Boolean.FALSE);
                filesToVerify.put(DummyOutputCommitter.FILE_ABORTJOB_CALLED + "1", Boolean.FALSE);
                filesToVerify.put(DummyOutputCommitter.FILE_ABORTJOB_CALLED + "2", Boolean.FALSE);
            }
        }
        Util.deleteFile(ps.getPigContext(), TESTDIR);
        ps.setBatchOn();
        Util.createInputFile(ps.getPigContext(),
                inputFileName, inputData);
        Util.registerMultiLineQuery(ps, script);
        ps.executeBatch();
        for (Entry<String, Boolean> entry : filesToVerify.entrySet()) {
            String condition = entry.getValue() ? "" : "not";
            assertEquals("Checking if file " + entry.getKey() +
                    " does " + condition + " exists in " + mode +
                    " mode", (boolean) entry.getValue(),
                    Util.exists(ps.getPigContext(), entry.getKey()));
        }
    }

    // Test that "_SUCCESS" file is created when "mapreduce.fileoutputcommitter.marksuccessfuljobs"
    // property is set to true
    // The test covers multi store and single store case in local and mapreduce mode
    // The test also checks that "_SUCCESS" file is NOT created when the property
    // is not set to true in all the modes.
    @Test
    public void testSuccessFileCreation1() throws Exception {
        
        try {
            String[] inputData = new String[]{"hello\tworld", "hi\tworld", "bye\tworld"};

            String multiStoreScript = "a = load '"+ inputFileName + "';" +
                    "b = filter a by $0 == 'hello';" +
                    "c = filter a by $0 == 'hi';" +
                    "d = filter a by $0 == 'bye';" +
                    "store b into '" + outputFileName + "_1';" +
                    "store c into '" + outputFileName + "_2';" +
                    "store d into '" + outputFileName + "_3';";

            String singleStoreScript =  "a = load '"+ inputFileName + "';" +
                "store a into '" + outputFileName + "_1';" ;

            for(boolean isPropertySet: new boolean[] { true, false}) {
                for(boolean isMultiStore: new boolean[] { true, false}) {
                    String script = (isMultiStore ? multiStoreScript :
                        singleStoreScript);
                    ps.getPigContext().getProperties().setProperty(
                            MRConfiguration.FILEOUTPUTCOMMITTER_MARKSUCCESSFULJOBS,
                            Boolean.toString(isPropertySet));
                    Util.deleteFile(ps.getPigContext(), TESTDIR);
                    ps.setBatchOn();
                    Util.createInputFile(ps.getPigContext(),
                            inputFileName, inputData);
                    Util.registerMultiLineQuery(ps, script);
                    ps.executeBatch();
                    for(int i = 1; i <= (isMultiStore ? 3 : 1); i++) {
                        String sucFile = outputFileName + "_" + i + "/" +
                                           MapReduceLauncher.SUCCEEDED_FILE_NAME;
                        assertEquals("Checking if _SUCCESS file exists in " +
                                mode + " mode", isPropertySet,
                                Util.exists(ps.getPigContext(), sucFile));
                    }
                }
            }
        } finally {
            Util.deleteFile(ps.getPigContext(), TESTDIR);
        }
    }

    // Test _SUCCESS file is NOT created when job fails and when
    // "mapreduce.fileoutputcommitter.marksuccessfuljobs" property is set to true
    // The test covers multi store and single store case in local and mapreduce mode
    // The test also checks that "_SUCCESS" file is NOT created when the property
    // is not set to true in all the modes.
    @Test
    public void testSuccessFileCreation2() throws Exception {
        try {
            String[] inputData = new String[]{"hello\tworld", "hi\tworld", "bye\tworld"};
            System.err.println("XXX: " + TestStore.FailUDF.class.getName());
            String multiStoreScript = "a = load '"+ inputFileName + "';" +
                    "b = filter a by $0 == 'hello';" +
                    "b = foreach b generate " + FAIL_UDF_NAME + "($0);" +
                    "c = filter a by $0 == 'hi';" +
                    "d = filter a by $0 == 'bye';" +
                    "store b into '" + outputFileName + "_1';" +
                    "store c into '" + outputFileName + "_2';" +
                    "store d into '" + outputFileName + "_3';";

            String singleStoreScript =  "a = load '"+ inputFileName + "';" +
                "b = foreach a generate " + FAIL_UDF_NAME + "($0);" +
                "store b into '" + outputFileName + "_1';" ;

            for(boolean isPropertySet: new boolean[] { true, false}) {
                for(boolean isMultiStore: new boolean[] { true, false}) {
                    String script = (isMultiStore ? multiStoreScript :
                        singleStoreScript);
                    if (mode.isLocal()) {
                        // since the job is guaranteed to fail, let's set
                        // number of retries to 1.
                        ps.getPigContext().getProperties().setProperty(MAP_MAX_ATTEMPTS, "1");
                    }
                    ps.getPigContext().getProperties().setProperty(
                            MRConfiguration.FILEOUTPUTCOMMITTER_MARKSUCCESSFULJOBS,
                            Boolean.toString(isPropertySet));
                    Util.deleteFile(ps.getPigContext(), TESTDIR);
                    ps.setBatchOn();
                    Util.createInputFile(ps.getPigContext(),
                            inputFileName, inputData);
                    Util.registerMultiLineQuery(ps, script);
                    try {
                        ps.executeBatch();
                    } catch(IOException ioe) {
                        if(!ioe.getMessage().equals("FailUDFException")) {
                            // an unexpected exception
                            throw ioe;
                        }
                    }
                    for(int i = 1; i <= (isMultiStore ? 3 : 1); i++) {
                        String sucFile = outputFileName + "_" + i + "/" +
                                           MapReduceLauncher.SUCCEEDED_FILE_NAME;
                        assertEquals("Checking if _SUCCESS file exists in " +
                                mode + " mode", false,
                                Util.exists(ps.getPigContext(), sucFile));
                    }
                }
            }
        } finally {
            Util.deleteFile(ps.getPigContext(), TESTDIR);
        }
    }

    /**
     * Test whether "part-m-00000" file is created on empty output when
     * {@link PigConfiguration#PIG_OUTPUT_LAZY} is set and if LazyOutputFormat is
     * supported by Hadoop.
     * The test covers multi store and single store case in local and mapreduce mode
     *
     * @throws IOException
     */
    @Test
    public void testEmptyPartFileCreation() throws Exception {

        boolean isLazyOutputPresent = true;
        try {
            Class<?> clazz = PigContext
                    .resolveClassName("org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat");
            clazz.getMethod("setOutputFormatClass", Job.class, Class.class);
        }
        catch (Exception e) {
            isLazyOutputPresent = false;
        }

        //skip test if LazyOutputFormat is not supported (<= Hadoop 1.0.0)
        Assume.assumeTrue("LazyOutputFormat couldn't be loaded, test is skipped", isLazyOutputPresent);

        try {
            String[] inputData = new String[]{"hello\tworld", "hi\tworld", "bye\tworld"};

            String multiStoreScript = "a = load '"+ inputFileName + "';" +
                    "b = filter a by $0 == 'hey';" +
                    "c = filter a by $1 == 'globe';" +
                    "d = limit a 2;" +
                    "e = foreach d generate *, 'x';" +
                    "f = filter e by $3 == 'y';" +
                    "store b into '" + outputFileName + "_1';" +
                    "store c into '" + outputFileName + "_2';" +
                    "store f into '" + outputFileName + "_3';";

            String singleStoreScript =  "a = load '"+ inputFileName + "';" +
                    "b = filter a by $0 == 'hey';" +
                    "store b into '" + outputFileName + "_1';" ;

            for(boolean isMultiStore: new boolean[] { true, false}) {
                if (isMultiStore && (mode.isLocal() ||
                        mode.equals(ExecType.MAPREDUCE))) {
                    // Skip this test for Mapreduce as MapReducePOStoreImpl
                    // does not handle LazyOutputFormat
                    continue;
                }

                String script = (isMultiStore ? multiStoreScript
                        : singleStoreScript);
                ps.getPigContext().getProperties().setProperty(
                        PigConfiguration.PIG_OUTPUT_LAZY, "true");
                Util.deleteFile(ps.getPigContext(), TESTDIR);
                ps.setBatchOn();
                Util.createInputFile(ps.getPigContext(),
                        inputFileName, inputData);
                Util.registerMultiLineQuery(ps, script);
                ps.executeBatch();
                Configuration conf = ConfigurationUtil.toConfiguration(ps.getPigContext().getProperties());
                for(int i = 1; i <= (isMultiStore ? 3 : 1); i++) {
                    assertEquals("For an empty output part-m-00000 should not exist in " + mode + " mode",
                            null,
                            getFirstOutputFile(conf, new Path(outputFileName + "_" + i), mode, true));
                }
            }
        } finally {
            Util.deleteFile(ps.getPigContext(), TESTDIR);
        }
    }

    public static Path getFirstOutputFile(Configuration conf, Path outputDir,
            ExecType exectype, boolean isMapOutput) throws Exception {
        FileSystem fs = outputDir.getFileSystem(conf);
        FileStatus[] outputFiles = fs.listStatus(outputDir,
                Util.getSuccessMarkerPathFilter());

        boolean filefound = false;
        if (outputFiles != null && outputFiles.length != 0) {
            String name = outputFiles[0].getPath().getName();
            if (exectype == Util.getLocalTestMode() || exectype == ExecType.MAPREDUCE) {
                if (isMapOutput) {
                    filefound = name.equals("part-m-00000");
                } else {
                    filefound = name.equals("part-r-00000");
                }
            } else {
                filefound = name.startsWith("part-");
            }
        }
        return filefound ? outputFiles[0].getPath() : null;
    }
}
