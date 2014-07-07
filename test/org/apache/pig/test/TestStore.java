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
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.pig.EvalFunc;
import org.apache.pig.ExecType;
import org.apache.pig.PigConfiguration;
import org.apache.pig.PigException;
import org.apache.pig.PigServer;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.StoreMetadata;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceLauncher;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTextOutputFormat;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POProject;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.util.MapRedUtil;
import org.apache.pig.builtin.BinStorage;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.PlanValidationException;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.logical.relational.LOStore;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.parser.ParserException;
import org.apache.pig.parser.QueryParserDriver;
import org.apache.pig.test.utils.GenRandomData;
import org.apache.pig.test.utils.TestHelper;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

public class TestStore {
    POStore st;
    DataBag inpDB;
    static MiniGenericCluster cluster = MiniGenericCluster.buildCluster();
    PigContext pc;
    POProject proj;
    PigServer pig;

    String inputFileName;
    String outputFileName;

    private static final String DUMMY_STORE_CLASS_NAME
    = "org.apache.pig.test.TestStore\\$DummyStore";

    private static final String FAIL_UDF_NAME
    = "org.apache.pig.test.TestStore\\$FailUDF";
    private static final String MAP_MAX_ATTEMPTS = "mapred.map.max.attempts";
    private static final String TESTDIR = "/tmp/" + TestStore.class.getSimpleName();
    private static ExecType[] modes = new ExecType[] { ExecType.LOCAL, cluster.getExecType() };

    @Before
    public void setUp() throws Exception {
        pig = new PigServer(cluster.getExecType(), cluster.getProperties());
        pc = pig.getPigContext();
        inputFileName = TESTDIR + "/TestStore-" + new Random().nextLong() + ".txt";
        outputFileName = TESTDIR + "/TestStore-output-" + new Random().nextLong() + ".txt";

        DateTimeZone.setDefault(DateTimeZone.forOffsetMillis(DateTimeZone.UTC.getOffset(null)));
    }

    @After
    public void tearDown() throws Exception {
        Util.resetStateForExecModeSwitch();
        Util.deleteDirectory(new File(TESTDIR));
        Util.deleteFile(cluster, TESTDIR);
    }

    private void storeAndCopyLocally(DataBag inpDB) throws Exception {
        setUpInputFileOnCluster(inpDB);
        String script = "a = load '" + inputFileName + "'; " +
                "store a into '" + outputFileName + "' using PigStorage('\t');" +
                "fs -ls " + TESTDIR;
        pig.setBatchOn();
        Util.registerMultiLineQuery(pig, script);
        pig.executeBatch();
        Path path = getFirstOutputFile(cluster.getConfiguration(),
                new Path(outputFileName), cluster.getExecType(), true);
        Util.copyFromClusterToLocal(
                cluster,
                path.toString(), outputFileName);
    }

    public static Path getFirstOutputFile(Configuration conf, Path outputDir,
            ExecType exectype, boolean isMapOutput) throws IOException {
        FileSystem fs = outputDir.getFileSystem(conf);
        FileStatus[] outputFiles = fs.listStatus(outputDir,
                Util.getSuccessMarkerPathFilter());

        boolean filefound = false;
        if (outputFiles != null && outputFiles.length != 0) {
            String name = outputFiles[0].getPath().getName();
            if (exectype == ExecType.LOCAL || exectype == ExecType.MAPREDUCE) {
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

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        cluster.shutDown();
    }

    @Test
    public void testValidation() throws Exception{
        String outputFileName = "test-output.txt";
        try {
            String query = "a = load '" + inputFileName + "' as (c:chararray, " +
                           "i:int,d:double);" +
                           "store a into '" + outputFileName + "' using " + "PigStorage();";
            org.apache.pig.newplan.logical.relational.LogicalPlan lp = Util.buildLp( pig, query );
        } catch (PlanValidationException e){
                // Since output file is not present, validation should pass
                // and not throw this exception.
                fail("Store validation test failed.");
        } finally {
            Util.deleteFile(pig.getPigContext(), outputFileName);
        }
    }

    @Test
    public void testValidationFailure() throws Exception{
        String input[] = new String[] { "some data" };
        String outputFileName = "test-output.txt";
        boolean sawException = false;
        try {
            Util.createInputFile(pig.getPigContext(),outputFileName, input);
            String query = "a = load '" + inputFileName + "' as (c:chararray, " +
                           "i:int,d:double);" +
                           "store a into '" + outputFileName + "' using PigStorage();";
            Util.buildLp( pig, query );
        } catch (InvocationTargetException e){
            FrontendException pve = (FrontendException)e.getCause();
            pve.printStackTrace();
            // Since output file is present, validation should fail
            // and throw this exception
            assertEquals(6000,pve.getErrorCode());
            assertEquals(PigException.REMOTE_ENVIRONMENT, pve.getErrorSource());
            assertTrue(pve.getCause() instanceof IOException);
            sawException = true;
        } finally {
            assertTrue(sawException);
            Util.deleteFile(pig.getPigContext(), outputFileName);
        }
    }

    @Test
    public void testStore() throws Exception {
        inpDB = GenRandomData.genRandSmallTupDataBag(new Random(), 10, 100);
        storeAndCopyLocally(inpDB);

        int size = 0;
        BufferedReader br = new BufferedReader(new FileReader(outputFileName));
        for(String line=br.readLine();line!=null;line=br.readLine()){
            String[] flds = line.split("\t",-1);
            Tuple t = new DefaultTuple();
            t.append(flds[0].compareTo("")!=0 ? flds[0] : null);
            t.append(flds[1].compareTo("")!=0 ? Integer.parseInt(flds[1]) : null);

            System.err.println("Simple data: ");
            System.err.println(line);
            System.err.println("t: ");
            System.err.println(t);
            assertTrue(TestHelper.bagContains(inpDB, t));
            ++size;
        }
        assertEquals(size, inpDB.size());
        br.close();
    }

    /**
     * @param inpD
     * @throws IOException
     */
    private void setUpInputFileOnCluster(DataBag inpD) throws IOException {
        String[] data = new String[(int) inpD.size()];
        int i = 0;
        for (Tuple tuple : inpD) {
            data[i] = toDelimitedString(tuple, "\t");
            i++;
        }
        Util.createInputFile(cluster, inputFileName, data);
    }

    @SuppressWarnings("unchecked")
    private String toDelimitedString(Tuple t, String delim) throws ExecException {
        StringBuilder buf = new StringBuilder();
        for (int i = 0; i < t.size(); i++) {
            Object field = t.get(i);
            if(field == null) {
                buf.append("");
            } else {
                if(field instanceof Map) {
                    Map<String, Object> m = (Map<String, Object>)field;
                    buf.append(DataType.mapToString(m));
                } else {
                    buf.append(field.toString());
                }
            }

            if (i != t.size() - 1)
                buf.append(delim);
        }
        return buf.toString();
    }

    @Test
    public void testStoreComplexData() throws Exception {
        inpDB = GenRandomData.genRandFullTupTextDataBag(new Random(), 10, 100);
        storeAndCopyLocally(inpDB);
        PigStorage ps = new PigStorage("\t");
        int size = 0;
        BufferedReader br = new BufferedReader(new FileReader(outputFileName));
        for(String line=br.readLine();line!=null;line=br.readLine()){
            String[] flds = line.split("\t",-1);
            Tuple t = new DefaultTuple();

            ResourceFieldSchema mapfs = GenRandomData.getRandMapFieldSchema();
            ResourceFieldSchema bagfs = GenRandomData.getSmallTupDataBagFieldSchema();
            ResourceFieldSchema tuplefs = GenRandomData.getSmallTupleFieldSchema();

            t.append(flds[0].compareTo("")!=0 ? ps.getLoadCaster().bytesToBag(flds[0].getBytes(), bagfs) : null);
            t.append(flds[1].compareTo("")!=0 ? new DataByteArray(flds[1].getBytes()) : null);
            t.append(flds[2].compareTo("")!=0 ? ps.getLoadCaster().bytesToCharArray(flds[2].getBytes()) : null);
            t.append(flds[3].compareTo("")!=0 ? ps.getLoadCaster().bytesToDouble(flds[3].getBytes()) : null);
            t.append(flds[4].compareTo("")!=0 ? ps.getLoadCaster().bytesToFloat(flds[4].getBytes()) : null);
            t.append(flds[5].compareTo("")!=0 ? ps.getLoadCaster().bytesToInteger(flds[5].getBytes()) : null);
            t.append(flds[6].compareTo("")!=0 ? ps.getLoadCaster().bytesToLong(flds[6].getBytes()) : null);
            t.append(flds[7].compareTo("")!=0 ? ps.getLoadCaster().bytesToMap(flds[7].getBytes(), mapfs) : null);
            t.append(flds[8].compareTo("")!=0 ? ps.getLoadCaster().bytesToTuple(flds[8].getBytes(), tuplefs) : null);
            t.append(flds[9].compareTo("")!=0 ? ps.getLoadCaster().bytesToBoolean(flds[9].getBytes()) : null);
            t.append(flds[10].compareTo("")!=0 ? ps.getLoadCaster().bytesToDateTime(flds[10].getBytes()) : null);
            assertEquals(true, TestHelper.bagContains(inpDB, t));
            ++size;
        }
        assertEquals(true, size==inpDB.size());
        br.close();
    }

    @Test
    public void testStoreComplexDataWithNull() throws Exception {
        Tuple inputTuple = GenRandomData.genRandSmallBagTextTupleWithNulls(new Random(), 10, 100);
        inpDB = DefaultBagFactory.getInstance().newDefaultBag();
        inpDB.add(inputTuple);
        storeAndCopyLocally(inpDB);
        PigStorage ps = new PigStorage("\t");
        BufferedReader br = new BufferedReader(new FileReader(outputFileName));
        for(String line=br.readLine();line!=null;line=br.readLine()){
            System.err.println("Complex data: ");
            System.err.println(line);
            String[] flds = line.split("\t",-1);
            Tuple t = new DefaultTuple();

            ResourceFieldSchema stringfs = new ResourceFieldSchema();
            stringfs.setType(DataType.CHARARRAY);
            ResourceFieldSchema intfs = new ResourceFieldSchema();
            intfs.setType(DataType.INTEGER);
            ResourceFieldSchema bytefs = new ResourceFieldSchema();
            bytefs.setType(DataType.BYTEARRAY);

            ResourceSchema tupleSchema = new ResourceSchema();
            tupleSchema.setFields(new ResourceFieldSchema[]{stringfs, intfs});
            ResourceFieldSchema tuplefs = new ResourceFieldSchema();
            tuplefs.setSchema(tupleSchema);
            tuplefs.setType(DataType.TUPLE);

            ResourceSchema bagSchema = new ResourceSchema();
            bagSchema.setFields(new ResourceFieldSchema[]{tuplefs});
            ResourceFieldSchema bagfs = new ResourceFieldSchema();
            bagfs.setSchema(bagSchema);
            bagfs.setType(DataType.BAG);

            ResourceSchema mapSchema = new ResourceSchema();
            mapSchema.setFields(new ResourceFieldSchema[]{bytefs});
            ResourceFieldSchema mapfs = new ResourceFieldSchema();
            mapfs.setSchema(mapSchema);
            mapfs.setType(DataType.MAP);

            t.append(flds[0].compareTo("")!=0 ? ps.getLoadCaster().bytesToBag(flds[0].getBytes(), bagfs) : null);
            t.append(flds[1].compareTo("")!=0 ? new DataByteArray(flds[1].getBytes()) : null);
            t.append(flds[2].compareTo("")!=0 ? ps.getLoadCaster().bytesToCharArray(flds[2].getBytes()) : null);
            t.append(flds[3].compareTo("")!=0 ? ps.getLoadCaster().bytesToDouble(flds[3].getBytes()) : null);
            t.append(flds[4].compareTo("")!=0 ? ps.getLoadCaster().bytesToFloat(flds[4].getBytes()) : null);
            t.append(flds[5].compareTo("")!=0 ? ps.getLoadCaster().bytesToInteger(flds[5].getBytes()) : null);
            t.append(flds[6].compareTo("")!=0 ? ps.getLoadCaster().bytesToLong(flds[6].getBytes()) : null);
            t.append(flds[7].compareTo("")!=0 ? ps.getLoadCaster().bytesToMap(flds[7].getBytes(), mapfs) : null);
            t.append(flds[8].compareTo("")!=0 ? ps.getLoadCaster().bytesToTuple(flds[8].getBytes(), tuplefs) : null);
            t.append(flds[9].compareTo("")!=0 ? ps.getLoadCaster().bytesToBoolean(flds[9].getBytes()) : null);
            t.append(flds[10].compareTo("")!=0 ? ps.getLoadCaster().bytesToDateTime(flds[10].getBytes()) : null);
            t.append(flds[11].compareTo("")!=0 ? ps.getLoadCaster().bytesToCharArray(flds[10].getBytes()) : null);
            assertTrue(TestHelper.tupleEquals(inputTuple, t));
        }
        br.close();
    }
    @Test
    public void testBinStorageGetSchema() throws IOException, ParserException {
        String input[] = new String[] { "hello\t1\t10.1", "bye\t2\t20.2" };
        String inputFileName = "testGetSchema-input.txt";
        String outputFileName = "testGetSchema-output.txt";
        try {
            Util.createInputFile(pig.getPigContext(),
                    inputFileName, input);
            String query = "a = load '" + inputFileName + "' as (c:chararray, " +
                    "i:int,d:double);store a into '" + outputFileName + "' using " +
                            "BinStorage();";
            pig.setBatchOn();
            Util.registerMultiLineQuery(pig, query);
            pig.executeBatch();
            ResourceSchema rs = new BinStorage().getSchema(outputFileName,
                    new Job(ConfigurationUtil.toConfiguration(pig.getPigContext().
                            getProperties())));
            Schema expectedSchema = Utils.getSchemaFromString(
                    "c:chararray,i:int,d:double");
            assertTrue("Checking binstorage getSchema output", Schema.equals(
                    expectedSchema, Schema.getPigSchema(rs), true, true));
        } finally {
            Util.deleteFile(pig.getPigContext(), inputFileName);
            Util.deleteFile(pig.getPigContext(), outputFileName);
        }
    }

    @Test
    public void testStoreRemoteRel() throws Exception {
        checkStorePath("test","/tmp/test");
    }

    @Test
    public void testStoreRemoteAbs() throws Exception {
        checkStorePath("/tmp/test","/tmp/test");
    }

    @Test
    public void testStoreRemoteRelScheme() throws Exception {
        checkStorePath("test","/tmp/test");
    }

    @Test
    public void testStoreRemoteAbsScheme() throws Exception {
        checkStorePath("hdfs:/tmp/test","hdfs:/tmp/test");
    }

    @Test
    public void testStoreRemoteAbsAuth() throws Exception {
        checkStorePath("hdfs://localhost:9000/test","/test");
    }

    @Test
    public void testStoreRemoteNormalize() throws Exception {
        checkStorePath("/tmp/foo/../././","/tmp/foo/.././.");
    }

    @Test
    public void testSetStoreSchema() throws Exception {
        PigServer ps = null;
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

        for (ExecType execType : modes) {
            Util.resetStateForExecModeSwitch();
            if(execType == cluster.getExecType()) {
                ps = new PigServer(cluster.getExecType(),
                        cluster.getProperties());
            } else {
                Properties props = new Properties();
                props.setProperty(MapRedUtil.FILE_SYSTEM_NAME, "file:///");
                ps = new PigServer(ExecType.LOCAL, props);
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
                        " does " + condition + " exists in " + execType +
                        " mode", (boolean) entry.getValue(),
                        Util.exists(ps.getPigContext(), entry.getKey()));
            }
        }
    }

    @Test
    public void testCleanupOnFailure() throws Exception {
        PigServer ps = null;
        String cleanupSuccessFile = outputFileName + "_cleanupOnFailure_succeeded";
        String cleanupFailFile = outputFileName + "_cleanupOnFailure_failed";
        String[] inputData = new String[]{"hello\tworld", "bye\tworld"};

        String script = "a = load '"+ inputFileName + "';" +
                "store a into '" + outputFileName + "' using " +
                DUMMY_STORE_CLASS_NAME + "('true');";

        for (ExecType execType : modes) {
            Util.resetStateForExecModeSwitch();
            if(execType == cluster.getExecType()) {
                ps = new PigServer(cluster.getExecType(),
                        cluster.getProperties());
            } else {
                Properties props = new Properties();
                props.setProperty(MapRedUtil.FILE_SYSTEM_NAME, "file:///");
                ps = new PigServer(ExecType.LOCAL, props);
            }
            Util.deleteFile(ps.getPigContext(), TESTDIR);
            ps.setBatchOn();
            Util.createInputFile(ps.getPigContext(),
                    inputFileName, inputData);
            Util.registerMultiLineQuery(ps, script);
            ps.executeBatch();
            assertEquals(
                    "Checking if file indicating that cleanupOnFailure failed " +
                    " does not exists in " + execType + " mode", false,
                    Util.exists(ps.getPigContext(), cleanupFailFile));
            assertEquals(
                    "Checking if file indicating that cleanupOnFailure was " +
                    "successfully called exists in " + execType + " mode", true,
                    Util.exists(ps.getPigContext(), cleanupSuccessFile));
        }
    }


    @Test
    public void testCleanupOnFailureMultiStore() throws Exception {
        PigServer ps = null;
        String outputFileName1 = TESTDIR + "/TestStore-output-" + new Random().nextLong() + ".txt";
        String outputFileName2 = TESTDIR + "/TestStore-output-" + new Random().nextLong() + ".txt";

        Map<String, Boolean> filesToVerify = new HashMap<String, Boolean>();
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

        String[] inputData = new String[]{"hello\tworld", "bye\tworld"};

        // though the second store should
        // not cause a failure, the first one does and the result should be
        // that both stores are considered to have failed
        String script = "a = load '"+ inputFileName + "';" +
                "store a into '" + outputFileName1 + "' using " +
                DUMMY_STORE_CLASS_NAME + "('true', '1');" +
                "store a into '" + outputFileName2 + "' using " +
                DUMMY_STORE_CLASS_NAME + "('false', '2');";

        for (ExecType execType : new ExecType[] {cluster.getExecType(), ExecType.LOCAL}) {
            Util.resetStateForExecModeSwitch();
            if(execType == cluster.getExecType()) {
                ps = new PigServer(cluster.getExecType(),
                        cluster.getProperties());
            } else {
                Properties props = new Properties();
                props.setProperty(MapRedUtil.FILE_SYSTEM_NAME, "file:///");
                ps = new PigServer(ExecType.LOCAL, props);
                // LocalJobRunner does not call abortTask
                filesToVerify.put(DummyOutputCommitter.FILE_ABORTTASK_CALLED + "1", Boolean.FALSE);
                filesToVerify.put(DummyOutputCommitter.FILE_ABORTTASK_CALLED + "2", Boolean.FALSE);
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
                        " does " + condition + " exists in " + execType +
                        " mode", (boolean) entry.getValue(),
                        Util.exists(ps.getPigContext(), entry.getKey()));
            }
        }
    }

    // Test that "_SUCCESS" file is created when "mapreduce.fileoutputcommitter.marksuccessfuljobs"
    // property is set to true
    // The test covers multi store and single store case in local and mapreduce mode
    // The test also checks that "_SUCCESS" file is NOT created when the property
    // is not set to true in all the modes.
    @Test
    public void testSuccessFileCreation1() throws Exception {
        PigServer ps = null;

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

            for (ExecType execType : modes) {
                for(boolean isPropertySet: new boolean[] { true, false}) {
                    for(boolean isMultiStore: new boolean[] { true, false}) {
                        String script = (isMultiStore ? multiStoreScript :
                            singleStoreScript);
                        Util.resetStateForExecModeSwitch();
                        if(execType == cluster.getExecType()) {
                            ps = new PigServer(cluster.getExecType(),
                                    cluster.getProperties());
                        } else {
                            Properties props = new Properties();
                            props.setProperty(MapRedUtil.FILE_SYSTEM_NAME, "file:///");
                            ps = new PigServer(ExecType.LOCAL, props);
                        }
                        ps.getPigContext().getProperties().setProperty(
                                MapReduceLauncher.SUCCESSFUL_JOB_OUTPUT_DIR_MARKER,
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
                                    execType + " mode", isPropertySet,
                                    Util.exists(ps.getPigContext(), sucFile));
                        }
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
        PigServer ps = null;
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

            for (ExecType execType : modes) {
                for(boolean isPropertySet: new boolean[] { true, false}) {
                    for(boolean isMultiStore: new boolean[] { true, false}) {
                        String script = (isMultiStore ? multiStoreScript :
                            singleStoreScript);
                        Util.resetStateForExecModeSwitch();
                        if(execType == cluster.getExecType()) {
                            // since the job is guaranteed to fail, let's set
                            // number of retries to 1.
                            Properties props = cluster.getProperties();
                            props.setProperty(MAP_MAX_ATTEMPTS, "1");
                            ps = new PigServer(cluster.getExecType(), props);
                        } else {
                            Properties props = new Properties();
                            props.setProperty(MapRedUtil.FILE_SYSTEM_NAME, "file:///");
                            // since the job is guaranteed to fail, let's set
                            // number of retries to 1.
                            props.setProperty(MAP_MAX_ATTEMPTS, "1");
                            ps = new PigServer(ExecType.LOCAL, props);
                        }
                        ps.getPigContext().getProperties().setProperty(
                                MapReduceLauncher.SUCCESSFUL_JOB_OUTPUT_DIR_MARKER,
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
                                    execType + " mode", false,
                                    Util.exists(ps.getPigContext(), sucFile));
                        }
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
    public void testEmptyPartFileCreation() throws IOException {

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

        PigServer ps = null;

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

            for (ExecType execType : modes) {
                for(boolean isMultiStore: new boolean[] { true, false}) {
                    if (isMultiStore && (execType.equals(ExecType.LOCAL) ||
                            execType.equals(ExecType.MAPREDUCE))) {
                        // Skip this test for Mapreduce as MapReducePOStoreImpl
                        // does not handle LazyOutputFormat
                        continue;
                    }

                    String script = (isMultiStore ? multiStoreScript
                            : singleStoreScript);
                    Util.resetStateForExecModeSwitch();
                    if(execType == cluster.getExecType()) {
                        ps = new PigServer(cluster.getExecType(),
                                cluster.getProperties());
                    } else {
                        Properties props = new Properties();
                        props.setProperty(MapRedUtil.FILE_SYSTEM_NAME, "file:///");
                        ps = new PigServer(ExecType.LOCAL, props);
                    }
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
                        assertEquals("For an empty output part-m-00000 should not exist in " + execType + " mode",
                                null,
                                getFirstOutputFile(conf, new Path(outputFileName + "_" + i), execType, true));
                    }
                }
            }
        } finally {
            Util.deleteFile(ps.getPigContext(), TESTDIR);
        }
    }

    // A UDF which always throws an Exception so that the job can fail
    public static class FailUDF extends EvalFunc<String> {

        @Override
        public String exec(Tuple input) throws IOException {
            throw new IOException("FailUDFException");
        }

    }

    public static class DummyStore extends PigStorage implements StoreMetadata{

        private boolean failInPutNext = false;

        private String outputFileSuffix= "";

        public DummyStore(String failInPutNextStr) {
            failInPutNext = Boolean.parseBoolean(failInPutNextStr);
        }

        public DummyStore(String failInPutNextStr, String outputFileSuffix) {
            failInPutNext = Boolean.parseBoolean(failInPutNextStr);
            this.outputFileSuffix = outputFileSuffix;
        }

        public DummyStore() {
        }


        @Override
        public void putNext(Tuple t) throws IOException {
            if(failInPutNext) {
                throw new IOException("Failing in putNext");
            }
            super.putNext(t);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public OutputFormat getOutputFormat() {
            return new DummyOutputFormat(outputFileSuffix);
        }

        @Override
        public void storeSchema(ResourceSchema schema, String location,
                Job job) throws IOException {
            FileSystem fs = FileSystem.get(job.getConfiguration());

            FileStatus[] outputFiles = fs.listStatus(new Path(location),
                    Util.getSuccessMarkerPathFilter());
            // verify that output is available prior to storeSchema call
            Path resultPath = null;
            if (outputFiles != null && outputFiles.length > 0
                    && outputFiles[0].getPath().getName().startsWith("part-")) {
                resultPath = outputFiles[0].getPath();
            }
            if (resultPath == null) {
                FileStatus[] listing = fs.listStatus(new Path(location));
                for (FileStatus fstat : listing) {
                    System.err.println("Output File:" + fstat.getPath());
                }
                // not creating the marker file below fails the test
                throw new IOException("" + resultPath + " not available in storeSchema");
            }
            // create a file to test that this method got called - if it gets called
            // multiple times, the create will throw an Exception
            fs.create(
                    new Path(location + "_storeSchema_test"),
                    false);
        }

        @Override
        public void cleanupOnFailure(String location, Job job)
                throws IOException {
            super.cleanupOnFailure(location, job);

            // check that the output file location is not present
            Configuration conf = job.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            if(fs.exists(new Path(location))) {
                // create a file to inidicate that the cleanup did not happen
                fs.create(new Path(location + "_cleanupOnFailure_failed" +
                        outputFileSuffix), false);
            }
            // create a file to test that this method got called successfully
            // if it gets called multiple times, the create will throw an Exception
            fs.create(
                    new Path(location + "_cleanupOnFailure_succeeded" +
                            outputFileSuffix), false);
        }

        @Override
        public void storeStatistics(ResourceStatistics stats, String location,
                Job job) throws IOException {

        }
    }

    private void checkStorePath(String orig, String expected) throws Exception {
        checkStorePath(orig, expected, false);
    }

    private void checkStorePath(String orig, String expected, boolean isTmp) throws Exception {
        pc.getProperties().setProperty(PigConfiguration.OPT_MULTIQUERY,""+true);

        DataStorage dfs = pc.getDfs();
        dfs.setActiveContainer(dfs.asContainer("/tmp"));
        Map<String, String> fileNameMap = new HashMap<String, String>();

        QueryParserDriver builder = new QueryParserDriver(pc, "Test-Store", fileNameMap);

        String query = "a = load 'foo';" + "store a into '"+orig+"';";
        LogicalPlan lp = builder.parse(query);

        assertTrue(lp.size()>1);
        Operator op = lp.getSinks().get(0);

        assertTrue(op instanceof LOStore);
        LOStore store = (LOStore)op;

        String p = store.getFileSpec().getFileName();
        p = p.replaceAll("hdfs://[\\-\\w:\\.]*/","/");

        if (isTmp) {
            assertTrue(p.matches("/tmp.*"));
        } else {
            assertEquals(expected, p);
        }
    }

    static class DummyOutputFormat extends PigTextOutputFormat {

        private String outputFileSuffix;

        public DummyOutputFormat(String outputFileSuffix) {
            super((byte) '\t');
            this.outputFileSuffix = outputFileSuffix;
        }

        @Override
        public synchronized OutputCommitter getOutputCommitter(
                TaskAttemptContext context) throws IOException {
            return new DummyOutputCommitter(outputFileSuffix,
                    super.getOutputCommitter(context));
        }

        @Override
        public Path getDefaultWorkFile(TaskAttemptContext context,
                String extension) throws IOException {
            FileOutputCommitter committer =
                    (FileOutputCommitter) super.getOutputCommitter(context);
            return new Path(committer.getWorkPath(), getUniqueFile(context,
                    "part", extension));
        }

    }

    static class DummyOutputCommitter extends OutputCommitter {

        static String FILE_SETUPJOB_CALLED = "/tmp/TestStore/_setupJob_called";
        static String FILE_SETUPTASK_CALLED = "/tmp/TestStore/_setupTask_called";
        static String FILE_COMMITTASK_CALLED = "/tmp/TestStore/_commitTask_called";
        static String FILE_ABORTTASK_CALLED = "/tmp/TestStore/_abortTask_called";
        static String FILE_CLEANUPJOB_CALLED = "/tmp/TestStore/_cleanupJob_called";
        static String FILE_COMMITJOB_CALLED = "/tmp/TestStore/_commitJob_called";
        static String FILE_ABORTJOB_CALLED = "/tmp/TestStore/_abortJob_called";

        private String outputFileSuffix;
        private OutputCommitter baseCommitter;

        public DummyOutputCommitter(String outputFileSuffix,
                OutputCommitter baseCommitter) throws IOException {
            this.outputFileSuffix = outputFileSuffix;
            this.baseCommitter = baseCommitter;
        }

        @Override
        public void setupJob(JobContext jobContext) throws IOException {
            baseCommitter.setupJob(jobContext);
            createFile(jobContext, FILE_SETUPJOB_CALLED + outputFileSuffix);
        }

        @Override
        public void setupTask(TaskAttemptContext taskContext)
                throws IOException {
            baseCommitter.setupTask(taskContext);
            createFile(taskContext, FILE_SETUPTASK_CALLED + outputFileSuffix);
        }

        @Override
        public boolean needsTaskCommit(TaskAttemptContext taskContext)
                throws IOException {
            return true;
        }

        @Override
        public void commitTask(TaskAttemptContext taskContext)
                throws IOException {
            baseCommitter.commitTask(taskContext);
            createFile(taskContext, FILE_COMMITTASK_CALLED + outputFileSuffix);
        }

        @Override
        public void abortTask(TaskAttemptContext taskContext)
                throws IOException {
            baseCommitter.abortTask(taskContext);
            createFile(taskContext, FILE_ABORTTASK_CALLED + outputFileSuffix);
        }

        @Override
        public void cleanupJob(JobContext jobContext) throws IOException {
            baseCommitter.cleanupJob(jobContext);
            createFile(jobContext, FILE_CLEANUPJOB_CALLED + outputFileSuffix);
        }

        @Override
        public void commitJob(JobContext jobContext) throws IOException {
            baseCommitter.commitJob(jobContext);
            createFile(jobContext, FILE_COMMITJOB_CALLED + outputFileSuffix);
        }

        @Override
        public void abortJob(JobContext jobContext, State state)
                throws IOException {
            baseCommitter.abortJob(jobContext, state);
            createFile(jobContext, FILE_ABORTJOB_CALLED + outputFileSuffix);
        }

        public void createFile(JobContext jobContext, String fileName)
                throws IOException {
            Configuration conf = jobContext.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            fs.mkdirs(new Path(fileName).getParent());
            FSDataOutputStream out = fs.create(new Path(fileName), true);
            out.close();
        }
    }
}
