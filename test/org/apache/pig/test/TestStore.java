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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.EvalFunc;
import org.apache.pig.ExecType;
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
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.PlanValidationException;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.logical.relational.LOStore;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.rules.InputOutputFileValidator;
import org.apache.pig.parser.ParserException;
import org.apache.pig.parser.QueryParserDriver;
import org.apache.pig.test.utils.GenRandomData;
import org.apache.pig.test.utils.TestHelper;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestStore extends junit.framework.TestCase {
    POStore st;
    DataBag inpDB;
    static MiniCluster cluster = MiniCluster.buildCluster();
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
    
    @Override
    @Before
    public void setUp() throws Exception {
        pig = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        pc = pig.getPigContext();
        inputFileName = "/tmp/TestStore-" + new Random().nextLong() + ".txt";
        outputFileName = "/tmp/TestStore-output-" + new Random().nextLong() + ".txt";
        
        DateTimeZone.setDefault(DateTimeZone.forOffsetMillis(DateTimeZone.UTC.getOffset(null)));
    }

    @Override
    @After
    public void tearDown() throws Exception {
        Util.deleteFile(cluster, inputFileName);
        Util.deleteFile(cluster, outputFileName);
        new File(outputFileName).delete();
    }

    private void storeAndCopyLocally(DataBag inpDB) throws Exception {
        setUpInputFileOnCluster(inpDB);
        String script = "a = load '" + inputFileName + "'; " +
                "store a into '" + outputFileName + "' using PigStorage('\t');" +
                "fs -ls /tmp";
        pig.setBatchOn();
        Util.registerMultiLineQuery(pig, script);
        pig.executeBatch();
        Util.copyFromClusterToLocal(cluster, outputFileName + "/part-m-00000", outputFileName);
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
            new InputOutputFileValidator(lp, pig.getPigContext()).validate();
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
            org.apache.pig.newplan.logical.relational.LogicalPlan lp = Util.buildLp( pig, query );
            new InputOutputFileValidator(lp, pig.getPigContext()).validate();
        } catch (FrontendException pve){
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
            assertEquals(true, TestHelper.bagContains(inpDB, t));
            ++size;
        }
        assertEquals(true, size==inpDB.size());
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
            
            ResourceFieldSchema bagfs = GenRandomData.getSmallTupDataBagFieldSchema();
            ResourceFieldSchema tuplefs = GenRandomData.getSmallTupleFieldSchema();
            
            t.append(flds[0].compareTo("")!=0 ? ps.getLoadCaster().bytesToBag(flds[0].getBytes(), bagfs) : null);
            t.append(flds[1].compareTo("")!=0 ? new DataByteArray(flds[1].getBytes()) : null);
            t.append(flds[2].compareTo("")!=0 ? ps.getLoadCaster().bytesToCharArray(flds[2].getBytes()) : null);
            t.append(flds[3].compareTo("")!=0 ? ps.getLoadCaster().bytesToDouble(flds[3].getBytes()) : null);
            t.append(flds[4].compareTo("")!=0 ? ps.getLoadCaster().bytesToFloat(flds[4].getBytes()) : null);
            t.append(flds[5].compareTo("")!=0 ? ps.getLoadCaster().bytesToInteger(flds[5].getBytes()) : null);
            t.append(flds[6].compareTo("")!=0 ? ps.getLoadCaster().bytesToLong(flds[6].getBytes()) : null);
            t.append(flds[7].compareTo("")!=0 ? ps.getLoadCaster().bytesToMap(flds[7].getBytes()) : null);
            t.append(flds[8].compareTo("")!=0 ? ps.getLoadCaster().bytesToTuple(flds[8].getBytes(), tuplefs) : null);
            t.append(flds[9].compareTo("")!=0 ? ps.getLoadCaster().bytesToBoolean(flds[9].getBytes()) : null);
            t.append(flds[10].compareTo("")!=0 ? ps.getLoadCaster().bytesToDateTime(flds[10].getBytes()) : null);
            assertEquals(true, TestHelper.bagContains(inpDB, t));
            ++size;
        }
        assertEquals(true, size==inpDB.size());
    }

    @Test
    public void testStoreComplexDataWithNull() throws Exception {
        Tuple inputTuple = GenRandomData.genRandSmallBagTextTupleWithNulls(new Random(), 10, 100);
        inpDB = DefaultBagFactory.getInstance().newDefaultBag();
        inpDB.add(inputTuple);
        storeAndCopyLocally(inpDB);
        PigStorage ps = new PigStorage("\t");
        int size = 0;
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
            
            t.append(flds[0].compareTo("")!=0 ? ps.getLoadCaster().bytesToBag(flds[0].getBytes(), bagfs) : null);
            t.append(flds[1].compareTo("")!=0 ? new DataByteArray(flds[1].getBytes()) : null);
            t.append(flds[2].compareTo("")!=0 ? ps.getLoadCaster().bytesToCharArray(flds[2].getBytes()) : null);
            t.append(flds[3].compareTo("")!=0 ? ps.getLoadCaster().bytesToDouble(flds[3].getBytes()) : null);
            t.append(flds[4].compareTo("")!=0 ? ps.getLoadCaster().bytesToFloat(flds[4].getBytes()) : null);
            t.append(flds[5].compareTo("")!=0 ? ps.getLoadCaster().bytesToInteger(flds[5].getBytes()) : null);
            t.append(flds[6].compareTo("")!=0 ? ps.getLoadCaster().bytesToLong(flds[6].getBytes()) : null);
            t.append(flds[7].compareTo("")!=0 ? ps.getLoadCaster().bytesToMap(flds[7].getBytes()) : null);
            t.append(flds[8].compareTo("")!=0 ? ps.getLoadCaster().bytesToTuple(flds[8].getBytes(), tuplefs) : null);
            t.append(flds[9].compareTo("")!=0 ? ps.getLoadCaster().bytesToBoolean(flds[9].getBytes()) : null);
            t.append(flds[10].compareTo("")!=0 ? ps.getLoadCaster().bytesToDateTime(flds[10].getBytes()) : null);
            t.append(flds[11].compareTo("")!=0 ? ps.getLoadCaster().bytesToCharArray(flds[10].getBytes()) : null);
            assertTrue(TestHelper.tupleEquals(inputTuple, t));
            ++size;
        }
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
            Assert.assertTrue("Checking binstorage getSchema output", Schema.equals( 
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
        String storeSchemaOutputFile = outputFileName + "_storeSchema_test";
        try {
            ExecType[] modes = new ExecType[] { ExecType.MAPREDUCE, ExecType.LOCAL};
            String[] inputData = new String[]{"hello\tworld", "bye\tworld"};
            
            String script = "a = load '"+ inputFileName + "' as (a0:chararray, a1:chararray);" +
            		"store a into '" + outputFileName + "' using " + 
            		DUMMY_STORE_CLASS_NAME + "();";
            
            for (ExecType execType : modes) {
                if(execType == ExecType.MAPREDUCE) {
                    ps = new PigServer(ExecType.MAPREDUCE, 
                            cluster.getProperties());
                    Util.deleteFile(ps.getPigContext(), inputFileName);
                    Util.deleteFile(ps.getPigContext(), outputFileName);
                    Util.deleteFile(ps.getPigContext(), storeSchemaOutputFile);
                } else {
                    Properties props = new Properties();                                          
                    props.setProperty(MapRedUtil.FILE_SYSTEM_NAME, "file:///");
                    ps = new PigServer(ExecType.LOCAL, props);
                    Util.deleteFile(ps.getPigContext(), inputFileName);
                    Util.deleteFile(ps.getPigContext(), outputFileName);
                    Util.deleteFile(ps.getPigContext(), storeSchemaOutputFile);
                }
                ps.setBatchOn();
                Util.createInputFile(ps.getPigContext(), 
                        inputFileName, inputData);
                Util.registerMultiLineQuery(ps, script);
                ps.executeBatch();
                assertEquals(
                        "Checking if file indicating that storeSchema was " +
                        "called exists in " + execType + " mode", true, 
                        Util.exists(ps.getPigContext(), storeSchemaOutputFile));
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Exception encountered - hence failing:" + e);
        } finally {
            Util.deleteFile(ps.getPigContext(), inputFileName);
            Util.deleteFile(ps.getPigContext(), outputFileName);
            Util.deleteFile(ps.getPigContext(), storeSchemaOutputFile);
        }
    }
    
    @Test
    public void testCleanupOnFailure() throws Exception {
        PigServer ps = null;
        String cleanupSuccessFile = outputFileName + "_cleanupOnFailure_succeeded";
        String cleanupFailFile = outputFileName + "_cleanupOnFailure_failed";
        try {
            ExecType[] modes = new ExecType[] { ExecType.LOCAL, ExecType.MAPREDUCE};
            String[] inputData = new String[]{"hello\tworld", "bye\tworld"};
            
            String script = "a = load '"+ inputFileName + "';" +
                    "store a into '" + outputFileName + "' using " + 
                    DUMMY_STORE_CLASS_NAME + "('true');";
            
            for (ExecType execType : modes) {
                if(execType == ExecType.MAPREDUCE) {
                    ps = new PigServer(ExecType.MAPREDUCE, 
                            cluster.getProperties());
                } else {
                    Properties props = new Properties();                                          
                    props.setProperty(MapRedUtil.FILE_SYSTEM_NAME, "file:///");
                    ps = new PigServer(ExecType.LOCAL, props);
                }
                Util.deleteFile(ps.getPigContext(), inputFileName);
                Util.deleteFile(ps.getPigContext(), outputFileName);
                Util.deleteFile(ps.getPigContext(), cleanupFailFile);
                Util.deleteFile(ps.getPigContext(), cleanupSuccessFile);
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
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Exception encountered - hence failing:" + e);
        } finally {
            Util.deleteFile(ps.getPigContext(), inputFileName);
            Util.deleteFile(ps.getPigContext(), outputFileName);
            Util.deleteFile(ps.getPigContext(), cleanupFailFile);
            Util.deleteFile(ps.getPigContext(), cleanupSuccessFile);
        }
    }
    
    
    @Test
    public void testCleanupOnFailureMultiStore() throws Exception {
        PigServer ps = null;
        String outputFileName1 = "/tmp/TestStore-output-" + new Random().nextLong() + ".txt";
        String outputFileName2 = "/tmp/TestStore-output-" + new Random().nextLong() + ".txt";
        String cleanupSuccessFile1 = outputFileName1 + "_cleanupOnFailure_succeeded1";
        String cleanupFailFile1 = outputFileName1 + "_cleanupOnFailure_failed1";
        String cleanupSuccessFile2 = outputFileName2 + "_cleanupOnFailure_succeeded2";
        String cleanupFailFile2 = outputFileName2 + "_cleanupOnFailure_failed2";
        
        try {
            ExecType[] modes = new ExecType[] { /*ExecType.LOCAL, */ExecType.MAPREDUCE};
            String[] inputData = new String[]{"hello\tworld", "bye\tworld"};
            
            // though the second store should
            // not cause a failure, the first one does and the result should be
            // that both stores are considered to have failed
            String script = "a = load '"+ inputFileName + "';" +
                    "store a into '" + outputFileName1 + "' using " + 
                    DUMMY_STORE_CLASS_NAME + "('true', '1');" +
                    "store a into '" + outputFileName2 + "' using " + 
                    DUMMY_STORE_CLASS_NAME + "('false', '2');"; 
            
            for (ExecType execType : modes) {
                if(execType == ExecType.MAPREDUCE) {
                    ps = new PigServer(ExecType.MAPREDUCE, 
                            cluster.getProperties());
                } else {
                    Properties props = new Properties();                                          
                    props.setProperty(MapRedUtil.FILE_SYSTEM_NAME, "file:///");
                    ps = new PigServer(ExecType.LOCAL, props);
                }
                Util.deleteFile(ps.getPigContext(), inputFileName);
                Util.deleteFile(ps.getPigContext(), outputFileName1);
                Util.deleteFile(ps.getPigContext(), outputFileName2);
                Util.deleteFile(ps.getPigContext(), cleanupFailFile1);
                Util.deleteFile(ps.getPigContext(), cleanupSuccessFile1);
                Util.deleteFile(ps.getPigContext(), cleanupFailFile2);
                Util.deleteFile(ps.getPigContext(), cleanupSuccessFile2);
                ps.setBatchOn();
                Util.createInputFile(ps.getPigContext(), 
                        inputFileName, inputData);
                Util.registerMultiLineQuery(ps, script);
                ps.executeBatch();
                assertEquals(
                        "Checking if file indicating that cleanupOnFailure failed " +
                        " does not exists in " + execType + " mode", false, 
                        Util.exists(ps.getPigContext(), cleanupFailFile1));
                assertEquals(
                        "Checking if file indicating that cleanupOnFailure failed " +
                        " does not exists in " + execType + " mode", false, 
                        Util.exists(ps.getPigContext(), cleanupFailFile2));
                assertEquals(
                        "Checking if file indicating that cleanupOnFailure was " +
                        "successfully called exists in " + execType + " mode", true, 
                        Util.exists(ps.getPigContext(), cleanupSuccessFile1));
                assertEquals(
                        "Checking if file indicating that cleanupOnFailure was " +
                        "successfully called exists in " + execType + " mode", true, 
                        Util.exists(ps.getPigContext(), cleanupSuccessFile2));
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Exception encountered - hence failing:" + e);
        } finally {
            Util.deleteFile(ps.getPigContext(), inputFileName);
            Util.deleteFile(ps.getPigContext(), outputFileName1);
            Util.deleteFile(ps.getPigContext(), outputFileName2);
            Util.deleteFile(ps.getPigContext(), cleanupFailFile1);
            Util.deleteFile(ps.getPigContext(), cleanupSuccessFile1);
            Util.deleteFile(ps.getPigContext(), cleanupFailFile2);
            Util.deleteFile(ps.getPigContext(), cleanupSuccessFile2);
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
        String[] files = new String[] { inputFileName, 
                outputFileName + "_1", outputFileName + "_2", outputFileName + "_3"};
        try {
            ExecType[] modes = new ExecType[] { ExecType.LOCAL, ExecType.MAPREDUCE};
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
                        // since we will be switching between map red and local modes
                        // we will need to make sure filelocalizer is reset before each
                        // run.
                        FileLocalizer.setInitialized(false);
                        if(execType == ExecType.MAPREDUCE) {
                            ps = new PigServer(ExecType.MAPREDUCE, 
                                    cluster.getProperties());
                        } else {
                            Properties props = new Properties();                                          
                            props.setProperty(MapRedUtil.FILE_SYSTEM_NAME, "file:///");
                            ps = new PigServer(ExecType.LOCAL, props);
                        }
                        ps.getPigContext().getProperties().setProperty(
                                MapReduceLauncher.SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, 
                                Boolean.toString(isPropertySet));
                        cleanupFiles(ps, files);
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
            cleanupFiles(ps, files);
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
        String[] files = new String[] { inputFileName, 
                outputFileName + "_1", outputFileName + "_2", outputFileName + "_3"};
        try {
            ExecType[] modes = new ExecType[] { ExecType.LOCAL, ExecType.MAPREDUCE};
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
                        // since we will be switching between map red and local modes
                        // we will need to make sure filelocalizer is reset before each
                        // run.
                        FileLocalizer.setInitialized(false);
                        if(execType == ExecType.MAPREDUCE) {
                            // since the job is guaranteed to fail, let's set 
                            // number of retries to 1.
                            Properties props = cluster.getProperties();
                            props.setProperty(MAP_MAX_ATTEMPTS, "1");
                            ps = new PigServer(ExecType.MAPREDUCE, props);
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
                        cleanupFiles(ps, files);
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
            cleanupFiles(ps, files);
        }
    }

    // A UDF which always throws an Exception so that the job can fail
    public static class FailUDF extends EvalFunc<String> {

        @Override
        public String exec(Tuple input) throws IOException {
            throw new IOException("FailUDFException");
        }
        
    }
    private void cleanupFiles(PigServer ps, String... files) throws IOException {
        for(String file:files) {
            Util.deleteFile(ps.getPigContext(), file);
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

        @Override
        public void storeSchema(ResourceSchema schema, String location,
                Job job) throws IOException {
            FileSystem fs = FileSystem.get(job.getConfiguration());
            // verify that output is available prior to storeSchema call
            Path resultPath = new Path(location, "part-m-00000");
            if (!fs.exists(resultPath)) {
	            FileStatus[] listing = fs.listStatus(new Path(location));
	            for (FileStatus fstat : listing) {
	            	System.err.println(fstat.getPath());
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
        pc.getProperties().setProperty("opt.multiquery",""+true);

        DataStorage dfs = pc.getDfs();
        dfs.setActiveContainer(dfs.asContainer("/tmp"));
        Map<String, String> fileNameMap = new HashMap<String, String>();
        
        QueryParserDriver builder = new QueryParserDriver(pc, "Test-Store", fileNameMap);
        
        String query = "a = load 'foo';" + "store a into '"+orig+"';";
        LogicalPlan lp = builder.parse(query);

        Assert.assertTrue(lp.size()>1);
        Operator op = lp.getSinks().get(0);
        
        Assert.assertTrue(op instanceof LOStore);
        LOStore store = (LOStore)op;

        String p = store.getFileSpec().getFileName();
        p = p.replaceAll("hdfs://[0-9a-zA-Z:\\.]*/","/");
        
        if (isTmp) {
            Assert.assertTrue(p.matches("/tmp.*"));
        } else {
            Assert.assertEquals(expected, p);
        }
    }
}
