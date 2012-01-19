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
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.backend.executionengine.ExecJob.JOB_STATUS;
import org.apache.pig.data.Tuple;
import org.apache.pig.tools.pigstats.PigStats;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestNativeMapReduce  {

    //NOTE:
    // Testing NativeMapReduce in LOCAL mode from unit test setup is not easy.
    // (ie current WordCount.jar does not work as-is).
    // the presence of build/classes/hadoop-site.xml created by MiniCluster
    // in the class path makes the MR job try to contact the MiniCluster.
    // if the MiniCluster shutdown is changed to delete the file, the other
    // test cases fail because the file in classpath does not exist

    // the jar has been created using the source at
    // http://svn.apache.org/repos/asf/hadoop/mapreduce/trunk/src/examples/org/apache/hadoop/examples/WordCount.java:816822
    private String jarFileName = "test//org/apache/pig/test/data/TestWordCount.jar";
    private String exp_msg_prefix = "Check if expected results contains: ";
    final static String INPUT_FILE = "TestNMapReduceInputFile";
    /**
     *stop word file - used to test distributed cache usage, words in this
     * file if specified will be skipped by the wordcount udf
     */
    final static String STOPWORD_FILE = "TestNMapReduceStopwFile";
    static MiniCluster cluster = MiniCluster.buildCluster();
    private PigServer pigServer = null;

    /**
     * TODO - Move to runtime jar creation approach
    private void createWordCountJar() {
    }*/

    @BeforeClass
    public static void oneTimeSetup() throws Exception{
        String[] input = {
                "one",
                "two",
                "three",
                "three",
                "two",
                "three"
        };
        //for stop word file
        String[] stopw = {
                "one"
        };

        Util.createInputFile(cluster, INPUT_FILE, input);
        Util.createLocalInputFile(STOPWORD_FILE, stopw);
    }

    //  createWordCountJar(){
    //  // its a manual process
    //  javac -cp build/ivy/lib/Pig/hadoop-core-0.20.2.jar:build/ivy/lib/Pig/commons-cli-1.2.jar test/org/apache/pig/test/utils/WordCount.java
    //  cd test/
    //  jar -cf WordCount.jar org/apache/pig/test/utils/WordCount*class
    //  mv WordCount.jar org/apache/pig/test/data/TestWordCount.jar
    //
    //
    //}

    @Before
    public void setUp() throws Exception{
        pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());

        //createWordCountJar();
    }



    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        Util.deleteFile(cluster, INPUT_FILE);
        new File(STOPWORD_FILE).delete();
        cluster.shutDown();
    }


    // See PIG-506
    @Test
    public void testNativeMRJobSimple() throws Exception{
        try{
            Collection<String> results = new HashSet<String>();
            results.add("(two,2)");
            results.add("(three,3)");

            pigServer.setBatchOn();
            pigServer.registerQuery("A = load '" + INPUT_FILE + "';");


            //also test distributed cache using the stopwords file for udf
            pigServer.registerQuery("B = mapreduce '" + jarFileName + "' " +
                    "Store A into 'table_testNativeMRJobSimple_input' "+
                    "Load 'table_testNativeMRJobSimple_output' "+
            "`org.apache.pig.test.utils.WordCount " +
            " -Dmapred.child.java.opts='-Xmx1536m -Xms128m' " +
            " -files " + STOPWORD_FILE +
            " table_testNativeMRJobSimple_input table_testNativeMRJobSimple_output " +
            STOPWORD_FILE + "`;");
            pigServer.registerQuery("Store B into 'table_testNativeMRJobSimpleDir';");
            List<ExecJob> execJobs = pigServer.executeBatch();

            assertEquals("num of jobs", execJobs.size(), 1);
            boolean foundNativeFeature = false;
            for(ExecJob job : execJobs){
                assertEquals("job status", job.getStatus(),JOB_STATUS.COMPLETED);
                if(job.getStatistics().getFeatures().contains("NATIVE")){
                    foundNativeFeature = true;
                }
            }
            assertTrue("foundNativeFeature", foundNativeFeature);

            // Check the output
            pigServer.registerQuery("C = load 'table_testNativeMRJobSimpleDir';");

            Iterator<Tuple> iter = pigServer.openIterator("C");
            Tuple t;

            assertTrue("iter.hasNext()",iter.hasNext());
            t = iter.next();
            assertTrue(exp_msg_prefix + t, results.contains(t.toString()));

            assertTrue("iter.hasNext()",iter.hasNext());
            t = iter.next();
            assertTrue(exp_msg_prefix + t, results.contains(t.toString()));

            assertFalse(iter.hasNext());

            // We have to manually delete intermediate mapreduce files
            Util.deleteFile(cluster,"table_testNativeMRJobSimple_input");
            Util.deleteFile(cluster,"table_testNativeMRJobSimple_output");

            // check in interactive mode
            iter = pigServer.openIterator("B");

            assertTrue("iter.hasNext()",iter.hasNext());
            t = iter.next();
            assertTrue(exp_msg_prefix + t, results.contains(t.toString()));

            assertTrue("iter.hasNext()",iter.hasNext());
            t = iter.next();
            assertTrue(exp_msg_prefix + t, results.contains(t.toString()));

            assertFalse(iter.hasNext());
        }
        finally{
            // We have to manually delete intermediate mapreduce files
            Util.deleteFile(cluster,"table_testNativeMRJobSimple_input");
            Util.deleteFile(cluster,"table_testNativeMRJobSimple_output");
            Util.deleteFile(cluster,"table_testNativeMRJobSimpleDir");
        }
    }


    @Test
    public void testNativeMRJobSimpleFailure() throws Exception{
        try{
            //test if correct return code is obtained when query fails
            // the native MR is writing to an exisiting and should fail

            Collection<String> results = new HashSet<String>();
            results.add("(one,1)");
            results.add("(two,2)");
            results.add("(three,3)");

            pigServer.setBatchOn();
            pigServer.registerQuery("A = load '" + INPUT_FILE + "';");
            pigServer.registerQuery("B = mapreduce '" + jarFileName + "' " +
                    "Store A into 'table_testNativeMRJobSimple_input' "+
                    "Load 'table_testNativeMRJobSimple_output' "+
            "`org.apache.pig.test.utils.WordCount table_testNativeMRJobSimple_input " + INPUT_FILE + "`;");
            pigServer.registerQuery("Store B into 'table_testNativeMRJobSimpleDir';");
            pigServer.executeBatch();

            assertTrue("job failed", PigStats.get().getReturnCode() != 0);

        }
        finally{
            // We have to manually delete intermediate mapreduce files
            Util.deleteFile(cluster, "table_testNativeMRJobSimple_input");
            Util.deleteFile(cluster, "table_testNativeMRJobSimpleDir");
        }
    }


    // See PIG-506
    @Test
    public void testNativeMRJobMultiStoreOnPred() throws Exception{
        try{

            Collection<String> results = new HashSet<String>();
            results.add("(one,1)");
            results.add("(two,2)");
            results.add("(three,3)");

            pigServer.setBatchOn();
            pigServer.registerQuery("A = load '" + INPUT_FILE + "';");
            pigServer.registerQuery("Store A into 'testNativeMRJobMultiStoreOnPredTemp';");
            pigServer.registerQuery("B = mapreduce '" + jarFileName + "' " +
                    "Store A into 'table_testNativeMRJobMultiStoreOnPred_input' "+
                    "Load 'table_testNativeMRJobMultiStoreOnPred_output' "+
            "`org.apache.pig.test.utils.WordCount table_testNativeMRJobMultiStoreOnPred_input table_testNativeMRJobMultiStoreOnPred_output`;");
            pigServer.registerQuery("Store B into 'table_testNativeMRJobMultiStoreOnPredDir';");
            pigServer.executeBatch();

            // Check the output
            pigServer.registerQuery("C = load 'table_testNativeMRJobMultiStoreOnPredDir';");

            Iterator<Tuple> iter = pigServer.openIterator("C");
            Tuple t;

            assertTrue("iter.hasNext()",iter.hasNext());
            t = iter.next();
            assertTrue(exp_msg_prefix + t, results.contains(t.toString()));

            assertTrue("iter.hasNext()",iter.hasNext());
            t = iter.next();
            assertTrue(exp_msg_prefix + t, results.contains(t.toString()));

            assertTrue("iter.hasNext()",iter.hasNext());
            t = iter.next();
            assertTrue(exp_msg_prefix + t, results.contains(t.toString()));

            assertFalse(iter.hasNext());

            Util.deleteFile(cluster,"table_testNativeMRJobMultiStoreOnPred_input");
            Util.deleteFile(cluster,"table_testNativeMRJobMultiStoreOnPred_output");

            // check in interactive mode
            iter = pigServer.openIterator("B");

            assertTrue("iter.hasNext()",iter.hasNext());
            t = iter.next();
            assertTrue(exp_msg_prefix + t, results.contains(t.toString()));

            assertTrue("iter.hasNext()",iter.hasNext());
            t = iter.next();
            assertTrue(exp_msg_prefix + t, results.contains(t.toString()));

            assertTrue("iter.hasNext()",iter.hasNext());
            t = iter.next();
            assertTrue(exp_msg_prefix + t, results.contains(t.toString()));

            assertFalse(iter.hasNext());
        }
        finally{
            Util.deleteFile(cluster,"testNativeMRJobMultiStoreOnPredTemp");
            Util.deleteFile(cluster,"table_testNativeMRJobMultiStoreOnPred_input");
            Util.deleteFile(cluster,"table_testNativeMRJobMultiStoreOnPred_output");
            Util.deleteFile(cluster,"table_testNativeMRJobMultiStoreOnPredDir");
        }
    }

    // See PIG-506
    @Test
    public void testNativeMRJobMultiQueryOpt() throws Exception{
        try{
            Collection<String> results = new HashSet<String>();
            results.add("(one,1)");
            results.add("(two,2)");
            results.add("(three,3)");

            pigServer.registerQuery("A = load '" + INPUT_FILE + "';");
            pigServer.registerQuery("B = mapreduce '" + jarFileName + "' " +
                    "Store A into 'table_testNativeMRJobMultiQueryOpt_inputB' "+
                    "Load 'table_testNativeMRJobMultiQueryOpt_outputB' "+
            "`org.apache.pig.test.utils.WordCount table_testNativeMRJobMultiQueryOpt_inputB table_testNativeMRJobMultiQueryOpt_outputB`;");
            pigServer.registerQuery("C = mapreduce '" + jarFileName + "' " +
                    "Store A into 'table_testNativeMRJobMultiQueryOpt_inputC' "+
                    "Load 'table_testNativeMRJobMultiQueryOpt_outputC' "+
            "`org.apache.pig.test.utils.WordCount table_testNativeMRJobMultiQueryOpt_inputC table_testNativeMRJobMultiQueryOpt_outputC`;");

            Iterator<Tuple> iter = pigServer.openIterator("C");
            Tuple t;

            assertTrue("iter.hasNext()",iter.hasNext());
            t = iter.next();
            assertTrue(exp_msg_prefix + t, results.contains(t.toString()));

            assertTrue("iter.hasNext()",iter.hasNext());
            t = iter.next();
            assertTrue(exp_msg_prefix + t, results.contains(t.toString()));

            assertTrue("iter.hasNext()",iter.hasNext());
            t = iter.next();
            assertTrue(exp_msg_prefix + t, results.contains(t.toString()));

            assertFalse(iter.hasNext());

            iter = pigServer.openIterator("B");

            assertTrue("iter.hasNext()",iter.hasNext());
            t = iter.next();
            assertTrue(exp_msg_prefix + t, results.contains(t.toString()));

            assertTrue("iter.hasNext()",iter.hasNext());
            t = iter.next();
            assertTrue(exp_msg_prefix + t, results.contains(t.toString()));

            assertTrue("iter.hasNext()",iter.hasNext());
            t = iter.next();
            assertTrue(exp_msg_prefix + t, results.contains(t.toString()));

            assertFalse(iter.hasNext());
        }finally{
            Util.deleteFile(cluster,"table_testNativeMRJobMultiQueryOpt_inputB");
            Util.deleteFile(cluster,"table_testNativeMRJobMultiQueryOpt_outputB");
            Util.deleteFile(cluster,"table_testNativeMRJobMultiQueryOpt_inputC");
            Util.deleteFile(cluster,"table_testNativeMRJobMultiQueryOpt_outputC");
        }
    }

    // See PIG-506
    @Test
    public void testNativeMRJobTypeCastInserter() throws Exception{
        try{
            Collection<String> results = new HashSet<String>();
            results.add("(2)");
            results.add("(3)");
            results.add("(4)");

            pigServer.registerQuery("A = load '" + INPUT_FILE + "';");
            pigServer.registerQuery("B = mapreduce '" + jarFileName + "' " +
                    "Store A into 'table_testNativeMRJobTypeCastInserter_input' "+
                    "Load 'table_testNativeMRJobTypeCastInserter_output' as (name:chararray, count: int)"+
            "`org.apache.pig.test.utils.WordCount table_testNativeMRJobTypeCastInserter_input table_testNativeMRJobTypeCastInserter_output`;");
            pigServer.registerQuery("C = foreach B generate count+1;");

            Iterator<Tuple> iter = pigServer.openIterator("C");
            Tuple t;

            assertTrue("iter.hasNext()",iter.hasNext());
            t = iter.next();
            assertTrue(exp_msg_prefix + t, results.contains(t.toString()));

            assertTrue("iter.hasNext()",iter.hasNext());
            t = iter.next();
            assertTrue(exp_msg_prefix + t, results.contains(t.toString()));

            assertTrue("iter.hasNext()",iter.hasNext());
            t = iter.next();
            assertTrue(exp_msg_prefix + t, results.contains(t.toString()));

            assertFalse(iter.hasNext());
        }finally{
            Util.deleteFile(cluster,"table_testNativeMRJobTypeCastInserter_input");
            Util.deleteFile(cluster,"table_testNativeMRJobTypeCastInserter_output");
        }
    }

}
