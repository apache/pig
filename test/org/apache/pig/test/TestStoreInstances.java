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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.StoreFunc;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.backend.executionengine.ExecJob.JOB_STATUS;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.parser.ParserException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 * Test to ensure that same instance of store func is used for multiple
 *  backend tasks. This enables sharing of information between putNext and
 *  output committer
 * 
 */
public class TestStoreInstances  {
    static MiniCluster cluster ;
    private static final String INP_FILE_2NUMS = "TestStoreInstances";

    @Before
    public void setUp() throws Exception {
        FileLocalizer.setInitialized(false);
    }


    @After
    public void tearDown() throws Exception {
    }

    @BeforeClass
    public static void oneTimeSetup() throws IOException, Exception {
        cluster = MiniCluster.buildCluster();

        String[] input = {
                "1\t5",
                "2\t10",
                "3\t20"
        };

        Util.createInputFile(cluster, INP_FILE_2NUMS, input);
        Util.createLocalInputFile(INP_FILE_2NUMS, input);
    }

    private static final String CHECK_INSTANCE_STORE_FUNC
    = "org.apache.pig.test.TestStoreInstances\\$STFuncCheckInstances";

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        new File(INP_FILE_2NUMS).delete();
        cluster.shutDown();
    }

    /**
     * Test that putnext is able to communicate to outputcommitter
     * @throws IOException
     * @throws ParseException
     */
    @Test
    public void testBackendStoreCommunication() throws IOException, ParserException {
        ExecType[] execTypes = { ExecType.MAPREDUCE, ExecType.LOCAL};
        PigServer pig = null;
        for(ExecType execType : execTypes){
            System.err.println("Starting test mode " + execType);
            if(execType == ExecType.MAPREDUCE) {
                pig = new PigServer(ExecType.MAPREDUCE, 
                        cluster.getProperties());
            }else{
                pig = new PigServer(ExecType.LOCAL);
            }
            final String outFile = "TestStoreInst1";
            Util.deleteFile(pig.getPigContext(), outFile);
            pig.setBatchOn();
            String query =
                "  l1 = load '" + INP_FILE_2NUMS + "' as (i : int, j : int);" +
                " store l1 into '" + outFile + "' using " + CHECK_INSTANCE_STORE_FUNC + 
                ";";
            Util.registerMultiLineQuery(pig, query);
            List<ExecJob> execJobs = pig.executeBatch();
            assertEquals("num jobs", 1, execJobs.size());
            assertEquals("status ", JOB_STATUS.COMPLETED, execJobs.get(0).getStatus());
        }
            
    }


    /**
     * Store func that records output rows in a variable
     */
    public static class STFuncCheckInstances extends StoreFunc {

        private ArrayList<Tuple> outRows;

        public STFuncCheckInstances(){
            super();
            this.outRows = new ArrayList<Tuple>();
        }

        @Override
        public OutputFormat getOutputFormat() throws IOException {
            return new OutFormatCheckInstances(outRows);
        }

        @Override
        public void prepareToWrite(RecordWriter writer) throws IOException {
            // TODO Auto-generated method stub

        }

        @Override
        public void putNext(Tuple t) throws IOException {
            outRows.add(t);

        }


        @Override
        public void setStoreLocation(String location, Job job)
                throws IOException {
            Configuration conf = job.getConfiguration();
            conf.set("mapred.output.dir", location);
            
        }


    }

    /**
     * OutputFormat class for the store func
     */
    public static class OutFormatCheckInstances extends TextOutputFormat {

        private ArrayList<Tuple> outRows;

        public OutFormatCheckInstances(ArrayList<Tuple> outRows) {
            super();
            this.outRows = outRows;
        }

        @Override
        public OutputCommitter getOutputCommitter(TaskAttemptContext arg0)
        throws IOException {
            return new OutputCommitterTestInstances(outRows, arg0);
        }

    }

    /**
     * OutputCommitter class that checks number of rows written by store func
     */
    public static class OutputCommitterTestInstances extends FileOutputCommitter {


        private ArrayList<Tuple> outRows;

        public OutputCommitterTestInstances(ArrayList<Tuple> outRows,
                TaskAttemptContext taskAttemptCtx) throws IOException {
            super(new Path(taskAttemptCtx.getConfiguration().get("mapred.output.dir")), taskAttemptCtx);
            this.outRows = outRows;
        }

   
        @Override
        public void commitTask(TaskAttemptContext arg0) {
            System.err.println("OutputCommitterTestInstances commitTask called");
            assertTrue("Number of output rows > 0 ",outRows.size() > 0);
        }

        @Override
        public boolean needsTaskCommit(TaskAttemptContext arg0)
        throws IOException {
            return true;
        }


    }


}
