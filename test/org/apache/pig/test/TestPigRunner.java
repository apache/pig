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
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.pig.ExecType;
import org.apache.pig.PigRunner;
import org.apache.pig.PigRunner.ReturnCode;
import org.apache.pig.experimental.plan.Operator;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.OutputStats;
import org.apache.pig.tools.pigstats.PigProgressNotificationListener;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.PigStatsUtil;
import org.junit.AfterClass;
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

    @Test
    public void simpleTest() throws Exception {
        PrintWriter w = new PrintWriter(new FileWriter(PIG_FILE));
        w.println("A = load '" + INPUT_FILE + "' as (a0:int, a1:int, a2:int);");
        w.println("B = group A by a0;");
        w.println("C = foreach B generate group, COUNT(A);");
        w.println("store C into '" + OUTPUT_FILE + "';");
        w.close();
        
        try {
            String[] args = { PIG_FILE };
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
            assertTrue(stats.getJobGraph().size() == 3);
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
            assertEquals(5, stats.getRecordWritten());
            assertEquals(28, stats.getBytesWritten());
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
            assertEquals(4, stats.getRecordWritten());           
            assertEquals(18, stats.getBytesWritten());
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
        assertTrue(stats.getErrorCode() == 1000);
        assertEquals("Error during parsing. Invalid alias: a in {a0: int,a1: int,a2: int}", 
                stats.getErrorMessage());
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
        String name = PigStatsUtil.getMultiInputsCounterName(s);
        assertEquals(PigStatsUtil.MULTI_INPUTS_RECORD_COUNTER + "batchtest", name);
    }
    
    @Test
    public void classLoaderTest() throws Exception {
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

    private static class TestNotificationListener implements PigProgressNotificationListener {
        
        private int numJobsToLaunch = 0;
        private int numJobsSubmitted = 0;
        private int numJobStarted = 0;
        private int numJobFinished = 0;
        
        @Override
        public void launchStartedNotification(int numJobsToLaunch) {
            System.out.println("++++ numJobsToLaunch: " + numJobsToLaunch);  
            this.numJobsToLaunch = numJobsToLaunch;
        }

        @Override
        public void jobFailedNotification(JobStats jobStats) {
            System.out.println("++++ job failed: " + jobStats.getJobId());           
        }

        @Override
        public void jobFinishedNotification(JobStats jobStats) {
            System.out.println("++++ job finished: " + jobStats.getJobId());  
            numJobFinished++;            
        }

        @Override
        public void jobStartedNotification(String assignedJobId) {
            System.out.println("++++ job started: " + assignedJobId);   
            numJobStarted++;
        }

        @Override
        public void jobsSubmittedNotification(int numJobsSubmitted) {
            System.out.println("++++ jobs submitted: " + numJobsSubmitted);
            this.numJobsSubmitted += numJobsSubmitted;
        }

        @Override
        public void launchCompletedNotification(int numJobsSucceeded) {
            System.out.println("++++ numJobsSucceeded: " + numJobsSucceeded);   
            System.out.println("");
            assertEquals(this.numJobsToLaunch, numJobsSucceeded);
            assertEquals(this.numJobsSubmitted, numJobsSucceeded);
            assertEquals(this.numJobStarted, numJobsSucceeded);
            assertEquals(this.numJobFinished, numJobsSucceeded);
        }

        @Override
        public void outputCompletedNotification(OutputStats outputStats) {
            System.out.println("++++ output done: " + outputStats.getLocation());
        }

        @Override
        public void progressUpdatedNotification(int progress) {
            System.out.println("++++ progress: " + progress + "%");           
        }
        
    }

}
