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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.pig.PigRunner;
import org.apache.pig.PigServer;
import org.apache.pig.impl.PigContext;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.mapreduce.MRJobStats;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.*;

/**
 * This class tests whether the number of reducers is correctly set for queries
 * involving a sample job. Note the sample job should use the actual #reducers
 * of the following order-by or skew-join job to generate the partition file,
 * and the challenge is that whether we can get the actual #reducer of the
 * order-by/skew-join job in advance.
 */
@RunWith(JUnit4.class)
public class TestNumberOfReducers {

    private static final String LOCAL_INPUT_FILE = "test/org/apache/pig/test/data/passwd";
    private static final String HDFS_INPUT_FILE = "passwd";
    private static final String OUTPUT_FILE = "output";
    private static final String PIG_FILE = "test.pig";

    static PigContext pc;
    static PigServer pigServer;
    private static MiniCluster cluster;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        cluster = MiniCluster.buildCluster();

        Util.copyFromLocalToCluster(cluster, LOCAL_INPUT_FILE, HDFS_INPUT_FILE);
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        cluster.shutDown();
    }

    /**
     * Specifying bytes_per_reducer, default_parallel and parallel, verifying
     * whether it uses the actual #reducers as actual_parallel.
     * 
     * Note the input file size is 555B.
     * 
     */
    private void verifyOrderBy(int bytes_per_reducer, int default_parallel,
            int parallel, int actual_parallel) throws IOException {
        PrintWriter w = new PrintWriter(new FileWriter(PIG_FILE));

        // the file is 555B
        w.println("SET pig.exec.reducers.bytes.per.reducer " + bytes_per_reducer + ";");
        
        w.println("SET default_parallel " + default_parallel + ";");
        w.println("A = load '" + HDFS_INPUT_FILE + "';");
        w.print("B = order A by $0 ");
        if (parallel > 0) 
            w.print("parallel " + parallel);
        w.println(";");
        w.println("store B into '" + OUTPUT_FILE + "';");
        w.close();

        doTest(bytes_per_reducer, default_parallel, parallel, actual_parallel);
    }
    
    /**
     * Specifying bytes_per_reducer, default_parallel and parallel, verifying
     * whether it uses the actual #reducers as actual_parallel.
     * 
     * Note the input file size is 555B.
     * 
     */
    private void verifySkewJoin(int bytes_per_reducer, int default_parallel,
            int parallel, int actual_parallel) throws IOException {
        PrintWriter w = new PrintWriter(new FileWriter(PIG_FILE));

        // the file is 555B
        w.println("SET pig.exec.reducers.bytes.per.reducer " + bytes_per_reducer + ";");
        w.println("SET default_parallel " + default_parallel + ";");
        w.println("A1 = load '" + HDFS_INPUT_FILE + "';");
        w.println("A2 = load '" + HDFS_INPUT_FILE + "';");
        w.print("B = join A1 by $0,  A2 by $0 using 'skewed' ");
        if (parallel > 0) 
            w.print("parallel " + parallel);
        w.println(";");
        w.println("store B into '" + OUTPUT_FILE + "';");
        w.close();

        doTest(bytes_per_reducer, default_parallel, parallel, actual_parallel);
    }

    private void doTest(double bytes_per_reducer, int default_parallel,
                        int parallel, int actual_parallel) throws IOException {
        try {
            String[] args = { PIG_FILE };
            PigStats stats = PigRunner.run(args, null);

            assertTrue(stats.isSuccessful());

            // get the skew-join job stat
            MRJobStats js = (MRJobStats) stats.getJobGraph().getSinks().get(0);
            assertEquals(actual_parallel, js.getNumberReduces());

            // estimation should only kick in if parallel and default_parallel are not set
            long estimatedReducers = -1;
            if (parallel < 1 && default_parallel < 1) {
                double fileSize = (double)(new File("test/org/apache/pig/test/data/passwd").length());
                int inputFiles = js.getInputs().size();
                estimatedReducers = Math.min((long)Math.ceil(fileSize/(double)bytes_per_reducer) * inputFiles, 999);
            }

            Util.assertParallelValues(default_parallel, parallel, estimatedReducers,
                    actual_parallel, js.getInputs().get(0).getConf());

        } catch (Exception e) {
            assertNull("Exception thrown during verifySkewJoin", e);
        } finally {
            new File(PIG_FILE).delete();
            Util.deleteFile(cluster, OUTPUT_FILE);
        }
    }

    /**
     * For 550B file and 300B per reducer, it estimates 2 #reducers.
     * @throws Exception
     */
    @Test
    public void testOrderByEstimate() throws Exception{
        verifyOrderBy(300, -1, -1, 2);
    }
    
    /**
     * For two 550B files and 600B per reducer, it estimates 2 #reducers.
     * @throws Exception
     */
    @Test
    public void testSkewJoinEstimate() throws Exception{
        verifySkewJoin(600, -1, -1, 2);
    }
    
    
    /**
     * Pig will estimate 2 reducers but we specify parallel 1, so it should use
     * 1 reducer.
     * 
     * @throws Exception
     */
    @Test
    public void testOrderByEstimate2Parallel1() throws Exception {
        verifyOrderBy(300, -1, 1, 1);
    }
    
    /**
     * Pig will estimate 2 reducers but we specify parallel 1, so it should use
     * 1 reducer.
     * 
     * @throws Exception
     */
    @Test
    public void testSkewJoinEstimate2Parallel1() throws Exception {
        verifySkewJoin(600, -1, 1, 1);
    }

    /**
     * Pig will estimate 2 reducers but we specify default parallel 1, so it
     * should use 1 reducer.
     * 
     * @throws Exception
     */
    @Test
    public void testOrderByEstimate2Default1() throws Exception {
        verifyOrderBy(300,  1, -1, 1);
    }
    
    /**
     * Pig will estimate 2 reducers but we specify default parallel 1, so it
     * should use 1 reducer.
     * 
     * @throws Exception
     */
    @Test
    public void testSkewJoinEstimate2Default1() throws Exception {
        verifySkewJoin(600, 1, -1, 1);
    }

    /**
     * Pig will estimate 2 reducers but we specify parallel 4, so it should use
     * 4 reducer.
     * 
     * TODO we need verify that the sample job generates 4 partitions instead of
     * 2.
     * 
     * @throws Exception
     */
    @Test
    public void testOrderByEstimate2Parallel4() throws Exception {
        verifyOrderBy(300, -1, 4, 4);
    }
    
    /**
     * Pig will estimate 2 reducers but we specify parallel 4, so it should use
     * 4 reducer.
     * 
     * TODO we need verify that the sample job generates 4 partitions instead of
     * 2.
     * 
     * @throws Exception
     */
    @Test
    public void testSkewJoinEstimate2Parallel4() throws Exception {
        verifySkewJoin(600, -1, 4, 4);
    }

    /**
     * Pig will estimate 6 reducers but we specify default parallel 2, so it
     * should use 2 reducer. Also, the sampler should generate 2 partitions,
     * instead of 6, otherwise the next job will fail.
     * 
     * @throws Exception
     */
    @Test
    public void testOrderByEstimate6Default2() throws Exception {
        verifyOrderBy(100, 2, -1, 2);
    }
    
    /**
     * Pig will estimate 6 reducers but we specify default parallel 2, so it
     * should use 2 reducer. Also, the sampler should generate 2 partitions,
     * instead of 6, otherwise the next job will fail.
     * 
     * @throws Exception
     */
    @Test
    public void testSkewJoinEstimate6Default2() throws Exception {
        verifySkewJoin(200, 2, -1, 2);
    }
}
