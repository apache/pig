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
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pig.PigRunner;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.mapreduce.MRJobStats;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestEmptyInputDir {

    private static MiniGenericCluster cluster = MiniGenericCluster.buildCluster();
    private static final String EMPTY_DIR = "emptydir";
    private static final String INPUT_FILE = "input";
    private static final String OUTPUT_FILE = "output";
    private static final String PIG_FILE = "test.pig";


    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        FileSystem fs = cluster.getFileSystem();
        if (!fs.mkdirs(new Path(EMPTY_DIR))) {
            throw new Exception("failed to create empty dir");
        }
        PrintWriter w = new PrintWriter(new FileWriter(INPUT_FILE));
        w.println("1\t2");
        w.println("5\t3");
        w.close();
        Util.copyFromLocalToCluster(cluster, INPUT_FILE, INPUT_FILE);
        new File(INPUT_FILE).delete();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        cluster.shutDown();
    }

    @Test
    public void testGroupBy() throws Exception {
        PrintWriter w = new PrintWriter(new FileWriter(PIG_FILE));
        w.println("A = load '" + EMPTY_DIR + "';");
        w.println("B = group A by $0;");
        w.println("store B into '" + OUTPUT_FILE + "';");
        w.close();

        try {
            String[] args = { "-x", cluster.getExecType().name(), PIG_FILE, };
            PigStats stats = PigRunner.run(args, null);

            assertTrue(stats.isSuccessful());

            // This assert fails on 205 due to MAPREDUCE-3606
            if (Util.isMapredExecType(cluster.getExecType())
                    && !Util.isHadoop205() && !Util.isHadoop1_x()) {
                MRJobStats js = (MRJobStats) stats.getJobGraph().getSources().get(0);
                assertEquals(0, js.getNumberMaps());
            }

            assertEmptyOutputFile();
        } finally {
            new File(PIG_FILE).delete();
            Util.deleteFile(cluster, OUTPUT_FILE);
        }
    }

    @Test
    public void testSkewedJoin() throws Exception {
        PrintWriter w = new PrintWriter(new FileWriter(PIG_FILE));
        w.println("A = load '" + INPUT_FILE + "';");
        w.println("B = load '" + EMPTY_DIR + "';");
        w.println("C = join B by $0, A by $0 using 'skewed';");
        w.println("store C into '" + OUTPUT_FILE + "';");
        w.close();

        try {
            String[] args = { "-x", cluster.getExecType().name(), PIG_FILE, };
            PigStats stats = PigRunner.run(args, null);

            assertTrue(stats.isSuccessful());

            // This assert fails on 205 due to MAPREDUCE-3606
            if (Util.isMapredExecType(cluster.getExecType())
                    && !Util.isHadoop205() && !Util.isHadoop1_x()) {
                // the sampler job has zero maps
                MRJobStats js = (MRJobStats) stats.getJobGraph().getSources().get(0);
                assertEquals(0, js.getNumberMaps());
            }

            assertEmptyOutputFile();
        } finally {
            new File(PIG_FILE).delete();
            Util.deleteFile(cluster, OUTPUT_FILE);
        }
    }

    @Test
    public void testMergeJoin() throws Exception {
        PrintWriter w = new PrintWriter(new FileWriter(PIG_FILE));
        w.println("A = load '" + INPUT_FILE + "';");
        w.println("B = load '" + EMPTY_DIR + "';");
        w.println("C = join A by $0, B by $0 using 'merge';");
        w.println("store C into '" + OUTPUT_FILE + "';");
        w.close();

        try {
            String[] args = { "-x", cluster.getExecType().name(), PIG_FILE, };
            PigStats stats = PigRunner.run(args, null);

            assertTrue(stats.isSuccessful());

            // This assert fails on 205 due to MAPREDUCE-3606
            if (Util.isMapredExecType(cluster.getExecType())
                    && !Util.isHadoop205() && !Util.isHadoop1_x()) {
                // the indexer job has zero maps
                MRJobStats js = (MRJobStats) stats.getJobGraph().getSources().get(0);
                assertEquals(0, js.getNumberMaps());
            }

            assertEmptyOutputFile();
        } finally {
            new File(PIG_FILE).delete();
            Util.deleteFile(cluster, OUTPUT_FILE);
        }
    }

    @Test
    public void testFRJoin() throws Exception {
        PrintWriter w = new PrintWriter(new FileWriter(PIG_FILE));
        w.println("A = load '" + INPUT_FILE + "';");
        w.println("B = load '" + EMPTY_DIR + "';");
        w.println("C = join A by $0, B by $0 using 'repl';");
        w.println("store C into '" + OUTPUT_FILE + "';");
        w.close();

        try {
            String[] args = { "-x", cluster.getExecType().name(), PIG_FILE, };
            PigStats stats = PigRunner.run(args, null);

            assertTrue(stats.isSuccessful());

            // This assert fails on 205 due to MAPREDUCE-3606
            if (Util.isMapredExecType(cluster.getExecType())
                    && !Util.isHadoop205() && !Util.isHadoop1_x()) {
                MRJobStats js = (MRJobStats) stats.getJobGraph().getSources().get(0);
                assertEquals(0, js.getNumberMaps());
            }

            assertEmptyOutputFile();
        } finally {
            new File(PIG_FILE).delete();
            Util.deleteFile(cluster, OUTPUT_FILE);
        }
    }

    @Test
    public void testRegularJoin() throws Exception {
        PrintWriter w = new PrintWriter(new FileWriter(PIG_FILE));
        w.println("A = load '" + INPUT_FILE + "';");
        w.println("B = load '" + EMPTY_DIR + "';");
        w.println("C = join B by $0, A by $0 PARALLEL 0;");
        w.println("store C into '" + OUTPUT_FILE + "';");
        w.close();

        try {
            String[] args = { "-x", cluster.getExecType().name(), PIG_FILE, };
            PigStats stats = PigRunner.run(args, null);

            assertTrue(stats.isSuccessful());

            assertEmptyOutputFile();

        } finally {
            new File(PIG_FILE).delete();
            Util.deleteFile(cluster, OUTPUT_FILE);
        }
    }

    @Test
    public void testRightOuterJoin() throws Exception {
        PrintWriter w = new PrintWriter(new FileWriter(PIG_FILE));
        w.println("A = load '" + INPUT_FILE + "';");
        w.println("B = load '" + EMPTY_DIR + "' as (x:int);");
        w.println("C = join B by $0 right outer, A by $0;");
        w.println("store C into '" + OUTPUT_FILE + "';");
        w.close();

        try {
            String[] args = { "-x", cluster.getExecType().name(), PIG_FILE, };
            PigStats stats = PigRunner.run(args, null);

            assertTrue(stats.isSuccessful());
            assertEquals(2, stats.getNumberRecords(OUTPUT_FILE));
        } finally {
            new File(PIG_FILE).delete();
            Util.deleteFile(cluster, OUTPUT_FILE);
        }
    }

    @Test
    public void testLeftOuterJoin() throws Exception {
        PrintWriter w = new PrintWriter(new FileWriter(PIG_FILE));
        w.println("A = load '" + INPUT_FILE + "' as (x:int);");
        w.println("B = load '" + EMPTY_DIR + "' as (x:int);");
        w.println("C = join B by $0 left outer, A by $0;");
        w.println("store C into '" + OUTPUT_FILE + "';");
        w.close();

        try {
            String[] args = { "-x", cluster.getExecType().name(), PIG_FILE, };
            PigStats stats = PigRunner.run(args, null);

            assertTrue(stats.isSuccessful());
            assertEquals(0, stats.getNumberRecords(OUTPUT_FILE));
        } finally {
            new File(PIG_FILE).delete();
            Util.deleteFile(cluster, OUTPUT_FILE);
        }
    }

    @Test
    public void testBloomJoin() throws Exception {
        PrintWriter w = new PrintWriter(new FileWriter(PIG_FILE));
        w.println("A = load '" + INPUT_FILE + "' as (x:int);");
        w.println("B = load '" + EMPTY_DIR + "' as (x:int);");
        w.println("C = join B by $0, A by $0 using 'bloom';");
        w.println("D = join A by $0, B by $0 using 'bloom';");
        w.println("store C into '" + OUTPUT_FILE + "';");
        w.println("store D into 'output1';");
        w.close();

        try {
            String[] args = { "-x", cluster.getExecType().name(), PIG_FILE, };
            PigStats stats = PigRunner.run(args, null);

            assertTrue(stats.isSuccessful());
            assertEquals(0, stats.getNumberRecords(OUTPUT_FILE));
            assertEquals(0, stats.getNumberRecords("output1"));
            assertEmptyOutputFile();
        } finally {
            new File(PIG_FILE).delete();
            Util.deleteFile(cluster, OUTPUT_FILE);
            Util.deleteFile(cluster, "output1");
        }
    }

    @Test
    public void testBloomJoinOuter() throws Exception {
        PrintWriter w = new PrintWriter(new FileWriter(PIG_FILE));
        w.println("A = load '" + INPUT_FILE + "' as (x:int);");
        w.println("B = load '" + EMPTY_DIR + "' as (x:int);");
        w.println("C = join B by $0 left outer, A by $0 using 'bloom';");
        w.println("D = join A by $0 left outer, B by $0 using 'bloom';");
        w.println("E = join B by $0 right outer, A by $0 using 'bloom';");
        w.println("F = join A by $0 right outer, B by $0 using 'bloom';");
        w.println("store C into '" + OUTPUT_FILE + "';");
        w.println("store D into 'output1';");
        w.println("store E into 'output2';");
        w.println("store F into 'output3';");
        w.close();

        try {
            String[] args = { "-x", cluster.getExecType().name(), PIG_FILE, };
            PigStats stats = PigRunner.run(args, null);

            assertTrue(stats.isSuccessful());
            assertEquals(0, stats.getNumberRecords(OUTPUT_FILE));
            assertEquals(2, stats.getNumberRecords("output1"));
            assertEquals(2, stats.getNumberRecords("output2"));
            assertEquals(0, stats.getNumberRecords("output3"));
            assertEmptyOutputFile();
        } finally {
            new File(PIG_FILE).delete();
            Util.deleteFile(cluster, OUTPUT_FILE);
            Util.deleteFile(cluster, "output1");
            Util.deleteFile(cluster, "output2");
            Util.deleteFile(cluster, "output3");
        }
    }

    private void assertEmptyOutputFile() throws IllegalArgumentException, IOException {
        FileSystem fs = cluster.getFileSystem();
        FileStatus status = fs.getFileStatus(new Path(OUTPUT_FILE));
        assertTrue(status.isDir());
        assertEquals(0, status.getLen());
        // output directory isn't empty. Has one empty file
        FileStatus[] files = fs.listStatus(status.getPath(), Util.getSuccessMarkerPathFilter());
        assertEquals(1, files.length);
        assertEquals(0, files[0].getLen());
        assertTrue(files[0].getPath().getName().startsWith("part-"));
    }
}
