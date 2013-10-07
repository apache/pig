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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pig.PigRunner;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.mapreduce.MRJobStats;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestEmptyInputDir {

    private static MiniCluster cluster; 
    private static final String EMPTY_DIR = "emptydir";
    private static final String INPUT_FILE = "input";
    private static final String OUTPUT_FILE = "output";
    private static final String PIG_FILE = "test.pig";

    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        cluster = MiniCluster.buildCluster();
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
    public void testSkewedJoin() throws Exception {
        PrintWriter w = new PrintWriter(new FileWriter(PIG_FILE));
        w.println("A = load '" + INPUT_FILE + "';");
        w.println("B = load '" + EMPTY_DIR + "';");
        w.println("C = join B by $0, A by $0 using 'skewed';");
        w.println("store C into '" + OUTPUT_FILE + "';");
        w.close();
        
        try {
            String[] args = { PIG_FILE };
            PigStats stats = PigRunner.run(args, null);
     
            assertTrue(stats.isSuccessful());
            // the sampler job has zero maps
            MRJobStats js = (MRJobStats)stats.getJobGraph().getSources().get(0);
            
            // This assert fails on 205 due to MAPREDUCE-3606
            if (!Util.isHadoop205()&&!Util.isHadoop1_x())
                assertEquals(0, js.getNumberMaps()); 
            
            FileSystem fs = cluster.getFileSystem();
            FileStatus status = fs.getFileStatus(new Path(OUTPUT_FILE));
            assertTrue(status.isDir());
            assertEquals(0, status.getLen());
            // output directory isn't empty
            assertTrue(fs.listStatus(status.getPath()).length > 0);
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
            String[] args = { PIG_FILE };
            PigStats stats = PigRunner.run(args, null);
     
            assertTrue(stats.isSuccessful());    
            // the indexer job has zero maps
            MRJobStats js = (MRJobStats)stats.getJobGraph().getSources().get(0);
            
            // This assert fails on 205 due to MAPREDUCE-3606
            if (!Util.isHadoop205()&&!Util.isHadoop1_x())
                assertEquals(0, js.getNumberMaps()); 
            
            FileSystem fs = cluster.getFileSystem();
            FileStatus status = fs.getFileStatus(new Path(OUTPUT_FILE));
            assertTrue(status.isDir());
            assertEquals(0, status.getLen());
            
            // output directory isn't empty
            assertTrue(fs.listStatus(status.getPath()).length > 0);            
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
            String[] args = { PIG_FILE };
            PigStats stats = PigRunner.run(args, null);
     
            assertTrue(stats.isSuccessful());    
            // the indexer job has zero maps
            MRJobStats js = (MRJobStats)stats.getJobGraph().getSources().get(0);
            
            // This assert fails on 205 due to MAPREDUCE-3606
            if (!Util.isHadoop205()&&!Util.isHadoop1_x())
                assertEquals(0, js.getNumberMaps()); 
            
            FileSystem fs = cluster.getFileSystem();
            FileStatus status = fs.getFileStatus(new Path(OUTPUT_FILE));
            assertTrue(status.isDir());
            assertEquals(0, status.getLen());
            
            // output directory isn't empty
            assertTrue(fs.listStatus(status.getPath()).length > 0);            
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
        w.println("C = join B by $0, A by $0;");
        w.println("store C into '" + OUTPUT_FILE + "';");
        w.close();
        
        try {
            String[] args = { PIG_FILE };
            PigStats stats = PigRunner.run(args, null);
     
            assertTrue(stats.isSuccessful());   
            
            FileSystem fs = cluster.getFileSystem();
            FileStatus status = fs.getFileStatus(new Path(OUTPUT_FILE));
            assertTrue(status.isDir());
            assertEquals(0, status.getLen());
            
            // output directory isn't empty
            assertTrue(fs.listStatus(status.getPath()).length > 0);            
            
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
            String[] args = { PIG_FILE };
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
            String[] args = { PIG_FILE };
            PigStats stats = PigRunner.run(args, null);
     
            assertTrue(stats.isSuccessful());               
            assertEquals(0, stats.getNumberRecords(OUTPUT_FILE));                  
        } finally {
            new File(PIG_FILE).delete();
            Util.deleteFile(cluster, OUTPUT_FILE);
        }
    }
}
