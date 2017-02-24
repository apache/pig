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

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.fs.Path;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.tools.pigstats.InputStats;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.PigStats.JobGraph;
import org.apache.pig.tools.pigstats.mapreduce.MRJobStats;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestCounters {
    String file = "input.txt";

    static MiniCluster cluster = MiniCluster.buildCluster();
    
    final int MAX = 100*1000;
    Random r = new Random();


    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        cluster.shutDown();
    }
    
    @Test
    public void testMapOnly() throws IOException, ExecException {
        int count = 0;
        PrintWriter pw = new PrintWriter(Util.createInputFile(cluster, file));
        for(int i = 0; i < MAX; i++) {
            int t = r.nextInt(100);
            pw.println(t);
            if(t > 50) count ++;
        }
        pw.close();
        PigServer pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        pigServer.registerQuery("a = load '" + file + "';");
        pigServer.registerQuery("b = filter a by $0 > 50;");
        pigServer.registerQuery("c = foreach b generate $0 - 50;");
        ExecJob job = pigServer.store("c", "output_map_only");
        PigStats pigStats = job.getStatistics();
        
        //counting the no. of bytes in the output file
        //long filesize = cluster.getFileSystem().getFileStatus(new Path("output_map_only")).getLen();
        InputStream is = FileLocalizer.open(FileLocalizer.fullPath(
                "output_map_only", pigServer.getPigContext()), pigServer
                .getPigContext());

        long filesize = 0;
        while(is.read() != -1) filesize++;
        
        is.close();
        
        cluster.getFileSystem().delete(new Path(file), true);
        cluster.getFileSystem().delete(new Path("output_map_only"), true);

        System.out.println("============================================");
        System.out.println("Test case Map Only");
        System.out.println("============================================");

        JobGraph jg = pigStats.getJobGraph();
        Iterator<JobStats> iter = jg.iterator();
        while (iter.hasNext()) {
            MRJobStats js = (MRJobStats) iter.next();                    

            System.out.println("Map input records : " + js.getMapInputRecords());
            assertEquals(MAX, js.getMapInputRecords());
            System.out.println("Map output records : " + js.getMapOutputRecords());
            assertEquals(count, js.getMapOutputRecords());
            assertEquals(0, js.getReduceInputRecords());
            assertEquals(0, js.getReduceOutputRecords());
            System.out.println("Hdfs bytes written : " + js.getHdfsBytesWritten());
            assertEquals(filesize, js.getHdfsBytesWritten());
        }

    }

    @Test
    public void testMapOnlyBinStorage() throws IOException, ExecException {
        int count = 0;
        PrintWriter pw = new PrintWriter(Util.createInputFile(cluster, file));
        for(int i = 0; i < MAX; i++) {
            int t = r.nextInt(100);
            pw.println(t);
            if(t > 50)
                count ++;
        }
        pw.close();
        PigServer pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        pigServer.registerQuery("a = load '" + file + "';");
        pigServer.registerQuery("b = filter a by $0 > 50;");
        pigServer.registerQuery("c = foreach b generate $0 - 50;");
        ExecJob job = pigServer.store("c", "output_map_only", "BinStorage");
        PigStats pigStats = job.getStatistics();
        
        InputStream is = FileLocalizer.open(FileLocalizer.fullPath(
                "output_map_only", pigServer.getPigContext()),
                pigServer.getPigContext());

        long filesize = 0;
        while(is.read() != -1) filesize++;
        
        is.close();

        cluster.getFileSystem().delete(new Path(file), true);
        cluster.getFileSystem().delete(new Path("output_map_only"), true);

        System.out.println("============================================");
        System.out.println("Test case Map Only");
        System.out.println("============================================");

        JobGraph jp = pigStats.getJobGraph();
        Iterator<JobStats> iter = jp.iterator();
        while (iter.hasNext()) {
            MRJobStats js = (MRJobStats) iter.next();
        
            System.out.println("Map input records : " + js.getMapInputRecords());
            assertEquals(MAX, js.getMapInputRecords());
            System.out.println("Map output records : " + js.getMapOutputRecords());
            assertEquals(count, js.getMapOutputRecords());
            assertEquals(0, js.getReduceInputRecords());
            assertEquals(0, js.getReduceOutputRecords());
        }
            
        System.out.println("Hdfs bytes written : " + pigStats.getBytesWritten());
        assertEquals(filesize, pigStats.getBytesWritten());
    }

    @Test
    public void testMapReduceOnly() throws IOException, ExecException {
        int count = 0;
        PrintWriter pw = new PrintWriter(Util.createInputFile(cluster, file));
        int [] nos = new int[10];
        for(int i = 0; i < 10; i++)
            nos[i] = 0;

        for(int i = 0; i < MAX; i++) {
            int index = r.nextInt(10);
            int value = r.nextInt(100);
            nos[index] += value;
            pw.println(index + "\t" + value);
        }
        pw.close();

        for(int i = 0; i < 10; i++) {
            if(nos[i] > 0) count ++;
        }

        PigServer pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        pigServer.registerQuery("a = load '" + file + "';");
        pigServer.registerQuery("b = group a by $0;");
        pigServer.registerQuery("c = foreach b generate group;");

        ExecJob job = pigServer.store("c", "output");
        PigStats pigStats = job.getStatistics();
        InputStream is = FileLocalizer.open(FileLocalizer.fullPath("output",
                pigServer.getPigContext()), pigServer.getPigContext());

        long filesize = 0;
        while(is.read() != -1) filesize++;
        
        is.close();

        cluster.getFileSystem().delete(new Path(file), true);
        cluster.getFileSystem().delete(new Path("output"), true);

        System.out.println("============================================");
        System.out.println("Test case MapReduce");
        System.out.println("============================================");

        JobGraph jp = pigStats.getJobGraph();
        Iterator<JobStats> iter = jp.iterator();
        while (iter.hasNext()) {
            MRJobStats js = (MRJobStats) iter.next();
            System.out.println("Map input records : " + js.getMapInputRecords());
            assertEquals(MAX, js.getMapInputRecords());
            System.out.println("Map output records : " + js.getMapOutputRecords());
            assertEquals(MAX, js.getMapOutputRecords());
            System.out.println("Reduce input records : " + js.getReduceInputRecords());
            assertEquals(MAX, js.getReduceInputRecords());
            System.out.println("Reduce output records : " + js.getReduceOutputRecords());
            assertEquals(count, js.getReduceOutputRecords());
        }
        System.out.println("Hdfs bytes written : " + pigStats.getBytesWritten());
        assertEquals(filesize, pigStats.getBytesWritten());
    }

    @Test
    public void testMapReduceOnlyBinStorage() throws IOException, ExecException {
        int count = 0;
        PrintWriter pw = new PrintWriter(Util.createInputFile(cluster, file));
        int [] nos = new int[10];
        for(int i = 0; i < 10; i++)
            nos[i] = 0;

        for(int i = 0; i < MAX; i++) {
            int index = r.nextInt(10);
            int value = r.nextInt(100);
            nos[index] += value;
            pw.println(index + "\t" + value);
        }
        pw.close();

        for(int i = 0; i < 10; i++) {
            if(nos[i] > 0) count ++;
        }

        PigServer pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        pigServer.registerQuery("a = load '" + file + "';");
        pigServer.registerQuery("b = group a by $0;");
        pigServer.registerQuery("c = foreach b generate group;");
        ExecJob job = pigServer.store("c", "output", "BinStorage");
        PigStats pigStats = job.getStatistics();

        InputStream is = FileLocalizer.open(FileLocalizer.fullPath("output",
                pigServer.getPigContext()), pigServer.getPigContext());
        long filesize = 0;
        while(is.read() != -1) filesize++;
        
        is.close();
        
        cluster.getFileSystem().delete(new Path(file), true);
        cluster.getFileSystem().delete(new Path("output"), true);

        System.out.println("============================================");
        System.out.println("Test case MapReduce");
        System.out.println("============================================");

        JobGraph jp = pigStats.getJobGraph();
        Iterator<JobStats> iter = jp.iterator();
        while (iter.hasNext()) {
            MRJobStats js = (MRJobStats) iter.next();
            System.out.println("Map input records : " + js.getMapInputRecords());
            assertEquals(MAX, js.getMapInputRecords());
            System.out.println("Map output records : " + js.getMapOutputRecords());
            assertEquals(MAX, js.getMapOutputRecords());
            System.out.println("Reduce input records : " + js.getReduceInputRecords());
            assertEquals(MAX, js.getReduceInputRecords());
            System.out.println("Reduce output records : " + js.getReduceOutputRecords());
            assertEquals(count, js.getReduceOutputRecords());
        }
        System.out.println("Hdfs bytes written : " + pigStats.getBytesWritten());
        assertEquals(filesize, pigStats.getBytesWritten());
    }

    @Test
    public void testMapCombineReduce() throws IOException, ExecException {
        int count = 0;
        PrintWriter pw = new PrintWriter(Util.createInputFile(cluster, file));
        int [] nos = new int[10];
        for(int i = 0; i < 10; i++)
            nos[i] = 0;

        for(int i = 0; i < MAX; i++) {
            int index = r.nextInt(10);
            int value = r.nextInt(100);
            nos[index] += value;
            pw.println(index + "\t" + value);
        }
        pw.close();

        for(int i = 0; i < 10; i++) {
            if(nos[i] > 0) count ++;
        }

        PigServer pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        pigServer.registerQuery("a = load '" + file + "';");
        pigServer.registerQuery("b = group a by $0;");
        pigServer.registerQuery("c = foreach b generate group, SUM(a.$1);");
        ExecJob job = pigServer.store("c", "output");
        PigStats pigStats = job.getStatistics();

        InputStream is = FileLocalizer.open(FileLocalizer.fullPath("output",
                pigServer.getPigContext()), pigServer.getPigContext());
        long filesize = 0;
        while(is.read() != -1) filesize++;
        
        is.close();
 
        cluster.getFileSystem().delete(new Path(file), true);
        cluster.getFileSystem().delete(new Path("output"), true);

        System.out.println("============================================");
        System.out.println("Test case MapCombineReduce");
        System.out.println("============================================");
        
        JobGraph jp = pigStats.getJobGraph();
        Iterator<JobStats> iter = jp.iterator();
        while (iter.hasNext()) {
            MRJobStats js = (MRJobStats) iter.next();
            System.out.println("Map input records : " + js.getMapInputRecords());
            assertEquals(MAX, js.getMapInputRecords());
            System.out.println("Map output records : " + js.getMapOutputRecords());
            assertEquals(MAX, js.getMapOutputRecords());
            System.out.println("Reduce input records : " + js.getReduceInputRecords());
            assertEquals(count, js.getReduceInputRecords());
            System.out.println("Reduce output records : " + js.getReduceOutputRecords());
            assertEquals(count, js.getReduceOutputRecords());
        }
        System.out.println("Hdfs bytes written : " + pigStats.getBytesWritten());
        assertEquals(filesize, pigStats.getBytesWritten());
    }
     
    @Test
    public void testMapCombineReduceBinStorage() throws IOException, ExecException {
        int count = 0;
        PrintWriter pw = new PrintWriter(Util.createInputFile(cluster, file));
        int [] nos = new int[10];
        for(int i = 0; i < 10; i++)
            nos[i] = 0;

        for(int i = 0; i < MAX; i++) {
            int index = r.nextInt(10);
            int value = r.nextInt(100);
            nos[index] += value;
            pw.println(index + "\t" + value);
        }
        pw.close();

        for(int i = 0; i < 10; i++) {
            if(nos[i] > 0) count ++;
        }

        PigServer pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        pigServer.registerQuery("a = load '" + file + "';");
        pigServer.registerQuery("b = group a by $0;");
        pigServer.registerQuery("c = foreach b generate group, SUM(a.$1);");

        ExecJob job = pigServer.store("c", "output", "BinStorage");
        PigStats pigStats = job.getStatistics();
        
        InputStream is = FileLocalizer.open(FileLocalizer.fullPath("output",
                pigServer.getPigContext()), pigServer.getPigContext());

        long filesize = 0;
        while(is.read() != -1) filesize++;
        
        is.close();
        cluster.getFileSystem().delete(new Path(file), true);
        cluster.getFileSystem().delete(new Path("output"), true);

        System.out.println("============================================");
        System.out.println("Test case MapCombineReduce");
        System.out.println("============================================");
 
        JobGraph jp = pigStats.getJobGraph();
        Iterator<JobStats> iter = jp.iterator();
        while (iter.hasNext()) {
            MRJobStats js = (MRJobStats) iter.next();
            System.out.println("Map input records : " + js.getMapInputRecords());
            assertEquals(MAX, js.getMapInputRecords());
            System.out.println("Map output records : " + js.getMapOutputRecords());
            assertEquals(MAX, js.getMapOutputRecords());
            System.out.println("Reduce input records : " + js.getReduceInputRecords());
            assertEquals(count, js.getReduceInputRecords());
            System.out.println("Reduce output records : " + js.getReduceOutputRecords());
            assertEquals(count, js.getReduceOutputRecords());
        }
        System.out.println("Hdfs bytes written : " + pigStats.getBytesWritten());
        assertEquals(filesize, pigStats.getBytesWritten());
    }

    @Test
    public void testMultipleMRJobs() throws IOException, ExecException {
        int count = 0;
        PrintWriter pw = new PrintWriter(Util.createInputFile(cluster, file));
        int [] nos = new int[10];
        for(int i = 0; i < 10; i++)
            nos[i] = 0;

        for(int i = 0; i < MAX; i++) {
            int index = r.nextInt(10);
            int value = r.nextInt(100);
            nos[index] += value;
            pw.println(index + "\t" + value);
        }
        pw.close();

        for(int i = 0; i < 10; i++) { 
            if(nos[i] > 0) count ++;
        }

        PigServer pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        pigServer.registerQuery("a = load '" + file + "';");
        pigServer.registerQuery("b = order a by $0;");
        pigServer.registerQuery("c = group b by $0;");
        pigServer.registerQuery("d = foreach c generate group, SUM(b.$1);");
        ExecJob job = pigServer.store("d", "output");
        PigStats pigStats = job.getStatistics();
        
        InputStream is = FileLocalizer.open(FileLocalizer.fullPath("output",
                pigServer.getPigContext()), pigServer.getPigContext());
        long filesize = 0;
        while(is.read() != -1) filesize++;
        
        is.close();
        
        cluster.getFileSystem().delete(new Path(file), true);
        cluster.getFileSystem().delete(new Path("output"), true);
        
        System.out.println("============================================");
        System.out.println("Test case MultipleMRJobs");
        System.out.println("============================================");
        
        JobGraph jp = pigStats.getJobGraph();
        MRJobStats js = (MRJobStats)jp.getSinks().get(0);
        
        System.out.println("Job id: " + js.getName());
        System.out.println(jp.toString());
        
        System.out.println("Map input records : " + js.getMapInputRecords());
        assertEquals(MAX, js.getMapInputRecords());
        System.out.println("Map output records : " + js.getMapOutputRecords());
        assertEquals(MAX, js.getMapOutputRecords());
        System.out.println("Reduce input records : " + js.getReduceInputRecords());
        assertEquals(count, js.getReduceInputRecords());
        System.out.println("Reduce output records : " + js.getReduceOutputRecords());
        assertEquals(count, js.getReduceOutputRecords());
        
        System.out.println("Hdfs bytes written : " + js.getHdfsBytesWritten());
        assertEquals(filesize, js.getHdfsBytesWritten());

    }
    
    @Test
    public void testMapOnlyMultiQueryStores() throws Exception {
        PrintWriter pw = new PrintWriter(Util.createInputFile(cluster, file));
        for(int i = 0; i < MAX; i++) {
            int t = r.nextInt(100);
            pw.println(t);
        }
        pw.close();
        
        PigServer pigServer = new PigServer(ExecType.MAPREDUCE, 
                cluster.getProperties());
        pigServer.setBatchOn();
        pigServer.registerQuery("a = load '" + file + "';");
        pigServer.registerQuery("b = filter a by $0 > 50;");
        pigServer.registerQuery("c = filter a by $0 <= 50;");
        pigServer.registerQuery("store b into '/tmp/outout1';");
        pigServer.registerQuery("store c into '/tmp/outout2';");
        List<ExecJob> jobs = pigServer.executeBatch();
        PigStats stats = jobs.get(0).getStatistics();
        assertTrue(stats.getOutputLocations().size() == 2);
        
        cluster.getFileSystem().delete(new Path(file), true);
        cluster.getFileSystem().delete(new Path("/tmp/outout1"), true);
        cluster.getFileSystem().delete(new Path("/tmp/outout2"), true);

        MRJobStats js = (MRJobStats)stats.getJobGraph().getSinks().get(0);
        
        Map<String, Long> entry = js.getMultiStoreCounters();
        long counter = 0;
        for (Long val : entry.values()) {
            counter += val;
        }
        
        assertEquals(MAX, counter);       
    }    
    
    @Test
    public void testMultiQueryStores() throws Exception {
        int[] nums = new int[100];
        PrintWriter pw = new PrintWriter(Util.createInputFile(cluster, file));
        for(int i = 0; i < MAX; i++) {
            int t = r.nextInt(100);
            pw.println(t);
            nums[t]++;
        }
        pw.close();
        
        int groups = 0;
        for (int i : nums) {
            if (i > 0) groups++;
        }
        
        PigServer pigServer = new PigServer(ExecType.MAPREDUCE, 
                cluster.getProperties());
        pigServer.setBatchOn();
        pigServer.registerQuery("a = load '" + file + "';");
        pigServer.registerQuery("b = filter a by $0 >= 50;");
        pigServer.registerQuery("c = group b by $0;");
        pigServer.registerQuery("d = foreach c generate group;");
        pigServer.registerQuery("e = filter a by $0 < 50;");
        pigServer.registerQuery("f = group e by $0;");
        pigServer.registerQuery("g = foreach f generate group;");
        pigServer.registerQuery("store d into '/tmp/outout1';");
        pigServer.registerQuery("store g into '/tmp/outout2';");
        List<ExecJob> jobs = pigServer.executeBatch();
        PigStats stats = jobs.get(0).getStatistics();
        
        assertTrue(stats.getOutputLocations().size() == 2);
               
        cluster.getFileSystem().delete(new Path(file), true);
        cluster.getFileSystem().delete(new Path("/tmp/outout1"), true);
        cluster.getFileSystem().delete(new Path("/tmp/outout2"), true);

        MRJobStats js = (MRJobStats)stats.getJobGraph().getSinks().get(0);
        
        Map<String, Long> entry = js.getMultiStoreCounters();
        long counter = 0;
        for (Long val : entry.values()) {
            counter += val;
        }
        
        assertEquals(groups, counter);       
    }    
    
    /*    
     * IMPORTANT NOTE:
     * COMMENTED OUT BECAUSE COUNTERS DO NOT CURRENTLY WORK IN LOCAL MODE -
     * SEE PIG-1286 - UNCOMMENT WHEN IT IS FIXED
     */ 
//    @Test
//    public void testLocal() throws IOException, ExecException {
//        int count = 0;
//        //PrintWriter pw = new PrintWriter(Util.createInputFile(cluster, file));
//        File file = File.createTempFile("data", ".txt");
//        PrintWriter pw = new PrintWriter(new FileOutputStream(file));
//        int [] nos = new int[10];
//        for(int i = 0; i < 10; i++)
//            nos[i] = 0;
//
//        for(int i = 0; i < MAX; i++) {
//            int index = r.nextInt(10);
//            int value = r.nextInt(100);
//            nos[index] += value;
//            pw.println(index + "\t" + value);
//        }
//        pw.close();
//
//        for(int i = 0; i < 10; i++) 
//            if(nos[i] > 0)
//                count ++;
//
//        File out = File.createTempFile("output", ".txt");
//        out.delete();
//        PigServer pigServer = new PigServer("local");
//        // FileLocalizer is initialized before using HDFS by previous tests
//        FileLocalizer.setInitialized(false);
//        pigServer.registerQuery("a = load '" + Util.encodeEscape(file.toString()) + "';");
//        pigServer.registerQuery("b = order a by $0;");
//        pigServer.registerQuery("c = group b by $0;");
//        pigServer.registerQuery("d = foreach c generate group, SUM(b.$1);");
//        PigStats pigStats = pigServer.store("d", "file://" + out.getAbsolutePath()).getStatistics();
//        InputStream is = FileLocalizer.open(FileLocalizer.fullPath(out.getAbsolutePath(), pigServer.getPigContext()), ExecType.MAPREDUCE, pigServer.getPigContext().getDfs());
//        long filesize = 0;
//        while(is.read() != -1) filesize++;
//        
//        is.close();
//        out.delete();
//        
//        //Map<String, Map<String, String>> stats = pigStats.getPigStats();
//        
//        assertEquals(10, pigStats.getRecordsWritten());
//        assertEquals(110, pigStats.getBytesWritten());
//
//    }

    @Test
    public void testJoinInputCounters() throws Exception {        
        testInputCounters("join");
    }
    
    @Test
    public void testCogroupInputCounters() throws Exception {        
        testInputCounters("cogroup");
    }
    
    @Test
    public void testSkewedInputCounters() throws Exception {        
        testInputCounters("skewed");
    }
    
    @Test
    public void testSelfJoinInputCounters() throws Exception {        
        testInputCounters("self-join");
    }
    
    private static boolean multiInputCreated = false;
    
    private static int count = 0;
            
    private void testInputCounters(String keyword) throws Exception {  
        String file1 = "multi-input1.txt";
        String file2 = "multi-input2.txt";
        
        String output = keyword;
        
        if (keyword.equals("self-join")) {
            file2 = file1;
            keyword = "join";
        }
         
        final int MAX_NUM_RECORDS = 100; 
        if (!multiInputCreated) {
            PrintWriter pw = new PrintWriter(Util.createInputFile(cluster, file1));
            for (int i = 0; i < MAX_NUM_RECORDS; i++) {
                int t = r.nextInt(100);
                pw.println(t);
            }
            pw.close();
                        
            PrintWriter pw2 = new PrintWriter(Util.createInputFile(cluster, file2));
            for (int i = 0; i < MAX_NUM_RECORDS; i++) {
                int t = r.nextInt(100);
                if (t > 50) {
                    count ++;
                    pw2.println(t);
                }
            }
            pw2.close();
            multiInputCreated = true;
        }
        
        PigServer pigServer = new PigServer(ExecType.MAPREDUCE, 
                cluster.getProperties());
        pigServer.setBatchOn();
        pigServer.registerQuery("a = load '" + file1 + "';");
        pigServer.registerQuery("b = load '" + file2 + "';");
        if (keyword.equals("join") || keyword.endsWith("cogroup")) {
            pigServer.registerQuery("c = " + keyword + " a by $0, b by $0;");
        } else if (keyword.equals("skewed")) {
            pigServer.registerQuery("c = join a by $0, b by $0 using 'skewed';");
        }
        ExecJob job = pigServer.store("c", output + "_output");
        
        PigStats stats = job.getStatistics();
        assertTrue(stats.isSuccessful());
        List<InputStats> inputs = stats.getInputStats();
        if (keyword.equals("join") || keyword.endsWith("cogroup")) {
            assertEquals(2, inputs.size());
        } else if (keyword.equals("skewed")) {
            assertEquals(3, inputs.size());
        }
        for (InputStats input : inputs) {
            if (file1.equals(input.getName()) && input.getInputType() == InputStats.INPUT_TYPE.regular) {
                assertEquals(MAX_NUM_RECORDS, input.getNumberRecords());
            } else if (file2.equals(input.getName())){
                assertEquals(count, input.getNumberRecords());
            } else {
                assertTrue(input.getInputType() == InputStats.INPUT_TYPE.sampler);
            }
        }
    }
}
