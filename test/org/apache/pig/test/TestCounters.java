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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.Map;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.hadoop.fs.Path;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.tools.pigstats.PigStats;
import org.junit.Test;

public class TestCounters extends TestCase {
    String file = "input.txt";

    MiniCluster cluster = MiniCluster.buildCluster();

    final int MAX = 100*1000;
    Random r = new Random();

    @Test
    public void testMapOnly() throws IOException, ExecException {
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
        PigStats pigStats = pigServer.store("c", "output_map_only").getStatistics();

        //PigStats pigStats = pigServer.getPigStats();
        Map<String, Map<String, String>> stats = pigStats.getPigStats();
        
        //counting the no. of bytes in the output file
        //long filesize = cluster.getFileSystem().getFileStatus(new Path("output_map_only")).getLen();
        InputStream is = FileLocalizer.open(FileLocalizer.fullPath("output_map_only", pigServer.getPigContext()), ExecType.MAPREDUCE, pigServer.getPigContext().getDfs());
        long filesize = 0;
        while(is.read() != -1) filesize++;
        
        is.close();
        
        cluster.getFileSystem().delete(new Path(file), true);
        cluster.getFileSystem().delete(new Path("output_map_only"), true);

        System.out.println("============================================");
        System.out.println("Test case Map Only");
        System.out.println("============================================");
        System.out.println("MRPlan : \n" + pigStats.getMRPlan());
        for(Map.Entry<String, Map<String, String>> entry : stats.entrySet()) {
            System.out.println("============================================");
            System.out.println("Job : " + entry.getKey());
            for(Map.Entry<String, String> e1 : entry.getValue().entrySet()) {
                System.out.println(" - " + e1.getKey() + " : \n" + e1.getValue());
            }
            System.out.println("============================================");
        }

        Map.Entry<String, Map<String, String>> e = stats.entrySet().iterator().next();

        //System.out.println("Job Name : " + e.getKey());

        Map<String, String> jobStats = e.getValue();

        System.out.println("Map input records : " + jobStats.get("PIG_STATS_MAP_INPUT_RECORDS"));
        assertEquals(MAX, Integer.parseInt(jobStats.get("PIG_STATS_MAP_INPUT_RECORDS")));
        System.out.println("Map output records : " + jobStats.get("PIG_STATS_MAP_OUTPUT_RECORDS"));
        assertEquals(count, Integer.parseInt(jobStats.get("PIG_STATS_MAP_OUTPUT_RECORDS")));
        assertNull(jobStats.get("PIG_STATS_REDUCE_PLAN"));
        assertNull(jobStats.get("PIG_STATS_COMBINE_PLAN"));
        assertNotNull(jobStats.get("PIG_STATS_MAP_PLAN"));
        assertEquals(0, Integer.parseInt(jobStats.get("PIG_STATS_REDUCE_INPUT_RECORDS")));
        assertEquals(0, Integer.parseInt(jobStats.get("PIG_STATS_REDUCE_OUTPUT_RECORDS")));

        assertEquals(count, pigStats.getRecordsWritten());
        assertEquals(filesize, pigStats.getBytesWritten());

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
        //pigServer.store("c", "output_map_only");
        PigStats pigStats = pigServer.store("c", "output_map_only", "BinStorage").getStatistics();
        
        InputStream is = FileLocalizer.open(FileLocalizer.fullPath("output_map_only", pigServer.getPigContext()), ExecType.MAPREDUCE, pigServer.getPigContext().getDfs());
        long filesize = 0;
        while(is.read() != -1) filesize++;
        
        is.close();

        Map<String, Map<String, String>> stats = pigStats.getPigStats();
        cluster.getFileSystem().delete(new Path(file), true);
        cluster.getFileSystem().delete(new Path("output_map_only"), true);

        System.out.println("============================================");
        System.out.println("Test case Map Only");
        System.out.println("============================================");
        System.out.println("MRPlan : \n" + pigStats.getMRPlan());
        for(Map.Entry<String, Map<String, String>> entry : stats.entrySet()) {
            System.out.println("============================================");
            System.out.println("Job : " + entry.getKey());
            for(Map.Entry<String, String> e1 : entry.getValue().entrySet()) {
                System.out.println(" - " + e1.getKey() + " : \n" + e1.getValue());
            }
            System.out.println("============================================");
        }

        Map.Entry<String, Map<String, String>> e = stats.entrySet().iterator().next();

        //System.out.println("Job Name : " + e.getKey());

        Map<String, String> jobStats = e.getValue();

        System.out.println("Map input records : " + jobStats.get("PIG_STATS_MAP_INPUT_RECORDS"));
        assertEquals(MAX, Integer.parseInt(jobStats.get("PIG_STATS_MAP_INPUT_RECORDS")));
        System.out.println("Map output records : " + jobStats.get("PIG_STATS_MAP_OUTPUT_RECORDS"));
        assertEquals(count, Integer.parseInt(jobStats.get("PIG_STATS_MAP_OUTPUT_RECORDS")));
        assertNull(jobStats.get("PIG_STATS_REDUCE_PLAN"));
        assertNull(jobStats.get("PIG_STATS_COMBINE_PLAN"));
        assertNotNull(jobStats.get("PIG_STATS_MAP_PLAN"));
        assertEquals(0, Integer.parseInt(jobStats.get("PIG_STATS_REDUCE_INPUT_RECORDS")));
        assertEquals(0, Integer.parseInt(jobStats.get("PIG_STATS_REDUCE_OUTPUT_RECORDS")));

        assertEquals(count, pigStats.getRecordsWritten());
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

        for(int i = 0; i < 10; i++) 
            if(nos[i] > 0)
                count ++;

        PigServer pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        pigServer.registerQuery("a = load '" + file + "';");
        pigServer.registerQuery("b = group a by $0;");
        pigServer.registerQuery("c = foreach b generate group;");
        PigStats pigStats = pigServer.store("c", "output").getStatistics();
        InputStream is = FileLocalizer.open(FileLocalizer.fullPath("output", pigServer.getPigContext()), ExecType.MAPREDUCE, pigServer.getPigContext().getDfs());
        long filesize = 0;
        while(is.read() != -1) filesize++;
        
        is.close();

        Map<String, Map<String, String>> stats = pigStats.getPigStats();
        cluster.getFileSystem().delete(new Path(file), true);
        cluster.getFileSystem().delete(new Path("output"), true);

        System.out.println("============================================");
        System.out.println("Test case MapReduce");
        System.out.println("============================================");
        System.out.println("MRPlan : \n" + pigStats.getMRPlan());
        for(Map.Entry<String, Map<String, String>> entry : stats.entrySet()) {
            System.out.println("============================================");
            System.out.println("Job : " + entry.getKey());
            for(Map.Entry<String, String> e1 : entry.getValue().entrySet()) {
                System.out.println(" - " + e1.getKey() + " : \n" + e1.getValue());
            }
            System.out.println("============================================");
        }

        Map.Entry<String, Map<String, String>> e = stats.entrySet().iterator().next();

        Map<String, String> jobStats = e.getValue();

        System.out.println("Map input records : " + jobStats.get("PIG_STATS_MAP_INPUT_RECORDS"));
        assertEquals(MAX, Integer.parseInt(jobStats.get("PIG_STATS_MAP_INPUT_RECORDS")));
        System.out.println("Map output records : " + jobStats.get("PIG_STATS_MAP_OUTPUT_RECORDS"));
        assertEquals(MAX, Integer.parseInt(jobStats.get("PIG_STATS_MAP_OUTPUT_RECORDS")));
        System.out.println("Reduce input records : " + jobStats.get("PIG_STATS_REDUCE_INPUT_RECORDS"));
        assertEquals(MAX, Integer.parseInt(jobStats.get("PIG_STATS_REDUCE_INPUT_RECORDS")));
        System.out.println("Reduce output records : " + jobStats.get("PIG_STATS_REDUCE_OUTPUT_RECORDS"));
        assertEquals(count, Integer.parseInt(jobStats.get("PIG_STATS_REDUCE_OUTPUT_RECORDS")));

        assertNull(jobStats.get("PIG_STATS_COMBINE_PLAN"));
        assertNotNull(jobStats.get("PIG_STATS_MAP_PLAN"));
        assertNotNull(jobStats.get("PIG_STATS_REDUCE_PLAN"));

        assertEquals(count, pigStats.getRecordsWritten());
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

        for(int i = 0; i < 10; i++) 
            if(nos[i] > 0)
                count ++;

        PigServer pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        pigServer.registerQuery("a = load '" + file + "';");
        pigServer.registerQuery("b = group a by $0;");
        pigServer.registerQuery("c = foreach b generate group;");
        PigStats pigStats = pigServer.store("c", "output", "BinStorage").getStatistics();

        InputStream is = FileLocalizer.open(FileLocalizer.fullPath("output", pigServer.getPigContext()), ExecType.MAPREDUCE, pigServer.getPigContext().getDfs());
        long filesize = 0;
        while(is.read() != -1) filesize++;
        
        is.close();
        
        Map<String, Map<String, String>> stats = pigStats.getPigStats();
        cluster.getFileSystem().delete(new Path(file), true);
        cluster.getFileSystem().delete(new Path("output"), true);

        System.out.println("============================================");
        System.out.println("Test case MapReduce");
        System.out.println("============================================");
        System.out.println("MRPlan : \n" + pigStats.getMRPlan());
        for(Map.Entry<String, Map<String, String>> entry : stats.entrySet()) {
            System.out.println("============================================");
            System.out.println("Job : " + entry.getKey());
            for(Map.Entry<String, String> e1 : entry.getValue().entrySet()) {
                System.out.println(" - " + e1.getKey() + " : \n" + e1.getValue());
            }
            System.out.println("============================================");
        }

        Map.Entry<String, Map<String, String>> e = stats.entrySet().iterator().next();

        Map<String, String> jobStats = e.getValue();

        System.out.println("Map input records : " + jobStats.get("PIG_STATS_MAP_INPUT_RECORDS"));
        assertEquals(MAX, Integer.parseInt(jobStats.get("PIG_STATS_MAP_INPUT_RECORDS")));
        System.out.println("Map output records : " + jobStats.get("PIG_STATS_MAP_OUTPUT_RECORDS"));
        assertEquals(MAX, Integer.parseInt(jobStats.get("PIG_STATS_MAP_OUTPUT_RECORDS")));
        System.out.println("Reduce input records : " + jobStats.get("PIG_STATS_REDUCE_INPUT_RECORDS"));
        assertEquals(MAX, Integer.parseInt(jobStats.get("PIG_STATS_REDUCE_INPUT_RECORDS")));
        System.out.println("Reduce output records : " + jobStats.get("PIG_STATS_REDUCE_OUTPUT_RECORDS"));
        assertEquals(count, Integer.parseInt(jobStats.get("PIG_STATS_REDUCE_OUTPUT_RECORDS")));

        assertNull(jobStats.get("PIG_STATS_COMBINE_PLAN"));
        assertNotNull(jobStats.get("PIG_STATS_MAP_PLAN"));
        assertNotNull(jobStats.get("PIG_STATS_REDUCE_PLAN"));
        
        assertEquals(count, pigStats.getRecordsWritten());
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

        for(int i = 0; i < 10; i++) 
            if(nos[i] > 0)
                count ++;

        PigServer pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        pigServer.registerQuery("a = load '" + file + "';");
        pigServer.registerQuery("b = group a by $0;");
        pigServer.registerQuery("c = foreach b generate group, SUM(a.$1);");
        PigStats pigStats = pigServer.store("c", "output").getStatistics();

        InputStream is = FileLocalizer.open(FileLocalizer.fullPath("output", pigServer.getPigContext()), ExecType.MAPREDUCE, pigServer.getPigContext().getDfs());
        long filesize = 0;
        while(is.read() != -1) filesize++;
        
        is.close();
        Map<String, Map<String, String>> stats = pigStats.getPigStats();
        cluster.getFileSystem().delete(new Path(file), true);
        cluster.getFileSystem().delete(new Path("output"), true);

        System.out.println("============================================");
        System.out.println("Test case MapCombineReduce");
        System.out.println("============================================");
        System.out.println("MRPlan : \n" + pigStats.getMRPlan());
        for(Map.Entry<String, Map<String, String>> entry : stats.entrySet()) {
            System.out.println("============================================");
            System.out.println("Job : " + entry.getKey());
            for(Map.Entry<String, String> e1 : entry.getValue().entrySet()) {
                System.out.println(" - " + e1.getKey() + " : \n" + e1.getValue());
            }
            System.out.println("============================================");
        }

        Map.Entry<String, Map<String, String>> e = stats.entrySet().iterator().next();

        Map<String, String> jobStats = e.getValue();

        System.out.println("Map input records : " + jobStats.get("PIG_STATS_MAP_INPUT_RECORDS"));
        assertEquals(MAX, Integer.parseInt(jobStats.get("PIG_STATS_MAP_INPUT_RECORDS")));
        System.out.println("Map output records : " + jobStats.get("PIG_STATS_MAP_OUTPUT_RECORDS"));
        assertEquals(MAX, Integer.parseInt(jobStats.get("PIG_STATS_MAP_OUTPUT_RECORDS")));
        System.out.println("Reduce input records : " + jobStats.get("PIG_STATS_REDUCE_INPUT_RECORDS"));
        assertEquals(count, Integer.parseInt(jobStats.get("PIG_STATS_REDUCE_INPUT_RECORDS")));
        System.out.println("Reduce output records : " + jobStats.get("PIG_STATS_REDUCE_OUTPUT_RECORDS"));
        assertEquals(count, Integer.parseInt(jobStats.get("PIG_STATS_REDUCE_OUTPUT_RECORDS")));

        assertNotNull(jobStats.get("PIG_STATS_COMBINE_PLAN"));
        assertNotNull(jobStats.get("PIG_STATS_MAP_PLAN"));
        assertNotNull(jobStats.get("PIG_STATS_REDUCE_PLAN"));

        assertEquals(count, pigStats.getRecordsWritten());
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

        for(int i = 0; i < 10; i++) 
            if(nos[i] > 0)
                count ++;

        PigServer pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        pigServer.registerQuery("a = load '" + file + "';");
        pigServer.registerQuery("b = group a by $0;");
        pigServer.registerQuery("c = foreach b generate group, SUM(a.$1);");
        PigStats pigStats = pigServer.store("c", "output", "BinStorage").getStatistics();

        InputStream is = FileLocalizer.open(FileLocalizer.fullPath("output", pigServer.getPigContext()), ExecType.MAPREDUCE, pigServer.getPigContext().getDfs());
        long filesize = 0;
        while(is.read() != -1) filesize++;
        
        is.close();
        Map<String, Map<String, String>> stats = pigStats.getPigStats();
        cluster.getFileSystem().delete(new Path(file), true);
        cluster.getFileSystem().delete(new Path("output"), true);

        System.out.println("============================================");
        System.out.println("Test case MapCombineReduce");
        System.out.println("============================================");
        System.out.println("MRPlan : \n" + pigStats.getMRPlan());
        for(Map.Entry<String, Map<String, String>> entry : stats.entrySet()) {
            System.out.println("============================================");
            System.out.println("Job : " + entry.getKey());
            for(Map.Entry<String, String> e1 : entry.getValue().entrySet()) {
                System.out.println(" - " + e1.getKey() + " : \n" + e1.getValue());
            }
            System.out.println("============================================");
        }

        Map.Entry<String, Map<String, String>> e = stats.entrySet().iterator().next();

        Map<String, String> jobStats = e.getValue();

        System.out.println("Map input records : " + jobStats.get("PIG_STATS_MAP_INPUT_RECORDS"));
        assertEquals(MAX, Integer.parseInt(jobStats.get("PIG_STATS_MAP_INPUT_RECORDS")));
        System.out.println("Map output records : " + jobStats.get("PIG_STATS_MAP_OUTPUT_RECORDS"));
        assertEquals(MAX, Integer.parseInt(jobStats.get("PIG_STATS_MAP_OUTPUT_RECORDS")));
        System.out.println("Reduce input records : " + jobStats.get("PIG_STATS_REDUCE_INPUT_RECORDS"));
        assertEquals(count, Integer.parseInt(jobStats.get("PIG_STATS_REDUCE_INPUT_RECORDS")));
        System.out.println("Reduce output records : " + jobStats.get("PIG_STATS_REDUCE_OUTPUT_RECORDS"));
        assertEquals(count, Integer.parseInt(jobStats.get("PIG_STATS_REDUCE_OUTPUT_RECORDS")));

        assertNotNull(jobStats.get("PIG_STATS_COMBINE_PLAN"));
        assertNotNull(jobStats.get("PIG_STATS_MAP_PLAN"));
        assertNotNull(jobStats.get("PIG_STATS_REDUCE_PLAN"));

        assertEquals(count, pigStats.getRecordsWritten());
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

        for(int i = 0; i < 10; i++) 
            if(nos[i] > 0)
                count ++;

        PigServer pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        pigServer.registerQuery("a = load '" + file + "';");
        pigServer.registerQuery("b = order a by $0;");
        pigServer.registerQuery("c = group b by $0;");
        pigServer.registerQuery("d = foreach c generate group, SUM(b.$1);");
        PigStats pigStats = pigServer.store("d", "output").getStatistics();
        
        InputStream is = FileLocalizer.open(FileLocalizer.fullPath("output", pigServer.getPigContext()), ExecType.MAPREDUCE, pigServer.getPigContext().getDfs());
        long filesize = 0;
        while(is.read() != -1) filesize++;
        
        is.close();
        Map<String, Map<String, String>> stats = pigStats.getPigStats();
        cluster.getFileSystem().delete(new Path(file), true);
        cluster.getFileSystem().delete(new Path("output"), true);
        
        System.out.println("============================================");
        System.out.println("Test case MultipleMRJobs");
        System.out.println("============================================");
        System.out.println("MRPlan : \n" + pigStats.getMRPlan());
        for(Map.Entry<String, Map<String, String>> entry : stats.entrySet()) {
            System.out.println("============================================");
            System.out.println("Job : " + entry.getKey());
            for(Map.Entry<String, String> e1 : entry.getValue().entrySet()) {
                System.out.println(" - " + e1.getKey() + " : \n" + e1.getValue());
            }
            System.out.println("============================================");
        }

        Map<String, String> jobStats = stats.get(pigStats.getRootJobIDs().get(0));

        System.out.println("Map input records : " + jobStats.get("PIG_STATS_MAP_INPUT_RECORDS"));
        assertEquals(MAX, Integer.parseInt(jobStats.get("PIG_STATS_MAP_INPUT_RECORDS")));
        System.out.println("Map output records : " + jobStats.get("PIG_STATS_MAP_OUTPUT_RECORDS"));
        assertEquals(MAX, Integer.parseInt(jobStats.get("PIG_STATS_MAP_OUTPUT_RECORDS")));
        System.out.println("Reduce input records : " + jobStats.get("PIG_STATS_REDUCE_INPUT_RECORDS"));
        assertEquals(count, Integer.parseInt(jobStats.get("PIG_STATS_REDUCE_INPUT_RECORDS")));
        System.out.println("Reduce output records : " + jobStats.get("PIG_STATS_REDUCE_OUTPUT_RECORDS"));
        assertEquals(count, Integer.parseInt(jobStats.get("PIG_STATS_REDUCE_OUTPUT_RECORDS")));
        
        assertEquals(count, pigStats.getRecordsWritten());
        assertEquals(filesize, pigStats.getBytesWritten());

    }
    
    @Test
    public void testLocal() throws IOException, ExecException {
        int count = 0;
        //PrintWriter pw = new PrintWriter(Util.createInputFile(cluster, file));
        File file = File.createTempFile("data", ".txt");
        PrintWriter pw = new PrintWriter(new FileOutputStream(file));
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

        for(int i = 0; i < 10; i++) 
            if(nos[i] > 0)
                count ++;

        File out = File.createTempFile("output", ".txt");
        out.delete();
        PigServer pigServer = new PigServer("local");
        pigServer.registerQuery("a = load '" + Util.encodeEscape(file.toString()) + "';");
        pigServer.registerQuery("b = order a by $0;");
        pigServer.registerQuery("c = group b by $0;");
        pigServer.registerQuery("d = foreach c generate group, SUM(b.$1);");
        PigStats pigStats = pigServer.store("d", out.getAbsolutePath()).getStatistics();
        InputStream is = FileLocalizer.open(FileLocalizer.fullPath(out.getAbsolutePath(), pigServer.getPigContext()), ExecType.MAPREDUCE, pigServer.getPigContext().getDfs());
        long filesize = 0;
        while(is.read() != -1) filesize++;
        
        is.close();
        out.delete();
        
        //Map<String, Map<String, String>> stats = pigStats.getPigStats();
        
        assertEquals(count, pigStats.getRecordsWritten());
        assertEquals(filesize, pigStats.getBytesWritten());

    }

}
