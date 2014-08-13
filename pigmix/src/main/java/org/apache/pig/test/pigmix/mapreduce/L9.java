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
package org.apache.pig.test.pigmix.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.pig.test.pigmix.mapreduce.Library;

public class L9 {

    public static class ReadPageViews extends MapReduceBase
        implements Mapper<LongWritable, Text, Text, Text> {

        public void map(
                LongWritable k,
                Text val,
                OutputCollector<Text, Text> oc,
                Reporter reporter) throws IOException {

            // Split the line
            List<Text> fields = Library.splitLine(val, '');
            if (fields.size() != 9) return;

            oc.collect(fields.get(3), val);
        }
    }

    public static class MyPartitioner implements Partitioner<Text, Text> {

        public Map<Character, Integer> map;

        public int getPartition(Text key, Text value, int numPartitions) {
            int rc = 0;
            String s = key.toString();
            if (s == null || s.length() < 1)  return 0;
            rc += map.get(s.charAt(0));
            return rc;
        }

        public void configure(JobConf conf) {
            // Don't actually do any configuration, do the setup of the hash
            // because this call is guaranteed to be made each time we set up
            // MyPartitioner
            map = new HashMap<Character, Integer>(59);
            map.put('A', 1);
            map.put('B', 1);
            map.put('C', 2);
            map.put('D', 2);
            map.put('E', 3);
            map.put('F', 3);
            map.put('G', 4);
            map.put('H', 4);
            map.put('I', 5);
            map.put('J', 5);
            map.put('K', 6);
            map.put('L', 6);
            map.put('M', 7);
            map.put('N', 7);
            map.put('O', 8);
            map.put('P', 8);
            map.put('Q', 9);
            map.put('R', 9);
            map.put('S', 10);
            map.put('T', 10);
            map.put('U', 11);
            map.put('V', 11);
            map.put('W', 12);
            map.put('X', 12);
            map.put('Y', 13);
            map.put('Z', 13);
            map.put('[', 14);
            map.put('\\', 14);
            map.put(']', 15);
            map.put('^', 15);
            map.put('_', 16);
            map.put('`', 16);
            map.put('a', 17);
            map.put('b', 17);
            map.put('c', 18);
            map.put('d', 18);
            map.put('e', 19);
            map.put('f', 20);
            map.put('g', 20);
            map.put('h', 21);
            map.put('i', 22);
            map.put('j', 23);
            map.put('k', 24);
            map.put('l', 25);
            map.put('m', 26);
            map.put('n', 27);
            map.put('o', 28);
            map.put('p', 29);
            map.put('q', 30);
            map.put('r', 31);
            map.put('s', 32);
            map.put('t', 33);
            map.put('u', 34);
            map.put('v', 35);
            map.put('w', 36);
            map.put('x', 37);
            map.put('y', 38);
            map.put('z', 39);
        }
    }

    public static class Group extends MapReduceBase
        implements Reducer<Text, Text, Text, Text> {

        public void reduce(
                Text key,
                Iterator<Text> iter, 
                OutputCollector<Text, Text> oc,
                Reporter reporter) throws IOException {
            while (iter.hasNext()) {
                oc.collect(null, iter.next());
            }
        }
    }

    public static void main(String[] args) throws IOException {

        if (args.length!=3) {
            System.out.println("Parameters: inputDir outputDir parallel");
            System.exit(1);
        }
        String inputDir = args[0];
        String outputDir = args[1];
        String parallel = args[2];
        JobConf lp = new JobConf(L9.class);
        lp.setJobName("L9 Load Page Views");
        lp.setInputFormat(TextInputFormat.class);
        lp.setOutputKeyClass(Text.class);
        lp.setOutputValueClass(Text.class);
        lp.setMapperClass(ReadPageViews.class);
        lp.setReducerClass(Group.class);
        lp.setPartitionerClass(MyPartitioner.class);
        Properties props = System.getProperties();
        for (Map.Entry<Object,Object> entry : props.entrySet()) {
            lp.set((String)entry.getKey(), (String)entry.getValue());
        }
        FileInputFormat.addInputPath(lp, new Path(inputDir + "/page_views"));
        FileOutputFormat.setOutputPath(lp, new Path(outputDir + "/L9out"));
        // Hardcode the parallel to 40 since MyPartitioner assumes it
        lp.setNumReduceTasks(40);
        Job group = new Job(lp);

        JobControl jc = new JobControl("L9 join");
        jc.addJob(group);

        new Thread(jc).start();
   
        int i = 0;
        while(!jc.allFinished()){
            ArrayList<Job> failures = jc.getFailedJobs();
            if (failures != null && failures.size() > 0) {
                for (Job failure : failures) {
                    System.err.println(failure.getMessage());
                }
                break;
            }

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {}

            if (i % 10000 == 0) {
                System.out.println("Running jobs");
                ArrayList<Job> running = jc.getRunningJobs();
                if (running != null && running.size() > 0) {
                    for (Job r : running) {
                        System.out.println(r.getJobName());
                    }
                }
                System.out.println("Ready jobs");
                ArrayList<Job> ready = jc.getReadyJobs();
                if (ready != null && ready.size() > 0) {
                    for (Job r : ready) {
                        System.out.println(r.getJobName());
                    }
                }
                System.out.println("Waiting jobs");
                ArrayList<Job> waiting = jc.getWaitingJobs();
                if (waiting != null && waiting.size() > 0) {
                    for (Job r : ready) {
                        System.out.println(r.getJobName());
                    }
                }
                System.out.println("Successful jobs");
                ArrayList<Job> success = jc.getSuccessfulJobs();
                if (success != null && success.size() > 0) {
                    for (Job r : ready) {
                        System.out.println(r.getJobName());
                    }
                }
            }
            i++;
        }
        ArrayList<Job> failures = jc.getFailedJobs();
        if (failures != null && failures.size() > 0) {
            for (Job failure : failures) {
                System.err.println(failure.getMessage());
            }
        }
        jc.stop();
    }

}
