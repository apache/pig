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
package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configuration.IntegerRanges;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.security.Credentials;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.io.PigNullableWritable;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.pen.FakeRawKeyValueIterator;

public class PigMapReduce extends PigGenericMapReduce {
    public static class Reduce extends PigGenericMapReduce.Reduce {
        /**
         * Get reducer's illustrator context
         * 
         * @param input Input buffer as output by maps
         * @param pkg package
         * @return reducer's illustrator context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public Context getIllustratorContext(Job job,
               List<Pair<PigNullableWritable, Writable>> input, POPackage pkg) throws IOException, InterruptedException {
            return new IllustratorContext(job, input, pkg);
        }
        
        @SuppressWarnings("unchecked")
        public class IllustratorContext extends Context {
            private PigNullableWritable currentKey = null, nextKey = null;
            private NullableTuple nextValue = null;
            private List<NullableTuple> currentValues = null;
            private Iterator<Pair<PigNullableWritable, Writable>> it;
            private final ByteArrayOutputStream bos;
            private final DataOutputStream dos;
            private final RawComparator sortComparator, groupingComparator;
            POPackage pack = null;

            public IllustratorContext(Job job,
                  List<Pair<PigNullableWritable, Writable>> input,
                  POPackage pkg
                  ) throws IOException, InterruptedException {
                bos = new ByteArrayOutputStream();
                dos = new DataOutputStream(bos);
                org.apache.hadoop.mapreduce.Job nwJob = new org.apache.hadoop.mapreduce.Job(job.getJobConf());
                sortComparator = nwJob.getSortComparator();
                groupingComparator = nwJob.getGroupingComparator();
                
                Collections.sort(input, new Comparator<Pair<PigNullableWritable, Writable>>() {
                        @Override
                        public int compare(Pair<PigNullableWritable, Writable> o1,
                                           Pair<PigNullableWritable, Writable> o2) {
                            try {
                                o1.first.write(dos);
                                int l1 = bos.size();
                                o2.first.write(dos);
                                int l2 = bos.size();
                                byte[] bytes = bos.toByteArray();
                                bos.reset();
                                return sortComparator.compare(bytes, 0, l1, bytes, l1, l2-l1);
                            } catch (IOException e) {
                                throw new RuntimeException("Serialization exception in sort:"+e.getMessage());
                            }
                        }
                    }
                );
                currentValues = new ArrayList<NullableTuple>();
                it = input.iterator();
                if (it.hasNext()) {
                    Pair<PigNullableWritable, Writable> entry = it.next();
                    nextKey = entry.first;
                    nextValue = (NullableTuple) entry.second;
                }
                pack = pkg;
            }
            
            @Override
            public PigNullableWritable getCurrentKey() {
                return currentKey;
            }
            
            @Override
            public boolean nextKey() {
                if (nextKey == null)
                    return false;
                currentKey = nextKey;
                currentValues.clear();
                currentValues.add(nextValue);
                nextKey = null;
                for(; it.hasNext(); ) {
                    Pair<PigNullableWritable, Writable> entry = it.next();
                    /* Why can't raw comparison be used?
                    byte[] bytes;
                    int l1, l2;
                    try {
                        currentKey.write(dos);
                        l1 = bos.size();
                        entry.first.write(dos);
                        l2 = bos.size();
                        bytes = bos.toByteArray();
                    } catch (IOException e) {
                        throw new RuntimeException("nextKey exception : "+e.getMessage());
                    }
                    bos.reset();
                    if (groupingComparator.compare(bytes, 0, l1, bytes, l1, l2-l1) == 0)
                    */
                    if (groupingComparator.compare(currentKey, entry.first) == 0)
                    {
                        currentValues.add((NullableTuple)entry.second);
                    } else {
                        nextKey = entry.first;
                        nextValue = (NullableTuple) entry.second;
                        break;
                    }
                }
                return true;
            }
            
            @Override
            public Iterable<NullableTuple> getValues() {
                return currentValues;
            }
            
            @Override
            public void write(PigNullableWritable k, Writable t) {
            }
            
            @Override
            public void progress() { 
            }

            @Override
            public Counter getCounter(Enum<?> arg0) {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public Counter getCounter(String arg0, String arg1) {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public NullableTuple getCurrentValue() throws IOException,
                    InterruptedException {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public OutputCommitter getOutputCommitter() {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public boolean nextKeyValue() throws IOException,
                    InterruptedException {
                // TODO Auto-generated method stub
                return false;
            }

            @Override
            public String getStatus() {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public TaskAttemptID getTaskAttemptID() {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public void setStatus(String arg0) {
                // TODO Auto-generated method stub
                
            }

            @Override
            public Path[] getArchiveClassPaths() {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public long[] getArchiveTimestamps() {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public URI[] getCacheArchives() throws IOException {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public URI[] getCacheFiles() throws IOException {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass()
                    throws ClassNotFoundException {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public Configuration getConfiguration() {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public Credentials getCredentials() {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public Path[] getFileClassPaths() {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public long[] getFileTimestamps() {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public RawComparator<?> getGroupingComparator() {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public Class<? extends InputFormat<?, ?>> getInputFormatClass()
                    throws ClassNotFoundException {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public String getJar() {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public JobID getJobID() {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public String getJobName() {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public boolean getJobSetupCleanupNeeded() {
                // TODO Auto-generated method stub
                return false;
            }

            @Override
            public Path[] getLocalCacheArchives() throws IOException {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public Path[] getLocalCacheFiles() throws IOException {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public Class<?> getMapOutputKeyClass() {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public Class<?> getMapOutputValueClass() {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass()
                    throws ClassNotFoundException {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public int getMaxMapAttempts() {
                // TODO Auto-generated method stub
                return 0;
            }

            @Override
            public int getMaxReduceAttempts() {
                // TODO Auto-generated method stub
                return 0;
            }

            @Override
            public int getNumReduceTasks() {
                // TODO Auto-generated method stub
                return 0;
            }

            @Override
            public Class<? extends OutputFormat<?, ?>> getOutputFormatClass()
                    throws ClassNotFoundException {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public Class<?> getOutputKeyClass() {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public Class<?> getOutputValueClass() {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public Class<? extends Partitioner<?, ?>> getPartitionerClass()
                    throws ClassNotFoundException {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public boolean getProfileEnabled() {
                // TODO Auto-generated method stub
                return false;
            }

            @Override
            public String getProfileParams() {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public IntegerRanges getProfileTaskRange(boolean arg0) {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass()
                    throws ClassNotFoundException {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public RawComparator<?> getSortComparator() {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public boolean getSymlink() {
                // TODO Auto-generated method stub
                return false;
            }

            @Override
            public String getUser() {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public Path getWorkingDirectory() throws IOException {
                // TODO Auto-generated method stub
                return null;
            }
        }
    }
}
