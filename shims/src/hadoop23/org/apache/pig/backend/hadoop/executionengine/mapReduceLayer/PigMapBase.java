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


import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configuration.IntegerRanges;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.security.Credentials;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigGenericMapBase;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.PigNullableWritable;
import org.apache.pig.impl.util.Pair;

abstract public class PigMapBase extends PigGenericMapBase {
    /**
     * 
     * Get mapper's illustrator context
     * 
     * @param conf  Configuration
     * @param input Input bag to serve as data source
     * @param output Map output buffer
     * @param split the split
     * @return Illustrator's context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public Context getIllustratorContext(Configuration conf, DataBag input,
          List<Pair<PigNullableWritable, Writable>> output, InputSplit split)
          throws IOException, InterruptedException {
        return new IllustratorContext(conf, input, output, split);
    }
    
    public class IllustratorContext extends Context {
        private DataBag input;
        List<Pair<PigNullableWritable, Writable>> output;
        private Iterator<Tuple> it = null;
        private Tuple value = null;
        private boolean init  = false;

        public IllustratorContext(Configuration conf, DataBag input,
              List<Pair<PigNullableWritable, Writable>> output,
              InputSplit split) throws IOException, InterruptedException {
              if (output == null)
                  throw new IOException("Null output can not be used");
              this.input = input; this.output = output;
        }
        
        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (input == null) {
                if (!init) {
                    init = true;
                    return true;
                }
                return false;
            }
            if (it == null)
                it = input.iterator();
            if (!it.hasNext())
                return false;
            value = it.next();
            return true;
        }
        
        @Override
        public Text getCurrentKey() {
          return null;
        }
        
        @Override
        public Tuple getCurrentValue() {
          return value;
        }
        
        @Override
        public void write(PigNullableWritable key, Writable value) 
            throws IOException, InterruptedException {
            output.add(new Pair<PigNullableWritable, Writable>(key, value));
        }
        
        @Override
        public void progress() {
          
        }

        @Override
        public InputSplit getInputSplit() {
            // TODO Auto-generated method stub
            return null;
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
        public OutputCommitter getOutputCommitter() {
            // TODO Auto-generated method stub
            return null;
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
