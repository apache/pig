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
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.Mapper.Context;
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
    
    @Override
    public boolean inIllustrator(Context context) {
        return (context instanceof PigMapBase.IllustratorContext);
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
            super(conf, new TaskAttemptID(), null, null, null, new IllustrateDummyReporter(), split);
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
    }
}
