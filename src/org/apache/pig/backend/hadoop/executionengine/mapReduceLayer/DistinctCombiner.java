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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.pig.impl.io.PigNullableWritable;
import org.apache.pig.impl.io.NullableTuple;

/**
 * A special implementation of combiner used only for distinct.  This combiner
 * does not even parse out the records.  It just throws away duplicate values
 * in the key in order ot minimize the data being sent to the reduce.
 */
public class DistinctCombiner {

    public static class Combine 
        extends Reducer<PigNullableWritable, NullableTuple, PigNullableWritable, Writable> {
        
        private final Log log = LogFactory.getLog(getClass());

        ProgressableReporter pigReporter;
        
        /**
         * Configures the reporter 
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            pigReporter = new ProgressableReporter();
        }
        
        /**
         * The reduce function which removes values.
         */
        @Override
        protected void reduce(PigNullableWritable key, Iterable<NullableTuple> tupIter, Context context) 
                throws IOException, InterruptedException {
            
            pigReporter.setRep(context);

            // Take the first value and the key and collect
            // just that.
            Iterator<NullableTuple> iter = tupIter.iterator();
            NullableTuple val = iter.next();
            context.write(key, val);
        }
    }
    
}
