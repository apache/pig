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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.Reducer.Context;
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
                super(job.getJobConf(), new TaskAttemptID(), new FakeRawKeyValueIterator(input.iterator().hasNext()),
                    null, null, null, null, new IllustrateDummyReporter(), null, PigNullableWritable.class, NullableTuple.class);
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
        }

        @Override
        public boolean inIllustrator(
                org.apache.hadoop.mapreduce.Reducer.Context context) {
            return (context instanceof PigMapReduce.Reduce.IllustratorContext);
        }

        @Override
        public POPackage getPack(
                org.apache.hadoop.mapreduce.Reducer.Context context) {
            return ((PigMapReduce.Reduce.IllustratorContext) context).pack;
        }
    }
}
