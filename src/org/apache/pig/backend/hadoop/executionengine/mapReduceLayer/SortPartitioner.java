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
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.pig.PigException;
import org.apache.pig.backend.hadoop.HDataType;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.builtin.BinStorage;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.io.NullableText;
import org.apache.pig.impl.io.NullableIntWritable;
import org.apache.pig.impl.io.NullableLongWritable;
import org.apache.pig.impl.io.NullableFloatWritable;
import org.apache.pig.impl.io.NullableDoubleWritable;
import org.apache.pig.impl.io.NullableBytesWritable;
import org.apache.pig.impl.io.PigNullableWritable;

public class SortPartitioner implements Partitioner<PigNullableWritable, Writable> {
    PigNullableWritable[] quantiles;
    RawComparator<PigNullableWritable> comparator;
    
    public int getPartition(PigNullableWritable key, Writable value,
            int numPartitions){
        int index = Arrays.binarySearch(quantiles, key, comparator);
        if (index < 0)
            index = -index-1;
        // Shouldn't this be index % numPartitions?
        return Math.min(index, numPartitions - 1);
    }

    public void configure(JobConf job) {
        String quantilesFile = job.get("pig.quantilesFile", "");
        if (quantilesFile.length() == 0)
            throw new RuntimeException("Sort paritioner used but no quantiles found");
        
        try{
            InputStream is = FileLocalizer.openDFSFile(quantilesFile,ConfigurationUtil.toProperties(job));
            BinStorage loader = new BinStorage();
            loader.bindTo(quantilesFile, new BufferedPositionedInputStream(is), 0, Long.MAX_VALUE);
            
            Tuple t;
            ArrayList<PigNullableWritable> quantiles = new ArrayList<PigNullableWritable>();
            
            while(true){
                t = loader.getNext();
                if (t==null)
                    break;
                // Need to strip the outer tuple and bag.
                Object o = t.get(0);
                if (o instanceof DataBag) {
                    for (Tuple it : (DataBag)o) {
                        addToQuantiles(job, quantiles, it);
                    }
                } else {
                    addToQuantiles(job, quantiles, t);
                }
            }
            convertToArray(job, quantiles);
        }catch (Exception e){
            throw new RuntimeException(e);
        }

        comparator = job.getOutputKeyComparator();
    }

    private void addToQuantiles(
            JobConf job,
            ArrayList<PigNullableWritable> q,
            Tuple t) throws ExecException {
        try {
            if ("true".equals(job.get("pig.usercomparator")) || t.size() > 1) {
                q.add(new NullableTuple(t));
            } else {
                Object o = t.get(0);
                String kts = job.get("pig.reduce.key.type");
                if (kts == null) {
                    int errCode = 2095;
                    String msg = "Did not get reduce key type "
                        + "from job configuration.";
                    throw new ExecException(msg, errCode, PigException.BUG);
                }
                q.add(HDataType.getWritableComparableTypes(o,
                    Byte.valueOf(kts)));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void convertToArray(
            JobConf job,
            ArrayList<PigNullableWritable> q) throws ExecException {
        if ("true".equals(job.get("pig.usercomparator")) ||
                q.get(0).getClass().equals(NullableTuple.class)) {
            quantiles = q.toArray(new NullableTuple[0]);
        } else if (q.get(0).getClass().equals(NullableBytesWritable.class)) {
            quantiles = q.toArray(new NullableBytesWritable[0]);
        } else if (q.get(0).getClass().equals(NullableDoubleWritable.class)) {
            quantiles = q.toArray(new NullableDoubleWritable[0]);
        } else if (q.get(0).getClass().equals(NullableFloatWritable.class)) {
            quantiles = q.toArray(new NullableFloatWritable[0]);
        } else if (q.get(0).getClass().equals(NullableIntWritable.class)) {
            quantiles = q.toArray(new NullableIntWritable[0]);
        } else if (q.get(0).getClass().equals(NullableLongWritable.class)) {
            quantiles = q.toArray(new NullableLongWritable[0]);
        } else if (q.get(0).getClass().equals(NullableText.class)) {
            quantiles = q.toArray(new NullableText[0]);
        } else {
            int errCode = 2096;
            String msg = "Unexpected class in SortPartitioner: " + q.get(0).getClass().getName();
            throw new ExecException(msg, errCode, PigException.BUG);
        }
    }

}
