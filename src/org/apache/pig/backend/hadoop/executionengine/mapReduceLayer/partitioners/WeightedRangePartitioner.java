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
package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.partitioners;


import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.pig.backend.hadoop.HDataType;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.builtin.BinStorage;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.NullableBytesWritable;
import org.apache.pig.impl.io.NullableDoubleWritable;
import org.apache.pig.impl.io.NullableFloatWritable;
import org.apache.pig.impl.io.NullableIntWritable;
import org.apache.pig.impl.io.NullableLongWritable;
import org.apache.pig.impl.io.NullableText;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.io.PigNullableWritable;

public class WeightedRangePartitioner implements Partitioner<PigNullableWritable, Writable> {
    PigNullableWritable[] quantiles;
    RawComparator<PigNullableWritable> comparator;
    Integer numQuantiles;
    DataBag samples;
    public static Map<PigNullableWritable,DiscreteProbabilitySampleGenerator> weightedParts = new HashMap<PigNullableWritable, DiscreteProbabilitySampleGenerator>();
    JobConf job;

    public int getPartition(PigNullableWritable key, Writable value,
            int numPartitions){
        if(!weightedParts.containsKey(key)){
            int index = Arrays.binarySearch(quantiles, key, comparator);
            if (index < 0)
                index = -index-1;
            else
                index = index + 1;
            return Math.min(index, numPartitions - 1);
        }
        DiscreteProbabilitySampleGenerator gen = weightedParts.get(key);
        return gen.getNext();
    }

    public void configure(JobConf job) {
        this.job = job;
        String quantilesFile = job.get("pig.quantilesFile", "");
        comparator = job.getOutputKeyComparator();
        if (quantilesFile.length() == 0)
            throw new RuntimeException(this.getClass().getSimpleName() + " used but no quantiles found");
        
        try{
            InputStream is = FileLocalizer.openDFSFile(quantilesFile,ConfigurationUtil.toProperties(job));
            BinStorage loader = new BinStorage();
            ArrayList<PigNullableWritable> quantilesList = new ArrayList<PigNullableWritable>();
            loader.bindTo(quantilesFile, new BufferedPositionedInputStream(is), 0, Long.MAX_VALUE);
            Tuple t = loader.getNext();
            if(t==null) throw new RuntimeException("Empty samples file");
            // the Quantiles file has a tuple as under:
            // (numQuantiles, bag of samples) 
            // numQuantiles here is the reduce parallelism
            numQuantiles = (Integer) t.get(0);
            samples = (DataBag) t.get(1);
            long numSamples = samples.size();
            long toSkip = numSamples / numQuantiles;
            if(toSkip == 0) {
                // numSamples is < numQuantiles;
                // set numQuantiles to numSamples
                numQuantiles = (int)numSamples;
                toSkip = 1;
            }
            
            long ind=0, j=-1, nextQuantile = toSkip-1;
            for (Tuple it : samples) {
                if (ind==nextQuantile){
                    ++j;
                    quantilesList.add(getPigNullableWritable(it));
                    nextQuantile+=toSkip;
                    if(j==numQuantiles-1)
                        break;
                }
                ind++;
                //TODO how do we report progress?
                //if (i % 1000 == 0) progress();
                // Currently there is no way to report progress since 
                // in configure() we cannot get a handle to the reporter
                // (even PhysicalOperator.getReporter() does not work! It is
                // set to null.) Hopefully the work done here wll be < 10 minutes
                // since we are dealing with 100* num_mapper samples. When
                // RandomSampleLoader becomes an operator or UDF instead of a
                // loader hopefully we can intelligently decide on the number
                // of samples (instead of the static 100) and then not being
                // able to report progress may not be a big issue.
            }
            convertToArray(quantilesList);
            long i=-1;
            Map<PigNullableWritable,CountingMap<Integer>> contribs = new HashMap<PigNullableWritable, CountingMap<Integer>>();
            for (Tuple it : samples){
                ++i;
                PigNullableWritable sample = getPigNullableWritable(it);
                int partInd = new Long(i/toSkip).intValue(); // which partition
                if(partInd==numQuantiles) break;
                // the quantiles array has the element from the sample which is the
                // last element for a given partition. For example: if numQunatiles 
                // is 5 and number of samples is 100, then toSkip = 20 
                // quantiles[0] = sample[19] // the 20th element
                // quantiles[1] = sample[39] // the 40th element
                // and so on. For any element in the sample between 0 and 19, partInd
                // will be 0. We want to check if a sample element which is
                // present between 0 and 19 and is also the 19th (quantiles[0] element).
                // This would mean that element might spread over the 0th and 1st 
                // partition. We are looking for contributions to a partition
                // from such elements. 
                
                // First We only check for sample elements in partitions other than the last one
                // < numQunatiles -1 (partInd is 0 indexed). 
                if(partInd<numQuantiles-1 && areEqual(sample,quantiles[partInd])){
                    if(!contribs.containsKey(sample)){
                        CountingMap<Integer> cm = new CountingMap<Integer>();
                        cm.put(partInd, 1);
                        contribs.put(sample, cm);
                    }
                    else
                        contribs.get(sample).put(partInd, 1);
                }
                else{ 
                    // we are either in the last partition (last quantile)
                    // OR the sample element we are currently processing is not
                    // the same as the element in the quantile array for this partition
                    // if we haven't seen this sample item earlier, this is not an
                    // element which crosses partitions - so ignore
                    if(!contribs.containsKey(sample))
                        continue;
                    else
                        // we have seen this sample before (in a previous partInd), 
                        // add to the contribution associated with this sample - if we had 
                        // not seen this sample in a previous partInd, then we have not
                        // had this in the contribs map! (because of the if above).This 
                        // sample can either go to the previous partInd or this partInd 
                        // in the final sort reduce stage. That is where the amount of 
                        // contribution to each partInd will matter and influence the choice.
                        contribs.get(sample).put(partInd, 1);
                }
            }
            for(Entry<PigNullableWritable, CountingMap<Integer>> ent : contribs.entrySet()){
                PigNullableWritable key = ent.getKey(); // sample item which repeats
                
                // this map will have the contributions of the sample item to the different partitions
                CountingMap<Integer> value = ent.getValue(); 
                
                long total = value.getTotalCount();
                float[] probVec = new float[numQuantiles];
                // for each partition that this sample item is present in,
                // compute the fraction of the total occurences for that
                // partition - this will be the probability with which we
                // will pick this partition in the final sort reduce job
                // for this sample item
                for (Entry<Integer,Integer> valEnt : value.entrySet()) {
                    probVec[valEnt.getKey()] = (float)valEnt.getValue()/total;
                }
//                weightedParts.put(key, new DiscreteProbabilitySampleGenerator(11317,probVec));
                weightedParts.put(key, new DiscreteProbabilitySampleGenerator(probVec));
            }
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    private PigNullableWritable getPigNullableWritable(Tuple t) {
        try {
            // user comparators work with tuples - so if user comparator
            // is being used OR if there are more than 1 sort cols, use
            // NullableTuple
            if ("true".equals(job.get("pig.usercomparator")) || t.size() > 1) {
                return new NullableTuple(t);
            } else {
                Object o = t.get(0);
                String kts = job.get("pig.reduce.key.type");
                if (kts == null) {
                    throw new RuntimeException("Didn't get reduce key type "
                        + "from config file.");
                }
                return HDataType.getWritableComparableTypes(o,
                    Byte.valueOf(kts));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private boolean areEqual(PigNullableWritable sample, PigNullableWritable writable) {
        return comparator.compare(sample, writable)==0;
    }

    private void convertToArray(
            ArrayList<PigNullableWritable> q) {
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
            throw new RuntimeException("Unexpected class in " + this.getClass().getSimpleName());
        }
    }
}
