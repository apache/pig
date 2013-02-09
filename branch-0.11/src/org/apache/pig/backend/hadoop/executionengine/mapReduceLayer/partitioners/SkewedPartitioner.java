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


import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce;
import org.apache.pig.backend.hadoop.executionengine.util.MapRedUtil;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.io.NullablePartitionWritable;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.io.PigNullableWritable;
import org.apache.pig.impl.util.Pair;


/**
  * This class is used by skewed join. For the partitioned table, the skewedpartitioner reads the key
  * distribution data from the sampler file and returns the reducer index in a round robin fashion.
  * For ex: if the key distribution file contains (k1, 5, 3) as an entry, reducers from 5 to 3 are returned 
  * in a round robin manner.
  */ 
public class SkewedPartitioner extends Partitioner<PigNullableWritable, Writable> implements Configurable {
    Map<Tuple, Pair<Integer, Integer> > reducerMap = new HashMap<Tuple, Pair<Integer, Integer> >();
    static Map<Tuple, Integer> currentIndexMap = new HashMap<Tuple, Integer> ();
    Integer totalReducers;
    Configuration conf;

    @Override
    public int getPartition(PigNullableWritable wrappedKey, Writable value,
            int numPartitions) {
		// for streaming tables, return the partition index blindly
		if (wrappedKey instanceof NullablePartitionWritable && (((NullablePartitionWritable)wrappedKey).getPartition()) != -1) {
			return ((NullablePartitionWritable)wrappedKey).getPartition();
		}

        // for partition table, compute the index based on the sampler output
        Pair <Integer, Integer> indexes;
        Integer curIndex = -1;
        Tuple keyTuple = TupleFactory.getInstance().newTuple(1);

        // extract the key from nullablepartitionwritable
        PigNullableWritable key = ((NullablePartitionWritable) wrappedKey).getKey();

        try {
            keyTuple.set(0, key.getValueAsPigType());
        } catch (ExecException e) {
            return -1;
        }

        // if the key is not null and key 
        if (key instanceof NullableTuple && key.getValueAsPigType() != null) {
            keyTuple = (Tuple)key.getValueAsPigType();
        }

        // if the partition file is empty, use numPartitions
        totalReducers = (totalReducers > 0) ? totalReducers : numPartitions;
        
        indexes = reducerMap.get(keyTuple);
        // if the reducerMap does not contain the key, do the default hash based partitioning
        if (indexes == null) {
            return (Math.abs(keyTuple.hashCode() % totalReducers));
        }

        if (currentIndexMap.containsKey(keyTuple)) {
            curIndex = currentIndexMap.get(keyTuple);
        }

        if (curIndex >= (indexes.first + indexes.second) || curIndex == -1) {
            curIndex = indexes.first;
        } else {
            curIndex++;
        }

        // set it in the map
        currentIndexMap.put(keyTuple, curIndex);
        return (curIndex % totalReducers);
    }

    @Override
    public void setConf(Configuration job) {
        conf = job;
        PigMapReduce.sJobConfInternal.set(conf);
        PigMapReduce.sJobConf = conf;
        String keyDistFile = job.get("pig.keyDistFile", "");
        if (keyDistFile.length() == 0)
            throw new RuntimeException(this.getClass().getSimpleName() + " used but no key distribution found");

        try {
            Integer [] redCnt = new Integer[1]; 
            reducerMap = MapRedUtil.loadPartitionFileFromLocalCache(
                    keyDistFile, redCnt, DataType.TUPLE, job);
            // check if the partition file is empty
            totalReducers = (redCnt[0] == null) ? -1 : redCnt[0];
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

}
