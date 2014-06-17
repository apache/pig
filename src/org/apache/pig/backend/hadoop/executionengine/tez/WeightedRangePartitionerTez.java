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
package org.apache.pig.backend.hadoop.executionengine.tez;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.partitioners.DiscreteProbabilitySampleGenerator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.partitioners.WeightedRangePartitioner;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.InternalMap;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.builtin.FindQuantiles;
import org.apache.pig.impl.io.PigNullableWritable;
import org.apache.tez.runtime.library.common.ConfigUtils;

public class WeightedRangePartitionerTez extends WeightedRangePartitioner {
    private static final Log LOG = LogFactory.getLog(WeightedRangePartitionerTez.class);

    private Integer estimatedNumPartitions;

    @Override
    public int getPartition(PigNullableWritable key, Writable value,
            int numPartitions){
        if (estimatedNumPartitions!=null) {
            numPartitions = estimatedNumPartitions;
        }
        return super.getPartition(key, value, numPartitions);
    }

    @Override
    public void init() {
        Map<String, Object> quantileMap = null;
        if (PigProcessor.sampleMap != null) {
            // We've collected sampleMap in PigProcessor
            quantileMap = PigProcessor.sampleMap;
        } else {
            LOG.warn("Quantiles map is empty");
            inited = true;
            return;
        }

        long start = System.currentTimeMillis();
        try {
            DataBag quantilesList = (DataBag) quantileMap.get(FindQuantiles.QUANTILES_LIST);
            InternalMap weightedPartsData = (InternalMap) quantileMap.get(FindQuantiles.WEIGHTED_PARTS);
            estimatedNumPartitions = (Integer)quantileMap.get(PigProcessor.ESTIMATED_NUM_PARALLELISM);
            convertToArray(quantilesList);
            for (Entry<Object, Object> ent : weightedPartsData.entrySet()) {
                Tuple key = (Tuple) ent.getKey(); // sample item which repeats
                float[] probVec = getProbVec((Tuple) ent.getValue());
                weightedParts.put(getPigNullableWritable(key),
                        new DiscreteProbabilitySampleGenerator(probVec));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        comparator = ConfigUtils.getIntermediateInputKeyComparator(job);
        LOG.info("Initialized WeightedRangePartitionerTez. Time taken: " + (System.currentTimeMillis() - start));
        inited = true;
    }
}
