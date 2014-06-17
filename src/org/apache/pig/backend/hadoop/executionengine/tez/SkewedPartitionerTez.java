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

import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.partitioners.SkewedPartitioner;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.builtin.PartitionSkewedKeys;
import org.apache.pig.impl.util.Pair;

public class SkewedPartitionerTez extends SkewedPartitioner {
    private static final Log LOG = LogFactory.getLog(SkewedPartitionerTez.class);

    @Override
    protected void init() {

        Map<String, Object> distMap = null;
        if (PigProcessor.sampleMap != null) {
            // We've collected sampleMap in PigProcessor
            distMap = PigProcessor.sampleMap;
        } else {
            LOG.info("Key distribution map is empty");
            inited = true;
            return;
        }

        long start = System.currentTimeMillis();

        try {
            // The distMap is structured as (key, min, max) where min, max
            // being the index of the reducers
            DataBag partitionList = (DataBag) distMap.get(PartitionSkewedKeys.PARTITION_LIST);
            totalReducers = Integer.valueOf("" + distMap.get(PartitionSkewedKeys.TOTAL_REDUCERS));
            Iterator<Tuple> it = partitionList.iterator();
            while (it.hasNext()) {
                Tuple idxTuple = it.next();
                Integer maxIndex = (Integer) idxTuple.get(idxTuple.size() - 1);
                Integer minIndex = (Integer) idxTuple.get(idxTuple.size() - 2);
                // Used to replace the maxIndex with the number of reducers
                if (maxIndex < minIndex) {
                    maxIndex = totalReducers + maxIndex;
                }

                Tuple keyT;
                // if the join is on more than 1 key
                if (idxTuple.size() > 3) {
                    // remove the last 2 fields of the tuple, i.e: minIndex and maxIndex and store
                    // it in the reducer map
                    Tuple keyTuple = tf.newTuple();
                    for (int i=0; i < idxTuple.size() - 2; i++) {
                        keyTuple.append(idxTuple.get(i));
                    }
                    keyT = keyTuple;
                } else {
                    keyT = tf.newTuple(1);
                    keyT.set(0,idxTuple.get(0));
                }
                // number of reducers
                Integer cnt = maxIndex - minIndex;
                // 1 is added to account for the 0 index
                reducerMap.put(keyT, new Pair<Integer, Integer>(minIndex, cnt));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        LOG.info("Initialized SkewedPartitionerTez. Time taken: " + (System.currentTimeMillis() - start));
        inited = true;
    }
}
