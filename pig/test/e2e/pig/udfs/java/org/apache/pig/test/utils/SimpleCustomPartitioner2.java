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
package org.apache.pig.test.utils;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.pig.impl.io.PigNullableWritable;

public class SimpleCustomPartitioner2 extends Partitioner<PigNullableWritable, Writable> {

    static Map<Integer, Integer> alternateMap = new HashMap<Integer, Integer>();
    @Override
    public int getPartition(PigNullableWritable key, Writable value, int numPartitions) {
        if(key.getValueAsPigType() instanceof Integer) {
            int i = ((Integer)key.getValueAsPigType()).intValue();
            if (alternateMap.containsKey(i)) {
                int red = alternateMap.get(i);
                if (red==0) {
                    alternateMap.put(i, 1);
                } else {
                    alternateMap.put(i, 0);
                }
                return red % numPartitions;
            } else {
                alternateMap.put(i, 1);
                return 0;
            }
        }
        else {
            return (key.hashCode()) % numPartitions;
        }
    }
}
