/**
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

package org.apache.pig.backend.hadoop.executionengine.tez.runtime;

import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.io.PigNullableWritable;

public class BloomFilterPartitioner extends Partitioner<PigNullableWritable, NullableTuple> {
    @Override
    public int getPartition(PigNullableWritable key, NullableTuple value, int numPartitions) {
        Tuple t = (Tuple) value.getValueAsPigType();
        try {
            return (int) t.get(0);
        }
        catch (ExecException e) {
            throw new RuntimeException("Failed to find partition", e);
        }
    }
}