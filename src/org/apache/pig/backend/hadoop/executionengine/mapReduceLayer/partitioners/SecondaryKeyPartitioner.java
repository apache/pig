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

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.HDataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.PigNullableWritable;

public class SecondaryKeyPartitioner extends HashPartitioner<PigNullableWritable
, Writable> implements Configurable {
    Byte kt;
    @Override
    public int getPartition(PigNullableWritable key, Writable value,
            int numPartitions) {
        try {
            Tuple t = (Tuple)key.getValueAsPigType();
            PigNullableWritable realKey = HDataType.getWritableComparableTypes(t.get(0), kt);
            return super.getPartition(realKey, value, numPartitions);
        } catch (ExecException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Configuration getConf() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setConf(Configuration conf) {
        String kts = conf.get("pig.reduce.key.type");
        kt = Byte.valueOf(kts);
    }
}
