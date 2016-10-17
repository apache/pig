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

import java.util.Map;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.NullableTuple;

public class HashValuePartitioner extends Partitioner<Writable, Writable> {

    @SuppressWarnings("rawtypes")
    @Override
    public int getPartition(Writable key, Writable value, int numPartitions) {
        int hash = 17;
        Tuple tuple;
        if (value instanceof Tuple) {
            // union optimizer turned off
            tuple = (Tuple) value;
        } else {
            // union followed by order by or skewed join
            tuple = (Tuple)((NullableTuple) value).getValueAsPigType();
        }
        if (tuple != null) {
            for (Object o : tuple.getAll()) {
                if (o != null) {
                    // Skip computing hashcode for bags.
                    // Order of elements in the map/bag may be different on each run
                    if (o instanceof DataBag) {
                        hash = 31 * hash;
                    } else if (o instanceof Map) {
                        // Including size of map as it is easily available
                        // Not doing for DataBag as some implementations actually
                        // iterate through all elements in the bag to get the size.
                        hash = 31 * hash + ((Map) o).size();
                    } else {
                        hash = 31 * hash + o.hashCode();
                    }
                }
            }
        }
        return (hash & Integer.MAX_VALUE) % numPartitions;
    }

}