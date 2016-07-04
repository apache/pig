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
package org.apache.pig.backend.hadoop.executionengine.spark;

import java.lang.reflect.Method;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.IndexedKey;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.PigNullableWritable;
import org.apache.spark.Partitioner;

/**
 * Spark Partitioner that wraps a custom partitioner that implements
 * org.apache.hadoop.mapreduce.Partitioner interface.
 *
 * Since Spark's shuffle API takes a different parititioner class
 * (@see org.apache.spark.Partitioner) compared to MapReduce, we need to
 * wrap custom partitioners written for MapReduce inside this Spark Partitioner.
 *
 * MR Custom partitioners are expected to implement getPartition() with
 * specific arguments:
 *   public int getPartition(PigNullableWritable key, Writable value, int numPartitions)
 * For an example of such a partitioner,
 * @see org.apache.pig.test.utils.SimpleCustomPartitioner
 */
public class MapReducePartitionerWrapper extends Partitioner {
    private static final Log LOG = LogFactory.getLog(MapReducePartitionerWrapper.class);

    private int numPartitions;
    private String partitionerName;
    // MR's Partitioner interface is not serializable.
    // And since it is not serializable, it cannot be initialized in the constructor
    // (in Spark's DAG scheduler thread in Spark Driver),
    // To workaround this, It will be lazily initialized inside the map task
    // (Executor thread) first time that getPartitions() gets called.
    transient private org.apache.hadoop.mapreduce.Partitioner<PigNullableWritable, Writable>
            mapredPartitioner = null;
    transient private Method getPartitionMethod = null;

    public MapReducePartitionerWrapper(String partitionerName,
                      int numPartitions) {
        if (partitionerName == null) {
            throw new RuntimeException("MapReduce Partitioner cannot be null.");
        }

        this.partitionerName = partitionerName;
        this.numPartitions = numPartitions;
    }

    public int numPartitions() {
        return numPartitions;
    }

    public int getPartition(final Object key) {
        try {

            PigNullableWritable writeableKey = new PigNullableWritable() {
                public Object getValueAsPigType() {
                    if (key instanceof IndexedKey) {
                        IndexedKey indexedKey = (IndexedKey) key;
                        this.setIndex(indexedKey.getIndex());
                        return indexedKey.getKey();
                    } else {
                        return key;
                    }
                }
            };


            // Lazy initialization
            // Synchronized because multiple (map) tasks in the same Spark Executor
            // may call getPartition, attempting to initialize at the same time.
            if (mapredPartitioner == null) {
                synchronized (this) {
                    // check again for race condition
                    if (mapredPartitioner == null) {
                        Class<?> mapredPartitionerClass =
                                PigContext.resolveClassName(partitionerName);
                        Configuration conf = new Configuration();
                        mapredPartitioner = (org.apache.hadoop.mapreduce.Partitioner<PigNullableWritable, Writable>)
                                ReflectionUtils.newInstance(mapredPartitionerClass, conf);
                        getPartitionMethod = mapredPartitionerClass.getMethod(
                                "getPartition",
                                PigNullableWritable.class,
                                org.apache.hadoop.io.Writable.class,
                                int.class);
                    }
                }
            }

            // MR Parititioner getPartition takes a value argument as well, but
            // Spark's Partitioner only accepts the key argument.
            // In practice, MR Partitioners ignore the value. However, it's
            // possible that some don't.
            // TODO: We could handle this case by packaging the value inside the
            // key (conditioned on some config option, since this will balloon
            // memory usage). PIG-4575.
            int partition = (Integer) getPartitionMethod.invoke(mapredPartitioner,
                    writeableKey, null, numPartitions);

            if (LOG.isDebugEnabled())
                LOG.debug("MapReduce Partitioner partition number for key " + key +
                        " is " + partition);
            return partition;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public boolean equals(Object other) {
        boolean var4;
        if(other instanceof MapReducePartitionerWrapper) {
            MapReducePartitionerWrapper var3 = (MapReducePartitionerWrapper)other;
            var4 = var3.numPartitions() == this.numPartitions();
        } else {
            var4 = false;
        }

        return var4;
    }

    public int hashCode() {
        return this.numPartitions();
    }
}
