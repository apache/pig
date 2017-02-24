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
package org.apache.pig.builtin;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * This partitioner should be used with extreme caution and only in cases
 * where the order of output records is guaranteed to be same. If the order of
 * output records can vary on retries which is mostly the case, map reruns
 * due to shuffle fetch failures can lead to data being partitioned differently
 * and result in incorrect output due to loss or duplication of data.
 * Refer PIG-5041 for more details.
 *
 * This will be removed in the next release as it is risky to use in most cases.
 */
@Deprecated
public class RoundRobinPartitioner extends Partitioner<Writable, Writable>
        implements Configurable {

    /**
     * Batch size for round robin partitioning. Batch size number of records
     * will be distributed to each partition in a round robin fashion. Default
     * value is 0 which distributes each record in a circular fashion. Higher
     * number for batch size can be used to increase probability of keeping
     * similar records in the same partition if output is already sorted and get
     * better compression.
     */
    public static String PIG_ROUND_ROBIN_PARTITIONER_BATCH_SIZE = "pig.round.robin.partitioner.batch.size";
    private int num = -1;
    private int batchSize = 0;
    private int currentBatchCount = 0;
    private Configuration conf;

    @Override
    public int getPartition(Writable key, Writable value, int numPartitions) {
        if (batchSize > 0) {
            if (currentBatchCount == 0) {
                num = ++num % numPartitions;
            }
            if (++currentBatchCount == batchSize) {
                currentBatchCount = 0;
            }
        } else {
            num = ++num % numPartitions;
        }
        return num;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        batchSize = conf.getInt(PIG_ROUND_ROBIN_PARTITIONER_BATCH_SIZE, 0);
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

}
