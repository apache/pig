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

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.pig.PigConstants;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.PigNullableWritable;

/**
 * This class provides a partitioner when the RollupOptimizer is activated. The
 * map output key space is partitioned by the dimension which the pivot position
 * is assigned to. For example, we have a tuple (year, month, day, payload) and
 * the pivot is 2, so the map output space will be partitioned by month, which
 * means that there will be one reducer per month.
 */
public class RollupHIIPartitioner extends HashPartitioner<PigNullableWritable, Writable> implements Configurable {

    protected MessageDigest m = null;
    protected int pivot = 0;
    protected int rollupFieldIndex = 0;
    protected int rollupOldFieldIndex = 0;
    protected boolean pivotZero = false;
    protected int length = 0;

    public RollupHIIPartitioner() throws NoSuchAlgorithmException {
        m = MessageDigest.getInstance("MD5");
    }

    public void setPivot(int pvt) {
        pivot = pvt;
    }

    public int getPartition(PigNullableWritable key, Writable value, int numPartitions) {
        try {
            Tuple t = (Tuple) key.getValueAsPigType();
            if (pivot == -1)
                return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;

            // We use IRG --> only one reducer.
            if (pivotZero) {
                return 0;
            } else {
                // We transfer them to the determined reducer.
                if (t.get(pivot - 1) == null) {
                    // Check if it is a marker tuple
                    if (t.size() > length) {
                        int lenSpecial = t.size();
                        // Send it to the reducer which has been decided before
                        // by the addition dimension we added in the cleanup
                        // phase of each map.
                        return (Integer) t.get(lenSpecial - 1);
                    } else
                        return 0;
                } else {
                    // We partition the key output space by the dimension which
                    // the pivot is assigned at. We use MD5 instead of hash
                    // partitioner because partition with MD5 will provide us a
                    // better randomization than the default hash.
                    m.reset();
                    for (int i = rollupFieldIndex; i < pivot; i++) {
                        Object a = t.get(i);
                        ByteArrayOutputStream bos = new ByteArrayOutputStream();
                        ObjectOutputStream oos = new ObjectOutputStream(bos);
                        oos.writeObject(a);
                        oos.flush();
                        oos.close();
                        bos.close();
                        byte []tmp = bos.toByteArray();
                        m.update(ByteBuffer.allocate(tmp.length).put(tmp).array());
                    }
                    return (m.digest()[15] & Integer.MAX_VALUE) % numPartitions;
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Configuration getConf() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setConf(Configuration conf) {
        //Get the pivot
        pivot = conf.getInt(PigConstants.PIG_HII_ROLLUP_PIVOT, -1);
        //Get the index of the first field involves in ROLLUP
        rollupFieldIndex = conf.getInt(PigConstants.PIG_HII_ROLLUP_FIELD_INDEX, 0);
        //Get the original index of the first field involves in ROLLUP in case it was moved to the end
        //(if we have the combination of cube and rollup)
        rollupOldFieldIndex = conf.getInt(PigConstants.PIG_HII_ROLLUP_OLD_FIELD_INDEX, 0);
        //Get the size of total fields that involve in CUBE clause
        length = conf.getInt(PigConstants.PIG_HII_NUMBER_TOTAL_FIELD, 0);
        // We must check the original pivot value before it is updated
        // if there are many rollup/cube.
        if (pivot == 0) {
            pivotZero = true;
        }

        //The Rollup was moved to the end of the clause, because there is(are)
        //Cube operator, so we need to update the pivot position.
        if (rollupFieldIndex != 0) {
            pivot = pivot + rollupFieldIndex;
        }
    }
}
