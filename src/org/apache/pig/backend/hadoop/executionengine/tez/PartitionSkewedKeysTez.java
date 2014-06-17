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

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.InputSizeReducerEstimator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.builtin.PartitionSkewedKeys;

public class PartitionSkewedKeysTez extends PartitionSkewedKeys {
    private static final Log LOG = LogFactory.getLog(PartitionSkewedKeysTez.class);

    public PartitionSkewedKeysTez() {
        super();
    }

    public PartitionSkewedKeysTez(String[] args) {
        super(args);
    }

    @Override
    public Map<String, Object> exec(Tuple in) throws IOException {
        if (in == null || in.size() == 0) {
            return null;
        }
        
        int estimatedNumReducers = -1;
        boolean estimate_sample_quantile = PigMapReduce.sJobConfInternal.get().getBoolean
                (PigProcessor.ESTIMATE_PARALLELISM, false);
        if (estimate_sample_quantile) {
            int specifiedNumReducer = (Integer) in.get(0);
            DataBag samples = (DataBag) in.get(1);

            long totalSampleSize = 0;
            long totalInputRows = 0;
            Iterator<Tuple> iter = samples.iterator();
            while (iter.hasNext()) {
                Tuple t = iter.next();
                totalInputRows += (Long)t.get(t.size() - 1);
                totalSampleSize += getMemorySize(t);
            }
            long totalSampleCount_ = samples.size();

            long estimatedInputSize = (long)((double)totalSampleSize/totalSampleCount_ * totalInputRows);

            long bytesPerTask = PigMapReduce.sJobConfInternal.get().getLong(InputSizeReducerEstimator.BYTES_PER_REDUCER_PARAM,
                    InputSizeReducerEstimator.DEFAULT_BYTES_PER_REDUCER);

            estimatedNumReducers = (int)Math.ceil((double)estimatedInputSize/bytesPerTask);
            estimatedNumReducers = Math.min(estimatedNumReducers, InputSizeReducerEstimator.DEFAULT_MAX_REDUCER_COUNT_PARAM);

            LOG.info("Estimating parallelism: estimatedInputSize is " + estimatedInputSize + ". bytesPerTask is " + bytesPerTask + ". estimatedNumReducers is " + estimatedNumReducers + ".");

            this.totalReducers_ = estimatedNumReducers;
            LOG.info("Use estimated reducer instead:" + estimatedNumReducers + ", orig: " + specifiedNumReducer);
        }
        Map<String, Object> result = super.exec(in);
        if (estimate_sample_quantile) {
            result.put(PigProcessor.ESTIMATED_NUM_PARALLELISM, totalReducers_);
        }
        PigProcessor.sampleMap = result;
        return result;
    }
}
