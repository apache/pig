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
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.InputSizeReducerEstimator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.builtin.FindQuantiles;

public class FindQuantilesTez extends FindQuantiles {

    private static final Log LOG = LogFactory.getLog(FindQuantilesTez.class);

    private static TupleFactory tf = TupleFactory.getInstance();
    
    public FindQuantilesTez() {
        super();
    }
    
    public FindQuantilesTez(String[] args) {
        super(args);
    }
    
    @Override
    public Map<String, Object> exec(Tuple in) throws IOException {
        // In Tez, we also need to estimate the quantiles with regard to sample
        // and the special tuple containing the total number of records
        int estimatedNumReducers = -1;
        boolean estimate_sample_quantile = PigMapReduce.sJobConfInternal.get().getBoolean
                (PigProcessor.ESTIMATE_PARALLELISM, false);
        DataBag mySamples = (DataBag)in.get(1);
        this.samples = BagFactory.getInstance().newDefaultBag();
        Iterator<Tuple> iter = mySamples.iterator();
        Tuple t;
        //total input rows for the order by
        long totalInputRows = 0;
        long sampleSize = 0;
        while (iter.hasNext()) {
            t = iter.next();
            if (t.get(t.size() - 1) != null) {
                totalInputRows += (Long)t.get(t.size() - 1);
            }
            if (t.get(t.size() - 2) != null) {
                sampleSize += getMemorySize(t);
            }
            if (t.size() > 2) {
                Tuple newTuple = tf.newTuple(t.size()-2);
                for (int i=0;i<t.size()-2;i++) {
                    newTuple.set(i, t.get(i));
                }
                this.samples.add(newTuple);
            }
        }
        if (estimate_sample_quantile) {
            Integer specifiedNumQuantiles = (Integer)in.get(0);

            long bytesPerTask = PigMapReduce.sJobConfInternal.get().getLong(InputSizeReducerEstimator.BYTES_PER_REDUCER_PARAM,
                    InputSizeReducerEstimator.DEFAULT_BYTES_PER_REDUCER);

            long estimatedInputSize = (long)((double)sampleSize/mySamples.size() * totalInputRows);
            estimatedNumReducers = (int)Math.ceil((double)estimatedInputSize/bytesPerTask);
            estimatedNumReducers = Math.min(estimatedNumReducers, InputSizeReducerEstimator.DEFAULT_MAX_REDUCER_COUNT_PARAM);

            LOG.info("Estimating parallelism: estimatedInputSize is " + estimatedInputSize + ". bytesPerTask is " + bytesPerTask + ". estimatedNumQuantiles is " + estimatedNumReducers + ".");

            this.numQuantiles = estimatedNumReducers;
            LOG.info("Use estimated parallelism instead:" + estimatedNumReducers);
        }
        Map<String, Object> result = super.exec(in);
        if (estimate_sample_quantile) {
            result.put(PigProcessor.ESTIMATED_NUM_PARALLELISM, numQuantiles);
        }
        PigProcessor.sampleMap = result;
        return result;
    }

    // the last field of the tuple is a tuple for memory size and disk size
    protected long getMemorySize(Tuple t) {
        int s = t.size();
        try {
            return (Long) t.get(s - 2);
        } catch (ExecException e) {
            throw new RuntimeException(
                    "Unable to retrive the size field from tuple.", e);
        }
    }
}
