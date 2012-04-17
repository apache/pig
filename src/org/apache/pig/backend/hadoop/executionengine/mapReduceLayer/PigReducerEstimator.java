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
package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;

import java.io.IOException;
import java.util.List;

/**
 * Interface to implement when you want to use a custom approach to estimating
 * the number of reducers for a job.
 *
 * @see InputSizeReducerEstimator
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface PigReducerEstimator {

    static final String BYTES_PER_REDUCER_PARAM = "pig.exec.reducers.bytes.per.reducer";
    static final String MAX_REDUCER_COUNT_PARAM = "pig.exec.reducers.max";

    static final long DEFAULT_BYTES_PER_REDUCER = 1000 * 1000 * 1000;
    static final int DEFAULT_MAX_REDUCER_COUNT_PARAM = 999;

    /**
     * Estimate the number of reducers for a given job based on the collection
     * of load funcs passed.
     *
     * @param conf the job configuration
     * @param poLoadList list of POLoads used in the jobs physical plan
     * @param job job instance
     * @return the number of reducers to use
     * @throws IOException
     */
    public int estimateNumberOfReducers(Configuration conf, List<POLoad> poLoadList, Job job)
        throws IOException;
}
