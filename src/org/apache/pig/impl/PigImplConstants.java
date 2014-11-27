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

package org.apache.pig.impl;

import org.apache.pig.classification.InterfaceAudience;

/**
 * Private, internal constants for use by Pig itself. Please see
 * {@link org.apache.pig.PigConstants} if looking for public constants.
 */
@InterfaceAudience.Private
public class PigImplConstants {

    private PigImplConstants() {
        throw new IllegalStateException();
    }

    /**
     * {@value} is a pig-internal properties key for serializing
     * the set of disabled optimizer rules.
     */
    public static final String PIG_OPTIMIZER_RULES_KEY = "pig.optimizer.rules";

    /**
     * Used by pig to indicate that current job has been converted to run in local mode
     */
    public static final String CONVERTED_TO_LOCAL = "pig.job.converted.local";

    /**
     * Used by pig to indicate that current job has been converted to run in fetch mode
     */
    public static final String CONVERTED_TO_FETCH = "pig.job.converted.fetch";

    /**
     * Indicate the split index of the task. Used by merge cogroup
     */
    public static final String PIG_SPLIT_INDEX = "pig.split.index";

    /**
     * Parallelism for the reducer
     */
    public static final String REDUCER_DEFAULT_PARALLELISM = "pig.info.reducers.default.parallel";
    public static final String REDUCER_REQUESTED_PARALLELISM = "pig.info.reducers.requested.parallel";
    public static final String REDUCER_ESTIMATED_PARALLELISM = "pig.info.reducers.estimated.parallel";

    /**
     * Parallelism to be used for CROSS operation by GFCross UDF
     */
    public static final String PIG_CROSS_PARALLELISM = "pig.cross.parallelism";
}
