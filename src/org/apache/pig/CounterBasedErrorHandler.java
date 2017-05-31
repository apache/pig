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
package org.apache.pig;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.tools.pigstats.PigStatusReporter;

public class CounterBasedErrorHandler implements ErrorHandler {

    public static final String ERROR_HANDLER_COUNTER_GROUP = "error_Handler";
    public static final String ERROR_COUNT = "bad_record_count";
    public static final String RECORD_COUNT = "record__count";

    private final long minErrors;
    private final float errorThreshold; // fraction of errors allowed

    public CounterBasedErrorHandler() {
        Configuration conf = UDFContext.getUDFContext().getJobConf();
        this.minErrors = conf.getLong(PigConfiguration.PIG_ERROR_HANDLING_MIN_ERROR_RECORDS,
                0);
        this.errorThreshold = conf.getFloat(
                PigConfiguration.PIG_ERROR_HANDLING_THRESHOLD_PERCENT, 0.0f);
    }

    @Override
    public void onSuccess(String uniqueSignature) {
        incAndGetCounter(uniqueSignature, RECORD_COUNT);
    }

    @Override
    public void onError(String uniqueSignature, Exception e) {
        long numErrors = incAndGetCounter(uniqueSignature, ERROR_COUNT);
        long numRecords = incAndGetCounter(uniqueSignature, RECORD_COUNT);
        boolean exceedThreshold = hasErrorExceededThreshold(numErrors,
                numRecords);
        if (exceedThreshold) {
            throw new RuntimeException(
                    "Exceeded the error rate while processing records. The latest error seen  ",
                    e);
        }
    }

    private boolean hasErrorExceededThreshold(long numErrors, long numRecords) {
        if (numErrors > 0 && errorThreshold <= 0) { // no errors are tolerated
            return true;
        }
        double errRate = numErrors / (double) numRecords;
        // If we have more than the min allowed errors and if it exceeds the
        // threshold
        if (numErrors >= minErrors && errRate > errorThreshold) {
            return true;
        }
        return false;
    }

    public long getRecordCount(String signature) {
        Counter counter = getCounter(signature, RECORD_COUNT);
        return counter.getValue();
    }

    private long incAndGetCounter(String signature, String counterName) {
        Counter counter = getCounter(signature, counterName);
        counter.increment(1);
        return counter.getValue();
    }

    /**
     * Get Counter for a given counterName and signature
     * 
     * @param counterName
     * @param signature
     * @return
     */
    private Counter getCounter(String signature, String counterName) {
        PigStatusReporter reporter = PigStatusReporter.getInstance();
        @SuppressWarnings("deprecation")
        Counter counter = reporter.getCounter(
                ERROR_HANDLER_COUNTER_GROUP,
                getCounterNameForStore(counterName, signature));
        return counter;
    }

    private String getCounterNameForStore(String counterNamePrefix,
            String signature) {
        StringBuilder counterName = new StringBuilder()
                .append(counterNamePrefix).append("_").append(signature);
        return counterName.toString();
    }
}
