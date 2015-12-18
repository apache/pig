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

package org.apache.pig.tools.pigstats.spark;

import org.apache.hadoop.mapred.JobConf;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.spark.JobMetricsListener;
import org.apache.pig.backend.hadoop.executionengine.spark.operator.NativeSparkOperator;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkOperator;
import org.apache.pig.tools.pigstats.PigStatsUtil;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.spark.JobExecutionStatus;
import org.apache.spark.SparkJobInfo;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkStatsUtil {

    public static final String SPARK_STORE_COUNTER_GROUP = "Spark Store Counters";
    public static final String SPARK_STORE_RECORD_COUNTER = "Output records in ";
    public static final String SPARK_INPUT_COUNTER_GROUP = "Spark Input Counters";
    public static final String SPARK_INPUT_RECORD_COUNTER = "Input records from ";

    public static void waitForJobAddStats(int jobID,
                                          POStore poStore, SparkOperator sparkOperator,
                                          JobMetricsListener jobMetricsListener,
                                          JavaSparkContext sparkContext,
                                          SparkPigStats sparkPigStats,
                                          JobConf jobConf)
            throws InterruptedException {
        // Even though we are not making any async calls to spark,
        // the SparkStatusTracker can still return RUNNING status
        // for a finished job.
        // Looks like there is a race condition between spark
        // "event bus" thread updating it's internal listener and
        // this driver thread calling SparkStatusTracker.
        // To workaround this, we will wait for this job to "finish".
        jobMetricsListener.waitForJobToEnd(jobID);
        sparkPigStats.addJobStats(poStore, sparkOperator, jobID, jobMetricsListener,
                sparkContext, jobConf);
        jobMetricsListener.cleanup(jobID);
    }

    public static void addFailJobStats(String jobID,
                                       POStore poStore, SparkOperator sparkOperator,
                                       SparkPigStats sparkPigStats,
                                       JobConf jobConf, Exception e) {
        JobMetricsListener jobMetricsListener = null;
        JavaSparkContext sparkContext = null;
        sparkPigStats.addFailJobStats(poStore, sparkOperator, jobID, jobMetricsListener,
                sparkContext, jobConf, e);
    }

    public static String getStoreSparkCounterName(POStore store) {
        String shortName = PigStatsUtil.getShortName(store.getSFile().getFileName());

        StringBuffer sb = new StringBuffer(SPARK_STORE_RECORD_COUNTER);
        sb.append("_");
        sb.append(store.getIndex());
        sb.append("_");
        sb.append(store.getOperatorKey());
        sb.append("_");
        sb.append(shortName);
        return sb.toString();
    }

    public static String getLoadSparkCounterName(POLoad load) {
        String shortName = PigStatsUtil.getShortName(load.getLFile().getFileName());

        StringBuffer sb = new StringBuffer(SPARK_INPUT_RECORD_COUNTER);
        sb.append("_");
        sb.append(load.getOperatorKey());
        sb.append("_");
        sb.append(shortName);
        return sb.toString();
    }

    public static long getStoreSparkCounterValue(POStore store) {
        SparkPigStatusReporter reporter = SparkPigStatusReporter.getInstance();
        return reporter.getCounters().getValue(SPARK_STORE_COUNTER_GROUP, getStoreSparkCounterName(store));
    }

    public static long getLoadSparkCounterValue(POLoad load) {
        SparkPigStatusReporter reporter = SparkPigStatusReporter.getInstance();
        return reporter.getCounters().getValue(SPARK_INPUT_COUNTER_GROUP, getLoadSparkCounterName(load));
    }

    public static boolean isJobSuccess(int jobID,
                                       JavaSparkContext sparkContext) {
        JobExecutionStatus status = getJobInfo(jobID, sparkContext).status();
        if (status == JobExecutionStatus.SUCCEEDED) {
            return true;
        } else if (status != JobExecutionStatus.FAILED) {
            throw new RuntimeException("Unexpected job execution status " +
                    status);
        }

        return false;
    }

    private static SparkJobInfo getJobInfo(int jobID,
                                           JavaSparkContext sparkContext) {
        SparkJobInfo jobInfo = sparkContext.statusTracker().getJobInfo(jobID);
        if (jobInfo == null) {
            throw new RuntimeException("No jobInfo available for jobID "
                    + jobID);
        }

        return jobInfo;
    }

    public static void addNativeJobStats(PigStats ps, NativeSparkOperator nativeSparkOperator) {
        ((SparkPigStats) ps).addNativeJobStats(nativeSparkOperator, nativeSparkOperator.getJobId(), true, null);
    }

    public static void addFailedNativeJobStats(PigStats ps, NativeSparkOperator nativeSparkOperator, Exception e) {
        ((SparkPigStats) ps).addNativeJobStats(nativeSparkOperator, nativeSparkOperator.getJobId(), false, e);
    }
}