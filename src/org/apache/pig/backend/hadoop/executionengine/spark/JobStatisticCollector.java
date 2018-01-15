/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.pig.backend.hadoop.executionengine.spark;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.SparkListener;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class JobStatisticCollector {

    private final Map<Integer, int[]> jobIdToStageId = Maps.newHashMap();
    private final Map<Integer, Integer> stageIdToJobId = Maps.newHashMap();
    private final Map<Integer, Map<String, List<TaskMetrics>>> allJobStatistics = Maps.newHashMap();
    private final Set<Integer> finishedJobIds = Sets.newHashSet();

    private SparkListener sparkListener;

    public SparkListener getSparkListener() {
        if (sparkListener == null) {
            sparkListener = SparkShims.getInstance()
                    .getJobMetricsListener(jobIdToStageId, stageIdToJobId, allJobStatistics, finishedJobIds);
        }
        return sparkListener;
    }

    public Map<String, List<TaskMetrics>> getJobMetric(int jobId) {
        synchronized (sparkListener) {
            return allJobStatistics.get(jobId);
        }
    }

    public void waitForJobToEnd(int jobId) throws InterruptedException {
        synchronized (sparkListener) {
            while (!finishedJobIds.contains(jobId)) {
                sparkListener.wait();
            }

            finishedJobIds.remove(jobId);
        }
    }

    public void cleanup(int jobId) {
        synchronized (sparkListener) {
            allJobStatistics.remove(jobId);
            jobIdToStageId.remove(jobId);
            Iterator<Map.Entry<Integer, Integer>> iterator = stageIdToJobId.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Integer, Integer> entry = iterator.next();
                if (entry.getValue() == jobId) {
                    iterator.remove();
                }
            }
        }
    }

    public void reset() {
        synchronized (sparkListener) {
            stageIdToJobId.clear();
            jobIdToStageId.clear();
            allJobStatistics.clear();
            finishedJobIds.clear();
        }
    }
}
