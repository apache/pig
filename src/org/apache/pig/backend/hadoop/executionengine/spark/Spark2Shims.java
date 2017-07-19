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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.data.Tuple;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.spark.Spark2JobStats;
import org.apache.pig.tools.pigstats.spark.SparkJobStats;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.rdd.PartitionCoalescer;
import org.apache.spark.rdd.RDD;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import scala.Option;
import scala.Tuple2;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Spark2Shims extends SparkShims {
    @Override
    public <T, R> FlatMapFunction flatMapFunction(final FlatMapFunctionAdapter<T, R> function) {
        return new FlatMapFunction<T, R>() {
            @Override
            public Iterator<R> call(T t) throws Exception {
                return function.call(t);
            }
        };
    }

    @Override
    public <T, K, V> PairFlatMapFunction<T, K, V> pairFlatMapFunction(final PairFlatMapFunctionAdapter<T, K, V> function) {
        return new PairFlatMapFunction<T, K, V>() {
            @Override
            public Iterator<Tuple2<K, V>> call(T t) throws Exception {
                return function.call(t);
            }
        };
    }

    @Override
    public RDD<Tuple> coalesce(RDD<Tuple> rdd, int numPartitions, boolean shuffle) {
        return rdd.coalesce(numPartitions, shuffle, Option.<PartitionCoalescer>empty(), null);
    }

    @Override
    public SparkJobStats sparkJobStats(int jobId, PigStats.JobGraph plan, Configuration conf) {
        return new Spark2JobStats(jobId, plan, conf);
    }

    @Override
    public SparkJobStats sparkJobStats(String jobId, PigStats.JobGraph plan, Configuration conf) {
        return new Spark2JobStats(jobId, plan, conf);
    }

    @Override
    public <T> OptionalWrapper<T> wrapOptional(T tuple) {
        final Optional<T> t = (Optional<T>) tuple;

        return new OptionalWrapper<T>() {
            @Override
            public boolean isPresent() {
                return t.isPresent();
            }

            @Override
            public T get() {
                return t.get();
            }
        };
    }

    private static class JobMetricsListener extends SparkListener {
        private final Log LOG = LogFactory.getLog(JobMetricsListener.class);

        private Map<Integer, int[]> jobIdToStageId;
        private Map<Integer, Integer> stageIdToJobId;
        private Map<Integer, Map<String, List<TaskMetrics>>> allJobMetrics;
        private Set<Integer> finishedJobIds;

        JobMetricsListener(final Map<Integer, int[]> jobIdToStageId,
                           final Map<Integer, Integer> stageIdToJobId,
                           final Map<Integer, Map<String, List<TaskMetrics>>> allJobMetrics,
                           final Set<Integer> finishedJobIds) {
            this.jobIdToStageId = jobIdToStageId;
            this.stageIdToJobId = stageIdToJobId;
            this.allJobMetrics = allJobMetrics;
            this.finishedJobIds = finishedJobIds;
        }

        @Override
        public synchronized void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
            int stageId = stageCompleted.stageInfo().stageId();
            int stageAttemptId = stageCompleted.stageInfo().attemptId();
            String stageIdentifier = stageId + "_" + stageAttemptId;
            Integer jobId = stageIdToJobId.get(stageId);
            if (jobId == null) {
                LOG.warn("Cannot find job id for stage[" + stageId + "].");
            } else {
                Map<String, List<TaskMetrics>> jobMetrics = allJobMetrics.get(jobId);
                if (jobMetrics == null) {
                    jobMetrics = Maps.newHashMap();
                    allJobMetrics.put(jobId, jobMetrics);
                }
                List<TaskMetrics> stageMetrics = jobMetrics.get(stageIdentifier);
                if (stageMetrics == null) {
                    stageMetrics = Lists.newLinkedList();
                    jobMetrics.put(stageIdentifier, stageMetrics);
                }
                stageMetrics.add(stageCompleted.stageInfo().taskMetrics());
            }
        }

        @Override
        public synchronized void onJobStart(SparkListenerJobStart jobStart) {
            int jobId = jobStart.jobId();
            int size = jobStart.stageIds().size();
            int[] intStageIds = new int[size];
            for (int i = 0; i < size; i++) {
                Integer stageId = (Integer) jobStart.stageIds().apply(i);
                intStageIds[i] = stageId;
                stageIdToJobId.put(stageId, jobId);
            }
            jobIdToStageId.put(jobId, intStageIds);
        }

        @Override
        public synchronized void onJobEnd(SparkListenerJobEnd jobEnd) {
            finishedJobIds.add(jobEnd.jobId());
            notify();
        }
    }

    @Override
    public SparkListener getJobMetricsListener(Map<Integer, int[]> jobIdToStageId,
                                               Map<Integer, Integer> stageIdToJobId,
                                               Map<Integer, Map<String, List<TaskMetrics>>> allJobMetrics,
                                               Set<Integer> finishedJobIds) {
        return new JobMetricsListener(jobIdToStageId, stageIdToJobId, allJobMetrics, finishedJobIds);
    }

    @Override
    public void addSparkListener(SparkContext sc, SparkListener sparkListener) {
        sc.addSparkListener(sparkListener);
    }

}
