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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.spark.SparkJobStats;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.rdd.RDD;
import org.apache.spark.scheduler.SparkListener;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class SparkShims implements Serializable {
    private static final Log LOG = LogFactory.getLog(SparkShims.class);
    public static final String SPARK_VERSION = "pig.spark.version";

    private static SparkShims sparkShims;

    private static SparkShims loadShims(String sparkVersion) throws ReflectiveOperationException {
        Class<?> sparkShimsClass;

        if ("2".equals(sparkVersion)) {
            LOG.info("Initializing shims for Spark 2.x");
            sparkShimsClass = Class.forName("org.apache.pig.backend.hadoop.executionengine.spark.Spark2Shims");
        } else {
            LOG.info("Initializing shims for Spark 1.x");
            sparkShimsClass = Class.forName("org.apache.pig.backend.hadoop.executionengine.spark.Spark1Shims");
        }

        Constructor c = sparkShimsClass.getConstructor();
        return (SparkShims) c.newInstance();
    }

    public static SparkShims getInstance() {
        if (sparkShims == null) {
            String sparkVersion = UDFContext.getUDFContext().getJobConf().get(SPARK_VERSION, "");
            LOG.info("Initializing SparkShims for Spark version: " + sparkVersion);
            String sparkMajorVersion = getSparkMajorVersion(sparkVersion);
            try {
                sparkShims = loadShims(sparkMajorVersion);
            } catch (ReflectiveOperationException e) {
                throw new RuntimeException(e);
            }
        }
        return sparkShims;
    }

    private static String getSparkMajorVersion(String sparkVersion) {
        return sparkVersion.startsWith("2") ? "2" : "1";
    }

    public abstract <T, R> FlatMapFunction<T, R> flatMapFunction(FlatMapFunctionAdapter<T, R> function);

    public abstract <T, K, V> PairFlatMapFunction<T, K, V> pairFlatMapFunction(PairFlatMapFunctionAdapter<T, K, V> function);

    public abstract RDD<Tuple> coalesce(RDD<Tuple> rdd, int numPartitions, boolean shuffle);

    public abstract SparkJobStats sparkJobStats(int jobId, PigStats.JobGraph plan, Configuration conf);

    public abstract SparkJobStats sparkJobStats(String jobId, PigStats.JobGraph plan, Configuration conf);

    public abstract <T> OptionalWrapper<T> wrapOptional(T tuple);

    public abstract SparkListener getJobMetricsListener(Map<Integer, int[]> jobIdToStageId,
                                                        Map<Integer, Integer> stageIdToJobId,
                                                        Map<Integer, Map<String, List<TaskMetrics>>> allJobMetrics,
                                                        Set<Integer> finishedJobIds);

    public abstract void addSparkListener(SparkContext sc, SparkListener sparkListener);

    public interface OptionalWrapper<T> {
        boolean isPresent();

        T get();
    }
}