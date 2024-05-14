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

import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Counters;
import org.apache.pig.PigWarning;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.spark.JobStatisticCollector;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkOperator;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.PlanVisitor;
import org.apache.pig.tools.pigstats.InputStats;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.OutputStats;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.PigStatsUtil;
import org.apache.spark.executor.ShuffleReadMetrics;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.executor.TaskMetrics;

import com.google.common.collect.Maps;

public class SparkJobStats extends JobStats {

    private int jobId;
    private Map<String, Long> stats = Maps.newLinkedHashMap();
    private boolean disableCounter;
    protected Counters counters = null;
    public static String FS_COUNTER_GROUP = "FS_GROUP";
    private Map<String, SparkCounter<Map<String, Long>>> warningCounters = null;

    protected SparkJobStats(int jobId, PigStats.JobGraph plan, Configuration conf) {
        this(String.valueOf(jobId), plan, conf);
        this.jobId = jobId;
    }

    protected SparkJobStats(String jobId, PigStats.JobGraph plan, Configuration conf) {
        super(jobId, plan);
        setConf(conf);
    }

    @Override
    public void setConf(Configuration conf) {
        super.setConf(conf);
        disableCounter = conf.getBoolean("pig.disable.counter", false);
        initializeHadoopCounter();
    }

    public void addOutputInfo(POStore poStore, boolean success,
                              JobStatisticCollector jobStatisticCollector) {
        if (!poStore.isTmpStore()) {
            long bytes = getOutputSize(poStore, conf);
            long recordsCount = -1;
            if (disableCounter == false) {
                recordsCount = SparkStatsUtil.getRecordCount(poStore);
            }
            OutputStats outputStats = new OutputStats(poStore.getSFile().getFileName(),
                    bytes, recordsCount, success);
            outputStats.setPOStore(poStore);
            outputStats.setConf(conf);

            outputs.add(outputStats);
        }
    }

    public void addInputStats(POLoad po, boolean success,
                              boolean singleInput) {

        long recordsCount = -1;
        if (disableCounter == false) {
            recordsCount = SparkStatsUtil.getRecordCount(po);
        }
        long bytesRead = -1;
        if (singleInput && stats.get("BytesRead") != null) {
            bytesRead = stats.get("BytesRead");
        }
        InputStats inputStats = new InputStats(po.getLFile().getFileName(),
                bytesRead, recordsCount, success);
        inputStats.setConf(conf);

        inputs.add(inputStats);
    }

    public void collectStats(JobStatisticCollector jobStatisticCollector) {
        if (jobStatisticCollector != null) {
            Map<String, List<TaskMetrics>> taskMetrics = jobStatisticCollector.getJobMetric(jobId);
            if (taskMetrics == null) {
                throw new RuntimeException("No task metrics available for jobId " + jobId);
            }
            stats = combineTaskMetrics(taskMetrics);
        }
    }

    protected Map<String, Long> combineTaskMetrics(
                    Map<String, List<TaskMetrics>> jobMetric) {
        Map<String, Long> results = Maps.newLinkedHashMap();

        long executorDeserializeTime = 0;
        long executorRunTime = 0;
        long resultSize = 0;
        long jvmGCTime = 0;
        long resultSerializationTime = 0;
        long memoryBytesSpilled = 0;
        long diskBytesSpilled = 0;
        long bytesRead = 0;
        long bytesWritten = 0;
        long remoteBlocksFetched = 0;
        long localBlocksFetched = 0;
        long fetchWaitTime = 0;
        long remoteBytesRead = 0;
        long shuffleBytesWritten = 0;
        long shuffleWriteTime = 0;

        for (List<TaskMetrics> stageMetric : jobMetric.values()) {
            if (stageMetric != null) {
                for (TaskMetrics taskMetrics : stageMetric) {
                    if (taskMetrics != null) {
                        executorDeserializeTime += taskMetrics.executorDeserializeTime();
                        executorRunTime += taskMetrics.executorRunTime();
                        resultSize += taskMetrics.resultSize();
                        jvmGCTime += taskMetrics.jvmGCTime();
                        resultSerializationTime += taskMetrics.resultSerializationTime();
                        memoryBytesSpilled += taskMetrics.memoryBytesSpilled();
                        diskBytesSpilled += taskMetrics.diskBytesSpilled();
                        bytesRead += taskMetrics.inputMetrics().bytesRead();

                        bytesWritten += taskMetrics.outputMetrics().bytesWritten();

                        ShuffleReadMetrics shuffleReadMetricsOption = taskMetrics.shuffleReadMetrics();
                        remoteBlocksFetched += shuffleReadMetricsOption.remoteBlocksFetched();
                        localBlocksFetched += shuffleReadMetricsOption.localBlocksFetched();
                        fetchWaitTime += shuffleReadMetricsOption.fetchWaitTime();
                        remoteBytesRead += shuffleReadMetricsOption.remoteBytesRead();

                        ShuffleWriteMetrics shuffleWriteMetricsOption = taskMetrics.shuffleWriteMetrics();
                        shuffleBytesWritten += shuffleWriteMetricsOption.bytesWritten();
                        shuffleWriteTime += shuffleWriteMetricsOption.writeTime();
                    }
                }
            }
        }

        results.put("ExcutorDeserializeTime", executorDeserializeTime);
        results.put("ExecutorRunTime", executorRunTime);
        results.put("ResultSize", resultSize);
        results.put("JvmGCTime", jvmGCTime);
        results.put("ResultSerializationTime", resultSerializationTime);
        results.put("MemoryBytesSpilled", memoryBytesSpilled);
        results.put("DiskBytesSpilled", diskBytesSpilled);

        results.put("BytesRead", bytesRead);
        hdfsBytesRead = bytesRead;
        counters.incrCounter(FS_COUNTER_GROUP, PigStatsUtil.HDFS_BYTES_READ, hdfsBytesRead);

        results.put("BytesWritten", bytesWritten);
        hdfsBytesWritten = bytesWritten;
        counters.incrCounter(FS_COUNTER_GROUP, PigStatsUtil.HDFS_BYTES_WRITTEN, hdfsBytesWritten);

        results.put("RemoteBlocksFetched", remoteBlocksFetched);
        results.put("LocalBlocksFetched", localBlocksFetched);
        results.put("TotalBlocksFetched", localBlocksFetched + remoteBlocksFetched);
        results.put("FetchWaitTime", fetchWaitTime);
        results.put("RemoteBytesRead", remoteBytesRead);

        results.put("ShuffleBytesWritten", shuffleBytesWritten);
        results.put("ShuffleWriteTime", shuffleWriteTime);

        return results;
    }
    public Map<String, Long> getStats() {
        return stats;
    }

    @Override
    public String getJobId() {
        return String.valueOf(jobId);
    }

    @Override
    public void accept(PlanVisitor v) throws FrontendException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getDisplayString() {
        return null;
    }

    @Override
    public int getNumberMaps() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getNumberReduces() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getMaxMapTime() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getMinMapTime() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getAvgMapTime() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getMaxReduceTime() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getMinReduceTime() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getAvgREduceTime() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getMapInputRecords() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getMapOutputRecords() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getReduceInputRecords() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getReduceOutputRecords() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getSMMSpillCount() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getProactiveSpillCountObjects() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getProactiveSpillCountRecs() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Counters getHadoopCounters() {
        return counters;
    }

    @Override
    public Map<String, Long> getMultiStoreCounters() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Long> getMultiInputCounters() {
        throw new UnsupportedOperationException();
    }

    public void setAlias(SparkOperator sparkOperator) {
        SparkScriptState ss = (SparkScriptState) SparkScriptState.get();
        SparkScriptState.SparkScriptInfo sparkScriptInfo = ss.getScriptInfo();
        annotate(ALIAS, sparkScriptInfo.getAlias(sparkOperator));
        annotate(ALIAS_LOCATION, sparkScriptInfo.getAliasLocation(sparkOperator));
        annotate(FEATURE, sparkScriptInfo.getPigFeatures(sparkOperator));
    }

    private void initializeHadoopCounter() {
        counters = new Counters();
        Counters.Group fsGrp = counters.addGroup(FS_COUNTER_GROUP, FS_COUNTER_GROUP);
        fsGrp.addCounter(PigStatsUtil.HDFS_BYTES_READ, PigStatsUtil.HDFS_BYTES_READ, 0);
        fsGrp.addCounter(PigStatsUtil.HDFS_BYTES_WRITTEN, PigStatsUtil.HDFS_BYTES_WRITTEN, 0);
    }


    public Map<String, SparkCounter<Map<String, Long>>> getWarningCounters() {
        return warningCounters;
    }

    public void initWarningCounters() {
        SparkCounters counters = SparkPigStatusReporter.getInstance().getCounters();
        SparkCounterGroup<Map<String, Long>> sparkCounterGroup = counters.getSparkCounterGroups().get(
                PigWarning.class.getCanonicalName());
        if (sparkCounterGroup != null) {
            this.warningCounters = sparkCounterGroup.getSparkCounters();
        }
    }
}
