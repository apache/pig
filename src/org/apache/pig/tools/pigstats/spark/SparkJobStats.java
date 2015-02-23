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
import scala.Option;

import com.google.common.collect.Maps;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Counters;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.spark.JobMetricsListener;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.PlanVisitor;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.OutputStats;
import org.apache.pig.tools.pigstats.PigStats;

import org.apache.spark.executor.ShuffleReadMetrics;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.executor.TaskMetrics;

public class SparkJobStats extends JobStats {

  private int jobId;
  private Map<String, Long> stats = Maps.newLinkedHashMap();

  protected SparkJobStats(int jobId, PigStats.JobGraph plan) {
      this(String.valueOf(jobId), plan);
      this.jobId = jobId;
  }

  protected SparkJobStats(String jobId, PigStats.JobGraph plan) {
      super(jobId, plan);
  }

  public void addOutputInfo(POStore poStore,  boolean success,
                            JobMetricsListener jobMetricsListener,
                            Configuration conf) {
      // TODO: Compute #records
      long bytes = getOutputSize(poStore, conf);
      OutputStats outputStats = new OutputStats(poStore.getSFile().getFileName(),
          bytes, 1, success);
      outputStats.setPOStore(poStore);
      outputStats.setConf(conf);
      if( !poStore.isTmpStore()) {
          outputs.add(outputStats);
      }
  }

  public void collectStats(JobMetricsListener jobMetricsListener) {
      Map<String, List<TaskMetrics>> taskMetrics = jobMetricsListener.getJobMetric(jobId);
      if (taskMetrics == null) {
        throw new RuntimeException("No task metrics available for jobId " + jobId);
      }

      stats = combineTaskMetrics(taskMetrics);
  }

  public Map<String, Long> getStats() {
      return stats;
  }

  private Map<String, Long> combineTaskMetrics(Map<String, List<TaskMetrics>> jobMetric) {
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
      boolean inputMetricExist = false;
      boolean outputMetricExist = false;
      boolean shuffleReadMetricExist = false;
      boolean shuffleWriteMetricExist = false;

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
              if (!taskMetrics.inputMetrics().isEmpty()) {
                inputMetricExist = true;
                bytesRead += taskMetrics.inputMetrics().get().bytesRead();
              }

              if (!taskMetrics.outputMetrics().isEmpty()) {
                outputMetricExist = true;
                bytesWritten += taskMetrics.outputMetrics().get().bytesWritten();
              }

              Option<ShuffleReadMetrics> shuffleReadMetricsOption = taskMetrics.shuffleReadMetrics();
              if (!shuffleReadMetricsOption.isEmpty()) {
                shuffleReadMetricExist = true;
                remoteBlocksFetched += shuffleReadMetricsOption.get().remoteBlocksFetched();
                localBlocksFetched += shuffleReadMetricsOption.get().localBlocksFetched();
                fetchWaitTime += shuffleReadMetricsOption.get().fetchWaitTime();
                remoteBytesRead += shuffleReadMetricsOption.get().remoteBytesRead();
              }

              Option<ShuffleWriteMetrics> shuffleWriteMetricsOption = taskMetrics.shuffleWriteMetrics();
              if (!shuffleWriteMetricsOption.isEmpty()) {
                shuffleWriteMetricExist = true;
                shuffleBytesWritten += shuffleWriteMetricsOption.get().shuffleBytesWritten();
                shuffleWriteTime += shuffleWriteMetricsOption.get().shuffleWriteTime();
              }

            }
          }
        }
      }

      results.put("EexcutorDeserializeTime", executorDeserializeTime);
      results.put("ExecutorRunTime", executorRunTime);
      results.put("ResultSize", resultSize);
      results.put("JvmGCTime", jvmGCTime);
      results.put("ResultSerializationTime", resultSerializationTime);
      results.put("MemoryBytesSpilled", memoryBytesSpilled);
      results.put("DiskBytesSpilled", diskBytesSpilled);
      if (inputMetricExist) {
        results.put("BytesRead", bytesRead);
      }

      if (outputMetricExist) {
        results.put("BytesWritten", bytesWritten);
      }

      if (shuffleReadMetricExist) {
        results.put("RemoteBlocksFetched", remoteBlocksFetched);
        results.put("LocalBlocksFetched", localBlocksFetched);
        results.put("TotalBlocksFetched", localBlocksFetched + remoteBlocksFetched);
        results.put("FetchWaitTime", fetchWaitTime);
        results.put("RemoteBytesRead", remoteBytesRead);
      }

      if (shuffleWriteMetricExist) {
        results.put("ShuffleBytesWritten", shuffleBytesWritten);
        results.put("ShuffleWriteTime", shuffleWriteTime);
      }

      return results;
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
      throw new UnsupportedOperationException();
  }

  @Override
  public Map<String, Long> getMultiStoreCounters() {
      throw new UnsupportedOperationException();
  }

  @Override
  public Map<String, Long> getMultiInputCounters() {
      throw new UnsupportedOperationException();
  }
}
