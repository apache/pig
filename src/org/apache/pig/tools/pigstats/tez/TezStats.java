/**
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
package org.apache.pig.tools.pigstats.tez;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobID;
import org.apache.pig.PigRunner.ReturnCode;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.NativeMapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.tez.NativeTezOper;
import org.apache.pig.backend.hadoop.executionengine.tez.TezExecType;
import org.apache.pig.backend.hadoop.executionengine.tez.TezJob;
import org.apache.pig.backend.hadoop.executionengine.tez.TezOpPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.tez.TezOperPlan;
import org.apache.pig.backend.hadoop.executionengine.tez.TezOperator;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.tools.pigstats.InputStats;
import org.apache.pig.tools.pigstats.OutputStats;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.mapreduce.MRJobStats;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.counters.CounterGroup;
import org.apache.tez.common.counters.DAGCounter;
import org.apache.tez.common.counters.FileSystemCounter;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGStatus;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class TezStats extends PigStats {
    private static final Log LOG = LogFactory.getLog(TezStats.class);

    public static final String DAG_COUNTER_GROUP = DAGCounter.class.getName();
    public static final String FS_COUNTER_GROUP = FileSystemCounter.class.getName();
    public static final String TASK_COUNTER_GROUP = TaskCounter.class.getName();

    private TezJob tezJob;
    private List<String> dagStatsStrings;
    private Map<String, TezTaskStats> tezOpVertexMap;
    private List<TezTaskStats> taskStatsToBeRemoved;

    /**
     * This class builds the Tez DAG from a Tez plan.
     */
    private class JobGraphBuilder extends TezOpPlanVisitor {
        public JobGraphBuilder(TezOperPlan plan) {
            super(plan, new DependencyOrderWalker<TezOperator, TezOperPlan>(plan));
        }

        @Override
        public void visitTezOp(TezOperator tezOp) throws VisitorException {
            TezTaskStats currStats =
                    new TezTaskStats(tezOp.getOperatorKey().toString(), jobPlan);
            jobPlan.add(currStats);
            List<TezOperator> preds = getPlan().getPredecessors(tezOp);
            if (preds != null) {
                for (TezOperator pred : preds) {
                    TezTaskStats predStats = tezOpVertexMap.get(pred);
                    if (!jobPlan.isConnected(predStats, currStats)) {
                        jobPlan.connect(predStats, currStats);
                    }
                }
            }
            // Remove VertexGroups (union) from JobGraph since they're not
            // materialized as real vertices by Tez.
            if (tezOp.isVertexGroup()) {
                taskStatsToBeRemoved.add(currStats);
            }
            tezOpVertexMap.put(tezOp.getOperatorKey().toString(), currStats);
        }
    }

    public TezStats(PigContext pigContext) {
        this.pigContext = pigContext;
        this.jobPlan = new JobGraph();
        this.tezOpVertexMap = Maps.newHashMap();
        this.dagStatsStrings = Lists.newArrayList();
        this.taskStatsToBeRemoved = Lists.newArrayList();
    }

    public void initialize(TezOperPlan tezPlan) {
        super.start();
        try {
            new JobGraphBuilder(tezPlan).visit();
            for (TezTaskStats taskStat : taskStatsToBeRemoved) {
                jobPlan.removeAndReconnect(taskStat);
            }
        } catch (FrontendException e) {
            LOG.warn("Unable to build Tez DAG", e);
        }
    }

    public void finish() {
        super.stop();
        display();
    }

    public TezTaskStats addTezJobStatsForNative(NativeTezOper tezOper, boolean success) {
        TezTaskStats js = tezOpVertexMap.get(tezOper.getOperatorKey().toString());
        js.setId(tezOper.getJobId());
        js.setSuccessful(success);
        return js;
    }

    private void display() {
        SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
        StringBuilder sb = new StringBuilder();
        sb.append("\n");
        sb.append(String.format("%1$20s: %2$-100s%n", "HadoopVersion", getHadoopVersion()));
        sb.append(String.format("%1$20s: %2$-100s%n", "PigVersion", getPigVersion()));
        sb.append(String.format("%1$20s: %2$-100s%n", "TezVersion", TezExecType.getTezVersion()));
        sb.append(String.format("%1$20s: %2$-100s%n", "UserId", userId));
        sb.append(String.format("%1$20s: %2$-100s%n", "FileName", getFileName()));
        sb.append(String.format("%1$20s: %2$-100s%n", "StartedAt", sdf.format(new Date(startTime))));
        sb.append(String.format("%1$20s: %2$-100s%n", "FinishedAt", sdf.format(new Date(endTime))));
        sb.append(String.format("%1$20s: %2$-100s%n", "Features", getFeatures()));
        sb.append("\n");
        if (returnCode == ReturnCode.SUCCESS) {
            sb.append("Success!\n");
        } else if (returnCode == ReturnCode.PARTIAL_FAILURE) {
            sb.append("Some tasks have failed! Stop running all dependent tasks\n");
        } else {
            sb.append("Failed!\n");
        }
        sb.append("\n");

        // Print diagnostic info in case of failure
        if (returnCode == ReturnCode.FAILURE
                || returnCode == ReturnCode.PARTIAL_FAILURE) {
            if (errorMessage != null) {
                String[] lines = errorMessage.split("\n");
                for (int i = 0; i < lines.length; i++) {
                    String s = lines[i].trim();
                    if (i == 0 || !StringUtils.isEmpty(s)) {
                        sb.append(String.format("%1$20s: %2$-100s%n", i == 0 ? "ErrorMessage" : "", s));
                    }
                }
                sb.append("\n");
            }
        }

        for (String s : dagStatsStrings) {
            sb.append(s);
            sb.append("\n");
        }

        sb.append("Input(s):\n");
        for (InputStats is : getInputStats()) {
            sb.append(is.getDisplayString().trim()).append("\n");
        }
        sb.append("\n");
        sb.append("Output(s):\n");
        for (OutputStats os : getOutputStats()) {
            sb.append(os.getDisplayString().trim()).append("\n");
        }
        LOG.info("Script Statistics:\n" + sb.toString());
    }

    public TezTaskStats getVertexStats(String vertexName) {
        return tezOpVertexMap.get(vertexName);
    }

    /**
     * Updates the statistics after DAG is finished.
     *
     * @param jc the job control
     */
    public void accumulateStats(TezJob tezJob) throws IOException {
        DAGStatus dagStatus = tezJob.getDAGStatus();
        if (dagStatus == null)
            return;
        if (dagStatus.getState() == DAGStatus.State.SUCCEEDED) {
            addVertexStats(tezJob, true);
            dagStatsStrings.add(getDisplayString(tezJob));
        } else if (dagStatus.getState() == DAGStatus.State.FAILED) {
            addVertexStats(tezJob, false);
            dagStatsStrings.add(getDisplayString(tezJob));
        }
    }

    private void addVertexStats(TezJob tezJob, boolean succeeded) throws IOException {
        DAG dag = tezJob.getDAG();
        for (String name : tezOpVertexMap.keySet()) {
            Vertex v = dag.getVertex(name);
            if (v != null) {
                UserPayload payload = v.getProcessorDescriptor().getUserPayload();
                Configuration conf = TezUtils.createConfFromUserPayload(payload);
                addVertexStats(name, conf, succeeded, v.getParallelism(), tezJob.getVertexCounters(name));
            }
        }
        if (!succeeded) {
            errorMessage = tezJob.getDiagnostics();
        }
    }

    private void addVertexStats(String tezOpName, Configuration conf, boolean succeeded, int parallelism,
            Map<String, Map<String, Long>> map) {
        TezTaskStats stats = tezOpVertexMap.get(tezOpName);
        stats.setConf(conf);
        stats.setId(tezOpName);
        stats.setSuccessful(succeeded);
        stats.setParallelism(parallelism);
        if (map == null) {
            if (stats.hasLoadOrStore()) {
                LOG.warn("Unable to get input(s)/output(s) of the job");
            }
        } else {
            stats.addInputStatistics(map);
            stats.addOutputStatistics(map);
        }
    }

    private static String getDisplayString(TezJob tezJob) {
        StringBuilder sb = new StringBuilder();
        TezCounters cnt = tezJob.getDAGCounters();
        if (cnt == null) {
            return "";
        }

        sb.append(String.format("%1$20s: %2$-100s%n", "ApplicationId",
                tezJob.getApplicationId()));

        CounterGroup dagGrp = cnt.getGroup(DAG_COUNTER_GROUP);
        TezCounter numTasks = dagGrp.findCounter("TOTAL_LAUNCHED_TASKS");
        sb.append(String.format("%1$20s: %2$-100s%n", "TotalLaunchedTasks",
                numTasks.getValue()));

        CounterGroup fsGrp = cnt.getGroup(FS_COUNTER_GROUP);
        TezCounter bytesRead = fsGrp.findCounter("FILE_BYTES_READ");
        TezCounter bytesWritten = fsGrp.findCounter("FILE_BYTES_WRITTEN");
        sb.append(String.format("%1$20s: %2$-100s%n", "FileBytesRead",
                bytesRead.getValue()));
        sb.append(String.format("%1$20s: %2$-100s%n", "FileBytesWritten",
                bytesWritten.getValue()));

        TezCounter hdfsBytesRead = fsGrp.findCounter("HDFS_BYTES_READ");
        TezCounter hdfsBytesWritten = fsGrp.findCounter("HDFS_BYTES_WRITTEN");
        sb.append(String.format("%1$20s: %2$-100s%n", "HdfsBytesRead",
                hdfsBytesRead.getValue()));
        sb.append(String.format("%1$20s: %2$-100s%n", "HdfsBytesWritten",
                hdfsBytesWritten.getValue()));

        return sb.toString();
    }

    public void setTezJob(TezJob tezJob) {
        this.tezJob = tezJob;
    }

    public TezJob getTezJob() {
        return tezJob;
    }

    @Override
    public boolean isEmbedded() {
        return false;
    }

    @Override
    public JobClient getJobClient() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, List<PigStats>> getAllStats() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getAllErrorMessages() {
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
    public long getProactiveSpillCountRecords() {
        throw new UnsupportedOperationException();
    }
}
