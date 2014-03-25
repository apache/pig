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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.pig.PigRunner.ReturnCode;
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
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.counters.CounterGroup;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.Vertex;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class TezStats extends PigStats {
    private static final Log LOG = LogFactory.getLog(TezStats.class);

    public static final String DAG_COUNTER =
            "org.apache.tez.common.counters.DAGCounter";
    public static final String FS_COUNTER =
            "org.apache.tez.common.counters.FileSystemCounter";
    public static final String TASK_COUNTER =
            "org.apache.tez.common.counters.TaskCounter";

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
            if (tezOp.isAliasVertex()) {
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
        boolean isLocal = pigContext.getExecType().isLocal();
        if (returnCode == ReturnCode.FAILURE
                || returnCode == ReturnCode.PARTIAL_FAILURE) {
            if (errorMessage != null) {
                String[] lines = errorMessage.split("\n");
                for (int i = 0; i < lines.length; i++) {
                    String s = lines[i].trim();
                    sb.append(String.format("%1$20s: %2$-100s%n", i == 0 ? "ErrorMessage" : "", s));
                }
                sb.append("\n");
            }
        }

        for (String s : dagStatsStrings) {
            sb.append(s);
            sb.append("\n");
        }

        List<InputStats> is = getInputStats();
        for (int i = 0; i < is.size(); i++) {
            String s = is.get(i).getDisplayString(isLocal).trim();
            sb.append(String.format("%1$20s: %2$-100s%n", i == 0 ? "Input(s)" : "", s));
        }
        List<OutputStats> os = getOutputStats();
        for (int i = 0; i < os.size(); i++) {
            String s = os.get(i).getDisplayString(isLocal).trim();
            sb.append(String.format("%1$20s: %2$-100s%n", i == 0 ? "Output(s)" : "", s));
        }
        LOG.info("Script Statistics:\n" + sb.toString());
    }

    /**
     * Updates the statistics after DAG is finished.
     *
     * @param jc the job control
     */
    public void accumulateStats(JobControl jc) throws IOException {
        for (ControlledJob job : jc.getSuccessfulJobList()) {
            addVertexStats((TezJob)job, true);
            dagStatsStrings.add(getDisplayString((TezJob)job));
        }
        for (ControlledJob job : jc.getFailedJobList()) {
            addVertexStats((TezJob)job, false);
            dagStatsStrings.add(getDisplayString((TezJob)job));
        }
    }

    private void addVertexStats(TezJob tezJob, boolean succeeded) throws IOException {
        DAG dag = tezJob.getDag();
        for (String name : tezOpVertexMap.keySet()) {
            Vertex v = dag.getVertex(name);
            if (v != null) {
                byte[] bb = v.getProcessorDescriptor().getUserPayload();
                Configuration conf = TezUtils.createConfFromUserPayload(bb);
                addVertexStats(name, conf, succeeded, tezJob.getVertexCounters(name));
            }
        }
        if (!succeeded) {
            errorMessage = tezJob.getMessage();
        }
    }

    private void addVertexStats(String tezOpName, Configuration conf, boolean succeeded,
            Map<String, Long> counters) {
        TezTaskStats stats = tezOpVertexMap.get(tezOpName);
        stats.setConf(conf);
        stats.setId(tezOpName);
        stats.setSuccessful(succeeded);
        stats.addInputStatistics(counters);
        stats.addOutputStatistics(counters);
    }

    private static String getDisplayString(TezJob tezJob) {
        StringBuilder sb = new StringBuilder();
        TezCounters cnt = tezJob.getDagCounters();

        sb.append(String.format("%1$20s: %2$-100s%n", "JobId",
                tezJob.getJobID()));

        CounterGroup dagGrp = cnt.getGroup(DAG_COUNTER);
        TezCounter numTasks = dagGrp.findCounter("TOTAL_LAUNCHED_TASKS");
        sb.append(String.format("%1$20s: %2$-100s%n", "TotalLaunchedTasks",
                numTasks.getValue()));

        CounterGroup fsGrp = cnt.getGroup(FS_COUNTER);
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
