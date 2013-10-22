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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.tools.pigstats.InputStats;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.JobStats.JobState;
import org.apache.pig.tools.pigstats.OutputStats;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.Progress;

import com.google.common.collect.Maps;

public class TezStats extends PigStats {
    private static final Log LOG = LogFactory.getLog(TezStats.class);

    private Map<String, TezTaskStats> tezOpVertexMap;

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
            tezOpVertexMap.put(tezOp.name(), currStats);
        }
    }

    public TezStats(PigContext pigContext) {
        this.pigContext = pigContext;
        this.jobPlan = new JobGraph();
        this.tezOpVertexMap = Maps.newHashMap();
    }

    public void initialize(TezOperPlan tezPlan) {
        try {
            new JobGraphBuilder(tezPlan).visit();
        } catch (VisitorException e) {
            LOG.warn("Unable to build Tez DAG", e);
        }

        startTime = System.currentTimeMillis();
        userId = System.getProperty("user.name");
    }

    public void finish() {
        endTime = System.currentTimeMillis();
        Pair<Integer, Integer> count = countFailedSucceededVertices();
        int failed = count.first;
        int succeeded = count.second;
        if (failed == 0 && succeeded > 0 && succeeded == jobPlan.size()) {
            returnCode = ReturnCode.SUCCESS;
        } else if (succeeded > 0 && succeeded < jobPlan.size()) {
            returnCode = ReturnCode.PARTIAL_FAILURE;
        } else {
            returnCode = ReturnCode.FAILURE;
        }

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
        LOG.info("Script Statistics: \n" + sb.toString());
    }

    /**
     * Updates the statistics after DAG is finished.
     *
     * @param jc the job control
     */
    public void accumulateStats(JobControl jc) {
        for (ControlledJob job : jc.getSuccessfulJobList()) {
            TezJob tezJob = (TezJob)job;
            DAGStatus dagStatus = tezJob.getDagStatus();
            Map<String, Progress> vertices = dagStatus.getVertexProgress();
            for (String name : vertices.keySet()) {
                // TODO: Add statistics per vertex that Progress class provides
                addVertexStats(name, tezJob, true);
            }
        }

        for (ControlledJob job : jc.getFailedJobList()) {
            TezJob tezJob = (TezJob)job;
            DAGStatus dagStatus = tezJob.getDagStatus();
            Map<String, Progress> vertices = dagStatus.getVertexProgress();
            for (String name : vertices.keySet()) {
                // TODO: Add statistics per vertex that Progress class provides
                addVertexStats(name, tezJob, false);
            }
        }
    }

    private void addVertexStats(String tezOpName, TezJob tezJob, boolean succeeded) {
        TezTaskStats stats = tezOpVertexMap.get(tezOpName);
        stats.setId(tezJob.getAppId());
        stats.setSuccessful(succeeded);
    }

    private Pair<Integer, Integer> countFailedSucceededVertices() {
        // Note that jobPlan returns the iterator of TezVertexStats, so this
        // method counts the number of failed/succeeded vertices not jobs.
        Iterator<JobStats> iter = jobPlan.iterator();
        int failed = 0;
        int succeeded = 0;
        while (iter.hasNext()) {
            JobState state = iter.next().getState();
            if (state == JobState.FAILED) {
                failed++;
            } else if (state == JobState.SUCCESS) {
                succeeded++;
            }
        }
        return new Pair<Integer, Integer>(failed, succeeded);
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
    public List<String> getOutputLocations() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getOutputNames() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getNumberBytes(String location) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getNumberRecords(String location) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getOutputAlias(String location) {
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

    @Override
    public long getBytesWritten() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getRecordWritten() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<OutputStats> getOutputStats() {
        throw new UnsupportedOperationException();
    }

    @Override
    public OutputStats result(String alias) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<InputStats> getInputStats() {
        throw new UnsupportedOperationException();
    }
}
