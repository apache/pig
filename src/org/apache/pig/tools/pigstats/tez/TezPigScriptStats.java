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
import org.apache.hadoop.mapred.JobClient;
import org.apache.pig.PigRunner.ReturnCode;
import org.apache.pig.backend.hadoop.executionengine.tez.TezExecType;
import org.apache.pig.backend.hadoop.executionengine.tez.TezJob;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezPlanContainer;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezPlanContainerNode;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezPlanContainerVisitor;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.operator.NativeTezOper;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.tools.pigstats.InputStats;
import org.apache.pig.tools.pigstats.OutputStats;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.ScriptState;
import org.apache.pig.tools.pigstats.tez.TezScriptState.TezDAGScriptInfo;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.client.DAGStatus;

import com.google.common.collect.Maps;

/*
 * TezPigScriptStats encapsulates the statistics collected from a running script executed in Tez mode.
 * It includes status of the execution, the Tez DAGs launched for the script, as well as
 * information about outputs and inputs of the script.
 *
 * TezPigScriptStats encapsulates multiple TezDAGStats. TezDAGStats encapsulates multiple
 * TezVertexStats
 */
public class TezPigScriptStats extends PigStats {
    private static final Log LOG = LogFactory.getLog(TezPigScriptStats.class);

    private TezScriptState tezScriptState;
    private Map<String, TezDAGStats> tezDAGStatsMap;

    /**
     * This class builds the graph of Tez DAGs to be executed.
     */
    private class DAGGraphBuilder extends TezPlanContainerVisitor {
        public DAGGraphBuilder(TezPlanContainer planContainer) {
            super(planContainer, new DependencyOrderWalker<TezPlanContainerNode, TezPlanContainer>(planContainer));
        }

        @Override
        public void visitTezPlanContainerNode(TezPlanContainerNode tezPlanNode) throws VisitorException {
            TezScriptState ss = TezScriptState.get();
            TezDAGScriptInfo dagScriptInfo = ss.setDAGScriptInfo(tezPlanNode);
            TezDAGStats.JobGraphBuilder jobGraphBuilder = new TezDAGStats.JobGraphBuilder(tezPlanNode.getTezOperPlan(), dagScriptInfo);
            jobGraphBuilder.visit();
            TezDAGStats currStats = new TezDAGStats(tezPlanNode.getOperatorKey().toString(), jobGraphBuilder.getJobPlan(), jobGraphBuilder.getTezVertexStatsMap());
            currStats.setAlias(dagScriptInfo);
            jobPlan.add(currStats);
            List<TezPlanContainerNode> preds = getPlan().getPredecessors(tezPlanNode);
            if (preds != null) {
                for (TezPlanContainerNode pred : preds) {
                    TezDAGStats predStats = tezDAGStatsMap.get(pred.getOperatorKey().toString());
                    if (!jobPlan.isConnected(predStats, currStats)) {
                        jobPlan.connect(predStats, currStats);
                    }
                }
            }
            tezDAGStatsMap.put(tezPlanNode.getOperatorKey().toString(), currStats);
        }
    }

    public TezPigScriptStats(PigContext pigContext) {
        this.pigContext = pigContext;
        this.jobPlan = new JobGraph();
        this.tezDAGStatsMap = Maps.newHashMap();
        this.tezScriptState = (TezScriptState) ScriptState.get();
    }

    public void initialize(TezPlanContainer tezPlanContainer) {
        super.start();
        try {
            new DAGGraphBuilder(tezPlanContainer).visit();
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

        for (TezDAGStats dagStats : tezDAGStatsMap.values()) {
            sb.append(dagStats.getDisplayString());
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

    /**
     * Updates the statistics after a DAG is finished.
     */
    public void accumulateStats(TezJob tezJob) throws IOException {
        DAGStatus dagStatus = tezJob.getDAGStatus();
        TezDAGStats tezDAGStats = tezDAGStatsMap.get(tezJob.getName());
        if (dagStatus == null) {
            tezDAGStats.setSuccessful(false);
            tezScriptState.emitJobFailedNotification(tezDAGStats);
            return;
        } else {
            tezDAGStats.accumulateStats(tezJob);
            for(OutputStats output: tezDAGStats.getOutputs()) {
                tezScriptState.emitOutputCompletedNotification(output);
            }
            if (dagStatus.getState() == DAGStatus.State.SUCCEEDED) {
                tezDAGStats.setSuccessful(true);
                tezScriptState.emitjobFinishedNotification(tezDAGStats);
            } else if (dagStatus.getState() == DAGStatus.State.FAILED) {
                tezDAGStats.setSuccessful(false);
                tezDAGStats.setErrorMsg(tezJob.getDiagnostics());
                tezScriptState.emitJobFailedNotification(tezDAGStats);
            }
            tezScriptState.dagCompletedNotification(tezJob.getName(), tezDAGStats);
        }

        if (!tezDAGStats.isSuccessful()) {
            String outputCommitOnDAGSuccess = pigContext.getProperties().getProperty(
                    TezConfiguration.TEZ_AM_COMMIT_ALL_OUTPUTS_ON_DAG_SUCCESS);
            if ((outputCommitOnDAGSuccess == null && TezConfiguration.TEZ_AM_COMMIT_ALL_OUTPUTS_ON_DAG_SUCCESS_DEFAULT)
                    || "true".equals(outputCommitOnDAGSuccess)) {
                for (OutputStats stats : tezDAGStats.getOutputs()) {
                    stats.setSuccessful(false);
                }
            }
        }
    }

    public TezDAGStats addTezJobStatsForNative(String dagName, NativeTezOper tezOper, boolean success) {
        TezDAGStats js = tezDAGStatsMap.get(dagName);
        js.setJobId(tezOper.getJobId());
        js.setSuccessful(success);
        return js;
    }

    public TezVertexStats getVertexStats(String dagName, String vertexName) {
        TezDAGStats tezDAGStats = tezDAGStatsMap.get(dagName);
        return tezDAGStats == null ? null : tezDAGStats.getVertexStats(vertexName);
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
        long ret = 0;
        for (TezDAGStats dagStats : tezDAGStatsMap.values()) {
            ret += dagStats.getSMMSpillCount();
        }
        return ret;
    }

    @Override
    public long getProactiveSpillCountObjects() {
        long ret = 0;
        for (TezDAGStats dagStats : tezDAGStatsMap.values()) {
            ret += dagStats.getProactiveSpillCountObjects();
        }
        return ret;
    }

    @Override
    public long getProactiveSpillCountRecords() {
        long ret = 0;
        for (TezDAGStats dagStats : tezDAGStatsMap.values()) {
            ret += dagStats.getProactiveSpillCountRecs();
        }
        return ret;
    }

}
