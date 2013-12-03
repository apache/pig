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
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.tools.pigstats.InputStats;
import org.apache.pig.tools.pigstats.OutputStats;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.Vertex;

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
            tezOpVertexMap.put(tezOp.getOperatorKey().toString(), currStats);
        }
    }

    public TezStats(PigContext pigContext) {
        this.pigContext = pigContext;
        this.jobPlan = new JobGraph();
        this.tezOpVertexMap = Maps.newHashMap();
    }

    public void initialize(TezOperPlan tezPlan) {
        super.start();
        try {
            new JobGraphBuilder(tezPlan).visit();
        } catch (VisitorException e) {
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
            addJobStats((TezJob)job, true);
        }
        for (ControlledJob job : jc.getFailedJobList()) {
            addJobStats((TezJob)job, false);
        }
    }

    private void addJobStats(TezJob tezJob, boolean succeeded) throws IOException {
        DAG dag = tezJob.getDag();
        for (String name : tezOpVertexMap.keySet()) {
            Vertex v = dag.getVertex(name);
            if (v != null) {
                byte[] bb = v.getProcessorDescriptor().getUserPayload();
                Configuration conf = TezUtils.createConfFromUserPayload(bb);
                addVertexStats(name, conf, succeeded);
            }
        }
        if (!succeeded) {
            errorMessage = tezJob.getMessage();
        }
    }

    private void addVertexStats(String tezOpName, Configuration conf, boolean succeeded) {
        TezTaskStats stats = tezOpVertexMap.get(tezOpName);
        stats.setConf(conf);
        stats.setId(tezOpName);
        stats.setSuccessful(succeeded);
        // TODO: Add error messages for each task in failure case
        stats.addInputStatistics();
        stats.addOutputStatistics();
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
