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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Group;
import org.apache.pig.backend.hadoop.executionengine.tez.TezJob;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezOpPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezOperPlan;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezOperator;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezPlanContainerNode;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezPrinter;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.newplan.PlanVisitor;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.OutputStats;
import org.apache.pig.tools.pigstats.PigStats.JobGraph;
import org.apache.pig.tools.pigstats.PigStats.JobGraphPrinter;
import org.apache.pig.tools.pigstats.PigStatsUtil;
import org.apache.pig.tools.pigstats.tez.TezScriptState.TezDAGScriptInfo;
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
import org.apache.tez.dag.api.client.VertexStatus;

/*
 * TezDAGStats encapsulates the statistics collected from a Tez DAG.
 * It includes status of the execution, the Tez Vertices, as well as
 * information about outputs and inputs of the DAG.
 */
public class TezDAGStats extends JobStats {

    private static final Log LOG = LogFactory.getLog(TezDAGStats.class);
    public static final String DAG_COUNTER_GROUP = DAGCounter.class.getName();
    public static final String FS_COUNTER_GROUP = FileSystemCounter.class.getName();
    public static final String TASK_COUNTER_GROUP = TaskCounter.class.getName();
    public static final String PIG_COUNTER_GROUP = org.apache.pig.PigCounters.class.getName();

    public static final String SUCCESS_HEADER = String.format("VertexId Parallelism TotalTasks"
            + " %1$14s %2$14s %3$14s %4$16s %5$14s %6$16s"
            + " Alias\tFeature\tOutputs",
            "InputRecords", "OutputRecords", "FileBytesRead", "FileBytesWritten", "HdfsBytesRead", "HdfsBytesWritten");

    public static final String FAILURE_HEADER = String.format("VertexId  State Parallelism TotalTasks"
            + " %1$14s %2$14s %3$14s %4$16s %5$14s %6$16s"
            + " Alias\tFeature\tOutputs",
            "InputRecords", "OutputRecords", "FileBytesRead", "FileBytesWritten", "HdfsBytesRead", "HdfsBytesWritten");

    private Map<String, TezVertexStats> tezVertexStatsMap;

    private String appId;
    private StringBuilder tezDAGPlan;

    private int totalTasks = -1;
    private long fileBytesRead = -1;
    private long fileBytesWritten = -1;

    private Counters counters = null;

    private int numberMaps = 0;
    private int numberReduces = 0;

    private long mapInputRecords = 0;
    private long mapOutputRecords = 0;
    private long reduceInputRecords = 0;
    private long reduceOutputRecords = 0;
    private long spillCount = 0;
    private long activeSpillCountObj = 0;
    private long activeSpillCountRecs = 0;

    private Map<String, Long> multiStoreCounters
            = new HashMap<String, Long>();

    private Map<String, OutputStats> outputsByLocation
            = new HashMap<String, OutputStats>();

    /**
     * This class builds the graph of a Tez DAG vertices.
     */
    static class TezDAGStatsBuilder extends TezOpPlanVisitor {

        private TezPlanContainerNode tezPlanNode;
        private JobGraph jobPlan;
        private Map<String, TezVertexStats> tezVertexStatsMap;
        private List<TezVertexStats> vertexStatsToBeRemoved;
        private TezDAGScriptInfo dagScriptInfo;
        private StringBuilder tezDAGPlan;

        public TezDAGStatsBuilder(TezPlanContainerNode tezPlanNode, TezDAGScriptInfo dagScriptInfo) {
            super(tezPlanNode.getTezOperPlan(), new DependencyOrderWalker<TezOperator, TezOperPlan>(tezPlanNode.getTezOperPlan()));
            this.tezPlanNode = tezPlanNode;
            this.tezVertexStatsMap = new HashMap<String, TezVertexStats>();
            this.vertexStatsToBeRemoved = new ArrayList<TezVertexStats>();
            this.jobPlan = new JobGraph();
            this.tezDAGPlan = new StringBuilder();
            this.dagScriptInfo = dagScriptInfo;
        }

        public TezDAGStats build() throws VisitorException {
            visit();
            TezDAGStats dagStats = new TezDAGStats(tezPlanNode.getOperatorKey().toString(), jobPlan, tezVertexStatsMap, tezDAGPlan);
            dagStats.setAlias(dagScriptInfo);
            return dagStats;
        }

        @Override
        public void visitTezOp(TezOperator tezOp) throws VisitorException {
            TezPrinter.TezGraphPrinter.writePlan(mPlan, tezOp, tezDAGPlan);
            TezVertexStats currStats =
                    new TezVertexStats(tezOp.getOperatorKey().toString(), jobPlan, tezOp.isUseMRMapSettings());
            jobPlan.add(currStats);
            List<TezOperator> preds = getPlan().getPredecessors(tezOp);
            if (preds != null) {
                for (TezOperator pred : preds) {
                    TezVertexStats predStats = tezVertexStatsMap.get(pred);
                    if (!jobPlan.isConnected(predStats, currStats)) {
                        jobPlan.connect(predStats, currStats);
                    }
                }
            }

            // Remove VertexGroups (union) from JobGraph since they're not
            // materialized as real vertices by Tez.
            if (tezOp.isVertexGroup()) {
                vertexStatsToBeRemoved.add(currStats);
            } else {
                currStats.annotate(ALIAS, dagScriptInfo.getAlias(tezOp));
                currStats.annotate(ALIAS_LOCATION, dagScriptInfo.getAliasLocation(tezOp));
                currStats.annotate(FEATURE, dagScriptInfo.getPigFeatures(tezOp));
            }

            tezVertexStatsMap.put(tezOp.getOperatorKey().toString(), currStats);
        }

        @Override
        public void visit() throws VisitorException {
            super.visit();
            try {
                for (TezVertexStats vertexStats : vertexStatsToBeRemoved) {
                    jobPlan.removeAndReconnect(vertexStats);
                }
            } catch (FrontendException e) {
                LOG.warn("Unable to build Tez DAG", e);
            }
        }

    }

    protected TezDAGStats(String name, JobGraph plan, Map<String, TezVertexStats> tezVertexStatsMap, StringBuilder tezDAGPlan) {
        super(name, plan);
        this.tezVertexStatsMap = tezVertexStatsMap;
        this.tezDAGPlan = tezDAGPlan;
    }

    public TezVertexStats getVertexStats(String vertexName) {
        return tezVertexStatsMap.get(vertexName);
    }

    void setAlias(TezDAGScriptInfo dagScriptInfo) {
        annotate(ALIAS, dagScriptInfo.getAlias());
        annotate(ALIAS_LOCATION, dagScriptInfo.getAliasLocation());
        annotate(FEATURE, dagScriptInfo.getPigFeatures());
    }

    /**
     * Updates the statistics after a DAG is finished.
     */
    public void accumulateStats(TezJob tezJob) throws IOException {
        //For Oozie Pig action job id matching compatibility with MR mode
        appId = tezJob.getApplicationId().toString().replace("application", "job");
        setConf(tezJob.getConfiguration());
        DAG dag = tezJob.getDAG();
        hdfsBytesRead = -1;
        hdfsBytesWritten = -1;
        TezCounters tezCounters = tezJob.getDAGCounters();
        if (tezCounters != null) {
            counters = covertToHadoopCounters(tezCounters);

            CounterGroup dagGrp = tezCounters.getGroup(DAG_COUNTER_GROUP);
            totalTasks = (int) dagGrp.findCounter("TOTAL_LAUNCHED_TASKS").getValue();

            CounterGroup fsGrp = tezCounters.getGroup(FS_COUNTER_GROUP);
            fileBytesRead = fsGrp.findCounter(PigStatsUtil.FILE_BYTES_READ).getValue();
            fileBytesWritten = fsGrp.findCounter(PigStatsUtil.FILE_BYTES_WRITTEN).getValue();
            hdfsBytesRead = fsGrp.findCounter(PigStatsUtil.HDFS_BYTES_READ).getValue();
            hdfsBytesWritten = fsGrp.findCounter(PigStatsUtil.HDFS_BYTES_WRITTEN).getValue();
        } else {
            LOG.warn("Failed to get counters for DAG: " + dag.getName());
        }

        for (Entry<String, TezVertexStats> entry : tezVertexStatsMap.entrySet()) {
            Vertex v = dag.getVertex(entry.getKey());
            if (v != null && tezVertexStatsMap.containsKey(v.getName())) {
                TezVertexStats vertexStats = entry.getValue();
                UserPayload payload = v.getProcessorDescriptor().getUserPayload();
                Configuration conf = TezUtils.createConfFromUserPayload(payload);
                vertexStats.setConf(conf);

                VertexStatus status = tezJob.getVertexStatus(v.getName());
                if (status == null) {
                    LOG.warn("Failed to get status for vertex: " + v.getName());
                    continue;
                }
                vertexStats.accumulateStats(status, v.getParallelism());
                if(vertexStats.getInputs() != null && !vertexStats.getInputs().isEmpty()) {
                    inputs.addAll(vertexStats.getInputs());
                }
                if(vertexStats.getOutputs() != null  && !vertexStats.getOutputs().isEmpty()) {
                    for (OutputStats output : vertexStats.getOutputs()) {
                        if (outputsByLocation.get(output.getLocation()) != null) {
                            OutputStats existingOut = outputsByLocation.get(output.getLocation());
                            // In case of multistore, bytesWritten is already calculated
                            // from size of all the files in the output directory.
                            if (!output.getPOStore().isMultiStore() && output.getBytes() > -1) {
                                long bytes = existingOut.getBytes() > -1
                                        ? (existingOut.getBytes() + output.getBytes())
                                        : output.getBytes();
                                existingOut.setBytes(bytes);
                            }
                            if (output.getRecords() > -1) {
                                long records = existingOut.getRecords() > -1
                                        ? (existingOut.getRecords() + output.getRecords())
                                        : output.getRecords();
                                existingOut.setRecords(records);
                            }
                        } else {
                            outputs.add(output);
                            outputsByLocation.put(output.getLocation(), output);
                        }
                    }
                }
                /*if (vertexStats.getHdfsBytesRead() >= 0) {
                    hdfsBytesRead = (hdfsBytesRead == -1) ? 0 : hdfsBytesRead;
                    hdfsBytesRead += vertexStats.getHdfsBytesRead();
                }
                if (vertexStats.getHdfsBytesWritten() >= 0) {
                    hdfsBytesWritten = (hdfsBytesWritten == -1) ? 0 : hdfsBytesWritten;
                    hdfsBytesWritten += vertexStats.getHdfsBytesWritten();
                }*/
                if (!vertexStats.getMultiStoreCounters().isEmpty()) {
                    multiStoreCounters.putAll(vertexStats.getMultiStoreCounters());
                }
                numberMaps += vertexStats.getNumberMaps();
                numberReduces += vertexStats.getNumberReduces();
                mapInputRecords += vertexStats.getMapInputRecords();
                mapOutputRecords += vertexStats.getMapOutputRecords();
                reduceInputRecords += vertexStats.getReduceInputRecords();
                reduceOutputRecords += vertexStats.getReduceOutputRecords();
                spillCount += vertexStats.getSMMSpillCount();
                activeSpillCountObj  += vertexStats.getProactiveSpillCountObjects();
                activeSpillCountRecs += vertexStats.getProactiveSpillCountRecs();
            }
        }
    }

    private Counters covertToHadoopCounters(TezCounters tezCounters) {
        Counters counters = new Counters();
        for (CounterGroup tezGrp : tezCounters) {
            Group grp = counters.addGroup(tezGrp.getName(), tezGrp.getDisplayName());
            for (TezCounter counter : tezGrp) {
                grp.addCounter(counter.getName(), counter.getDisplayName(), counter.getValue());
            }
        }
        return counters;
    }

    @Override
    public String getJobId() {
        return appId;
    }

    public void setJobId(String appId) {
        this.appId = appId;
    }

    @Override
    public void accept(PlanVisitor v) throws FrontendException {
        if (v instanceof JobGraphPrinter) {
            JobGraphPrinter jpp = (JobGraphPrinter)v;
            jpp.visit(this);
        }
    }

    @Override
    public String getDisplayString() {
            StringBuilder sb = new StringBuilder();

            sb.append(String.format("%1$40s: %2$-100s%n", "Name",
                    name));
            sb.append(String.format("%1$40s: %2$-100s%n", "ApplicationId",
                    appId));
            sb.append(String.format("%1$40s: %2$-100s%n", "TotalLaunchedTasks",
                    totalTasks));

            sb.append(String.format("%1$40s: %2$-100s%n", "FileBytesRead",
                    fileBytesRead));
            sb.append(String.format("%1$40s: %2$-100s%n", "FileBytesWritten",
                    fileBytesWritten));
            sb.append(String.format("%1$40s: %2$-100s%n", "HdfsBytesRead",
                    hdfsBytesRead));
            sb.append(String.format("%1$40s: %2$-100s%n", "HdfsBytesWritten",
                    hdfsBytesWritten));

            sb.append(String.format("%1$40s: %2$-100s%n", "SpillableMemoryManager spill count",
                    spillCount));
            sb.append(String.format("%1$40s: %2$-100s%n", "Bags proactively spilled",
                    activeSpillCountObj));
            sb.append(String.format("%1$40s: %2$-100s%n", "Records proactively spilled",
                    activeSpillCountRecs));


            sb.append("\nDAG Plan:\n");
            sb.append(tezDAGPlan);

            List<JobStats> success = ((JobGraph)getPlan()).getSuccessfulJobs();
            List<JobStats> failed = ((JobGraph)getPlan()).getFailedJobs();

            if (success != null && !success.isEmpty()) {
                sb.append("\nVertex Stats:\n");
                sb.append(SUCCESS_HEADER).append("\n");
                for (JobStats js : success) {
                    sb.append(js.getDisplayString());
                }
            }

            if (failed != null && !failed.isEmpty()) {
                sb.append("\nFailed vertices:\n");
                sb.append(FAILURE_HEADER).append("\n");
                for (JobStats js : failed) {
                    sb.append(js.getDisplayString());
                }
                sb.append("\n");
            }

            return sb.toString();
    }

    @Override
    @Deprecated
    public int getNumberMaps() {
        return numberMaps;
    }

    @Override
    @Deprecated
    public int getNumberReduces() {
        return numberReduces;
    }

    @Override
    @Deprecated
    public long getMaxMapTime() {
        return -1;
    }

    @Override
    @Deprecated
    public long getMinMapTime() {
        return -1;
    }

    @Override
    @Deprecated
    public long getAvgMapTime() {
        return -1;
    }

    @Override
    @Deprecated
    public long getMaxReduceTime() {
        return -1;
    }

    @Override
    @Deprecated
    public long getMinReduceTime() {
        return -1;
    }

    @Override
    @Deprecated
    public long getAvgREduceTime() {
        return -1;
    }

    @Override
    @Deprecated
    public long getMapInputRecords() {
        return mapInputRecords;
    }

    @Override
    @Deprecated
    public long getMapOutputRecords() {
        return mapOutputRecords;
    }

    @Override
    @Deprecated
    public long getReduceInputRecords() {
        return reduceInputRecords;
    }

    @Override
    @Deprecated
    public long getReduceOutputRecords() {
        return reduceOutputRecords;
    }

    @Override
    public long getSMMSpillCount() {
        return spillCount;
    }

    @Override
    public long getProactiveSpillCountObjects() {
        return activeSpillCountObj;
    }

    @Override
    public long getProactiveSpillCountRecs() {
        return activeSpillCountRecs;
    }

    @Override
    @Deprecated
    public Counters getHadoopCounters() {
        return counters;
    }

    @Override
    @Deprecated
    public Map<String, Long> getMultiStoreCounters() {
        return multiStoreCounters;
    }

    @Override
    @Deprecated
    public Map<String, Long> getMultiInputCounters() {
        throw new UnsupportedOperationException();
    }

}
