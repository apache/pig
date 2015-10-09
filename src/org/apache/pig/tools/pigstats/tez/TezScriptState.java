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
package org.apache.pig.tools.pigstats.tez;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezOpPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezOperPlan;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezOperator;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezPlanContainerNode;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.OperatorPlan;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.PigProgressNotificationListener;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.ScriptState;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * ScriptStates encapsulates settings for a Pig script that runs on a hadoop
 * cluster. These settings are added to all Tez jobs spawned by the script and
 * in turn are persisted in the hadoop job xml. With the properties already in
 * the job xml, users who want to know the relations between the script and Tez
 * jobs can derive them from the job xmls.
 */
public class TezScriptState extends ScriptState {
    private static final Log LOG = LogFactory.getLog(TezScriptState.class);

    private List<PigTezProgressNotificationListener> tezListeners = Lists.newArrayList();
    private Map<String, TezDAGScriptInfo> dagScriptInfo = Maps.newHashMap();

    public TezScriptState(String id) {
        super(id);
    }

    public static TezScriptState get() {
        return (TezScriptState) ScriptState.get();
    }

    @Override
    public void registerListener(PigProgressNotificationListener listener) {
        super.registerListener(listener);
        if (listener instanceof PigTezProgressNotificationListener) {
            tezListeners.add((PigTezProgressNotificationListener) listener);
        }
    }

    public void dagLaunchNotification(String dagName, OperatorPlan<?> dagPlan, int numVerticesToLaunch)  {
        for (PigTezProgressNotificationListener listener: tezListeners) {
            listener.dagLaunchNotification(id, dagName, dagPlan, numVerticesToLaunch);
        }
    }

    public void dagStartedNotification(String dagName, String assignedApplicationId)  {
        for (PigTezProgressNotificationListener listener: tezListeners) {
            listener.dagStartedNotification(id, dagName, assignedApplicationId);
        }
    }

    public void dagProgressNotification(String dagName, int numVerticesCompleted, int progress) {
        for (PigTezProgressNotificationListener listener: tezListeners) {
            listener.dagProgressNotification(id, dagName, numVerticesCompleted, progress);
        }
    }

    public void dagCompletedNotification(String dagName, TezDAGStats tezDAGStats) {
        for (PigTezProgressNotificationListener listener: tezListeners) {
            listener.dagCompletedNotification(id, dagName, tezDAGStats.isSuccessful(), tezDAGStats);
        }
    }

    public void addDAGSettingsToConf(Configuration conf) {
        LOG.info("Pig script settings are added to the job");
        conf.set(PIG_PROPERTY.HADOOP_VERSION.toString(), getHadoopVersion());
        conf.set(PIG_PROPERTY.VERSION.toString(), getPigVersion());
        conf.set(PIG_PROPERTY.SCRIPT_ID.toString(), id);
        conf.set(PIG_PROPERTY.SCRIPT.toString(), getSerializedScript());
        conf.set(PIG_PROPERTY.COMMAND_LINE.toString(), getCommandLine());
    }

    public void addVertexSettingsToConf(String dagName, TezOperator tezOp, Configuration conf) {

        try {
            List<POStore> stores = PlanHelper.getPhysicalOperators(tezOp.plan, POStore.class);
            ArrayList<String> outputDirs = new ArrayList<String>();
            for (POStore st: stores) {
                outputDirs.add(st.getSFile().getFileName());
            }
            conf.set(PIG_PROPERTY.MAP_OUTPUT_DIRS.toString(), LoadFunc.join(outputDirs, ","));
        } catch (VisitorException e) {
            LOG.warn("unable to get the map stores", e);
        }
        try {
            List<POLoad> lds = PlanHelper.getPhysicalOperators(tezOp.plan, POLoad.class);
            ArrayList<String> inputDirs = new ArrayList<String>();
            if (lds != null && lds.size() > 0){
                for (POLoad ld : lds) {
                    inputDirs.add(ld.getLFile().getFileName());
                }
                conf.set(PIG_PROPERTY.INPUT_DIRS.toString(), LoadFunc.join(inputDirs, ","));
            }
        } catch (VisitorException e) {
            LOG.warn("unable to get the map loads", e);
        }

        setPigFeature(dagName, tezOp, conf);
        setJobParents(dagName, tezOp, conf);

        conf.set("mapreduce.workflow.id", "pig_" + id);
        conf.set("mapreduce.workflow.name", getFileName().isEmpty() ? "default" : getFileName());
        conf.set("mapreduce.workflow.node.name", tezOp.getOperatorKey().toString());
    }

    public void addWorkflowAdjacenciesToConf(TezOperPlan tezPlan, Configuration conf) {
        for (TezOperator source : tezPlan) {
            List<String> targets = new ArrayList<String>();
            if (tezPlan.getSuccessors(source) != null) {
                for (TezOperator target : tezPlan.getSuccessors(source)) {
                    targets.add(target.getOperatorKey().toString());
                }
            }
            String[] s = new String[targets.size()];
            conf.setStrings("mapreduce.workflow.adjacency." + source.getOperatorKey().toString(), targets.toArray(s));
        }
    }

    private void setPigFeature(String dagName, TezOperator tezOp, Configuration conf) {
        if (tezOp.isVertexGroup()) {
            return;
        }
        TezDAGScriptInfo dagInfo = getDAGScriptInfo(dagName);
        conf.set(PIG_PROPERTY.JOB_FEATURE.toString(), dagInfo.getPigFeatures(tezOp));
        if (scriptFeatures != 0) {
            conf.set(PIG_PROPERTY.SCRIPT_FEATURES.toString(),
                    String.valueOf(scriptFeatures));
        }
        conf.set(PIG_PROPERTY.JOB_ALIAS.toString(), dagInfo.getAlias(tezOp));
        conf.set(PIG_PROPERTY.JOB_ALIAS_LOCATION.toString(), dagInfo.getAliasLocation(tezOp));
    }

    private void setJobParents(String dagName, TezOperator tezOp, Configuration conf) {
        if (tezOp.isVertexGroup()) {
            return;
        }
        // PigStats maintains a job DAG with the job id being updated
        // upon available. Therefore, before a job is submitted, the ids
        // of its parent jobs are already available.
        JobStats js = ((TezPigScriptStats)PigStats.get()).getVertexStats(dagName, tezOp.getOperatorKey().toString());
        if (js != null) {
            List<Operator> preds = js.getPlan().getPredecessors(js);
            if (preds != null) {
                StringBuilder sb = new StringBuilder();
                for (Operator op : preds) {
                    JobStats job = (JobStats)op;
                    if (sb.length() > 0) sb.append(",");
                    sb.append(job.getJobId());
                }
                conf.set(PIG_PROPERTY.JOB_PARENTS.toString(), sb.toString());
            }
        }
    }

    public TezDAGScriptInfo setDAGScriptInfo(TezPlanContainerNode tezPlanNode) {
        TezDAGScriptInfo info = new TezDAGScriptInfo(tezPlanNode.getTezOperPlan());
        dagScriptInfo.put(tezPlanNode.getOperatorKey().toString(), info);
        return info;
    }

    public TezDAGScriptInfo getDAGScriptInfo(String dagName) {
        return dagScriptInfo.get(dagName);
    }

    public static class TezDAGScriptInfo {

        private static final Log LOG = LogFactory.getLog(TezDAGScriptInfo.class);
        private TezOperPlan tezPlan;
        private String alias;
        private String aliasLocation;
        private String features;

        private Map<OperatorKey, String> featuresMap = Maps.newHashMap();
        private Map<OperatorKey, String> aliasMap = Maps.newHashMap();
        private Map<OperatorKey, String> aliasLocationMap = Maps.newHashMap();

        class DAGAliasVisitor extends TezOpPlanVisitor {

            private Set<String> aliases;
            private Set<String> aliasLocations;
            private BitSet featureSet;

            public DAGAliasVisitor(TezOperPlan plan) {
                super(plan, new DependencyOrderWalker<TezOperator, TezOperPlan>(plan));
                this.aliases = new HashSet<String>();
                this.aliasLocations = new HashSet<String>();
                this.featureSet = new BitSet();
            }

            @Override
            public void visitTezOp(TezOperator tezOp) throws VisitorException {
                if (tezOp.isVertexGroup()) {
                    featureSet.set(PIG_FEATURE.UNION.ordinal());
                    return;
                }
                ArrayList<String> aliasList = new ArrayList<String>();
                String aliasLocationStr = "";
                try {
                    ArrayList<String> aliasLocationList = new ArrayList<String>();
                    new AliasVisitor(tezOp.plan, aliasList, aliasLocationList).visit();
                    aliasLocationStr += LoadFunc.join(aliasLocationList, ",");
                    if (!aliasList.isEmpty()) {
                        Collections.sort(aliasList);
                        aliases.addAll(aliasList);
                        aliasLocations.addAll(aliasLocationList);
                    }
                } catch (VisitorException e) {
                    LOG.warn("unable to get alias", e);
                }
                aliasMap.put(tezOp.getOperatorKey(), LoadFunc.join(aliasList, ","));
                aliasLocationMap.put(tezOp.getOperatorKey(), aliasLocationStr);


                BitSet feature = new BitSet();
                feature.clear();
                if (tezOp.isSkewedJoin()) {
                    feature.set(PIG_FEATURE.SKEWED_JOIN.ordinal());
                }
                if (tezOp.isGlobalSort()) {
                    feature.set(PIG_FEATURE.ORDER_BY.ordinal());
                }
                if (tezOp.isSampler()) {
                    feature.set(PIG_FEATURE.SAMPLER.ordinal());
                }
                if (tezOp.isIndexer()) {
                    feature.set(PIG_FEATURE.INDEXER.ordinal());
                }
                if (tezOp.isCogroup()) {
                    feature.set(PIG_FEATURE.COGROUP.ordinal());
                }
                if (tezOp.isGroupBy()) {
                    feature.set(PIG_FEATURE.GROUP_BY.ordinal());
                }
                if (tezOp.isRegularJoin()) {
                    feature.set(PIG_FEATURE.HASH_JOIN.ordinal());
                }
                if (tezOp.isUnion()) {
                    feature.set(PIG_FEATURE.UNION.ordinal());
                }
                if (tezOp.isNative()) {
                    feature.set(PIG_FEATURE.NATIVE.ordinal());
                }
                if (tezOp.isLimit() || tezOp.isLimitAfterSort()) {
                    feature.set(PIG_FEATURE.LIMIT.ordinal());
                }
                try {
                    new FeatureVisitor(tezOp.plan, feature).visit();
                } catch (VisitorException e) {
                    LOG.warn("Feature visitor failed", e);
                }
                StringBuilder sb = new StringBuilder();
                for (int i=feature.nextSetBit(0); i>=0; i=feature.nextSetBit(i+1)) {
                    if (sb.length() > 0) sb.append(",");
                    sb.append(PIG_FEATURE.values()[i].name());
                }
                featuresMap.put(tezOp.getOperatorKey(), sb.toString());
                for (int i=0; i < feature.length(); i++) {
                    if (feature.get(i)) {
                        featureSet.set(i);
                    }
                }
            }

            @Override
            public void visit() throws VisitorException {
                super.visit();
                if (!aliases.isEmpty()) {
                    ArrayList<String> aliasList = new ArrayList<String>(aliases);
                    ArrayList<String> aliasLocationList = new ArrayList<String>(aliasLocations);
                    Collections.sort(aliasList);
                    Collections.sort(aliasLocationList);
                    alias = LoadFunc.join(aliasList, ",");
                    aliasLocation = LoadFunc.join(aliasLocationList, ",");
                }
                StringBuilder sb = new StringBuilder();
                for (int i = featureSet.nextSetBit(0); i >= 0; i = featureSet.nextSetBit(i+1)) {
                    if (sb.length() > 0) sb.append(",");
                    sb.append(PIG_FEATURE.values()[i].name());
                }
                features = sb.toString();
            }

        }

        public TezDAGScriptInfo(TezOperPlan tezPlan) {
            this.tezPlan = tezPlan;
            initialize();
        }

        private void initialize() {
            try {
                new DAGAliasVisitor(tezPlan).visit();
            } catch (VisitorException e) {
                LOG.warn("Cannot calculate alias information for DAG", e);
            }
        }

        public String getAlias() {
            return alias;
        }

        public String getAliasLocation() {
            return aliasLocation;
        }

        public String getPigFeatures() {
            return features;
        }

        public String getAlias(TezOperator tezOp) {
            return aliasMap.get(tezOp.getOperatorKey());
        }

        public String getAliasLocation(TezOperator tezOp) {
            return aliasLocationMap.get(tezOp.getOperatorKey());
        }

        public String getPigFeatures(TezOperator tezOp) {
            return featuresMap.get(tezOp.getOperatorKey());
        }

    }

}

