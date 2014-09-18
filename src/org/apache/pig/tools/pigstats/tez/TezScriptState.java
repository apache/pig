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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezOperPlan;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezOperator;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.PigStats.JobGraph;
import org.apache.pig.tools.pigstats.ScriptState;

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

    private Map<TezOperator, String> featureMap = null;
    private Map<TezOperator, String> aliasMap = Maps.newHashMap();
    private Map<TezOperator, String> aliasLocationMap = Maps.newHashMap();

    public TezScriptState(String id) {
        super(id);
    }

    public static TezScriptState get() {
        return (TezScriptState) ScriptState.get();
    }

    public void addSettingsToConf(TezOperator tezOp, Configuration conf) {
        LOG.info("Pig script settings are added to the job");
        conf.set(PIG_PROPERTY.HADOOP_VERSION.toString(), getHadoopVersion());
        conf.set(PIG_PROPERTY.VERSION.toString(), getPigVersion());
        conf.set(PIG_PROPERTY.SCRIPT_ID.toString(), id);
        conf.set(PIG_PROPERTY.SCRIPT.toString(), getScript());
        conf.set(PIG_PROPERTY.COMMAND_LINE.toString(), getCommandLine());

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

        setPigFeature(tezOp, conf);
        setJobParents(tezOp, conf);

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

    private void setPigFeature(TezOperator tezOp, Configuration conf) {
        conf.set(PIG_PROPERTY.JOB_FEATURE.toString(), getPigFeature(tezOp));
        if (scriptFeatures != 0) {
            conf.set(PIG_PROPERTY.SCRIPT_FEATURES.toString(),
                    String.valueOf(scriptFeatures));
        }
        conf.set(PIG_PROPERTY.JOB_ALIAS.toString(), getAlias(tezOp));
        conf.set(PIG_PROPERTY.JOB_ALIAS_LOCATION.toString(), getAliasLocation(tezOp));
    }

    private void setJobParents(TezOperator tezOp, Configuration conf) {
        // PigStats maintains a job DAG with the job id being updated
        // upon available. Therefore, before a job is submitted, the ids
        // of its parent jobs are already available.
        JobGraph jg = PigStats.get().getJobGraph();
        JobStats js = null;
        Iterator<JobStats> iter = jg.iterator();
        while (iter.hasNext()) {
            JobStats job = iter.next();
            if (job.getName().equals(tezOp.getOperatorKey().toString())) {
                js = job;
                break;
            }
        }
        if (js != null) {
            List<Operator> preds = jg.getPredecessors(js);
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

    public String getAlias(TezOperator tezOp) {
        if (!aliasMap.containsKey(tezOp)) {
            setAlias(tezOp);
        }
        return aliasMap.get(tezOp);
    }

    private void setAlias(TezOperator tezOp) {
        ArrayList<String> alias = new ArrayList<String>();
        String aliasLocationStr = "";
        try {
            ArrayList<String> aliasLocation = new ArrayList<String>();
            new AliasVisitor(tezOp.plan, alias, aliasLocation).visit();
            aliasLocationStr += LoadFunc.join(aliasLocation, ",");
            if (!alias.isEmpty()) {
                Collections.sort(alias);
            }
        } catch (VisitorException e) {
            LOG.warn("unable to get alias", e);
        }
        aliasMap.put(tezOp, LoadFunc.join(alias, ","));
        aliasLocationMap.put(tezOp, aliasLocationStr);
    }

    public String getAliasLocation(TezOperator tezOp) {
        if (!aliasLocationMap.containsKey(tezOp)) {
            setAlias(tezOp);
        }
        return aliasLocationMap.get(tezOp);
    }

    public String getPigFeature(TezOperator tezOp) {
        if (featureMap == null) {
            featureMap = Maps.newHashMap();
        }

        String retStr = featureMap.get(tezOp);
        if (retStr == null) {
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
            retStr = sb.toString();
            featureMap.put(tezOp, retStr);
        }
        return retStr;
    }

}

