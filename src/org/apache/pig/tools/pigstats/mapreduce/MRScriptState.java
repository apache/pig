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
package org.apache.pig.tools.pigstats.mapreduce;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.NativeMapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.tools.pigstats.PigStats.JobGraph;
import org.apache.pig.tools.pigstats.ScriptState;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.PigStats;

import com.google.common.collect.Maps;

/**
 * ScriptStates encapsulates settings for a Pig script that runs on a hadoop
 * cluster. These settings are added to all MR jobs spawned by the script and
 * in turn are persisted in the hadoop job xml. With the properties already in
 * the job xml, users who want to know the relations between the script and MR
 * jobs can derive them from the job xmls.
 */
public class MRScriptState extends ScriptState {
    private static final Log LOG = LogFactory.getLog(MRScriptState.class);

    private Map<MapReduceOper, String> featureMap = null;
    private Map<MapReduceOper, String> aliasMap = Maps.newHashMap();
    private Map<MapReduceOper, String> aliasLocationMap = Maps.newHashMap();

    public MRScriptState(String id) {
        super(id);
    }

    public static MRScriptState get() {
        return (MRScriptState) ScriptState.get();
    }

    public void addSettingsToConf(MapReduceOper mro, Configuration conf) {
        LOG.info("Pig script settings are added to the job");
        conf.set(PIG_PROPERTY.HADOOP_VERSION.toString(), getHadoopVersion());
        conf.set(PIG_PROPERTY.VERSION.toString(), getPigVersion());
        conf.set(PIG_PROPERTY.SCRIPT_ID.toString(), id);
        conf.set(PIG_PROPERTY.SCRIPT.toString(), getScript());
        conf.set(PIG_PROPERTY.COMMAND_LINE.toString(), getCommandLine());

        try {
            LinkedList<POStore> stores = PlanHelper.getPhysicalOperators(mro.mapPlan, POStore.class);
            ArrayList<String> outputDirs = new ArrayList<String>();
            for (POStore st: stores) {
                outputDirs.add(st.getSFile().getFileName());
            }
            conf.set(PIG_PROPERTY.MAP_OUTPUT_DIRS.toString(), LoadFunc.join(outputDirs, ","));
        } catch (VisitorException e) {
            LOG.warn("unable to get the map stores", e);
        }
        if (!mro.reducePlan.isEmpty()) {
            try {
                LinkedList<POStore> stores = PlanHelper.getPhysicalOperators(mro.reducePlan, POStore.class);
                ArrayList<String> outputDirs = new ArrayList<String>();
                for (POStore st: stores) {
                    outputDirs.add(st.getSFile().getFileName());
                }
                conf.set(PIG_PROPERTY.REDUCE_OUTPUT_DIRS.toString(), LoadFunc.join(outputDirs, ","));
            } catch (VisitorException e) {
                LOG.warn("unable to get the reduce stores", e);
            }
        }
        try {
            List<POLoad> lds = PlanHelper.getPhysicalOperators(mro.mapPlan, POLoad.class);
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

        setPigFeature(mro, conf);
        setJobParents(mro, conf);

        conf.set("mapreduce.workflow.id", "pig_" + id);
        conf.set("mapreduce.workflow.name", getFileName().isEmpty() ? "default" : getFileName());
        conf.set("mapreduce.workflow.node.name", mro.getOperatorKey().toString());
    }

    public void addWorkflowAdjacenciesToConf(MROperPlan mrop, Configuration conf) {
        for (MapReduceOper source : mrop) {
            List<String> targets = new ArrayList<String>();
            if (mrop.getSuccessors(source) != null) {
                for (MapReduceOper target : mrop.getSuccessors(source)) {
                    targets.add(target.getOperatorKey().toString());
                }
            }
            String[] s = new String[targets.size()];
            conf.setStrings("mapreduce.workflow.adjacency." + source.getOperatorKey().toString(), targets.toArray(s));
        }
    }

    private void setPigFeature(MapReduceOper mro, Configuration conf) {
        conf.set(PIG_PROPERTY.JOB_FEATURE.toString(), getPigFeature(mro));
        if (scriptFeatures != 0) {
            conf.set(PIG_PROPERTY.SCRIPT_FEATURES.toString(),
                    String.valueOf(scriptFeatures));
        }
        conf.set(PIG_PROPERTY.JOB_ALIAS.toString(), getAlias(mro));
        conf.set(PIG_PROPERTY.JOB_ALIAS_LOCATION.toString(), getAliasLocation(mro));
    }

    private void setJobParents(MapReduceOper mro, Configuration conf) {
        // PigStats maintains a job DAG with the job id being updated
        // upon available. Therefore, before a job is submitted, the ids
        // of its parent jobs are already available.
        JobGraph jg = PigStats.get().getJobGraph();
        JobStats js = null;
        Iterator<JobStats> iter = jg.iterator();
        while (iter.hasNext()) {
            JobStats job = iter.next();
            if (job.getName().equals(mro.getOperatorKey().toString())) {
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

    public String getAlias(MapReduceOper mro) {
        if (!aliasMap.containsKey(mro)) {
            setAlias(mro);
        }
        return aliasMap.get(mro);
    }

    private void setAlias(MapReduceOper mro) {
        ArrayList<String> alias = new ArrayList<String>();
        String aliasLocationStr = "";
        try {
            ArrayList<String> aliasLocation = new ArrayList<String>();
            new AliasVisitor(mro.mapPlan, alias, aliasLocation).visit();
            aliasLocationStr += "M: "+LoadFunc.join(aliasLocation, ",");
            if (mro.combinePlan != null) {
                aliasLocation = new ArrayList<String>();
                new AliasVisitor(mro.combinePlan, alias, aliasLocation).visit();
                aliasLocationStr += " C: "+LoadFunc.join(aliasLocation, ",");
            }
            aliasLocation = new ArrayList<String>();
            new AliasVisitor(mro.reducePlan, alias, aliasLocation).visit();
            aliasLocationStr += " R: "+LoadFunc.join(aliasLocation, ",");
            if (!alias.isEmpty()) {
                Collections.sort(alias);
            }
        } catch (VisitorException e) {
            LOG.warn("unable to get alias", e);
        }
        aliasMap.put(mro, LoadFunc.join(alias, ","));
        aliasLocationMap.put(mro, aliasLocationStr);
    }

    public String getAliasLocation(MapReduceOper mro) {
        if (!aliasLocationMap.containsKey(mro)) {
            setAlias(mro);
        }
        return aliasLocationMap.get(mro);
    }

    public String getPigFeature(MapReduceOper mro) {
        if (featureMap == null) {
            featureMap = new HashMap<MapReduceOper, String>();
        }

        String retStr = featureMap.get(mro);
        if (retStr == null) {
            BitSet feature = new BitSet();
            feature.clear();
            if (mro.isSkewedJoin()) {
                feature.set(PIG_FEATURE.SKEWED_JOIN.ordinal());
            }
            if (mro.isGlobalSort()) {
                feature.set(PIG_FEATURE.ORDER_BY.ordinal());
            }
            if (mro.isSampler()) {
                feature.set(PIG_FEATURE.SAMPLER.ordinal());
            }
            if (mro.isIndexer()) {
                feature.set(PIG_FEATURE.INDEXER.ordinal());
            }
            if (mro.isCogroup()) {
                feature.set(PIG_FEATURE.COGROUP.ordinal());
            }
            if (mro.isGroupBy()) {
                feature.set(PIG_FEATURE.GROUP_BY.ordinal());
            }
            if (mro.isRegularJoin()) {
                feature.set(PIG_FEATURE.HASH_JOIN.ordinal());
            }
            if (mro.needsDistinctCombiner()) {
                feature.set(PIG_FEATURE.DISTINCT.ordinal());
            }
            if (!mro.combinePlan.isEmpty()) {
                feature.set(PIG_FEATURE.COMBINER.ordinal());
            }
            if (mro instanceof NativeMapReduceOper) {
                feature.set(PIG_FEATURE.NATIVE.ordinal());
            }
            else{// if it is NATIVE MR , don't explore its plans
                try {
                    new FeatureVisitor(mro.mapPlan, feature).visit();
                    if (mro.reducePlan.isEmpty()) {
                        feature.set(PIG_FEATURE.MAP_ONLY.ordinal());
                    } else {
                        new FeatureVisitor(mro.reducePlan, feature).visit();
                    }
                } catch (VisitorException e) {
                    LOG.warn("Feature visitor failed", e);
                }
            }
            StringBuilder sb = new StringBuilder();
            for (int i=feature.nextSetBit(0); i>=0; i=feature.nextSetBit(i+1)) {
                if (sb.length() > 0) sb.append(",");
                sb.append(PIG_FEATURE.values()[i].name());
            }
            retStr = sb.toString();
            featureMap.put(mro, retStr);
        }
        return retStr;
    }

}
