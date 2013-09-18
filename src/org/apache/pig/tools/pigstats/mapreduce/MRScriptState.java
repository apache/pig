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
import java.util.HashSet;
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
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator.OriginalLocation;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POCollectedGroup;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.PODemux;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.PODistinct;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFRJoin;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFilter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLimit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POMergeCogroup;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POMergeJoin;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPartialAgg;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSkewedJoin;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSort;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSplit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStream;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POUnion;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.logical.relational.LOJoin;
import org.apache.pig.tools.pigstats.PigProgressNotificationListener;
import org.apache.pig.tools.pigstats.PigStats.JobGraph;
import org.apache.pig.tools.pigstats.ScriptState;
import org.apache.pig.tools.pigstats.JobStatsBase;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.OutputStats;


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
    private Map<MapReduceOper, String> aliasMap = new HashMap<MapReduceOper, String>();
    private Map<MapReduceOper, String> aliasLocationMap = new HashMap<MapReduceOper, String>();
    

    public MRScriptState(String id) {
        super(id);
    }
    
    public static MRScriptState get() {
        return (MRScriptState) ScriptState.get();
    }
    
    public void registerListener(PigProgressNotificationListener listener) {
        listeners.add(listener);
    }

    public List<PigProgressNotificationListener> getAllListeners() {
        return listeners;
    }

    public void emitInitialPlanNotification(MROperPlan plan) {
        for (PigProgressNotificationListener listener: listeners) {
            try {
                listener.initialPlanNotification(id, plan);
            } catch (NoSuchMethodError e) {
                LOG.warn("PigProgressNotificationListener implementation doesn't "
                       + "implement initialPlanNotification(..) method: "
                       + listener.getClass().getName(), e);
            }
        }
    }

    public void emitLaunchStartedNotification(int numJobsToLaunch) {
        for (PigProgressNotificationListener listener: listeners) {
            listener.launchStartedNotification(id, numJobsToLaunch);
        }
    }
    

    public void emitJobsSubmittedNotification(int numJobsSubmitted) {        
        for (PigProgressNotificationListener listener: listeners) {
            listener.jobsSubmittedNotification(id, numJobsSubmitted);
        }        
    }
    
    public void emitJobStartedNotification(String assignedJobId) {
        for (PigProgressNotificationListener listener: listeners) {
            listener.jobStartedNotification(id, assignedJobId);
        }
    }
    
    public void emitjobFinishedNotification(JobStatsBase jobStats) {
        for (PigProgressNotificationListener listener: listeners) {
            listener.jobFinishedNotification(id, jobStats);
        }
    }
    
    public void emitJobFailedNotification(JobStatsBase jobStats) {
        for (PigProgressNotificationListener listener: listeners) {
            listener.jobFailedNotification(id, jobStats);
        }
    }
    
    public void emitOutputCompletedNotification(OutputStats outputStats) {
        for (PigProgressNotificationListener listener: listeners) {
            listener.outputCompletedNotification(id, outputStats);
        }
    }
    
    public void emitProgressUpdatedNotification(int progress) {
        for (PigProgressNotificationListener listener: listeners) {
            listener.progressUpdatedNotification(id, progress);
        }
    }
    
    public void emitLaunchCompletedNotification(int numJobsSucceeded) {
        for (PigProgressNotificationListener listener: listeners) {
            listener.launchCompletedNotification(id, numJobsSucceeded);
        }
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
        conf.set("mapreduce.workflow.name", (getFileName() != null) ? getFileName() : "default");
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
        JobStatsBase js = null;
        Iterator<JobStatsBase> iter = jg.iterator();
        while (iter.hasNext()) {
            JobStatsBase job = iter.next();
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
                    JobStatsBase job = (JobStatsBase)op;
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


    private static class FeatureVisitor extends PhyPlanVisitor {
        private BitSet feature;

        public FeatureVisitor(PhysicalPlan plan, BitSet feature) {
            super(plan, new DepthFirstWalker<PhysicalOperator, PhysicalPlan>(
                    plan));
            this.feature = feature;
        }

        @Override
        public void visitFRJoin(POFRJoin join) throws VisitorException {
            feature.set(PIG_FEATURE.REPLICATED_JOIN.ordinal());
        }

        @Override
        public void visitMergeJoin(POMergeJoin join) throws VisitorException {
            if (join.getJoinType()==LOJoin.JOINTYPE.MERGESPARSE)
                feature.set(PIG_FEATURE.MERGE_SPARSE_JOIN.ordinal());
            else
                feature.set(PIG_FEATURE.MERGE_JOIN.ordinal());
        }

        @Override
        public void visitMergeCoGroup(POMergeCogroup mergeCoGrp)
                throws VisitorException {
            feature.set(PIG_FEATURE.MERGE_COGROUP.ordinal());;
        }

        @Override
        public void visitCollectedGroup(POCollectedGroup mg)
                throws VisitorException {
            feature.set(PIG_FEATURE.COLLECTED_GROUP.ordinal());
        }

        @Override
        public void visitDistinct(PODistinct distinct) throws VisitorException {
            feature.set(PIG_FEATURE.DISTINCT.ordinal());
        }

        @Override
        public void visitStream(POStream stream) throws VisitorException {
            feature.set(PIG_FEATURE.STREAMING.ordinal());
        }

        @Override
        public void visitSplit(POSplit split) throws VisitorException {
            feature.set(PIG_FEATURE.MULTI_QUERY.ordinal());
        }

        @Override
        public void visitDemux(PODemux demux) throws VisitorException {
            feature.set(PIG_FEATURE.MULTI_QUERY.ordinal());
        }

        @Override
        public void visitPartialAgg(POPartialAgg partAgg){
            feature.set(PIG_FEATURE.MAP_PARTIALAGG.ordinal());
        }

    }

    private static class AliasVisitor extends PhyPlanVisitor {

        private HashSet<String> aliasSet;

        private List<String> alias;

        private final List<String> aliasLocation;

        public AliasVisitor(PhysicalPlan plan, List<String> alias, List<String> aliasLocation) {
            super(plan, new DepthFirstWalker<PhysicalOperator, PhysicalPlan>(
                    plan));
            this.alias = alias;
            this.aliasLocation = aliasLocation;
            aliasSet = new HashSet<String>();
            if (!alias.isEmpty()) {
                for (String s : alias) aliasSet.add(s);
            }
        }

        @Override
        public void visitLoad(POLoad load) throws VisitorException {
            setAlias(load);
            super.visitLoad(load);
        }

        @Override
        public void visitFRJoin(POFRJoin join) throws VisitorException {
            setAlias(join);
            super.visitFRJoin(join);
        }

        @Override
        public void visitMergeJoin(POMergeJoin join) throws VisitorException {
            setAlias(join);
            super.visitMergeJoin(join);
        }

        @Override
        public void visitMergeCoGroup(POMergeCogroup mergeCoGrp)
                throws VisitorException {
            setAlias(mergeCoGrp);
            super.visitMergeCoGroup(mergeCoGrp);
        }

        @Override
        public void visitCollectedGroup(POCollectedGroup mg)
                throws VisitorException {
            setAlias(mg);
            super.visitCollectedGroup(mg);
        }

        @Override
        public void visitDistinct(PODistinct distinct) throws VisitorException {
            setAlias(distinct);
            super.visitDistinct(distinct);
        }

        @Override
        public void visitStream(POStream stream) throws VisitorException {
            setAlias(stream);
            super.visitStream(stream);
        }

        @Override
        public void visitFilter(POFilter fl) throws VisitorException {
            setAlias(fl);
            super.visitFilter(fl);
        }

        @Override
        public void visitLocalRearrange(POLocalRearrange lr) throws VisitorException {
            setAlias(lr);
            super.visitLocalRearrange(lr);
        }

        @Override
        public void visitPOForEach(POForEach nfe) throws VisitorException {
            setAlias(nfe);
            super.visitPOForEach(nfe);
        }

        @Override
        public void visitUnion(POUnion un) throws VisitorException {
            setAlias(un);
            super.visitUnion(un);
        }

        @Override
        public void visitSort(POSort sort) throws VisitorException {
            setAlias(sort);
            super.visitSort(sort);
        }

        @Override
        public void visitLimit(POLimit lim) throws VisitorException {
            setAlias(lim);
            super.visitLimit(lim);
        }

        @Override
        public void visitSkewedJoin(POSkewedJoin sk) throws VisitorException {
            setAlias(sk);
            super.visitSkewedJoin(sk);
        }

        private void setAlias(PhysicalOperator op) {
            String s = op.getAlias();
            if (s != null) {
                if (!aliasSet.contains(s)) {
                    alias.add(s);
                    aliasSet.add(s);
                }
            }
            List<OriginalLocation> originalLocations = op.getOriginalLocations();
            for (OriginalLocation originalLocation : originalLocations) {
                aliasLocation.add(originalLocation.toString());
            }
        }
    }

}
