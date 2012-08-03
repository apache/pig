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
package org.apache.pig.tools.pigstats;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.VersionInfo;
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
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.JarManager;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.logical.relational.LOCogroup;
import org.apache.pig.newplan.logical.relational.LOCross;
import org.apache.pig.newplan.logical.relational.LODistinct;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOJoin;
import org.apache.pig.newplan.logical.relational.LOLimit;
import org.apache.pig.newplan.logical.relational.LONative;
import org.apache.pig.newplan.logical.relational.LOSort;
import org.apache.pig.newplan.logical.relational.LOSplit;
import org.apache.pig.newplan.logical.relational.LOStream;
import org.apache.pig.newplan.logical.relational.LOUnion;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalNodesVisitor;
import org.apache.pig.newplan.logical.relational.LOCogroup.GROUPTYPE;
import org.apache.pig.newplan.logical.relational.LOJoin.JOINTYPE;
import org.apache.pig.tools.pigstats.PigStats.JobGraph;

/**
 * ScriptStates encapsulates settings for a Pig script that runs on a hadoop 
 * cluster. These settings are added to all MR jobs spawned by the script and 
 * in turn are persisted in the hadoop job xml. With the properties already in 
 * the job xml, users who want to know the relations between the script and MR
 * jobs can derive them from the job xmls.  
 */
public class ScriptState {
    
    /**
     * Keys of Pig settings added in MR job
     */
    private enum PIG_PROPERTY {
        SCRIPT_ID           ("pig.script.id"),
        SCRIPT              ("pig.script"),       
        COMMAND_LINE        ("pig.command.line"),
        HADOOP_VERSION      ("pig.hadoop.version"),
        VERSION             ("pig.version"),
        INPUT_DIRS          ("pig.input.dirs"),
        MAP_OUTPUT_DIRS     ("pig.map.output.dirs"),
        REDUCE_OUTPUT_DIRS  ("pig.reduce.output.dirs"),
        JOB_PARENTS         ("pig.parent.jobid"),
        JOB_FEATURE         ("pig.job.feature"),
        SCRIPT_FEATURES     ("pig.script.features"),
        JOB_ALIAS           ("pig.alias"),
        JOB_ALIAS_LOCATION  ("pig.alias.location");
       
        private String displayStr;
        
        private PIG_PROPERTY(String s) {
            displayStr = s;
        }
        
        @Override
        public String toString() { return displayStr; }
    };
    
    /**
     * Features used in a Pig script
     */
    static enum PIG_FEATURE {
        UNKNOWN,
        MERGE_JOIN,
        MERGE_SPARSE_JOIN,
        REPLICATED_JOIN,
        SKEWED_JOIN,
        HASH_JOIN,
        COLLECTED_GROUP,
        MERGE_COGROUP,
        COGROUP,
        GROUP_BY,
        ORDER_BY,
        DISTINCT,
        STREAMING,
        SAMPLER,
        INDEXER,
        MULTI_QUERY,
        FILTER,
        MAP_ONLY,
        CROSS,
        LIMIT,
        UNION,
        COMBINER,
        NATIVE,
        MAP_PARTIALAGG;
    };
    
    /**
     * Pig property that allows user to turn off the inclusion of settings
     * in the jobs 
     */
    public static final String INSERT_ENABLED = "pig.script.info.enabled";
    
    /**
     * Restricts the size of Pig script stored in job xml 
     */
    public static final int MAX_SCRIPT_SIZE = 10240; 
    
    private static final Log LOG = LogFactory.getLog(ScriptState.class);

    private static ThreadLocal<ScriptState> tss = new ThreadLocal<ScriptState>();
       
    private String id;
    
    private String script;
    private String commandLine;
    private String fileName;
    
    private String pigVersion;
    private String hodoopVersion;
    
    private long scriptFeatures;
    
    private PigContext pigContext;
    
    private Map<MapReduceOper, String> featureMap = null;
    private Map<MapReduceOper, String> aliasMap = new HashMap<MapReduceOper, String>();
    private Map<MapReduceOper, String> aliasLocationMap = new HashMap<MapReduceOper, String>();
    
    private List<PigProgressNotificationListener> listeners
            = new ArrayList<PigProgressNotificationListener>();
    
    public static ScriptState start(String commandLine, PigContext pigContext) {
        ScriptState ss = new ScriptState(UUID.randomUUID().toString());
        ss.setCommandLine(commandLine);
        ss.setPigContext(pigContext);
        tss.set(ss);
        return ss;
    }
    
    private ScriptState(String id) {
        this.id = id;
        this.script = ""; 
    }

    public static ScriptState get() {
        if (tss.get() == null) {
            ScriptState.start("", null);
        }
        return tss.get();
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
    
    public void emitjobFinishedNotification(JobStats jobStats) {
        for (PigProgressNotificationListener listener: listeners) {
            listener.jobFinishedNotification(id, jobStats);
        }
    }
    
    public void emitJobFailedNotification(JobStats jobStats) {
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
    }
 
    public void setScript(File file) {            
        try {
            setScript(new BufferedReader(new FileReader(file)));
        } catch (FileNotFoundException e) {
            LOG.warn("unable to find the file", e);
        }
    }

    public void setScript(String script) {        
        if (script == null) return;
        
        // restrict the size of the script to be stored in job conf
        script = (script.length() > MAX_SCRIPT_SIZE) ? script.substring(0,
                MAX_SCRIPT_SIZE) : script;
        
        // XML parser cann't handle certain characters, including
        // the control character (&#1). Use Base64 encoding to
        // get around this problem        
        this.script = new String(Base64.encodeBase64(script.getBytes()));
    }
   
    public void setScriptFeatures(LogicalPlan plan) {
        BitSet bs = new BitSet();
        try {
            new LogicalPlanFeatureVisitor(plan, bs).visit();
        } catch (FrontendException e) {
            LOG.warn("unable to get script feature", e);
        }
        scriptFeatures = bitSetToLong(bs);        
        
        LOG.info("Pig features used in the script: "
                + featureLongToString(scriptFeatures));
    }
    
    public String getHadoopVersion() {
        if (hodoopVersion == null) {
            hodoopVersion = VersionInfo.getVersion();
        }
        return (hodoopVersion == null) ? "" : hodoopVersion;
    }
    
    public String getPigVersion() {
        if (pigVersion == null) {
            String findContainingJar = JarManager.findContainingJar(ScriptState.class);
            if (findContainingJar != null) {
            try { 
                JarFile jar = new JarFile(findContainingJar); 
                final Manifest manifest = jar.getManifest(); 
                final Map <String,Attributes> attrs = manifest.getEntries(); 
                Attributes attr = attrs.get("org/apache/pig");
                pigVersion = attr.getValue("Implementation-Version");
            } catch (Exception e) { 
                LOG.warn("unable to read pigs manifest file"); 
            } 
            } else {
                LOG.warn("unable to read pigs manifest file. Not running from the Pig jar");
        }
        }
        return (pigVersion == null) ? "" : pigVersion;
    }
        
    public String getFileName() { return fileName; }
    
    public void setFileName(String fileName) {
        this.fileName = fileName;
    }
    
    String getId() { return id; }
        
    private String getCommandLine() {
        return (commandLine == null) ? "" : commandLine;
    }
    
    private void setCommandLine(String commandLine) {
        this.commandLine = commandLine;
    }
    
    private String getScript() {
        return (script == null) ? "" : script;
    }
    
    private void setScript(BufferedReader reader) {
        StringBuilder sb = new StringBuilder();
        try {
            String line = reader.readLine();
            while (line != null) {
                if (line.length() > 0) {
                    sb.append(line).append("\n");
                }                
                line = reader.readLine();
            }            
        } catch (IOException e) {
            LOG.warn("unable to parse the script", e);
        }
        setScript(sb.toString());
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
    
    String getScriptFeatures() {
        return featureLongToString(scriptFeatures);
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
    
    private long bitSetToLong(BitSet bs) {
        long ret = 0;
        for (int i = bs.nextSetBit(0); i >= 0; i = bs.nextSetBit(i+1)) {
            ret |= (1L << i);
        }
        return ret;
    }
    
    private String featureLongToString(long l) {
        if (l == 0) return PIG_FEATURE.UNKNOWN.name();
        
        StringBuilder sb = new StringBuilder();
        for (int i=0; i<PIG_FEATURE.values().length; i++) {
            if (((l >> i) & 0x00000001) != 0) {
                if (sb.length() > 0) sb.append(",");
                sb.append(PIG_FEATURE.values()[i].name());
            }
        }
        return sb.toString();
    }
    
    public void setPigContext(PigContext pigContext) {
        this.pigContext = pigContext;
    }

    public PigContext getPigContext() {
        return pigContext;
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
    
    static class LogicalPlanFeatureVisitor extends LogicalRelationalNodesVisitor {
        
        private BitSet feature;
        
        protected LogicalPlanFeatureVisitor(LogicalPlan plan, BitSet feature) throws FrontendException {
            super(plan, new org.apache.pig.newplan.DepthFirstWalker(plan));            
            this.feature = feature;
        }
        
        @Override
        public void visit(LOCogroup op) {
            if (op.getGroupType() == GROUPTYPE.COLLECTED) {
                feature.set(PIG_FEATURE.COLLECTED_GROUP.ordinal());
            } else if (op.getGroupType() == GROUPTYPE.MERGE) {
                feature.set(PIG_FEATURE.MERGE_COGROUP.ordinal());                
            } else if (op.getGroupType() == GROUPTYPE.REGULAR) {
                if (op.getExpressionPlans().size() > 1) {
                    feature.set(PIG_FEATURE.COGROUP.ordinal());
                } else {
                    feature.set(PIG_FEATURE.GROUP_BY.ordinal());
                }
            }
        }
        
        @Override
        public void visit(LOCross op) {
            feature.set(PIG_FEATURE.CROSS.ordinal());
        }
        
        @Override
        public void visit(LODistinct op) {
            feature.set(PIG_FEATURE.DISTINCT.ordinal());
        }
        
        @Override
        public void visit(LOFilter op) {
            feature.set(PIG_FEATURE.FILTER.ordinal());
        }
        
        @Override
        public void visit(LOForEach op) {
            
        }
                
        @Override
        public void visit(LOJoin op) {
            if (op.getJoinType() == JOINTYPE.HASH) {
                feature.set(PIG_FEATURE.HASH_JOIN.ordinal());
            } else if (op.getJoinType() == JOINTYPE.MERGE) {
                feature.set(PIG_FEATURE.MERGE_JOIN.ordinal());
            } else if (op.getJoinType() == JOINTYPE.MERGESPARSE) {
                feature.set(PIG_FEATURE.MERGE_SPARSE_JOIN.ordinal());
            } else if (op.getJoinType() == JOINTYPE.REPLICATED) {
                feature.set(PIG_FEATURE.REPLICATED_JOIN.ordinal());
            } else if (op.getJoinType() == JOINTYPE.SKEWED) {
                feature.set(PIG_FEATURE.SKEWED_JOIN.ordinal());
            }
        }
        
        @Override
        public void visit(LOLimit op) {
            feature.set(PIG_FEATURE.LIMIT.ordinal());
        }
        
        @Override
        public void visit(LOSort op) {
            feature.set(PIG_FEATURE.ORDER_BY.ordinal());
        }
        
        @Override
        public void visit(LOStream op) {
            feature.set(PIG_FEATURE.STREAMING.ordinal());
        }
        
        @Override
        public void visit(LOSplit op) {
            
        }
        
        @Override
        public void visit(LOUnion op) {
            feature.set(PIG_FEATURE.UNION.ordinal());
        }
        
        @Override
        public void visit(LONative n) {
            feature.set(PIG_FEATURE.NATIVE.ordinal());
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
