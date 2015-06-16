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
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.VersionInfo;
import org.apache.pig.PigConfiguration;
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
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStream;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POUnion;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.OperatorPlan;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.JarManager;
import org.apache.pig.newplan.logical.relational.LOCogroup;
import org.apache.pig.newplan.logical.relational.LOCogroup.GROUPTYPE;
import org.apache.pig.newplan.logical.relational.LOCross;
import org.apache.pig.newplan.logical.relational.LODistinct;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOJoin;
import org.apache.pig.newplan.logical.relational.LOJoin.JOINTYPE;
import org.apache.pig.newplan.logical.relational.LOLimit;
import org.apache.pig.newplan.logical.relational.LONative;
import org.apache.pig.newplan.logical.relational.LORank;
import org.apache.pig.newplan.logical.relational.LOSort;
import org.apache.pig.newplan.logical.relational.LOSplit;
import org.apache.pig.newplan.logical.relational.LOStream;
import org.apache.pig.newplan.logical.relational.LOUnion;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalNodesVisitor;
import org.apache.pig.tools.pigstats.mapreduce.MRScriptState;

import com.google.common.collect.Lists;

/**
 * ScriptStates encapsulates settings for a Pig script that runs on a hadoop
 * cluster. These settings are added to all MR jobs spawned by the script and in
 * turn are persisted in the hadoop job xml. With the properties already in the
 * job xml, users who want to know the relations between the script and MR jobs
 * can derive them from the job xmls.
 */
public abstract class ScriptState {

    /**
     * Keys of Pig settings added to Jobs
     */
    protected enum PIG_PROPERTY {
        SCRIPT_ID("pig.script.id"),
        SCRIPT("pig.script"),
        COMMAND_LINE("pig.command.line"),
        HADOOP_VERSION("pig.hadoop.version"),
        VERSION("pig.version"),
        INPUT_DIRS("pig.input.dirs"),
        MAP_OUTPUT_DIRS("pig.map.output.dirs"),
        REDUCE_OUTPUT_DIRS("pig.reduce.output.dirs"),
        JOB_PARENTS("pig.parent.jobid"),
        JOB_FEATURE("pig.job.feature"),
        SCRIPT_FEATURES("pig.script.features"),
        JOB_ALIAS("pig.alias"),
        JOB_ALIAS_LOCATION("pig.alias.location");

        private String displayStr;

        private PIG_PROPERTY(String s) {
            displayStr = s;
        }

        @Override
        public String toString() {
            return displayStr;
        }
    };

    /**
     * Features used in a Pig script
     */
    public static enum PIG_FEATURE {
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
        RANK,
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

    private static final Log LOG = LogFactory.getLog(ScriptState.class);

    /**
     * PIG-3844. Each thread should have its own copy of ScriptState. We initialize the ScriptState
     * for new threads with the ScriptState of its parent thread, using InheritableThreadLocal.
     * Used eg. in PPNL running in separate thread.
     */
    private static InheritableThreadLocal<ScriptState> tss = new InheritableThreadLocal<ScriptState>();

    protected String id;

    protected String script;
    protected String commandLine;
    protected String fileName;

    protected String pigVersion;
    protected String hadoopVersion;

    protected long scriptFeatures;

    protected PigContext pigContext;

    protected List<PigProgressNotificationListener> listeners = Lists.newArrayList();

    protected ScriptState(String id) {
        this.id = id;
        this.script = "";
    }

    public static ScriptState get() {
        return tss.get();
    }

    public static ScriptState start(ScriptState state) {
        tss.set(state);
        return tss.get();
    }

    /**
     * @deprecated use {@link org.apache.pig.tools.pigstats.ScriptState#start(ScriptState)} instead.
     */
    @Deprecated
    public static ScriptState start(String commandLine, PigContext pigContext) {
        ScriptState ss = new MRScriptState(UUID.randomUUID().toString());
        ss.setCommandLine(commandLine);
        ss.setPigContext(pigContext);
        tss.set(ss);
        return ss;
    }

    public void registerListener(PigProgressNotificationListener listener) {
        listeners.add(listener);
    }

    public List<PigProgressNotificationListener> getAllListeners() {
        return listeners;
    }

    public void emitInitialPlanNotification(OperatorPlan<?> plan) {
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

    public void setScript(File file) {
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            setScript(reader);
        } catch (FileNotFoundException e) {
            LOG.warn("unable to find the file", e);
        } finally {
            if (reader != null) {
              try {
                  reader.close();
              } catch (IOException ignored) {
              }
            }
        }
    }

    public void setScript(String script) {
        if (script == null)
            return;

        // restrict the size of the script to be stored in job conf
        int maxScriptSize = 10240;
        if (pigContext != null) {
            String prop = pigContext.getProperties().getProperty(PigConfiguration.PIG_SCRIPT_MAX_SIZE);
            if (prop != null) {
                maxScriptSize = Integer.valueOf(prop);
            }
        }
        script = (script.length() > maxScriptSize) ? script.substring(0, maxScriptSize)
                                                   : script;

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
        if (hadoopVersion == null) {
            hadoopVersion = VersionInfo.getVersion();
        }
        return (hadoopVersion == null) ? "" : hadoopVersion;
    }

    public String getPigVersion() {
        if (pigVersion == null) {
            String findContainingJar = JarManager
                    .findContainingJar(ScriptState.class);
            if (findContainingJar != null) {
                try {
                    JarFile jar = new JarFile(findContainingJar);
                    final Manifest manifest = jar.getManifest();
                    final Map<String, Attributes> attrs = manifest.getEntries();
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

    public String getFileName() {
        return (fileName == null) ? "" : fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getId() {
        return id;
    }

    public void setCommandLine(String commandLine) {
        this.commandLine = new String(Base64.encodeBase64(commandLine
                .getBytes()));
    }

    public String getCommandLine() {
        return (commandLine == null) ? "" : commandLine;
    }

    public String getScript() {
        return (script == null) ? "" : script;
    }

    protected void setScript(BufferedReader reader) {
        StringBuilder sb = new StringBuilder();
        try {
            String line = reader.readLine();
            while (line != null) {
                sb.append(line).append("\n");
                line = reader.readLine();
            }
        } catch (IOException e) {
            LOG.warn("unable to parse the script", e);
        }
        setScript(sb.toString());
    }

    protected long bitSetToLong(BitSet bs) {
        long ret = 0;
        for (int i = bs.nextSetBit(0); i >= 0; i = bs.nextSetBit(i + 1)) {
            ret |= (1L << i);
        }
        return ret;
    }

    protected String featureLongToString(long l) {
        if (l == 0)
            return PIG_FEATURE.UNKNOWN.name();

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < PIG_FEATURE.values().length; i++) {
            if (((l >> i) & 0x00000001) != 0) {
                if (sb.length() > 0)
                    sb.append(",");
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

    public String getScriptFeatures() {
        return featureLongToString(scriptFeatures);
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
        public void visit(LORank op) {
            feature.set(PIG_FEATURE.RANK.ordinal());
        }

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

    protected static class FeatureVisitor extends PhyPlanVisitor {
        private BitSet feature;

        public FeatureVisitor(PhysicalPlan plan, BitSet feature) {
            super(plan, new DepthFirstWalker<PhysicalOperator, PhysicalPlan>(plan));
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

    protected static class AliasVisitor extends PhyPlanVisitor {
        private HashSet<String> aliasSet;
        private List<String> alias;
        private final List<String> aliasLocation;

        public AliasVisitor(PhysicalPlan plan, List<String> alias, List<String> aliasLocation) {
            super(plan, new DepthFirstWalker<PhysicalOperator, PhysicalPlan>(plan));
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
