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
package org.apache.pig.tools.pigstats.spark;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkOpPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkOperPlan;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkOperator;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.tools.pigstats.ScriptState;

import com.google.common.collect.Maps;

/**
 * ScriptStates encapsulates settings for a Pig script that runs on a hadoop
 * cluster. These settings are added to all Spark jobs spawned by the script and
 * in turn are persisted in the hadoop job xml. With the properties already in
 * the job xml, users who want to know the relations between the script and Spark
 * jobs can derive them from the job xmls.
 */
public class SparkScriptState extends ScriptState {
    public SparkScriptState(String id) {
        super(id);
    }

    private SparkScriptInfo scriptInfo = null;

    public void setScriptInfo(SparkOperPlan plan) {
        this.scriptInfo = new SparkScriptInfo(plan);
    }

    public SparkScriptInfo getScriptInfo() {
        return scriptInfo;
    }

    public static class SparkScriptInfo {

        private static final Log LOG = LogFactory.getLog(SparkScriptInfo.class);
        private SparkOperPlan sparkPlan;
        private String alias;
        private String aliasLocation;
        private String features;

        private Map<OperatorKey, String> featuresMap = Maps.newHashMap();
        private Map<OperatorKey, String> aliasMap = Maps.newHashMap();
        private Map<OperatorKey, String> aliasLocationMap = Maps.newHashMap();

        public SparkScriptInfo(SparkOperPlan sparkPlan) {
            this.sparkPlan = sparkPlan;
            initialize();
        }

        private void initialize() {
            try {
                new DAGAliasVisitor(sparkPlan).visit();
            } catch (VisitorException e) {
                LOG.warn("Cannot calculate alias information for DAG", e);
            }
        }

        public String getAlias(SparkOperator sparkOp) {
            return aliasMap.get(sparkOp.getOperatorKey());
        }

        public String getAliasLocation(SparkOperator sparkOp) {
            return aliasLocationMap.get(sparkOp.getOperatorKey());
        }

        public String getPigFeatures(SparkOperator sparkOp) {
            return featuresMap.get(sparkOp.getOperatorKey());
        }

        class DAGAliasVisitor extends SparkOpPlanVisitor {

            private Set<String> aliases;
            private Set<String> aliasLocations;
            private BitSet featureSet;

            public DAGAliasVisitor(SparkOperPlan plan) {
                super(plan, new DependencyOrderWalker<SparkOperator, SparkOperPlan>(plan));
                this.aliases = new HashSet<String>();
                this.aliasLocations = new HashSet<String>();
                this.featureSet = new BitSet();
            }

            @Override
            public void visitSparkOp(SparkOperator sparkOp) throws VisitorException {

                ArrayList<String> aliasList = new ArrayList<String>();
                String aliasLocationStr = "";
                try {
                    ArrayList<String> aliasLocationList = new ArrayList<String>();
                    new AliasVisitor(sparkOp.physicalPlan, aliasList, aliasLocationList).visit();
                    aliasLocationStr += LoadFunc.join(aliasLocationList, ",");
                    if (!aliasList.isEmpty()) {
                        Collections.sort(aliasList);
                        aliases.addAll(aliasList);
                        aliasLocations.addAll(aliasLocationList);
                    }
                } catch (VisitorException e) {
                    LOG.warn("unable to get alias", e);
                }
                aliasMap.put(sparkOp.getOperatorKey(), LoadFunc.join(aliasList, ","));
                aliasLocationMap.put(sparkOp.getOperatorKey(), aliasLocationStr);


                BitSet feature = new BitSet();
                feature.clear();
                if (sparkOp.isSampler()) {
                    feature.set(PIG_FEATURE.SAMPLER.ordinal());
                }
                if (sparkOp.isIndexer()) {
                    feature.set(PIG_FEATURE.INDEXER.ordinal());
                }
                if (sparkOp.isCogroup()) {
                    feature.set(PIG_FEATURE.COGROUP.ordinal());
                }
                if (sparkOp.isGroupBy()) {
                    feature.set(PIG_FEATURE.GROUP_BY.ordinal());
                }
                if (sparkOp.isRegularJoin()) {
                    feature.set(PIG_FEATURE.HASH_JOIN.ordinal());
                }
                if (sparkOp.isUnion()) {
                    feature.set(PIG_FEATURE.UNION.ordinal());
                }
                if (sparkOp.isNative()) {
                    feature.set(PIG_FEATURE.NATIVE.ordinal());
                }
                if (sparkOp.isLimit() || sparkOp.isLimitAfterSort()) {
                    feature.set(PIG_FEATURE.LIMIT.ordinal());
                }
                try {
                    new FeatureVisitor(sparkOp.physicalPlan, feature).visit();
                } catch (VisitorException e) {
                    LOG.warn("Feature visitor failed", e);
                }
                StringBuilder sb = new StringBuilder();
                for (int i = feature.nextSetBit(0); i >= 0; i = feature.nextSetBit(i + 1)) {
                    if (sb.length() > 0) sb.append(",");
                    sb.append(PIG_FEATURE.values()[i].name());
                }
                featuresMap.put(sparkOp.getOperatorKey(), sb.toString());
                for (int i = 0; i < feature.length(); i++) {
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
                for (int i = featureSet.nextSetBit(0); i >= 0; i = featureSet.nextSetBit(i + 1)) {
                    if (sb.length() > 0) sb.append(",");
                    sb.append(PIG_FEATURE.values()[i].name());
                }
                features = sb.toString();
            }
        }
    }
}
