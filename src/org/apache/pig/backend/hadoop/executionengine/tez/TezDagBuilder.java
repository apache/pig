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

package org.apache.pig.backend.hadoop.executionengine.tez;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.ClientDistributedCacheManager;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigConfiguration;
import org.apache.pig.PigException;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.HDataType;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.executionengine.JobCreationException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.InputSizeReducerEstimator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler.PigGroupingBigDecimalWritableComparator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler.PigGroupingBigIntegerWritableComparator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler.PigGroupingBooleanWritableComparator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler.PigGroupingCharArrayWritableComparator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler.PigGroupingDBAWritableComparator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler.PigGroupingDateTimeWritableComparator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler.PigGroupingDoubleWritableComparator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler.PigGroupingFloatWritableComparator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler.PigGroupingIntWritableComparator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler.PigGroupingLongWritableComparator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler.PigGroupingPartitionWritableComparator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler.PigGroupingTupleWritableComparator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler.PigSecondaryKeyGroupComparator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PhyPlanSetter;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigBigDecimalRawComparator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigBigIntegerRawComparator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigBooleanRawComparator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigBytesRawComparator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigCombiner;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigDateTimeRawComparator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigDoubleRawComparator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigFloatRawComparator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigInputFormat;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigIntRawComparator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigLongRawComparator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigOutputFormat;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSecondaryKeyComparator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTextRawComparator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTupleSortComparator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.partitioners.SecondaryKeyPartitioner;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.EndOfAllInputSetter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POUserFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSplit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.backend.hadoop.executionengine.tez.TezPOPackageAnnotator.LoRearrangeDiscoverer;
import org.apache.pig.backend.hadoop.executionengine.tez.util.MRToTezHelper;
import org.apache.pig.backend.hadoop.executionengine.tez.util.SecurityHelper;
import org.apache.pig.backend.hadoop.executionengine.util.ParallelConstantVisitor;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.io.NullablePartitionWritable;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.UDFContext;
import org.apache.tez.common.TezJobConfig;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.EdgeManagerDescriptor;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.GroupInputEdge;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.VertexGroup;
import org.apache.tez.dag.api.VertexManagerPluginDescriptor;
import org.apache.tez.dag.library.vertexmanager.ShuffleVertexManager;
import org.apache.tez.mapreduce.combine.MRCombiner;
import org.apache.tez.mapreduce.committer.MROutputCommitter;
import org.apache.tez.mapreduce.common.MRInputSplitDistributor;
import org.apache.tez.mapreduce.hadoop.InputSplitInfo;
import org.apache.tez.mapreduce.hadoop.MRHelpers;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.mapreduce.partition.MRPartitioner;
import org.apache.tez.runtime.library.input.ConcatenatedMergedKeyValueInput;
import org.apache.tez.runtime.library.input.ShuffledMergedInput;
import org.apache.tez.runtime.library.input.SortedGroupedMergedInput;

/**
 * A visitor to construct DAG out of Tez plan.
 */
public class TezDagBuilder extends TezOpPlanVisitor {
    private static final Log log = LogFactory.getLog(TezJobControlCompiler.class);

    private static final String REDUCER_ESTIMATOR_KEY = "pig.exec.reducer.estimator";
    private static final String REDUCER_ESTIMATOR_ARG_KEY =  "pig.exec.reducer.estimator.arg";

    private DAG dag;
    private Map<String, LocalResource> localResources;
    private PigContext pc;
    private Configuration globalConf;

    private String scope;
    private NodeIdGenerator nig;

    public TezDagBuilder(PigContext pc, TezOperPlan plan, DAG dag,
            Map<String, LocalResource> localResources) {
        super(plan, new DependencyOrderWalker<TezOperator, TezOperPlan>(plan));
        this.pc = pc;
        this.globalConf = ConfigurationUtil.toConfiguration(pc.getProperties(), true);
        this.localResources = localResources;
        this.dag = dag;
        this.scope = plan.getRoots().get(0).getOperatorKey().getScope();
        this.nig = NodeIdGenerator.getGenerator();

        try {
            // Add credentials from binary token file and get tokens for namenodes
            // specified in mapreduce.job.hdfs-servers
            SecurityHelper.populateTokenCache(globalConf, dag.getCredentials());
        } catch (IOException e) {
            throw new RuntimeException("Error while fetching delegation tokens", e);
        }
    }

    @Override
    public void visitTezOp(TezOperator tezOp) throws VisitorException {
        TezOperPlan tezPlan = getPlan();
        List<TezOperator> predecessors = tezPlan.getPredecessors(tezOp);

        // Construct vertex for the current Tez operator
        Vertex to = null;
        try {
            if (!tezOp.isVertexGroup()) {
                boolean isMap = (predecessors == null || predecessors.isEmpty()) ? true : false;
                to = newVertex(tezOp, isMap);
                dag.addVertex(to);
            } else {
                // For union, we construct VertexGroup after iterating the
                // predecessors.
            }
        } catch (Exception e) {
            throw new VisitorException("Cannot create vertex for "
                    + tezOp.name(), e);
        }

        // Connect the new vertex with predecessor vertices
        if (predecessors != null) {
            Vertex[] groupMembers = new Vertex[predecessors.size()];

            for (int i = 0; i < predecessors.size(); i++) {
                // Since this is a dependency order walker, predecessor vertices
                // must have already been created.
                TezOperator pred = predecessors.get(i);
                try {
                    if (pred.isVertexGroup()) {
                        VertexGroup from = pred.getVertexGroupInfo().getVertexGroup();
                        // The plan of vertex group is empty. Since we create the Edge based on
                        // some of the operators in the plan refer to one of the vertex group members.
                        // Both the vertex group and its members reference same EdgeDescriptor object to the
                        // the successor
                        GroupInputEdge edge = newGroupInputEdge(
                                getPlan().getOperator(pred.getVertexGroupMembers().get(0)), tezOp, from, to);
                        dag.addEdge(edge);
                    } else {
                        Vertex from = dag.getVertex(pred.getOperatorKey().toString());
                        if (tezOp.isVertexGroup()) {
                            groupMembers[i] = from;
                        } else {
                            EdgeProperty prop = newEdge(pred, tezOp);
                            Edge edge = new Edge(from, to, prop);
                            dag.addEdge(edge);
                        }
                    }
                } catch (IOException e) {
                    throw new VisitorException("Cannot create edge from "
                            + pred.name() + " to " + tezOp.name(), e);
                }
            }

            if (tezOp.isVertexGroup()) {
                String groupName = tezOp.getOperatorKey().toString();
                VertexGroup vertexGroup = dag.createVertexGroup(groupName, groupMembers);
                tezOp.getVertexGroupInfo().setVertexGroup(vertexGroup);
                POStore store = tezOp.getVertexGroupInfo().getStore();
                if (store != null) {
                    vertexGroup.addOutput(store.getOperatorKey().toString(),
                            tezOp.getVertexGroupInfo().getStoreOutputDescriptor(),
                            MROutputCommitter.class);
                }
            }
        }
    }

    private GroupInputEdge newGroupInputEdge(TezOperator fromOp,
            TezOperator toOp, VertexGroup from, Vertex to) throws IOException {

        EdgeProperty edgeProperty = newEdge(fromOp, toOp);

        String groupInputClass = ConcatenatedMergedKeyValueInput.class.getName();

        // In case of SCATTER_GATHER and ShuffledUnorderedKVInput it will still be
        // ConcatenatedMergedKeyValueInput
        if(edgeProperty.getDataMovementType().equals(DataMovementType.SCATTER_GATHER)
                && edgeProperty.getEdgeDestination().getClassName().equals(ShuffledMergedInput.class.getName())) {
            groupInputClass = SortedGroupedMergedInput.class.getName();
        }

        return new GroupInputEdge(from, to, edgeProperty,
                new InputDescriptor(groupInputClass).setUserPayload(edgeProperty.getEdgeDestination().getUserPayload()));
    }

    /**
     * Return EdgeProperty that connects two vertices.
     *
     * @param from
     * @param to
     * @return EdgeProperty
     * @throws IOException
     */
    private EdgeProperty newEdge(TezOperator from, TezOperator to)
            throws IOException {
        TezEdgeDescriptor edge = to.inEdges.get(from.getOperatorKey());
        PhysicalPlan combinePlan = edge.combinePlan;

        InputDescriptor in = new InputDescriptor(edge.inputClassName);
        OutputDescriptor out = new OutputDescriptor(edge.outputClassName);

        Configuration conf = ConfigurationUtil.toConfiguration(pc.getProperties(), false);
        if (!combinePlan.isEmpty()) {
            addCombiner(combinePlan, to, conf);
        }

        List<POLocalRearrangeTez> lrs = PlanHelper.getPhysicalOperators(from.plan,
                POLocalRearrangeTez.class);

        for (POLocalRearrangeTez lr : lrs) {
            if (lr.getOutputKey().equals(to.getOperatorKey().toString())) {
                byte keyType = lr.getKeyType();
                setIntermediateOutputKeyValue(keyType, conf, to, lr.isConnectedToPackage());
                // In case of secondary key sort, main key type is the actual key type
                conf.set("pig.reduce.key.type", Byte.toString(lr.getMainKeyType()));
                break;
            }
        }

        conf.setIfUnset(TezJobConfig.TEZ_RUNTIME_PARTITIONER_CLASS,
                MRPartitioner.class.getName());

        if (edge.getIntermediateOutputKeyClass() != null) {
            conf.set(TezJobConfig.TEZ_RUNTIME_KEY_CLASS,
                    edge.getIntermediateOutputKeyClass());
        }

        if (edge.getIntermediateOutputValueClass() != null) {
            conf.set(TezJobConfig.TEZ_RUNTIME_VALUE_CLASS,
                    edge.getIntermediateOutputValueClass());
        }

        if (edge.getIntermediateOutputKeyComparatorClass() != null) {
            conf.set(TezJobConfig.TEZ_RUNTIME_KEY_COMPARATOR_CLASS,
                    edge.getIntermediateOutputKeyComparatorClass());
        }

        conf.setBoolean("mapred.mapper.new-api", true);
        conf.set("pig.pigContext", ObjectSerializer.serialize(pc));
        conf.set("udf.import.list",
                ObjectSerializer.serialize(PigContext.getPackageImportList()));

        if(to.isGlobalSort() || to.isLimitAfterSort()){
            conf.set("pig.sortOrder",
                    ObjectSerializer.serialize(to.getSortOrder()));
        }

        if (edge.isUseSecondaryKey()) {
            conf.set("pig.secondarySortOrder",
                    ObjectSerializer.serialize(edge.getSecondarySortOrder()));
            conf.set(org.apache.hadoop.mapreduce.MRJobConfig.PARTITIONER_CLASS_ATTR,
                    SecondaryKeyPartitioner.class.getName());
            // These needs to be on the vertex as well for POShuffleTezLoad to pick it up.
            // Tez framework also expects this to be per vertex and not edge. IFile.java picks
            // up keyClass and valueClass from vertex config. TODO - check with Tez folks
            // In MR - job.setSortComparatorClass() or MRJobConfig.KEY_COMPARATOR
            conf.set(TezJobConfig.TEZ_RUNTIME_KEY_COMPARATOR_CLASS,
                    PigSecondaryKeyComparator.class.getName());
            // In MR - job.setOutputKeyClass() or MRJobConfig.OUTPUT_KEY_CLASS
            conf.set(TezJobConfig.TEZ_RUNTIME_KEY_CLASS, NullableTuple.class.getName());
            setGroupingComparator(conf, PigSecondaryKeyGroupComparator.class.getName());
        }

        if (edge.partitionerClass != null) {
            conf.set(org.apache.hadoop.mapreduce.MRJobConfig.PARTITIONER_CLASS_ATTR,
                    edge.partitionerClass.getName());
        }

        conf.set("udf.import.list",
                ObjectSerializer.serialize(PigContext.getPackageImportList()));

        MRToTezHelper.processMRSettings(conf, globalConf);

        in.setUserPayload(TezUtils.createUserPayloadFromConf(conf));
        out.setUserPayload(TezUtils.createUserPayloadFromConf(conf));

        if (to.getEstimatedParallelism()!=-1 && (to.isGlobalSort()||to.isSkewedJoin())) {
            // Use custom edge
            return new EdgeProperty((EdgeManagerDescriptor)null,
                    edge.dataSourceType, edge.schedulingType, out, in);
            }

        return new EdgeProperty(edge.dataMovementType, edge.dataSourceType,
                edge.schedulingType, out, in);
    }

    private void addCombiner(PhysicalPlan combinePlan, TezOperator pkgTezOp,
            Configuration conf) throws IOException {
        POPackage combPack = (POPackage) combinePlan.getRoots().get(0);
        POLocalRearrange combRearrange = (POLocalRearrange) combinePlan
                .getLeaves().get(0);
        setIntermediateOutputKeyValue(combRearrange.getKeyType(), conf, pkgTezOp);

        LoRearrangeDiscoverer lrDiscoverer = new LoRearrangeDiscoverer(
                combinePlan, pkgTezOp, combPack);
        lrDiscoverer.visit();

        combinePlan.remove(combPack);
        conf.set(TezJobConfig.TEZ_RUNTIME_COMBINER_CLASS,
                MRCombiner.class.getName());
        conf.set(MRJobConfig.COMBINE_CLASS_ATTR,
                PigCombiner.Combine.class.getName());
        conf.setBoolean("mapred.mapper.new-api", true);
        conf.set("pig.pigContext", ObjectSerializer.serialize(pc));
        conf.set("udf.import.list",
                ObjectSerializer.serialize(PigContext.getPackageImportList()));
        conf.set("pig.combinePlan", ObjectSerializer.serialize(combinePlan));
        conf.set("pig.combine.package", ObjectSerializer.serialize(combPack));
        conf.set("pig.map.keytype", ObjectSerializer
                .serialize(new byte[] { combRearrange.getKeyType() }));
    }

    private Vertex newVertex(TezOperator tezOp, boolean isMap) throws IOException,
            ClassNotFoundException, InterruptedException {
        ProcessorDescriptor procDesc = new ProcessorDescriptor(
                tezOp.getProcessorName());

        // Pass physical plans to vertex as user payload.
        JobConf payloadConf = new JobConf(ConfigurationUtil.toConfiguration(pc.getProperties(), false));

        // We do this so that dag.getCredentials(), job.getCredentials(),
        // job.getConfiguration().getCredentials() all reference the same Credentials object
        // Unfortunately there is no setCredentials() on Job
        payloadConf.setCredentials(dag.getCredentials());
        // We won't actually use this job, but we need it to talk with the Load Store funcs
        @SuppressWarnings("deprecation")
        Job job = new Job(payloadConf);
        payloadConf = (JobConf) job.getConfiguration();

        if (tezOp.sampleOperator != null) {
            payloadConf.set(PigProcessor.SAMPLE_VERTEX, tezOp.sampleOperator.getOperatorKey().toString());
        }

        if (tezOp.sortOperator != null) {
            payloadConf.set(PigProcessor.SORT_VERTEX, tezOp.sortOperator.getOperatorKey().toString());
        }

        String tmp;
        long maxCombinedSplitSize = 0;
        if (!tezOp.combineSmallSplits() || pc.getProperties().getProperty(PigConfiguration.PIG_SPLIT_COMBINATION, "true").equals("false"))
            payloadConf.setBoolean(PigConfiguration.PIG_NO_SPLIT_COMBINATION, true);
        else if ((tmp = pc.getProperties().getProperty(PigConfiguration.PIG_MAX_COMBINED_SPLIT_SIZE, null)) != null) {
            try {
                maxCombinedSplitSize = Long.parseLong(tmp);
            } catch (NumberFormatException e) {
                log.warn("Invalid numeric format for pig.maxCombinedSplitSize; use the default maximum combined split size");
            }
        }
        if (maxCombinedSplitSize > 0)
            payloadConf.setLong("pig.maxCombinedSplitSize", maxCombinedSplitSize);

        List<POLoad> loads = processLoads(tezOp, payloadConf, job);
        LinkedList<POStore> stores = processStores(tezOp, payloadConf, job);

        payloadConf.set("pig.pigContext", ObjectSerializer.serialize(pc));
        payloadConf.set("udf.import.list",
                ObjectSerializer.serialize(PigContext.getPackageImportList()));
        payloadConf.set("exectype", "TEZ");
        payloadConf.setBoolean("mapred.mapper.new-api", true);
        payloadConf.setClass("mapreduce.inputformat.class",
                PigInputFormat.class, InputFormat.class);

        // Set parent plan for all operators in the Tez plan.
        new PhyPlanSetter(tezOp.plan).visit();

        // Set the endOfAllInput flag on the physical plan if certain operators that
        // use this property (such as STREAM) are present in the plan.
        EndOfAllInputSetter.EndOfAllInputChecker checker =
                new EndOfAllInputSetter.EndOfAllInputChecker(tezOp.plan);
        checker.visit();
        if (checker.isEndOfAllInputPresent()) {
            payloadConf.set(JobControlCompiler.END_OF_INP_IN_MAP, "true");
        }

        // Configure the classes for incoming shuffles to this TezOp
        // TODO: Refactor out resetting input keys, PIG-3957
        List<PhysicalOperator> roots = tezOp.plan.getRoots();
        if (roots.size() == 1 && roots.get(0) instanceof POPackage) {
            POPackage pack = (POPackage) roots.get(0);

            List<PhysicalOperator> succsList = tezOp.plan.getSuccessors(pack);
            if (succsList != null) {
                succsList = new ArrayList<PhysicalOperator>(succsList);
            }
            byte keyType = pack.getPkgr().getKeyType();
            tezOp.plan.remove(pack);
            payloadConf.set("pig.reduce.package", ObjectSerializer.serialize(pack));
            setIntermediateOutputKeyValue(keyType, payloadConf, tezOp);
            POShuffleTezLoad newPack;
            newPack = new POShuffleTezLoad(pack);
            if (tezOp.isSkewedJoin()) {
                newPack.setSkewedJoins(true);
            }
            tezOp.plan.add(newPack);

            // Set input keys for POShuffleTezLoad. This is used to identify
            // the inputs that are attached to the POShuffleTezLoad in the
            // backend.
            Map<Integer, String> localRearrangeMap = new TreeMap<Integer, String>();
            for (TezOperator pred : mPlan.getPredecessors(tezOp)) {
                if (tezOp.sampleOperator != null && tezOp.sampleOperator == pred) {
                    // skip sample vertex input
                } else {
                    String inputKey = pred.getOperatorKey().toString();
                    if (pred.isVertexGroup()) {
                        pred = mPlan.getOperator(pred.getVertexGroupMembers().get(0));
                    }
                    LinkedList<POLocalRearrangeTez> lrs =
                            PlanHelper.getPhysicalOperators(pred.plan, POLocalRearrangeTez.class);
                    for (POLocalRearrangeTez lr : lrs) {
                        if (lr.isConnectedToPackage()
                                && lr.getOutputKey().equals(tezOp.getOperatorKey().toString())) {
                            localRearrangeMap.put((int) lr.getIndex(), inputKey);
                        }
                    }
                }
            }
            for (Map.Entry<Integer, String> entry : localRearrangeMap.entrySet()) {
                newPack.addInputKey(entry.getValue());
            }

            if (succsList != null) {
                for (PhysicalOperator succs : succsList) {
                    tezOp.plan.connect(newPack, succs);
                }
            }

            setIntermediateOutputKeyValue(pack.getPkgr().getKeyType(), payloadConf, tezOp);
        } else if (roots.size() == 1 && roots.get(0) instanceof POIdentityInOutTez) {
            POIdentityInOutTez identityInOut = (POIdentityInOutTez) roots.get(0);
            // TODO Need to fix multiple input key mapping
            TezOperator identityInOutPred = null;
            for (TezOperator pred : mPlan.getPredecessors(tezOp)) {
                if (!pred.isSampleAggregation()) {
                    identityInOutPred = pred;
                    break;
                }
            }
            identityInOut.setInputKey(identityInOutPred.getOperatorKey().toString());
        } else if (roots.size() == 1 && roots.get(0) instanceof POValueInputTez) {
            POValueInputTez valueInput = (POValueInputTez) roots.get(0);

            LinkedList<String> scalarInputs = new LinkedList<String>();
            for (POUserFunc userFunc : PlanHelper.getPhysicalOperators(tezOp.plan, POUserFunc.class) ) {
                if (userFunc.getFunc() instanceof ReadScalarsTez) {
                    scalarInputs.add(((ReadScalarsTez)userFunc.getFunc()).getTezInputs()[0]);
                }
            }
            // Make sure we don't find the scalar
            for (TezOperator pred : mPlan.getPredecessors(tezOp)) {
                if (!scalarInputs.contains(pred.getOperatorKey().toString())) {
                    valueInput.setInputKey(pred.getOperatorKey().toString());
                    break;
                }
            }
        }
        JobControlCompiler.setOutputFormat(job);

        // set parent plan in all operators. currently the parent plan is really
        // used only when POStream, POSplit are present in the plan
        new PhyPlanSetter(tezOp.plan).visit();

        // Serialize the execution plan
        payloadConf.set(PigProcessor.PLAN,
                ObjectSerializer.serialize(tezOp.plan));

        UDFContext.getUDFContext().serialize(payloadConf);

        MRToTezHelper.processMRSettings(payloadConf, globalConf);

        if (!pc.inIllustrator) {
            for (POStore store : stores) {
                // unset inputs for POStore, otherwise, map/reduce plan will be unnecessarily deserialized
                store.setInputs(null);
                store.setParentPlan(null);
            }
            // We put them in the reduce because PigOutputCommitter checks the
            // ID of the task to see if it's a map, and if not, calls the reduce
            // committers.
            payloadConf.set(JobControlCompiler.PIG_MAP_STORES,
                    ObjectSerializer.serialize(new ArrayList<POStore>()));
            payloadConf.set(JobControlCompiler.PIG_REDUCE_STORES,
                    ObjectSerializer.serialize(stores));
        }

        // Can only set parallelism here if the parallelism isn't derived from
        // splits
        int parallelism = -1;
        InputSplitInfo inputSplitInfo = null;
        if (loads != null && loads.size() > 0) {
            // Not using MRInputAMSplitGenerator because delegation tokens are
            // fetched in FileInputFormat
            inputSplitInfo = MRHelpers.generateInputSplitsToMem(payloadConf);
            // TODO: Can be set to -1 if TEZ-601 gets fixed and getting input
            // splits can be moved to if(loads) block below
            parallelism = inputSplitInfo.getNumTasks();
            tezOp.setRequestedParallelism(parallelism);
        } else {
            int prevParallelism = -1;
            boolean isOneToOneParallelism = false;
            for (Map.Entry<OperatorKey,TezEdgeDescriptor> entry : tezOp.inEdges.entrySet()) {
                if (entry.getValue().dataMovementType == DataMovementType.ONE_TO_ONE) {
                    TezOperator pred = mPlan.getOperator(entry.getKey());
                    parallelism = pred.getEffectiveParallelism();
                    if (prevParallelism == -1) {
                        prevParallelism = parallelism;
                    } else if (prevParallelism != parallelism) {
                        throw new IOException("one to one sources parallelism for vertex "
                                + tezOp.getOperatorKey().toString() + " are not equal");
                    }
                    if (pred.getRequestedParallelism()!=-1) {
                        tezOp.setRequestedParallelism(pred.getRequestedParallelism());
                    } else {
                        tezOp.setEstimatedParallelism(pred.getEstimatedParallelism());
                    }
                    isOneToOneParallelism = true;
                    parallelism = -1;
                }
            }
            if (!isOneToOneParallelism) {
                if (tezOp.getRequestedParallelism()!=-1) {
                    parallelism = tezOp.getRequestedParallelism();
                } else if (pc.defaultParallel!=-1) {
                    parallelism = pc.defaultParallel;
                } else {
                    parallelism = estimateParallelism(job, mPlan, tezOp);
                    tezOp.setEstimatedParallelism(parallelism);
                    if (tezOp.isGlobalSort()||tezOp.isSkewedJoin()) {
                        // Vertex manager will set parallelism
                        parallelism = -1;
                    }
                }
            }
        }

        // Once we decide the parallelism of the sampler, propagate to
        // downstream operators if necessary
        if (tezOp.isSampler()) {
            // There could be multiple sampler and share the same sample aggregation job
            // and partitioner job
            TezOperator sampleAggregationOper = null;
            TezOperator sampleBasedPartionerOper = null;
            TezOperator sortOper = null;
            for (TezOperator succ : mPlan.getSuccessors(tezOp)) {
                if (succ.isVertexGroup()) {
                    succ = mPlan.getSuccessors(succ).get(0);
                }
                if (succ.isSampleAggregation()) {
                    sampleAggregationOper = succ;
                } else if (succ.isSampleBasedPartitioner()) {
                    sampleBasedPartionerOper = succ;
                }
            }
            sortOper = mPlan.getSuccessors(sampleBasedPartionerOper).get(0);

            if (sortOper.getRequestedParallelism()==-1 && pc.defaultParallel==-1) {
                // set estimate parallelism for order by/skewed join to sampler parallelism
                // that include:
                // 1. sort operator
                // 2. constant for sample aggregation oper
                sortOper.setEstimatedParallelism(parallelism);
                ParallelConstantVisitor visitor =
                        new ParallelConstantVisitor(sampleAggregationOper.plan, parallelism);
                visitor.visit();
            }
        }

        if (tezOp.isNeedEstimateParallelism()) {
            payloadConf.setBoolean(PigProcessor.ESTIMATE_PARALLELISM, true);
            log.info("Estimate quantile for sample aggregation vertex " + tezOp.getOperatorKey().toString());
        }

        // Take our assembled configuration and create a vertex
        byte[] userPayload = TezUtils.createUserPayloadFromConf(payloadConf);
        procDesc.setUserPayload(userPayload);

        Vertex vertex = new Vertex(tezOp.getOperatorKey().toString(), procDesc, parallelism,
                isMap ? MRHelpers.getMapResource(globalConf) : MRHelpers.getReduceResource(globalConf));

        Map<String, String> taskEnv = new HashMap<String, String>();
        MRHelpers.updateEnvironmentForMRTasks(globalConf, taskEnv, isMap);
        vertex.setTaskEnvironment(taskEnv);

        // All these classes are @InterfaceAudience.Private in Hadoop. Switch to Tez methods in TEZ-1012
        // set the timestamps, public/private visibility of the archives and files
        ClientDistributedCacheManager
                .determineTimestampsAndCacheVisibilities(globalConf);
        // get DelegationToken for each cached file
        ClientDistributedCacheManager.getDelegationTokens(globalConf,
                job.getCredentials());
        MRApps.setupDistributedCache(globalConf, localResources);
        vertex.setTaskLocalFiles(localResources);

        vertex.setTaskLaunchCmdOpts(isMap ? MRHelpers.getMapJavaOpts(globalConf)
                : MRHelpers.getReduceJavaOpts(globalConf));

        log.info("For vertex - " + tezOp.getOperatorKey().toString()
                + ": parallelism=" + parallelism
                + ", memory=" + vertex.getTaskResource().getMemory()
                + ", java opts=" + vertex.getTaskLaunchCmdOpts()
                );

        // Right now there can only be one of each of these. Will need to be
        // more generic when there can be more.
        for (POLoad ld : loads) {

            // TODO: These should get the globalConf, or a merged version that
            // keeps settings like pig.maxCombinedSplitSize
            vertex.setTaskLocationsHint(inputSplitInfo.getTaskLocationHints());
            vertex.addInput(ld.getOperatorKey().toString(),
                    new InputDescriptor(MRInput.class.getName())
                            .setUserPayload(MRHelpers.createMRInputPayload(
                                    userPayload,
                                    inputSplitInfo.getSplitsProto())),
                    MRInputSplitDistributor.class);
        }

        for (POStore store : stores) {

            ArrayList<POStore> emptyList = new ArrayList<POStore>();
            ArrayList<POStore> singleStore = new ArrayList<POStore>();
            singleStore.add(store);

            Configuration outputPayLoad = new Configuration(payloadConf);
            outputPayLoad.set(JobControlCompiler.PIG_MAP_STORES,
                    ObjectSerializer.serialize(emptyList));
            outputPayLoad.set(JobControlCompiler.PIG_REDUCE_STORES,
                    ObjectSerializer.serialize(singleStore));

            OutputDescriptor storeOutDescriptor = new OutputDescriptor(
                    MROutput.class.getName()).setUserPayload(TezUtils
                    .createUserPayloadFromConf(outputPayLoad));
            if (tezOp.getVertexGroupStores() != null) {
                OperatorKey vertexGroupKey = tezOp.getVertexGroupStores().get(store.getOperatorKey());
                if (vertexGroupKey != null) {
                    getPlan().getOperator(vertexGroupKey).getVertexGroupInfo()
                            .setStoreOutputDescriptor(storeOutDescriptor);
                    continue;
                }
            }
            vertex.addOutput(store.getOperatorKey().toString(),
                    storeOutDescriptor, MROutputCommitter.class);
        }

        // LoadFunc and StoreFunc add delegation tokens to Job Credentials in
        // setLocation and setStoreLocation respectively. For eg: HBaseStorage
        // InputFormat add delegation token in getSplits and OutputFormat in
        // checkOutputSpecs. For eg: FileInputFormat and FileOutputFormat
        if (stores.size() > 0) {
            new PigOutputFormat().checkOutputSpecs(job);
        }

        // Set the right VertexManagerPlugin
        if (tezOp.getEstimatedParallelism() != -1) {
            if (tezOp.isGlobalSort()||tezOp.isSkewedJoin()) {
                // Set VertexManagerPlugin to PartitionerDefinedVertexManager, which is able
                // to decrease/increase parallelism of sorting vertex dynamically
                // based on the numQuantiles calculated by sample aggregation vertex
                vertex.setVertexManagerPlugin(new VertexManagerPluginDescriptor(
                        PartitionerDefinedVertexManager.class.getName()));
                log.info("Set VertexManagerPlugin to PartitionerDefinedParallelismVertexManager for vertex " + tezOp.getOperatorKey().toString());
            } else {
                boolean containScatterGather = false;
                boolean containCustomPartitioner = false;
                for (TezEdgeDescriptor edge : tezOp.inEdges.values()) {
                    if (edge.dataMovementType == DataMovementType.SCATTER_GATHER) {
                        containScatterGather = true;
                    }
                    if (edge.partitionerClass!=null) {
                        containCustomPartitioner = true;
                    }
                }
                if (containScatterGather && !containCustomPartitioner) {
                    // Use auto-parallelism feature of ShuffleVertexManager to dynamically
                    // reduce the parallelism of the vertex
                    VertexManagerPluginDescriptor vmPluginDescriptor = new VertexManagerPluginDescriptor(
                            ShuffleVertexManager.class.getName());
                    Configuration vmPluginConf = ConfigurationUtil.toConfiguration(pc.getProperties(), false);
                    vmPluginConf.setBoolean(ShuffleVertexManager.TEZ_AM_SHUFFLE_VERTEX_MANAGER_ENABLE_AUTO_PARALLEL, true);
                    if (vmPluginConf.getLong(InputSizeReducerEstimator.BYTES_PER_REDUCER_PARAM,
                            InputSizeReducerEstimator.DEFAULT_BYTES_PER_REDUCER)!=
                                    InputSizeReducerEstimator.DEFAULT_BYTES_PER_REDUCER) {
                        vmPluginConf.setLong(ShuffleVertexManager.TEZ_AM_SHUFFLE_VERTEX_MANAGER_DESIRED_TASK_INPUT_SIZE,
                                vmPluginConf.getLong(InputSizeReducerEstimator.BYTES_PER_REDUCER_PARAM,
                                        InputSizeReducerEstimator.DEFAULT_BYTES_PER_REDUCER));
                    }
                    vmPluginDescriptor.setUserPayload(TezUtils.createUserPayloadFromConf(vmPluginConf));
                    vertex.setVertexManagerPlugin(vmPluginDescriptor);
                    log.info("Set auto parallelism for vertex " + tezOp.getOperatorKey().toString());
                }
            }
        }

        // Reset udfcontext jobconf. It is not supposed to be set in the front end
        UDFContext.getUDFContext().addJobConf(null);
        return vertex;
    }

    /**
     * Do the final configuration of LoadFuncs and store what goes where. This
     * will need to be changed as the inputs get un-bundled
     *
     * @param tezOp
     * @param conf
     * @param job
     * @return true if any POLoads were found, else false.
     * @throws VisitorException
     * @throws IOException
     */
    private List<POLoad> processLoads(TezOperator tezOp, Configuration conf,
            Job job) throws VisitorException, IOException {
        ArrayList<FileSpec> inp = new ArrayList<FileSpec>();
        ArrayList<List<OperatorKey>> inpTargets = new ArrayList<List<OperatorKey>>();
        ArrayList<String> inpSignatureLists = new ArrayList<String>();
        ArrayList<Long> inpLimits = new ArrayList<Long>();

        List<POLoad> lds = PlanHelper.getPhysicalOperators(tezOp.plan,
                POLoad.class);

        if (lds != null && lds.size() > 0) {
            for (POLoad ld : lds) {
                LoadFunc lf = ld.getLoadFunc();
                lf.setLocation(ld.getLFile().getFileName(), job);

                // Store the inp filespecs
                inp.add(ld.getLFile());
            }
        }

        if (lds != null && lds.size() > 0) {
            for (POLoad ld : lds) {
                // Store the target operators for tuples read
                // from this input
                List<PhysicalOperator> ldSucs = new ArrayList<PhysicalOperator>(
                        tezOp.plan.getSuccessors(ld));
                List<OperatorKey> ldSucKeys = new ArrayList<OperatorKey>();
                if (ldSucs != null) {
                    for (PhysicalOperator operator2 : ldSucs) {
                        ldSucKeys.add(operator2.getOperatorKey());
                    }
                }
                inpTargets.add(ldSucKeys);
                inpSignatureLists.add(ld.getSignature());
                inpLimits.add(ld.getLimit());
                // Remove the POLoad from the plan
                tezOp.plan.remove(ld);
                // Now add the input handling operator for the Tez backend
                // TODO: Move this upstream to the PhysicalPlan generation
                POSimpleTezLoad tezLoad = new POSimpleTezLoad(new OperatorKey(
                        scope, nig.getNextNodeId(scope)), ld.getLFile());
                tezLoad.setInputKey(ld.getOperatorKey().toString());
                tezLoad.setAlias(ld.getAlias());
                tezOp.plan.add(tezLoad);
                for (PhysicalOperator sucs : ldSucs) {
                    tezOp.plan.connect(tezLoad, sucs);
                }

            }
        }

        conf.set("pig.inputs", ObjectSerializer.serialize(inp));
        conf.set("pig.inpTargets", ObjectSerializer.serialize(inpTargets));
        conf.set("pig.inpSignatures", ObjectSerializer.serialize(inpSignatureLists));
        conf.set("pig.inpLimits", ObjectSerializer.serialize(inpLimits));

        return lds;
    }

    private LinkedList<POStore> processStores(TezOperator tezOp,
            Configuration payloadConf, Job job) throws VisitorException,
            IOException {
        LinkedList<POStore> stores = PlanHelper.getPhysicalOperators(
                tezOp.plan, POStore.class);

        if (stores.size() > 0) {

            ArrayList<POStore> storeLocations = new ArrayList<POStore>();
            for (POStore st : stores) {
                storeLocations.add(st);
                StoreFuncInterface sFunc = st.getStoreFunc();
                sFunc.setStoreLocation(st.getSFile().getFileName(), job);
            }

            Path tmpLocation = null;
            if (stores.size() == 1) {
                POStore st = stores.get(0);

                // set out filespecs
                String outputPathString = st.getSFile().getFileName();
                if (!outputPathString.contains("://")
                        || outputPathString.startsWith("hdfs://")) {
                    payloadConf.set("pig.streaming.log.dir", new Path(
                            outputPathString, JobControlCompiler.LOG_DIR)
                            .toString());
                } else {
                    String tmpLocationStr = FileLocalizer.getTemporaryPath(pc)
                            .toString();
                    tmpLocation = new Path(tmpLocationStr);
                    payloadConf.set("pig.streaming.log.dir", new Path(tmpLocation,
                            JobControlCompiler.LOG_DIR).toString());
                }
                payloadConf.set("pig.streaming.task.output.dir", outputPathString);

                if(tezOp.plan.getLeaves().get(0) instanceof POSplit) {
                    // Set this so that we get correct counters
                    st.setMultiStore(true);
                }

            } else { // multi store case
                log.info("Setting up multi store job");
                String tmpLocationStr = FileLocalizer.getTemporaryPath(pc)
                        .toString();
                tmpLocation = new Path(tmpLocationStr);

                boolean disableCounter = payloadConf.getBoolean(
                        "pig.disable.counter", false);
                if (disableCounter) {
                    log.info("Disable Pig custom output counters");
                }
                int idx = 0;
                for (POStore sto : storeLocations) {
                    sto.setDisableCounter(disableCounter);
                    sto.setMultiStore(true);
                    sto.setIndex(idx++);
                }

                payloadConf.set("pig.streaming.log.dir", new Path(tmpLocation,
                        JobControlCompiler.LOG_DIR).toString());
                payloadConf.set("pig.streaming.task.output.dir",
                        tmpLocation.toString());
            }

        }
        return stores;
    }

    private void setIntermediateOutputKeyValue(byte keyType, Configuration conf, TezOperator tezOp)
            throws JobCreationException, ExecException {
        setIntermediateOutputKeyValue(keyType, conf, tezOp, true);
    }

    @SuppressWarnings("rawtypes")
    private void setIntermediateOutputKeyValue(byte keyType, Configuration conf, TezOperator tezOp,
            boolean isConnectedToPackage) throws JobCreationException, ExecException {
        if (tezOp != null && tezOp.isUseSecondaryKey() && isConnectedToPackage) {
            conf.set(TezJobConfig.TEZ_RUNTIME_KEY_CLASS,
                    NullableTuple.class.getName());
        } else if (tezOp != null && tezOp.isSkewedJoin() && isConnectedToPackage) {
            conf.set(TezJobConfig.TEZ_RUNTIME_KEY_CLASS,
                    NullablePartitionWritable.class.getName());
        } else {
            Class<? extends WritableComparable> keyClass = HDataType
                    .getWritableComparableTypes(keyType).getClass();
            conf.set(TezJobConfig.TEZ_RUNTIME_KEY_CLASS,
                    keyClass.getName());

        }
        conf.set(TezJobConfig.TEZ_RUNTIME_VALUE_CLASS,
                NullableTuple.class.getName());
        conf.set(TezJobConfig.TEZ_RUNTIME_PARTITIONER_CLASS,
                MRPartitioner.class.getName());
        selectOutputComparator(keyType, conf, tezOp);
    }

    private static Class<? extends WritableComparator> comparatorForKeyType(byte keyType, boolean hasOrderBy)
            throws JobCreationException {

        switch (keyType) {
        case DataType.BOOLEAN:
            return PigBooleanRawComparator.class;

        case DataType.INTEGER:
            return PigIntRawComparator.class;

        case DataType.BIGINTEGER:
            return PigBigIntegerRawComparator.class;

        case DataType.BIGDECIMAL:
            return PigBigDecimalRawComparator.class;

        case DataType.LONG:
            return PigLongRawComparator.class;

        case DataType.FLOAT:
            return PigFloatRawComparator.class;

        case DataType.DOUBLE:
            return PigDoubleRawComparator.class;

        case DataType.DATETIME:
            return PigDateTimeRawComparator.class;

        case DataType.CHARARRAY:
            return PigTextRawComparator.class;

        case DataType.BYTEARRAY:
            //if (hasOrderBy) {
                return PigBytesRawComparator.class;
            //} else {
            //    return PigDBAWritableComparator.class;
            //}

        case DataType.MAP:
            int errCode = 1068;
            String msg = "Using Map as key not supported.";
            throw new JobCreationException(msg, errCode, PigException.INPUT);

        case DataType.TUPLE:
            //TODO: PigTupleWritableComparator gives wrong results with cogroup in
            //Checkin_2 and few other e2e tests. But MR has PigTupleWritableComparator
            //Investigate the difference later
            //if (hasOrderBy) {
                return PigTupleSortComparator.class;
            //} else {
            //    return PigTupleWritableComparator.class;
            //}

        case DataType.BAG:
            errCode = 1068;
            msg = "Using Bag as key not supported.";
            throw new JobCreationException(msg, errCode, PigException.INPUT);

        default:
            errCode = 2036;
            msg = "Unhandled key type " + DataType.findTypeName(keyType);
            throw new JobCreationException(msg, errCode, PigException.BUG);
        }
    }

    private static Class<? extends WritableComparator> getGroupingComparatorForKeyType(byte keyType)
            throws JobCreationException {

        switch (keyType) {
        case DataType.BOOLEAN:
            return PigGroupingBooleanWritableComparator.class;

        case DataType.INTEGER:
            return PigGroupingIntWritableComparator.class;

        case DataType.BIGINTEGER:
            return PigGroupingBigIntegerWritableComparator.class;

        case DataType.BIGDECIMAL:
            return PigGroupingBigDecimalWritableComparator.class;

        case DataType.LONG:
            return PigGroupingLongWritableComparator.class;

        case DataType.FLOAT:
            return PigGroupingFloatWritableComparator.class;

        case DataType.DOUBLE:
            return PigGroupingDoubleWritableComparator.class;

        case DataType.DATETIME:
            return PigGroupingDateTimeWritableComparator.class;

        case DataType.CHARARRAY:
            return PigGroupingCharArrayWritableComparator.class;

        case DataType.BYTEARRAY:
            return PigGroupingDBAWritableComparator.class;

        case DataType.MAP:
            int errCode = 1068;
            String msg = "Using Map as key not supported.";
            throw new JobCreationException(msg, errCode, PigException.INPUT);

        case DataType.TUPLE:
            return PigGroupingTupleWritableComparator.class;

        case DataType.BAG:
            errCode = 1068;
            msg = "Using Bag as key not supported.";
            throw new JobCreationException(msg, errCode, PigException.INPUT);

        default:
            errCode = 2036;
            msg = "Unhandled key type " + DataType.findTypeName(keyType);
            throw new JobCreationException(msg, errCode, PigException.BUG);
        }
    }

    void selectOutputComparator(byte keyType, Configuration conf, TezOperator tezOp)
            throws JobCreationException {
        // TODO: Handle sorting like in JobControlCompiler
        // TODO: Group comparators as in JobControlCompiler
        if (tezOp != null && tezOp.isUseSecondaryKey()) {
            conf.set(TezJobConfig.TEZ_RUNTIME_KEY_COMPARATOR_CLASS,
                    PigSecondaryKeyComparator.class.getName());
            setGroupingComparator(conf, PigSecondaryKeyGroupComparator.class.getName());
        } else {
            if (tezOp != null && tezOp.isSkewedJoin()) {
                // TODO: PigGroupingPartitionWritableComparator only used as Group comparator in MR.
                // What should be TEZ_RUNTIME_KEY_COMPARATOR_CLASS if same as MR?
                conf.set(TezJobConfig.TEZ_RUNTIME_KEY_COMPARATOR_CLASS,
                        PigGroupingPartitionWritableComparator.class.getName());
                setGroupingComparator(conf, PigGroupingPartitionWritableComparator.class.getName());
            } else {
                boolean hasOrderby = hasOrderby(tezOp);
                conf.setClass(
                        TezJobConfig.TEZ_RUNTIME_KEY_COMPARATOR_CLASS,
                        comparatorForKeyType(keyType, hasOrderby), RawComparator.class);
                if (!hasOrderby) {
                    setGroupingComparator(conf, getGroupingComparatorForKeyType(keyType).getName());
                }
            }
        }
    }

    private boolean hasOrderby(TezOperator tezOp) {
        boolean hasOrderBy = tezOp.isGlobalSort() || tezOp.isLimitAfterSort();
        if (!hasOrderBy) {
            // Check if it is a Orderby sampler job
            List<TezOperator> succs = getPlan().getSuccessors(tezOp);
            if (succs != null && succs.size() == 1) {
                if (succs.get(0).isGlobalSort()) {
                    hasOrderBy = true;
                }
            }
        }
        return hasOrderBy;
    }

    private void setGroupingComparator(Configuration conf, String comparatorClass) {
        // In MR - job.setGroupingComparatorClass() or MRJobConfig.GROUP_COMPARATOR_CLASS
        // TODO: Check why tez-mapreduce ReduceProcessor use two different tez
        // settings for the same MRJobConfig.GROUP_COMPARATOR_CLASS and use only one
        conf.set(TezJobConfig.TEZ_RUNTIME_GROUP_COMPARATOR_CLASS,
                comparatorClass);
        conf.set(TezJobConfig.TEZ_RUNTIME_KEY_SECONDARY_COMPARATOR_CLASS,
                comparatorClass);
    }

    public static int estimateParallelism(Job job, TezOperPlan tezPlan,
            TezOperator tezOp) throws IOException {
        Configuration conf = job.getConfiguration();

        TezParallelismEstimator estimator = conf.get(REDUCER_ESTIMATOR_KEY) == null ? new TezOperDependencyParallelismEstimator()
                : PigContext.instantiateObjectFromParams(conf,
                        REDUCER_ESTIMATOR_KEY, REDUCER_ESTIMATOR_ARG_KEY,
                        TezParallelismEstimator.class);

        log.info("Using parallel estimator: " + estimator.getClass().getName());
        int numberOfReducers = estimator.estimateParallelism(tezPlan, tezOp, conf);
        return numberOfReducers;
    }

}
