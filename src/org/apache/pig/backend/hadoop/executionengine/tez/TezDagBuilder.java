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
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
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
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.ConverterUtils;
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
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MRConfiguration;
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
import org.apache.pig.backend.hadoop.executionengine.shims.HadoopShims;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezEdgeDescriptor;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezOpPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezOperPlan;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezOperator;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezPOPackageAnnotator.LoRearrangeDiscoverer;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.operator.POIdentityInOutTez;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.operator.POLocalRearrangeTez;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.operator.POShuffleTezLoad;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.operator.POStoreTez;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.operator.POValueInputTez;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.udf.ReadScalarsTez;
import org.apache.pig.backend.hadoop.executionengine.tez.runtime.PartitionerDefinedVertexManager;
import org.apache.pig.backend.hadoop.executionengine.tez.runtime.PigGraceShuffleVertexManager;
import org.apache.pig.backend.hadoop.executionengine.tez.runtime.PigOutputFormatTez;
import org.apache.pig.backend.hadoop.executionengine.tez.runtime.PigProcessor;
import org.apache.pig.backend.hadoop.executionengine.tez.util.MRToTezHelper;
import org.apache.pig.backend.hadoop.executionengine.tez.util.SecurityHelper;
import org.apache.pig.backend.hadoop.executionengine.tez.util.TezCompilerUtil;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.PigImplConstants;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.NullablePartitionWritable;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.tools.pigstats.tez.TezScriptState;
import org.apache.pig.tools.pigstats.tez.TezScriptState.TezDAGScriptInfo;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.DataSinkDescriptor;
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.EdgeManagerPluginDescriptor;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.GroupInputEdge;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.InputInitializerDescriptor;
import org.apache.tez.dag.api.OutputCommitterDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.VertexGroup;
import org.apache.tez.dag.api.VertexLocationHint;
import org.apache.tez.dag.api.VertexManagerPluginDescriptor;
import org.apache.tez.dag.library.vertexmanager.ShuffleVertexManager;
import org.apache.tez.mapreduce.combine.MRCombiner;
import org.apache.tez.mapreduce.committer.MROutputCommitter;
import org.apache.tez.mapreduce.common.MRInputSplitDistributor;
import org.apache.tez.mapreduce.hadoop.InputSplitInfo;
import org.apache.tez.mapreduce.hadoop.InputSplitInfoMem;
import org.apache.tez.mapreduce.hadoop.MRHelpers;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.mapreduce.partition.MRPartitioner;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRInputUserPayloadProto.Builder;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRSplitsProto;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.input.ConcatenatedMergedKeyValueInput;
import org.apache.tez.runtime.library.input.OrderedGroupedKVInput;
import org.apache.tez.runtime.library.input.OrderedGroupedMergedKVInput;
import org.apache.tez.runtime.library.input.UnorderedKVInput;

/**
 * A visitor to construct DAG out of Tez plan.
 */
public class TezDagBuilder extends TezOpPlanVisitor {
    private static final Log log = LogFactory.getLog(TezJobCompiler.class);

    private DAG dag;
    private Map<String, LocalResource> localResources;
    private PigContext pc;
    private Configuration globalConf;
    private FileSystem fs;
    private long intermediateTaskInputSize;
    private Set<String> inputSplitInDiskVertices;

    public TezDagBuilder(PigContext pc, TezOperPlan plan, DAG dag,
            Map<String, LocalResource> localResources) {
        super(plan, new DependencyOrderWalker<TezOperator, TezOperPlan>(plan));
        this.pc = pc;
        this.globalConf = ConfigurationUtil.toConfiguration(pc.getProperties(), true);
        this.localResources = localResources;
        this.dag = dag;
        this.inputSplitInDiskVertices = new HashSet<String>();

        try {
            // Add credentials from binary token file and get tokens for namenodes
            // specified in mapreduce.job.hdfs-servers
            SecurityHelper.populateTokenCache(globalConf, dag.getCredentials());
        } catch (IOException e) {
            throw new RuntimeException("Error while fetching delegation tokens", e);
        }

        try {
            fs = FileSystem.get(globalConf);
            intermediateTaskInputSize = HadoopShims.getDefaultBlockSize(fs, FileLocalizer.getTemporaryResourcePath(pc));
        } catch (Exception e) {
            log.warn("Unable to get the block size for temporary directory, defaulting to 128MB", e);
            intermediateTaskInputSize = 134217728L;
        }
        // At least 128MB. Else we will end up with too many tasks
        intermediateTaskInputSize = Math.max(intermediateTaskInputSize, 134217728L);
        intermediateTaskInputSize = Math.min(intermediateTaskInputSize,
                globalConf.getLong(
                        InputSizeReducerEstimator.BYTES_PER_REDUCER_PARAM,
                        InputSizeReducerEstimator.DEFAULT_BYTES_PER_REDUCER));
    }

    // Hack to turn off relocalization till TEZ-2192 is fixed.
    public void avoidContainerReuseIfInputSplitInDisk() throws IOException {
        if (!inputSplitInDiskVertices.isEmpty()) {
            // Create empty job.split file and add as resource to all other
            // vertices that are not reading splits from disk so that their
            // containers are not reused by vertices that read splits from disk
            Path jobSplitFile = new Path(FileLocalizer.getTemporaryPath(pc),
                    MRJobConfig.JOB_SPLIT);
            FSDataOutputStream out = fs.create(jobSplitFile);
            out.close();
            log.info("Creating empty job.split in " + jobSplitFile);
            FileStatus splitFileStatus = fs.getFileStatus(jobSplitFile);
            LocalResource localResource = LocalResource.newInstance(
                    ConverterUtils.getYarnUrlFromPath(jobSplitFile),
                    LocalResourceType.FILE,
                    LocalResourceVisibility.APPLICATION,
                    splitFileStatus.getLen(),
                    splitFileStatus.getModificationTime());
            for (Vertex vertex : dag.getVertices()) {
                if (!inputSplitInDiskVertices.contains(vertex.getName())) {
                    if (vertex.getTaskLocalFiles().containsKey(
                            MRJobConfig.JOB_SPLIT)) {
                        throw new RuntimeException(
                                "LocalResources already contains a"
                                        + " resource named "
                                        + MRJobConfig.JOB_SPLIT);
                    }
                    vertex.getTaskLocalFiles().put(MRJobConfig.JOB_SPLIT,
                            localResource);
                }
            }
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
                to = newVertex(tezOp);
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
                            Edge edge = Edge.create(from, to, prop);
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
                    vertexGroup.addDataSink(store.getOperatorKey().toString(),
                            new DataSinkDescriptor(tezOp.getVertexGroupInfo().getStoreOutputDescriptor(),
                            OutputCommitterDescriptor.create(MROutputCommitter.class.getName()), dag.getCredentials()));
                }
            }
        }
    }

    private GroupInputEdge newGroupInputEdge(TezOperator fromOp,
            TezOperator toOp, VertexGroup from, Vertex to) throws IOException {

        EdgeProperty edgeProperty = newEdge(fromOp, toOp);

        String groupInputClass = ConcatenatedMergedKeyValueInput.class.getName();

        // In case of SCATTER_GATHER and UnorderedKVInput it will still be
        // ConcatenatedMergedKeyValueInput
        if(edgeProperty.getDataMovementType().equals(DataMovementType.SCATTER_GATHER)
                && edgeProperty.getEdgeDestination().getClassName().equals(OrderedGroupedKVInput.class.getName())) {
            groupInputClass = OrderedGroupedMergedKVInput.class.getName();
        }

        return GroupInputEdge.create(from, to, edgeProperty,
                InputDescriptor.create(groupInputClass).setUserPayload(edgeProperty.getEdgeDestination().getUserPayload()));
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

        InputDescriptor in = InputDescriptor.create(edge.inputClassName);
        OutputDescriptor out = OutputDescriptor.create(edge.outputClassName);

        Configuration conf = ConfigurationUtil.toConfiguration(pc.getProperties(), false);
        UDFContext.getUDFContext().serialize(conf);
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

        conf.setIfUnset(TezRuntimeConfiguration.TEZ_RUNTIME_PARTITIONER_CLASS,
                MRPartitioner.class.getName());

        if (edge.getIntermediateOutputKeyClass() != null) {
            conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS,
                    edge.getIntermediateOutputKeyClass());
        }

        if (edge.getIntermediateOutputValueClass() != null) {
            conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS,
                    edge.getIntermediateOutputValueClass());
        }

        if (edge.getIntermediateOutputKeyComparatorClass() != null) {
            conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_COMPARATOR_CLASS,
                    edge.getIntermediateOutputKeyComparatorClass());
        }

        conf.setBoolean(MRConfiguration.MAPPER_NEW_API, true);
        conf.setBoolean(MRConfiguration.REDUCER_NEW_API, true);
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
            conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_COMPARATOR_CLASS,
                    PigSecondaryKeyComparator.class.getName());
            // In MR - job.setOutputKeyClass() or MRJobConfig.OUTPUT_KEY_CLASS
            conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS, NullableTuple.class.getName());
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

        if (edge.dataMovementType!=DataMovementType.BROADCAST && to.getEstimatedParallelism()!=-1 && to.getVertexParallelism()==-1 && (to.isGlobalSort()||to.isSkewedJoin())) {
            // Use custom edge
            return EdgeProperty.create((EdgeManagerPluginDescriptor)null,
                    edge.dataSourceType, edge.schedulingType, out, in);
            }

        if (to.isUseGraceParallelism()) {
            // Put datamovement to null to prevent vertex "to" from starting. It will be started by PigGraceShuffleVertexManager
            return EdgeProperty.create((EdgeManagerPluginDescriptor)null, edge.dataSourceType,
                    edge.schedulingType, out, in);
        }
        return EdgeProperty.create(edge.dataMovementType, edge.dataSourceType,
                edge.schedulingType, out, in);
    }

    private void addCombiner(PhysicalPlan combinePlan, TezOperator pkgTezOp,
            Configuration conf) throws IOException {
        POPackage combPack = (POPackage) combinePlan.getRoots().get(0);
        POLocalRearrange combRearrange = (POLocalRearrange) combinePlan
                .getLeaves().get(0);
        setIntermediateOutputKeyValue(combRearrange.getKeyType(), conf, pkgTezOp);

        LoRearrangeDiscoverer lrDiscoverer = new LoRearrangeDiscoverer(
                combinePlan, null, pkgTezOp, combPack);
        lrDiscoverer.visit();

        combinePlan.remove(combPack);
        conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_COMBINER_CLASS,
                MRCombiner.class.getName());
        conf.set(MRJobConfig.COMBINE_CLASS_ATTR,
                PigCombiner.Combine.class.getName());
        conf.setBoolean(MRConfiguration.MAPPER_NEW_API, true);
        conf.setBoolean(MRConfiguration.REDUCER_NEW_API, true);
        conf.set("pig.pigContext", ObjectSerializer.serialize(pc));
        conf.set("udf.import.list",
                ObjectSerializer.serialize(PigContext.getPackageImportList()));
        conf.set("pig.combinePlan", ObjectSerializer.serialize(combinePlan));
        conf.set("pig.combine.package", ObjectSerializer.serialize(combPack));
        conf.set("pig.map.keytype", ObjectSerializer
                .serialize(new byte[] { combRearrange.getKeyType() }));
    }

    private Vertex newVertex(TezOperator tezOp) throws IOException,
            ClassNotFoundException, InterruptedException {
        ProcessorDescriptor procDesc = ProcessorDescriptor.create(
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

        if (tezOp.getSampleOperator() != null) {
            payloadConf.set(PigProcessor.SAMPLE_VERTEX, tezOp.getSampleOperator().getOperatorKey().toString());
        }

        if (tezOp.getSortOperator() != null) {
            // Required by Sample Aggregation job for estimating quantiles
            payloadConf.set(PigProcessor.SORT_VERTEX, tezOp.getSortOperator().getOperatorKey().toString());
            // PIG-4162: Order by/Skew Join in intermediate stage.
            // Increasing order by parallelism may not be required as it is
            // usually followed by limit other than store. But would benefit
            // cases like skewed join followed by group by.
            if (tezOp.getSortOperator().getEstimatedParallelism() != -1
                    && TezCompilerUtil.isIntermediateReducer(tezOp.getSortOperator())) {
                payloadConf.setLong(
                        InputSizeReducerEstimator.BYTES_PER_REDUCER_PARAM,
                        intermediateTaskInputSize);
            }

        }

        payloadConf.set("pig.inputs", ObjectSerializer.serialize(tezOp.getLoaderInfo().getInp()));
        payloadConf.set("pig.inpSignatures", ObjectSerializer.serialize(tezOp.getLoaderInfo().getInpSignatureLists()));
        payloadConf.set("pig.inpLimits", ObjectSerializer.serialize(tezOp.getLoaderInfo().getInpLimits()));
        // Process stores
        LinkedList<POStore> stores = processStores(tezOp, payloadConf, job);

        payloadConf.set("pig.pigContext", ObjectSerializer.serialize(pc));
        payloadConf.set("udf.import.list",
                ObjectSerializer.serialize(PigContext.getPackageImportList()));
        payloadConf.set("exectype", "TEZ");
        payloadConf.setBoolean(MRConfiguration.MAPPER_NEW_API, true);
        payloadConf.setBoolean(MRConfiguration.REDUCER_NEW_API, true);
        payloadConf.setClass(MRConfiguration.INPUTFORMAT_CLASS,
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
            POShuffleTezLoad newPack = new POShuffleTezLoad(pack);
            if (tezOp.isSkewedJoin()) {
                newPack.setSkewedJoins(true);
            }
            tezOp.plan.add(newPack);

            // Set input keys for POShuffleTezLoad. This is used to identify
            // the inputs that are attached to the POShuffleTezLoad in the
            // backend.
            Map<Integer, String> localRearrangeMap = new TreeMap<Integer, String>();
            for (TezOperator pred : mPlan.getPredecessors(tezOp)) {
                if (tezOp.getSampleOperator() != null && tezOp.getSampleOperator() == pred) {
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
        setOutputFormat(job);

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

        if (tezOp.isNeedEstimateParallelism()) {
            payloadConf.setBoolean(PigProcessor.ESTIMATE_PARALLELISM, true);
            log.info("Estimate quantile for sample aggregation vertex " + tezOp.getOperatorKey().toString());
        }

        // set various parallelism into the job conf for later analysis, PIG-2779
        payloadConf.setInt(PigImplConstants.REDUCER_DEFAULT_PARALLELISM, pc.defaultParallel);
        payloadConf.setInt(PigImplConstants.REDUCER_REQUESTED_PARALLELISM, tezOp.getRequestedParallelism());
        payloadConf.setInt(PigImplConstants.REDUCER_ESTIMATED_PARALLELISM, tezOp.getEstimatedParallelism());

        TezScriptState ss = TezScriptState.get();
        ss.addVertexSettingsToConf(dag.getName(), tezOp, payloadConf);

        // Take our assembled configuration and create a vertex
        UserPayload userPayload = TezUtils.createUserPayloadFromConf(payloadConf);
        TezDAGScriptInfo dagScriptInfo = TezScriptState.get().getDAGScriptInfo(dag.getName());
        String vertexInfo = dagScriptInfo.getAliasLocation(tezOp) + " (" + dagScriptInfo.getPigFeatures(tezOp) + ")" ;
        procDesc.setUserPayload(userPayload).setHistoryText(TezUtils.convertToHistoryText(vertexInfo, payloadConf));

        String vmPluginName = null;
        Configuration vmPluginConf = null;

        // Set the right VertexManagerPlugin
        if (tezOp.getEstimatedParallelism() != -1) {
            if (tezOp.isGlobalSort()||tezOp.isSkewedJoin()) {
                if (tezOp.getVertexParallelism()==-1 && (
                        tezOp.isGlobalSort() &&getPlan().getPredecessors(tezOp).size()==1||
                        tezOp.isSkewedJoin() &&getPlan().getPredecessors(tezOp).size()==2)) {
                    // Set VertexManagerPlugin to PartitionerDefinedVertexManager, which is able
                    // to decrease/increase parallelism of sorting vertex dynamically
                    // based on the numQuantiles calculated by sample aggregation vertex
                    vmPluginName = PartitionerDefinedVertexManager.class.getName();
                    log.info("Set VertexManagerPlugin to PartitionerDefinedParallelismVertexManager for vertex " + tezOp.getOperatorKey().toString());
                }
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
                    if (payloadConf.getBoolean(PigConfiguration.PIG_TEZ_GRACE_PARALLELISM, true)
                            && !TezOperPlan.getGrandParentsForGraceParallelism(getPlan(), tezOp).isEmpty()) {
                        vmPluginName = PigGraceShuffleVertexManager.class.getName();
                        tezOp.setUseGraceParallelism(true);
                    } else {
                        vmPluginName = ShuffleVertexManager.class.getName();
                    }
                    vmPluginConf = (vmPluginConf == null) ? ConfigurationUtil.toConfiguration(pc.getProperties(), false) : vmPluginConf;
                    vmPluginConf.setBoolean(ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_ENABLE_AUTO_PARALLEL, true);
                    vmPluginConf.set("pig.tez.plan", ObjectSerializer.serialize(getPlan()));
                    vmPluginConf.set("pig.pigContext", ObjectSerializer.serialize(pc));
                    if (stores.size() <= 0) {
                        // Intermediate reduce. Set the bytes per reducer to be block size.
                        vmPluginConf.setLong(ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_DESIRED_TASK_INPUT_SIZE,
                                        intermediateTaskInputSize);
                    } else if (vmPluginConf.getLong(InputSizeReducerEstimator.BYTES_PER_REDUCER_PARAM,
                                    InputSizeReducerEstimator.DEFAULT_BYTES_PER_REDUCER) !=
                                    InputSizeReducerEstimator.DEFAULT_BYTES_PER_REDUCER) {
                        vmPluginConf.setLong(ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_DESIRED_TASK_INPUT_SIZE,
                                vmPluginConf.getLong(InputSizeReducerEstimator.BYTES_PER_REDUCER_PARAM,
                                        InputSizeReducerEstimator.DEFAULT_BYTES_PER_REDUCER));
                    }
                    log.info("Set auto parallelism for vertex " + tezOp.getOperatorKey().toString());
                }
            }
        }
        if (tezOp.isLimit() && (vmPluginName == null || vmPluginName.equals(PigGraceShuffleVertexManager.class.getName())||
                vmPluginName.equals(ShuffleVertexManager.class.getName()))) {
            if (tezOp.inEdges.values().iterator().next().inputClassName.equals(UnorderedKVInput.class.getName())) {
                // Setting SRC_FRACTION to 0.00001 so that even if there are 100K source tasks,
                // limit job starts when 1 source task finishes.
                // If limit is part of a group by or join because their parallelism is 1,
                // we should leave the configuration with the defaults.
                vmPluginConf = (vmPluginConf == null) ? ConfigurationUtil.toConfiguration(pc.getProperties(), false) : vmPluginConf;
                vmPluginConf.set(ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MIN_SRC_FRACTION, "0.00001");
                vmPluginConf.set(ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MAX_SRC_FRACTION, "0.00001");
                log.info("Set " + ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MIN_SRC_FRACTION + " to 0.00001 for limit vertex " + tezOp.getOperatorKey().toString());
            }
        }

        int parallel = tezOp.getVertexParallelism();
        if (tezOp.isUseGraceParallelism()) {
            parallel = -1;
        }
        Resource resource;
        if (globalConf.get(TezConfiguration.TEZ_TASK_RESOURCE_MEMORY_MB)!=null && 
                globalConf.get(TezConfiguration.TEZ_TASK_RESOURCE_CPU_VCORES)!=null) {
            resource = Resource.newInstance(globalConf.getInt(TezConfiguration.TEZ_TASK_RESOURCE_MEMORY_MB,
                    TezConfiguration.TEZ_TASK_RESOURCE_MEMORY_MB_DEFAULT),
                    globalConf.getInt(TezConfiguration.TEZ_TASK_RESOURCE_CPU_VCORES,
                    TezConfiguration.TEZ_TASK_RESOURCE_CPU_VCORES_DEFAULT));
        } else {
            // If tez setting is not defined, try MR setting
            resource = tezOp.isUseMRMapSettings() ? MRHelpers.getResourceForMRMapper(globalConf) : MRHelpers.getResourceForMRReducer(globalConf);
        }
        Vertex vertex = Vertex.create(tezOp.getOperatorKey().toString(), procDesc, parallel, resource);
        Map<String, String> taskEnv = new HashMap<String, String>();
        MRHelpers.updateEnvBasedOnMRTaskEnv(globalConf, taskEnv, tezOp.isUseMRMapSettings());
        vertex.setTaskEnvironment(taskEnv);

        // All these classes are @InterfaceAudience.Private in Hadoop. Switch to Tez methods in TEZ-1012
        // set the timestamps, public/private visibility of the archives and files
        ClientDistributedCacheManager
                .determineTimestampsAndCacheVisibilities(globalConf);
        // get DelegationToken for each cached file
        ClientDistributedCacheManager.getDelegationTokens(globalConf,
                job.getCredentials());
        MRApps.setupDistributedCache(globalConf, localResources);
        vertex.addTaskLocalFiles(localResources);

        String javaOpts;
        if (globalConf.get(TezConfiguration.TEZ_TASK_LAUNCH_CMD_OPTS)!=null) {
            javaOpts = globalConf.get(TezConfiguration.TEZ_TASK_LAUNCH_CMD_OPTS);
        } else {
            // If tez setting is not defined, try MR setting
            javaOpts = tezOp.isUseMRMapSettings() ? MRHelpers.getJavaOptsForMRMapper(globalConf)
                    : MRHelpers.getJavaOptsForMRReducer(globalConf);
        }
        vertex.setTaskLaunchCmdOpts(javaOpts);

        log.info("For vertex - " + tezOp.getOperatorKey().toString()
                + ": parallelism=" + tezOp.getVertexParallelism()
                + ", memory=" + vertex.getTaskResource().getMemory()
                + ", java opts=" + vertex.getTaskLaunchCmdOpts()
                );
        // Right now there can only be one of each of these. Will need to be
        // more generic when there can be more.
        for (POLoad ld : tezOp.getLoaderInfo().getLoads()) {

            // TODO: These should get the globalConf, or a merged version that
            // keeps settings like pig.maxCombinedSplitSize
            Builder userPayLoadBuilder = MRRuntimeProtos.MRInputUserPayloadProto.newBuilder();

            InputSplitInfo inputSplitInfo = tezOp.getLoaderInfo().getInputSplitInfo();
            Map<String, LocalResource> additionalLocalResources = null;
            int spillThreshold = payloadConf
                    .getInt(PigConfiguration.PIG_TEZ_INPUT_SPLITS_MEM_THRESHOLD,
                            PigConfiguration.PIG_TEZ_INPUT_SPLITS_MEM_THRESHOLD_DEFAULT);

            // Currently inputSplitInfo is always InputSplitInfoMem at this point
            if (inputSplitInfo instanceof InputSplitInfoMem) {
                MRSplitsProto splitsProto = inputSplitInfo.getSplitsProto();
                int splitsSerializedSize = splitsProto.getSerializedSize();
                if(splitsSerializedSize > spillThreshold) {
                    payloadConf.setBoolean(
                            org.apache.tez.mapreduce.hadoop.MRJobConfig.MR_TEZ_SPLITS_VIA_EVENTS,
                            false);
                    // Write splits to disk
                    Path inputSplitsDir = FileLocalizer.getTemporaryPath(pc);
                    log.info("Writing input splits to " + inputSplitsDir
                            + " for vertex " + vertex.getName()
                            + " as the serialized size in memory is "
                            + splitsSerializedSize + ". Configured "
                            + PigConfiguration.PIG_TEZ_INPUT_SPLITS_MEM_THRESHOLD
                            + " is " + spillThreshold);
                    inputSplitInfo = MRToTezHelper.writeInputSplitInfoToDisk(
                            (InputSplitInfoMem)inputSplitInfo, inputSplitsDir, payloadConf, fs);
                    additionalLocalResources = new HashMap<String, LocalResource>();
                    MRToTezHelper.updateLocalResourcesForInputSplits(
                            fs, inputSplitInfo,
                            additionalLocalResources);
                    inputSplitInDiskVertices.add(vertex.getName());
                } else {
                    // Send splits via RPC to AM
                    userPayLoadBuilder.setSplits(splitsProto);
                }
                //Free up memory
                tezOp.getLoaderInfo().setInputSplitInfo(null);
            }
            userPayLoadBuilder.setConfigurationBytes(TezUtils.createByteStringFromConf(payloadConf));

            vertex.setLocationHint(VertexLocationHint.create(inputSplitInfo.getTaskLocationHints()));
            vertex.addDataSource(ld.getOperatorKey().toString(),
                    DataSourceDescriptor.create(InputDescriptor.create(MRInput.class.getName())
                            .setUserPayload(UserPayload.create(userPayLoadBuilder.build().toByteString().asReadOnlyByteBuffer())),
                    InputInitializerDescriptor.create(MRInputSplitDistributor.class.getName()),
                    inputSplitInfo.getNumTasks(),
                    dag.getCredentials(),
                    null,
                    additionalLocalResources));
        }

        // Union within a split can have multiple stores writing to same output
        Set<String> uniqueStoreOutputs = new HashSet<String>();
        for (POStore store : stores) {

            ArrayList<POStore> emptyList = new ArrayList<POStore>();
            ArrayList<POStore> singleStore = new ArrayList<POStore>();
            singleStore.add(store);

            Configuration outputPayLoad = new Configuration(payloadConf);
            outputPayLoad.set(JobControlCompiler.PIG_MAP_STORES,
                    ObjectSerializer.serialize(emptyList));
            outputPayLoad.set(JobControlCompiler.PIG_REDUCE_STORES,
                    ObjectSerializer.serialize(singleStore));

            OutputDescriptor storeOutDescriptor = OutputDescriptor.create(
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
            String outputKey = ((POStoreTez) store).getOutputKey();
            if (!uniqueStoreOutputs.contains(outputKey)) {
                vertex.addDataSink(outputKey.toString(),
                        new DataSinkDescriptor(storeOutDescriptor,
                        OutputCommitterDescriptor.create(MROutputCommitter.class.getName()),
                        dag.getCredentials()));
                uniqueStoreOutputs.add(outputKey);
            }
        }

        // LoadFunc and StoreFunc add delegation tokens to Job Credentials in
        // setLocation and setStoreLocation respectively. For eg: HBaseStorage
        // InputFormat add delegation token in getSplits and OutputFormat in
        // checkOutputSpecs. For eg: FileInputFormat and FileOutputFormat
        if (stores.size() > 0) {
            new PigOutputFormat().checkOutputSpecs(job);
        }

        // else if(tezOp.isLimitAfterSort())
        // TODO: PIG-4049 If standalone Limit we need a new VertexManager or new input
        // instead of ShuffledMergedInput. For limit part of the sort (order by parallel 1) itself
        // need to enhance PartitionerDefinedVertexManager

        if (vmPluginName != null) {
            VertexManagerPluginDescriptor vmPluginDescriptor = VertexManagerPluginDescriptor.create(vmPluginName);
            if (vmPluginConf != null) {
                vmPluginDescriptor.setUserPayload(TezUtils.createUserPayloadFromConf(vmPluginConf));
            }
            vertex.setVertexManagerPlugin(vmPluginDescriptor);
        }
        // Reset udfcontext jobconf. It is not supposed to be set in the front end
        UDFContext.getUDFContext().addJobConf(null);
        return vertex;
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
            conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS,
                    NullableTuple.class.getName());
        } else if (tezOp != null && tezOp.isSkewedJoin() && isConnectedToPackage) {
            conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS,
                    NullablePartitionWritable.class.getName());
        } else {
            Class<? extends WritableComparable> keyClass = HDataType
                    .getWritableComparableTypes(keyType).getClass();
            conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS,
                    keyClass.getName());

        }
        conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS,
                NullableTuple.class.getName());
        conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_PARTITIONER_CLASS,
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
            conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_COMPARATOR_CLASS,
                    PigSecondaryKeyComparator.class.getName());
            setGroupingComparator(conf, PigSecondaryKeyGroupComparator.class.getName());
        } else {
            if (tezOp != null && tezOp.isSkewedJoin()) {
                // TODO: PigGroupingPartitionWritableComparator only used as Group comparator in MR.
                // What should be TEZ_RUNTIME_KEY_COMPARATOR_CLASS if same as MR?
                conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_COMPARATOR_CLASS,
                        PigGroupingPartitionWritableComparator.class.getName());
                setGroupingComparator(conf, PigGroupingPartitionWritableComparator.class.getName());
            } else {
                boolean hasOrderby = hasOrderby(tezOp);
                conf.setClass(
                        TezRuntimeConfiguration.TEZ_RUNTIME_KEY_COMPARATOR_CLASS,
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
        conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_GROUP_COMPARATOR_CLASS,
                comparatorClass);
        conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_SECONDARY_COMPARATOR_CLASS,
                comparatorClass);
    }

    private void setOutputFormat(org.apache.hadoop.mapreduce.Job job) {
        // the OutputFormat we report to Hadoop is always PigOutputFormat which
        // can be wrapped with LazyOutputFormat provided if it is supported by
        // the Hadoop version and PigConfiguration.PIG_OUTPUT_LAZY is set
        if ("true".equalsIgnoreCase(job.getConfiguration().get(PigConfiguration.PIG_OUTPUT_LAZY))) {
            try {
                Class<?> clazz = PigContext
                        .resolveClassName("org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat");
                Method method = clazz.getMethod("setOutputFormatClass",
                        org.apache.hadoop.mapreduce.Job.class, Class.class);
                method.invoke(null, job, PigOutputFormatTez.class);
            } catch (Exception e) {
                job.setOutputFormatClass(PigOutputFormatTez.class);
                log.warn(PigConfiguration.PIG_OUTPUT_LAZY
                        + " is set but LazyOutputFormat couldn't be loaded. Default PigOutputFormat will be used");
            }
        } else {
            job.setOutputFormatClass(PigOutputFormatTez.class);
        }
    }
}
