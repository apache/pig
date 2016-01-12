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
package org.apache.pig.backend.hadoop.executionengine.tez.plan.optimizer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.PigConfiguration;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POUserFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSplit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezEdgeDescriptor;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezOpPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezOperPlan;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezOperator;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezOperator.OPER_FEATURE;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezOperator.VertexGroupInfo;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.operator.POStoreTez;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.operator.POValueOutputTez;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.udf.ReadScalarsTez;
import org.apache.pig.backend.hadoop.executionengine.tez.runtime.TezInput;
import org.apache.pig.backend.hadoop.executionengine.tez.runtime.TezOutput;
import org.apache.pig.backend.hadoop.executionengine.tez.util.TezCompilerUtil;
import org.apache.pig.backend.hadoop.hbase.HBaseStorage;
import org.apache.pig.builtin.AvroStorage;
import org.apache.pig.builtin.JsonStorage;
import org.apache.pig.builtin.OrcStorage;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.builtin.RoundRobinPartitioner;
import org.apache.pig.builtin.mock.Storage;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.ReverseDependencyOrderWalker;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.runtime.library.input.UnorderedKVInput;
import org.apache.tez.runtime.library.output.UnorderedPartitionedKVOutput;

/**
 * Optimizes union by removing the intermediate union vertex and making the
 * successor get input from the predecessor vertices directly using VertexGroup.
 * This should be run after MultiQueryOptimizer so that it handles cases like
 * union followed by split and then store.
 *
 * For eg:
 * 1) Union followed by store
 * Vertex 1 (Load), Vertex 2 (Load) -> Vertex 3 (Union + Store) will be optimized to
 * Vertex 1 (Load + Store), Vertex 2 (Load + Store). Both the vertices will be writing output
 * to same store location directly which is supported by Tez.
 * 2) Union followed by groupby
 * Vertex 1 (Load), Vertex 2 (Load) -> Vertex 3 (Union + POLocalRearrange) -> Vertex 4 (Group by)
 * will be optimized to Vertex 1 (Load + POLR), Vertex 2 (Load + POLR) -> Vertex 4 (Group by)
 *
 */
public class UnionOptimizer extends TezOpPlanVisitor {

    private static final Log LOG = LogFactory.getLog(UnionOptimizer.class);
    private TezOperPlan tezPlan;
    private static Set<String> builtinSupportedStoreFuncs = new HashSet<String>();
    private List<String> supportedStoreFuncs;
    private List<String> unsupportedStoreFuncs;

    static {
        builtinSupportedStoreFuncs.add(PigStorage.class.getName());
        builtinSupportedStoreFuncs.add(JsonStorage.class.getName());
        builtinSupportedStoreFuncs.add(OrcStorage.class.getName());
        builtinSupportedStoreFuncs.add(HBaseStorage.class.getName());
        builtinSupportedStoreFuncs.add(AvroStorage.class.getName());
        builtinSupportedStoreFuncs.add("org.apache.pig.piggybank.storage.avro.AvroStorage");
        builtinSupportedStoreFuncs.add("org.apache.pig.piggybank.storage.avro.CSVExcelStorage");
        builtinSupportedStoreFuncs.add(Storage.class.getName());
    }

    public UnionOptimizer(TezOperPlan plan, List<String> supportedStoreFuncs, List<String> unsupportedStoreFuncs) {
        super(plan, new ReverseDependencyOrderWalker<TezOperator, TezOperPlan>(plan));
        tezPlan = plan;
        this.supportedStoreFuncs = supportedStoreFuncs;
        this.unsupportedStoreFuncs = unsupportedStoreFuncs;
    }

    public static boolean isOptimizable(TezOperator tezOp,
            List<String> supportedStoreFuncs, List<String> unsupportedStoreFuncs)
            throws VisitorException {
        if((tezOp.isLimit() || tezOp.isLimitAfterSort()) && tezOp.getRequestedParallelism() == 1) {
            return false;
        }
        // Two vertices separately ranking with 1 to n and writing to output directly
        // will make each rank repeate twice which is wrong. Rank always needs to be
        // done from single vertex to have the counting correct.
        if (tezOp.isRankCounter()) {
            return false;
        }
        if (supportedStoreFuncs != null || unsupportedStoreFuncs != null) {
            List<POStoreTez> stores = PlanHelper.getPhysicalOperators(tezOp.plan, POStoreTez.class);
            for (POStoreTez store : stores) {
                String name = store.getStoreFunc().getClass().getName();
                if (unsupportedStoreFuncs != null
                        && unsupportedStoreFuncs.contains(name)) {
                    return false;
                }
                if (supportedStoreFuncs != null
                        && !supportedStoreFuncs.contains(name)) {
                    if (!builtinSupportedStoreFuncs.contains(name)) {
                        LOG.warn(PigConfiguration.PIG_TEZ_OPT_UNION_SUPPORTED_STOREFUNCS
                                + " does not contain " + name
                                + " and so disabling union optimization. There will be some performance degradation. "
                                + "If your storefunc does not hardcode part file names and can work with multiple vertices writing to the output location,"
                                + " run pig with -D"
                                + PigConfiguration.PIG_TEZ_OPT_UNION_SUPPORTED_STOREFUNCS
                                + "=<Comma separated list of fully qualified StoreFunc class names> to enable the optimization. Refer PIG-4691");
                        return false;
                    }
                }
            }
        }
        return true;
    }

    @Override
    public void visitTezOp(TezOperator tezOp) throws VisitorException {
        if (!tezOp.isUnion()) {
            return;
        }

        if (!isOptimizable(tezOp, supportedStoreFuncs, unsupportedStoreFuncs)) {
            return;
        }

        TezOperator unionOp = tezOp;
        String scope = unionOp.getOperatorKey().scope;
        PhysicalPlan unionOpPlan = unionOp.plan;

        // TODO: PIG-3856 Handle replicated join and skewed join sample.
        // Replicate join small table/skewed join sample that was broadcast to union vertex
        // now needs to be broadcast to all the union predecessors. How do we do that??
        // Wait for shared edge and do it or write multiple times??
        // For now don't optimize except in the case of Split where we need to write only once

        Set<OperatorKey> uniqueUnionMembers = new HashSet<OperatorKey>(unionOp.getUnionMembers());
        List<TezOperator> predecessors = new ArrayList<TezOperator>(tezPlan.getPredecessors(unionOp));
        List<TezOperator> successors = tezPlan.getSuccessors(unionOp) == null ? null
                : new ArrayList<TezOperator>(tezPlan.getSuccessors(unionOp));

        if (predecessors.size() > unionOp.getUnionMembers().size()
                && uniqueUnionMembers.size() != 1) {
            return; // TODO: PIG-3856
        }
        if (uniqueUnionMembers.size() == 1) {
            // We actually don't need VertexGroup in this case. The multiple
            // sub-plans of Split can write to same MROutput or the Tez LogicalOutput
            OperatorKey splitPredKey = uniqueUnionMembers.iterator().next();
            TezOperator splitPredOp = tezPlan.getOperator(splitPredKey);
            PhysicalPlan splitPredPlan = splitPredOp.plan;
            if (splitPredPlan.getLeaves().get(0) instanceof POSplit) { //It has to be. But check anyways

                try {
                    connectUnionNonMemberPredecessorsToSplit(unionOp, splitPredOp, predecessors);

                    // Remove POShuffledValueInputTez from union plan root
                    unionOpPlan.remove(unionOpPlan.getRoots().get(0));
                    // Clone union plan into split subplans
                    for (int i=0; i < Collections.frequency(unionOp.getUnionMembers(), splitPredKey); i++ ) {
                        cloneAndMergeUnionPlan(unionOp, splitPredOp);
                    }
                    copyOperatorProperties(splitPredOp, unionOp);
                    tezPlan.disconnect(splitPredOp, unionOp);

                    connectSplitOpToUnionSuccessors(unionOp, splitPredOp, successors);
                } catch (PlanException e) {
                    throw new VisitorException(e);
                }

                //Remove union operator from the plan
                tezPlan.remove(unionOp);
                return;
            } else {
                throw new VisitorException("Expected POSplit but found " + splitPredPlan.getLeaves().get(0));
            }
        }

        // Create vertex group operator for each store. Union followed by Split
        // followed by Store could have multiple stores
        List<POStoreTez> unionStoreOutputs = PlanHelper.getPhysicalOperators(unionOpPlan, POStoreTez.class);
        TezOperator[] storeVertexGroupOps = new TezOperator[unionStoreOutputs.size()];
        for (int i=0; i < storeVertexGroupOps.length; i++) {
            TezOperator existingVertexGroup = null;
            if (successors != null) {
                for (TezOperator succ : successors) {
                    if (succ.isVertexGroup() && unionStoreOutputs.get(i).getSFile().equals(succ.getVertexGroupInfo().getSFile())) {
                        existingVertexGroup = succ;
                    }
                }
            }
            if (existingVertexGroup != null) {
                storeVertexGroupOps[i] = existingVertexGroup;
                existingVertexGroup.getVertexGroupMembers().remove(unionOp.getOperatorKey());
                existingVertexGroup.getVertexGroupMembers().addAll(unionOp.getUnionMembers());
                existingVertexGroup.getVertexGroupInfo().removeInput(unionOp.getOperatorKey());
            } else {
                storeVertexGroupOps[i] = new TezOperator(OperatorKey.genOpKey(scope));
                storeVertexGroupOps[i].setVertexGroupInfo(new VertexGroupInfo(unionStoreOutputs.get(i)));
                storeVertexGroupOps[i].getVertexGroupInfo().setSFile(unionStoreOutputs.get(i).getSFile());
                storeVertexGroupOps[i].setVertexGroupMembers(new ArrayList<OperatorKey>(unionOp.getUnionMembers()));
                tezPlan.add(storeVertexGroupOps[i]);
            }
        }

        // Create vertex group operator for each output. Case of split, orderby,
        // skewed join, rank, etc will have multiple outputs
        List<TezOutput> unionOutputs = PlanHelper.getPhysicalOperators(unionOpPlan, TezOutput.class);
        // One TezOutput can write to multiple LogicalOutputs (POCounterTez, POValueOutputTez, etc)
        List<String> unionOutputKeys = new ArrayList<String>();
        for (TezOutput output : unionOutputs) {
            if (output instanceof POStoreTez) {
                continue;
            }
            for (String key : output.getTezOutputs()) {
                unionOutputKeys.add(key);
            }
        }
        TezOperator[] outputVertexGroupOps = new TezOperator[unionOutputKeys.size()];
        String[] newOutputKeys = new String[unionOutputKeys.size()];
        for (int i=0; i < outputVertexGroupOps.length; i++) {
            outputVertexGroupOps[i] = new TezOperator(OperatorKey.genOpKey(scope));
            outputVertexGroupOps[i].setVertexGroupInfo(new VertexGroupInfo());
            outputVertexGroupOps[i].getVertexGroupInfo().setOutput(unionOutputKeys.get(i));
            outputVertexGroupOps[i].setVertexGroupMembers(new ArrayList<OperatorKey>(unionOp.getUnionMembers()));
            newOutputKeys[i] = outputVertexGroupOps[i].getOperatorKey().toString();
            tezPlan.add(outputVertexGroupOps[i]);
        }

        // Change plan from Predecessors -> Union -> Successor(s) to
        // Predecessors -> Vertex Group(s) -> Successor(s)
        try {
             // Remove POShuffledValueInputTez from union plan root
            unionOpPlan.remove(unionOpPlan.getRoots().get(0));

            for (OperatorKey predKey : unionOp.getUnionMembers()) {
                TezOperator pred = tezPlan.getOperator(predKey);
                PhysicalPlan clonePlan = cloneAndMergeUnionPlan(unionOp, pred);
                connectPredecessorsToVertexGroups(unionOp, pred, clonePlan,
                        storeVertexGroupOps, outputVertexGroupOps);
            }

            connectVertexGroupsToSuccessors(unionOp, successors,
                    unionOutputKeys, outputVertexGroupOps);

            replaceSuccessorInputsAndDisconnect(unionOp, successors, unionOutputKeys, newOutputKeys);

            //Remove union operator from the plan
            tezPlan.remove(unionOp);
        } catch (VisitorException e) {
            throw e;
        }  catch (Exception e) {
            throw new VisitorException(e);
        }

    }

    /**
     * Connect the predecessors of the union which are not members of the union
     * (usually FRJoin replicated table orSkewedJoin sample) to the Split op
     * which is the only member of the union. Disconnect those predecessors from the union.
     *
     * Replace the output keys of those predecessors with the split operator
     * key instead of the union operator key.
     *
     * @param unionOp Union operator
     * @param splitPredOp Split operator which is the only member of the union and its predecessor
     * @param unionPredecessors Predecessors of the union including the split operator
     * @throws PlanException
     * @throws VisitorException
     */
    private void connectUnionNonMemberPredecessorsToSplit(TezOperator unionOp,
            TezOperator splitPredOp,
            List<TezOperator> unionPredecessors) throws PlanException, VisitorException {
        String unionOpKey = unionOp.getOperatorKey().toString();
        OperatorKey splitPredKey = splitPredOp.getOperatorKey();
        for (TezOperator pred : unionPredecessors) {

            if (!pred.getOperatorKey().equals(splitPredKey)) { //Skip splitPredOp which is also a predecessor
                // Get actual predecessors if predecessor is a vertex group
                TezOperator predVertexGroup = null;
                List<TezOperator> actualPreds = new ArrayList<TezOperator>();
                if (pred.isVertexGroup()) {
                    predVertexGroup = pred;
                    for (OperatorKey opKey : pred.getVertexGroupMembers()) {
                        // There should not be multiple levels of vertex group. So no recursion required.
                        actualPreds.add(tezPlan.getOperator(opKey));
                    }
                    tezPlan.disconnect(predVertexGroup, unionOp);
                    tezPlan.connect(predVertexGroup, splitPredOp);
                } else {
                    actualPreds.add(pred);
                }

                for (TezOperator actualPred : actualPreds) {

                    TezCompilerUtil.replaceOutput(actualPred, unionOpKey, splitPredKey.toString());

                    TezEdgeDescriptor edge = actualPred.outEdges.remove(unionOp.getOperatorKey());
                    if (edge == null) {
                        throw new VisitorException("Edge description is empty");
                    }
                    actualPred.outEdges.put(splitPredKey, edge);
                    splitPredOp.inEdges.put(actualPred.getOperatorKey(), edge);
                    if (predVertexGroup == null) {
                        // Disconnect FRJoin table/SkewedJoin sample edge to
                        // union op and connect to POSplit
                        tezPlan.disconnect(actualPred, unionOp);
                        tezPlan.connect(actualPred, splitPredOp);
                    }
                }
            }
        }
    }

    /**
     * Connect the split operator to the successors of the union operators and update the edges.
     * Also change the inputs of the successor from the union operator to the split operator.
     *
     * @param unionOp Union operator
     * @param splitPredOp Split operator which is the only member of the union
     * @param successors Successors of the union operator
     * @throws PlanException
     * @throws VisitorException
     */
    private void connectSplitOpToUnionSuccessors(TezOperator unionOp,
            TezOperator splitPredOp, List<TezOperator> successors)
            throws PlanException, VisitorException {
        String unionOpKey = unionOp.getOperatorKey().toString();
        String splitPredOpKey = splitPredOp.getOperatorKey().toString();
        if (successors != null) {
            for (TezOperator succ : successors) {
                TezOperator successorVertexGroup = null;
                boolean removeSuccessorVertexGroup = false;
                List<TezOperator> actualSuccs = new ArrayList<TezOperator>();
                if (succ.isVertexGroup()) {
                    successorVertexGroup = succ;
                    if (tezPlan.getSuccessors(successorVertexGroup) != null) {
                        // There should not be multiple levels of vertex group. So no recursion required.
                        actualSuccs.addAll(tezPlan.getSuccessors(successorVertexGroup));
                    }
                    int index = succ.getVertexGroupMembers().indexOf(unionOp.getOperatorKey());
                    while (index > -1) {
                        succ.getVertexGroupMembers().set(index, splitPredOp.getOperatorKey());
                        index = succ.getVertexGroupMembers().indexOf(unionOp.getOperatorKey());
                    }
                    // Store vertex group
                    POStore store = successorVertexGroup.getVertexGroupInfo().getStore();
                    if (store != null) {
                        //Clone changes the operator keys
                        List<POStoreTez> storeOutputs = PlanHelper.getPhysicalOperators(splitPredOp.plan, POStoreTez.class);
                        for (POStoreTez storeOut : storeOutputs) {
                            if (storeOut.getOutputKey().equals(store.getOperatorKey().toString())) {
                                splitPredOp.addVertexGroupStore(storeOut.getOperatorKey(), successorVertexGroup.getOperatorKey());
                            }
                        }
                    }
                    tezPlan.disconnect(unionOp, successorVertexGroup);
                    Set<OperatorKey> uniqueVertexGroupMembers = new HashSet<OperatorKey>(succ.getVertexGroupMembers());
                    if (uniqueVertexGroupMembers.size() == 1) {
                        //Only splitPredOp is member of the vertex group. Get rid of the vertex group
                        removeSuccessorVertexGroup = true;
                    } else {
                        tezPlan.connect(splitPredOp, successorVertexGroup);
                    }
                } else {
                    actualSuccs.add(succ);
                }

                // Store vertex group
                if (actualSuccs.isEmpty() && removeSuccessorVertexGroup) {
                    splitPredOp.removeVertexGroupStore(successorVertexGroup.getOperatorKey());
                    tezPlan.remove(successorVertexGroup);
                }

                for (TezOperator actualSucc : actualSuccs) {

                    TezCompilerUtil.replaceInput(actualSucc, unionOpKey, splitPredOpKey);

                    TezEdgeDescriptor edge = actualSucc.inEdges.remove(unionOp.getOperatorKey());
                    if (edge == null) {
                        throw new VisitorException("Edge description is empty");
                    }
                    actualSucc.inEdges.put(splitPredOp.getOperatorKey(), edge);
                    splitPredOp.outEdges.put(actualSucc.getOperatorKey(), edge);
                    if (successorVertexGroup == null || removeSuccessorVertexGroup) {
                        if (removeSuccessorVertexGroup) {
                            // Changes plan from SplitOp -> Union -> VertexGroup - > Successor
                            // to SplitOp -> Successor
                            tezPlan.disconnect(successorVertexGroup, actualSucc);
                            tezPlan.remove(successorVertexGroup);
                            TezCompilerUtil.replaceInput(actualSucc, successorVertexGroup.getOperatorKey().toString(), splitPredOpKey);
                        } else {
                            // Changes plan from SplitOp -> Union -> Successor
                            // to SplitOp -> Successor
                            tezPlan.disconnect(unionOp, actualSucc);
                        }
                        tezPlan.connect(splitPredOp, actualSucc);
                    }
                }
            }
        }
    }

    /**
     * Clone plan of union and merge it into the predecessor operator
     *
     * @param unionOp Union operator
     * @param predOp Predecessor operator of union to which union plan should be merged to
     */
    private PhysicalPlan cloneAndMergeUnionPlan(TezOperator unionOp, TezOperator predOp) throws VisitorException {
        try {
            PhysicalPlan predPlan = predOp.plan;
            PhysicalOperator predLeaf = predPlan.getLeaves().get(0);
            // if predLeaf not POValueOutputTez
            if (predLeaf instanceof POSplit) {
                // Find the subPlan that connects to the union operator
                predPlan = getUnionPredPlanFromSplit(predPlan, unionOp.getOperatorKey().toString());
                predLeaf = predPlan.getLeaves().get(0);
            }
            PhysicalPlan clonePlan = unionOp.plan.clone();

            // Remove POValueOutputTez from predecessor leaf
            predPlan.remove(predLeaf);
            boolean isEmptyPlan = predPlan.isEmpty();
            if (!isEmptyPlan) {
                predLeaf = predPlan.getLeaves().get(0);
            }
            predPlan.merge(clonePlan);
            if (!isEmptyPlan) {
                predPlan.connect(predLeaf, clonePlan.getRoots().get(0));
            }
            return clonePlan;
        } catch (Exception e) {
            throw new VisitorException(e);
        }
    }

    /**
     * Connects the unionOp predecessor to the store vertex groups and the output vertex groups
     * and disconnects it from the unionOp.
     *
     * @param pred Predecessor of union which will be made part of the vertex group
     * @param unionOp Union operator
     * @param predClonedUnionPlan Cloned plan of the union merged to the predecessor
     * @param storeVertexGroupOps Store vertex groups to connect to
     * @param outputVertexGroupOps Tez LogicalOutput vertex groups to connect to
     */
    public void connectPredecessorsToVertexGroups(TezOperator unionOp,
            TezOperator pred, PhysicalPlan predClonedUnionPlan,
            TezOperator[] storeVertexGroupOps,
            TezOperator[] outputVertexGroupOps) throws VisitorException,PlanException {

        //Clone changes the operator keys
        List<POStoreTez> clonedUnionStoreOutputs = PlanHelper.getPhysicalOperators(predClonedUnionPlan, POStoreTez.class);

        // Connect predecessor to the storeVertexGroups
        int i = 0;
        for (TezOperator storeVertexGroup : storeVertexGroupOps) {
            storeVertexGroup.getVertexGroupInfo().addInput(pred.getOperatorKey());
            pred.addVertexGroupStore(clonedUnionStoreOutputs.get(i++).getOperatorKey(),
                    storeVertexGroup.getOperatorKey());
            tezPlan.connect(pred, storeVertexGroup);
        }

        for (TezOperator outputVertexGroup : outputVertexGroupOps) {
            outputVertexGroup.getVertexGroupInfo().addInput(pred.getOperatorKey());
            tezPlan.connect(pred, outputVertexGroup);
        }

        copyOperatorProperties(pred, unionOp);
        tezPlan.disconnect(pred, unionOp);
    }

    /**
     * Connect vertexgroup operator to successor operator in the plan.
     *
     * Copy the output edge between union operator and successor to between
     * predecessors and successor. Predecessor output key and output edge points
     * to successor so that we have all the edge configuration, but they are
     * connected to the vertex group in the plan.
     *
     * @param unionOp Union operator
     * @param successors Successors of the union operator
     * @param unionOutputKeys Output keys of union
     * @param outputVertexGroupOp  Tez LogicalOutput vertex groups corresponding to the output keys
     *
     * @throws PlanException
     */
    private void connectVertexGroupsToSuccessors(TezOperator unionOp,
            List<TezOperator> successors, List<String> unionOutputKeys,
            TezOperator[] outputVertexGroupOps) throws PlanException {
        // Connect to outputVertexGroupOps
        for (Entry<OperatorKey, TezEdgeDescriptor> entry : unionOp.outEdges.entrySet()) {
            TezOperator succOp = tezPlan.getOperator(entry.getKey());
            // Case of union followed by union.
            // unionOp.outEdges will not point to vertex group, but to its output.
            // So find the vertex group if there is one.
            TezOperator succOpVertexGroup = null;
            for (TezOperator succ : successors) {
                if (succ.isVertexGroup()
                        && succOp.getOperatorKey().toString()
                                .equals(succ.getVertexGroupInfo().getOutput())) {
                    succOpVertexGroup = succ;
                    break;
                }
            }
            TezEdgeDescriptor edge = entry.getValue();
            // Edge cannot be one to one as it will get input from two or
            // more union predecessors. Change it to SCATTER_GATHER
            if (edge.dataMovementType == DataMovementType.ONE_TO_ONE) {
                edge.dataMovementType = DataMovementType.SCATTER_GATHER;
                edge.partitionerClass = RoundRobinPartitioner.class;
                edge.outputClassName = UnorderedPartitionedKVOutput.class.getName();
                edge.inputClassName = UnorderedKVInput.class.getName();
            }
            TezOperator vertexGroupOp = outputVertexGroupOps[unionOutputKeys.indexOf(entry.getKey().toString())];
            for (OperatorKey predKey : vertexGroupOp.getVertexGroupMembers()) {
                TezOperator pred = tezPlan.getOperator(predKey);
                // Keep the output edge directly to successor
                // Don't need to keep output edge for vertexgroup
                pred.outEdges.put(entry.getKey(), edge);
                succOp.inEdges.put(predKey, edge);
                if (succOpVertexGroup != null) {
                    succOpVertexGroup.getVertexGroupMembers().add(predKey);
                    succOpVertexGroup.getVertexGroupInfo().addInput(predKey);
                    // Connect directly to the successor vertex group
                    tezPlan.disconnect(pred, vertexGroupOp);
                    tezPlan.connect(pred, succOpVertexGroup);
                }
            }
            if (succOpVertexGroup != null) {
                succOpVertexGroup.getVertexGroupMembers().remove(unionOp.getOperatorKey());
                succOpVertexGroup.getVertexGroupInfo().removeInput(unionOp.getOperatorKey());
                //Discard the new vertex group created
                tezPlan.remove(vertexGroupOp);
            } else {
                tezPlan.connect(vertexGroupOp, succOp);
            }
        }
    }

    private void replaceSuccessorInputsAndDisconnect(TezOperator unionOp,
            List<TezOperator> successors,
            List<String> unionOutputKeys,
            String[] newOutputKeys)
            throws VisitorException {
        if (successors != null) {
            String unionOpKey = unionOp.getOperatorKey().toString();
            // Successor inputs should now point to the vertex groups.
            for (TezOperator succ : successors) {
                LinkedList<TezInput> inputs = PlanHelper.getPhysicalOperators(succ.plan, TezInput.class);
                for (TezInput input : inputs) {
                    for (String key : input.getTezInputs()) {
                        if (key.equals(unionOpKey)) {
                            input.replaceInput(key,
                                    newOutputKeys[unionOutputKeys.indexOf(succ.getOperatorKey().toString())]);
                        }
                    }
                }

                List<POUserFunc> userFuncs = PlanHelper.getPhysicalOperators(succ.plan, POUserFunc.class);
                for (POUserFunc userFunc : userFuncs) {
                    if (userFunc.getFunc() instanceof ReadScalarsTez) {
                        TezInput tezInput = (TezInput)userFunc.getFunc();
                        for (String inputKey : tezInput.getTezInputs()) {
                            if (inputKey.equals(unionOpKey)) {
                                tezInput.replaceInput(inputKey,
                                        newOutputKeys[unionOutputKeys.indexOf(succ.getOperatorKey().toString())]);
                                userFunc.getFuncSpec().setCtorArgs(tezInput.getTezInputs());
                            }
                        }
                    }
                }

                tezPlan.disconnect(unionOp, succ);
            }
        }
    }

    private void copyOperatorProperties(TezOperator pred, TezOperator unionOp) throws VisitorException {
        pred.UDFs.addAll(unionOp.UDFs);
        pred.scalars.addAll(unionOp.scalars);
        // Copy only map side properties. For eg: crossKeys.
        // Do not copy reduce side specific properties. For eg: useSecondaryKey, segmentBelow, sortOrder, etc
        // Also ignore parallelism settings
        if (unionOp.getCrossKeys() != null) {
            for (String key : unionOp.getCrossKeys()) {
                pred.addCrossKey(key);
            }
        }
        pred.copyFeatures(unionOp, Arrays.asList(new OPER_FEATURE[]{OPER_FEATURE.UNION}));

        // For skewed join right input
        if (unionOp.getSampleOperator() !=  null) {
            if (pred.getSampleOperator() == null) {
                pred.setSampleOperator(unionOp.getSampleOperator());
            } else if (!pred.getSampleOperator().equals(unionOp.getSampleOperator())) {
                throw new VisitorException("Conflicting sample operators "
                        + pred.getSampleOperator().toString() + " and "
                        + unionOp.getSampleOperator().toString());
            }
        }
    }

    public static PhysicalPlan getUnionPredPlanFromSplit(PhysicalPlan plan, String unionOpKey) throws VisitorException {
        List<POSplit> splits = PlanHelper.getPhysicalOperators(plan, POSplit.class);
        for (POSplit split : splits) {
            for (PhysicalPlan subPlan : split.getPlans()) {
                if (subPlan.getLeaves().get(0) instanceof POValueOutputTez) {
                    POValueOutputTez out = (POValueOutputTez) subPlan.getLeaves().get(0);
                    if (out.containsOutputKey(unionOpKey)) {
                        return subPlan;
                    }
                }
            }
        }
        throw new VisitorException("Did not find the union predecessor in the split plan");
    }

}
