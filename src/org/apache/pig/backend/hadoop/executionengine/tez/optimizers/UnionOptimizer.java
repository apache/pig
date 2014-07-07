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
package org.apache.pig.backend.hadoop.executionengine.tez.optimizers;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSplit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.backend.hadoop.executionengine.tez.POStoreTez;
import org.apache.pig.backend.hadoop.executionengine.tez.POValueOutputTez;
import org.apache.pig.backend.hadoop.executionengine.tez.RoundRobinPartitioner;
import org.apache.pig.backend.hadoop.executionengine.tez.TezEdgeDescriptor;
import org.apache.pig.backend.hadoop.executionengine.tez.TezInput;
import org.apache.pig.backend.hadoop.executionengine.tez.TezOpPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.tez.TezOperPlan;
import org.apache.pig.backend.hadoop.executionengine.tez.TezOperator;
import org.apache.pig.backend.hadoop.executionengine.tez.TezOperator.VertexGroupInfo;
import org.apache.pig.backend.hadoop.executionengine.tez.TezOutput;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.ReverseDependencyOrderWalker;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.runtime.library.input.ShuffledUnorderedKVInput;
import org.apache.tez.runtime.library.output.OnFileUnorderedPartitionedKVOutput;

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

    public UnionOptimizer(TezOperPlan plan) {
        super(plan, new ReverseDependencyOrderWalker<TezOperator, TezOperPlan>(plan));
    }

    @Override
    public void visitTezOp(TezOperator tezOp) throws VisitorException {
        if (!tezOp.isUnion()) {
            return;
        }

        TezOperator unionOp = tezOp;
        String unionOpKey = unionOp.getOperatorKey().toString();
        String scope = unionOp.getOperatorKey().scope;
        TezOperPlan tezPlan = getPlan();

        //TODO: PIG-3856 Handle replicated join. Replicate join input that was broadcast to union vertex
        // now needs to be broadcast to all the union predecessors. How do we do that??
        // Wait for shared edge and do it or write multiple times??
        // For now don't optimize
        // Create a copy as disconnect while iterating modifies the original list
        List<TezOperator> predecessors = new ArrayList<TezOperator>(tezPlan.getPredecessors(unionOp));
        if (predecessors.size() > unionOp.getVertexGroupMembers().size()) {
            return;
        }

        PhysicalPlan unionOpPlan = unionOp.plan;

        // Union followed by Split followed by Store could have multiple stores
        List<POStoreTez> unionStoreOutputs = PlanHelper.getPhysicalOperators(unionOpPlan, POStoreTez.class);
        TezOperator[] storeVertexGroupOps = new TezOperator[unionStoreOutputs.size()];
        for (int i=0; i < storeVertexGroupOps.length; i++) {
            storeVertexGroupOps[i] = new TezOperator(OperatorKey.genOpKey(scope));
            storeVertexGroupOps[i].setVertexGroupInfo(new VertexGroupInfo(unionStoreOutputs.get(i)));
            storeVertexGroupOps[i].setVertexGroupMembers(unionOp.getVertexGroupMembers());
            tezPlan.add(storeVertexGroupOps[i]);
        }

        // Case of split, orderby, skewed join, rank, etc will have multiple outputs
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

        // Create vertex group operator for each output
        TezOperator[] outputVertexGroupOps = new TezOperator[unionOutputKeys.size()];
        String[] newOutputKeys = new String[unionOutputKeys.size()];
        for (int i=0; i < outputVertexGroupOps.length; i++) {
            outputVertexGroupOps[i] = new TezOperator(OperatorKey.genOpKey(scope));
            outputVertexGroupOps[i].setVertexGroupInfo(new VertexGroupInfo());
            outputVertexGroupOps[i].getVertexGroupInfo().setOutput(unionOutputKeys.get(i));
            outputVertexGroupOps[i].setVertexGroupMembers(unionOp.getVertexGroupMembers());
            newOutputKeys[i] = outputVertexGroupOps[i].getOperatorKey().toString();
            tezPlan.add(outputVertexGroupOps[i]);
        }

        try {

             // Clone plan of union and merge it into the predecessor operators
             // Remove POShuffledValueInputTez from union plan root
            unionOpPlan.remove(unionOpPlan.getRoots().get(0));
            for (OperatorKey predKey : unionOp.getVertexGroupMembers()) {
                TezOperator pred = tezPlan.getOperator(predKey);
                PhysicalPlan predPlan = pred.plan;
                PhysicalOperator predLeaf = predPlan.getLeaves().get(0);
                // if predLeaf not POValueOutputTez
                if (predLeaf instanceof POSplit) {
                    // Find the subPlan that connects to the union operator
                    predPlan = getUnionPredPlanFromSplit(predPlan, unionOpKey);
                    predLeaf = predPlan.getLeaves().get(0);
                }

                PhysicalPlan clonePlan = unionOpPlan.clone();
                //Clone changes the operator keys
                List<POStoreTez> clonedUnionStoreOutputs = PlanHelper.getPhysicalOperators(clonePlan, POStoreTez.class);

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

                // Connect predecessor to the storeVertexGroups
                int i = 0;
                for (TezOperator storeVertexGroup : storeVertexGroupOps) {
                    storeVertexGroup.getVertexGroupInfo().addInput(pred.getOperatorKey());
                    //Set the output key of cloned POStore to that of the initial union POStore.
                    clonedUnionStoreOutputs.get(i).setOutputKey(
                            storeVertexGroup.getVertexGroupInfo().getStore()
                                    .getOperatorKey().toString());
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

            List<TezOperator> successors = tezPlan.getSuccessors(unionOp);
            List<TezOutput> valueOnlyOutputs = new ArrayList<TezOutput>();
            for (TezOutput tezOutput : unionOutputs) {
                if (tezOutput instanceof POValueOutputTez) {
                    valueOnlyOutputs.add(tezOutput);
                }
            }
            // Connect to outputVertexGroupOps
            // Copy output edges of union -> successor to predecessor->successor, vertexgroup -> successor
            // and connect vertexgroup -> successor in the plan.
            for (Entry<OperatorKey, TezEdgeDescriptor> entry : unionOp.outEdges.entrySet()) {
                TezOperator succOp = tezPlan.getOperator(entry.getKey());
                // Case of union followed by union.
                // unionOp.outEdges will not point to vertex group, but to its output.
                // So find the vertex group if there is one.
                TezOperator succOpVertexGroup = null;
                for (TezOperator succ : successors) {
                    if (succ.isVertexGroup()
                            && succ.getVertexGroupInfo().getOutput()
                                    .equals(succOp.getOperatorKey().toString())) {
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
                    edge.outputClassName = OnFileUnorderedPartitionedKVOutput.class.getName();
                    edge.inputClassName = ShuffledUnorderedKVInput.class.getName();
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
        } catch (Exception e) {
            throw new VisitorException(e);
        }

        List<TezOperator> succs = tezPlan.getSuccessors(unionOp);
        // Create a copy as disconnect while iterating modifies the original list
        List<TezOperator> successors = succs == null ? null : new ArrayList<TezOperator>(succs);
        if (successors != null) {
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
                tezPlan.disconnect(unionOp, succ);
            }
        }

        //Remove union operator from the plan
        tezPlan.remove(unionOp);

    }

    private void copyOperatorProperties(TezOperator pred, TezOperator unionOp) {
        pred.setUseSecondaryKey(unionOp.isUseSecondaryKey());
        pred.UDFs.addAll(unionOp.UDFs);
        pred.scalars.addAll(unionOp.scalars);
        if (unionOp.isSampler()) {
            pred.markSampler();
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
