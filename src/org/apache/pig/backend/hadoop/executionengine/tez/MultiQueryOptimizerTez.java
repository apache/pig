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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFRJoin;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPoissonSample;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POReservoirSample;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSplit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.backend.hadoop.executionengine.tez.util.TezCompilerUtil;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.ReverseDependencyOrderWalker;
import org.apache.pig.impl.plan.VisitorException;

public class MultiQueryOptimizerTez extends TezOpPlanVisitor {
    public MultiQueryOptimizerTez(TezOperPlan plan) {
        super(plan, new ReverseDependencyOrderWalker<TezOperator, TezOperPlan>(plan));
    }

    @Override
    public void visitTezOp(TezOperator tezOp) throws VisitorException {
        try {
            if (!tezOp.isSplitter()) {
                return;
            }

            List<TezOperator> splittees = new ArrayList<TezOperator>();

            List<TezOperator> successors = getPlan().getSuccessors(tezOp);
            List<TezOperator> succ_successors = new ArrayList<TezOperator>();
            for (TezOperator successor : successors) {
                // don't want to be complicated by nested split
                if (successor.isSplitter()) {
                    continue;
                }
                // If has other dependency, don't merge into split,
                if (getPlan().getPredecessors(successor).size()!=1) {
                    continue;
                }
                boolean containsBlacklistedOp = false;
                for (PhysicalOperator op : successor.plan) {
                    if (op instanceof POReservoirSample || op instanceof POPoissonSample) {
                        containsBlacklistedOp = true;
                        break;
                    }
                }
                if (containsBlacklistedOp) {
                    continue;
                }
                // Detect diamond shape, we cannot merge it into split, since Tez
                // does not handle double edge between vertexes
                boolean sharedSucc = false;
                if (getPlan().getSuccessors(successor)!=null) {
                    for (TezOperator succ_successor : getPlan().getSuccessors(successor)) {
                        if (succ_successors.contains(succ_successor)) {
                            sharedSucc = true;
                            break;
                        }
                    }
                    succ_successors.addAll(getPlan().getSuccessors(successor));
                }
                if (sharedSucc) {
                    continue;
                }
                splittees.add(successor);
            }

            if (splittees.size()==0) {
                return;
            }

            if (splittees.size()==1 && successors.size()==1) {
                // We don't need a POSplit here, we can merge the splittee into spliter
                PhysicalOperator firstNodeLeaf = tezOp.plan.getLeaves().get(0);
                PhysicalOperator firstNodeLeafPred  = tezOp.plan.getPredecessors(firstNodeLeaf).get(0);

                TezOperator singleSplitee = splittees.get(0);
                PhysicalOperator secondNodeRoot =  singleSplitee.plan.getRoots().get(0);
                PhysicalOperator secondNodeSucc = singleSplitee.plan.getSuccessors(secondNodeRoot).get(0);

                tezOp.plan.remove(firstNodeLeaf);
                singleSplitee.plan.remove(secondNodeRoot);

                //TODO remove filter all

                tezOp.plan.merge(singleSplitee.plan);
                tezOp.plan.connect(firstNodeLeafPred, secondNodeSucc);

                addSubPlanPropertiesToParent(tezOp, singleSplitee);

                removeSplittee(getPlan(), tezOp, singleSplitee);
            } else {
                POValueOutputTez valueOutput = (POValueOutputTez)tezOp.plan.getLeaves().get(0);
                POSplit split = new POSplit(OperatorKey.genOpKey(valueOutput.getOperatorKey().getScope()));
                for (TezOperator splitee : splittees) {
                    PhysicalOperator spliteeRoot =  splitee.plan.getRoots().get(0);
                    splitee.plan.remove(spliteeRoot);
                    split.addPlan(splitee.plan);

                    addSubPlanPropertiesToParent(tezOp, splitee);

                    removeSplittee(getPlan(), tezOp, splitee);
                    valueOutput.outputKeys.remove(splitee.getOperatorKey().toString());
                }
                if (!valueOutput.outputKeys.isEmpty()) {
                    // We still need valueOutput
                    PhysicalPlan phyPlan = new PhysicalPlan();
                    phyPlan.addAsLeaf(valueOutput);
                    split.addPlan(phyPlan);
                }
                PhysicalOperator pred = tezOp.plan.getPredecessors(valueOutput).get(0);
                tezOp.plan.disconnect(pred, valueOutput);
                tezOp.plan.remove(valueOutput);
                tezOp.plan.add(split);
                tezOp.plan.connect(pred, split);
            }
        } catch (PlanException e) {
            throw new VisitorException(e);
        }
    }

    static public void removeSplittee(TezOperPlan plan, TezOperator splitter, TezOperator splittee) throws PlanException {
        if (plan.getSuccessors(splittee)!=null) {
            List<TezOperator> succs = new ArrayList<TezOperator>();
            succs.addAll(plan.getSuccessors(splittee));
            plan.disconnect(splitter, splittee);
            for (TezOperator succTezOperator : succs) {
                TezEdgeDescriptor edge = succTezOperator.inEdges.get(splittee.getOperatorKey());

                splitter.outEdges.remove(splittee.getOperatorKey());
                succTezOperator.inEdges.remove(splittee.getOperatorKey());
                plan.disconnect(splittee, succTezOperator);
                TezCompilerUtil.connect(plan, splitter, succTezOperator, edge);

                for (TezOperator succ : succs) {
                    try {
                        List<POFRJoinTez> frJoins = PlanHelper.getPhysicalOperators(succ.plan, POFRJoinTez.class);
                        for (POFRJoinTez frJoin : frJoins) {
                            if (frJoin.getInputKeys().contains(splittee.getOperatorKey().toString())) {
                                frJoin.getInputKeys().set(frJoin.getInputKeys().indexOf(splittee.getOperatorKey().toString()),
                                        splitter.getOperatorKey().toString());
                            }
                        }
                    } catch (VisitorException e) {
                        throw new PlanException(e);
                    }
                }
            }
        }
        plan.remove(splittee);
    }

    static public void addSubPlanPropertiesToParent(TezOperator parentOper, TezOperator subPlanOper) {
        if (subPlanOper.getRequestedParallelism() > parentOper.getRequestedParallelism()) {
            parentOper.setRequestedParallelism(subPlanOper.getRequestedParallelism());
        }
        subPlanOper.setRequestedParallelismByReference(parentOper);
        if (subPlanOper.UDFs != null) {
            parentOper.UDFs.addAll(subPlanOper.UDFs);
        }
        if (subPlanOper.scalars != null) {
            parentOper.scalars.addAll(subPlanOper.scalars);
        }
        if (subPlanOper.outEdges != null) {
            for (Entry<OperatorKey, TezEdgeDescriptor> entry: subPlanOper.outEdges.entrySet()) {
                parentOper.outEdges.put(entry.getKey(), entry.getValue());
            }
        }
    }
}
