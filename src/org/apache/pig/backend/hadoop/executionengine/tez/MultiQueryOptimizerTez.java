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
import java.util.List;
import java.util.Map.Entry;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POUserFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
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

                // If has other dependency, don't merge into split,
                if (getPlan().getPredecessors(successor).size()!=1) {
                    continue;
                }

                // Detect diamond shape, we cannot merge it into split, since Tez
                // does not handle double edge between vertexes
                // TODO: PIG-3876 to handle this by writing to same edge
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

                tezOp.plan.merge(singleSplitee.plan);
                tezOp.plan.connect(firstNodeLeafPred, secondNodeSucc);

                addSubPlanPropertiesToParent(tezOp, singleSplitee);

                removeSplittee(getPlan(), tezOp, singleSplitee);
            } else {
                POValueOutputTez valueOutput = (POValueOutputTez)tezOp.plan.getLeaves().get(0);
                POSplit split = new POSplit(OperatorKey.genOpKey(valueOutput.getOperatorKey().getScope()));
                split.setAlias(valueOutput.getAlias());
                for (TezOperator splitee : splittees) {
                    PhysicalOperator spliteeRoot =  splitee.plan.getRoots().get(0);
                    splitee.plan.remove(spliteeRoot);
                    split.addPlan(splitee.plan);

                    addSubPlanPropertiesToParent(tezOp, splitee);

                    removeSplittee(getPlan(), tezOp, splitee);
                    valueOutput.removeOutputKey(splitee.getOperatorKey().toString());
                }
                if (valueOutput.getTezOutputs().length > 0) {
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

                try {
                    List<TezInput> inputs = PlanHelper.getPhysicalOperators(succTezOperator.plan, TezInput.class);
                    for (TezInput input : inputs) {
                        input.replaceInput(splittee.getOperatorKey().toString(),
                                splitter.getOperatorKey().toString());
                    }
                    List<POUserFunc> userFuncs = PlanHelper.getPhysicalOperators(succTezOperator.plan, POUserFunc.class);
                    for (POUserFunc userFunc : userFuncs) {
                        if (userFunc.getFunc() instanceof ReadScalarsTez) {
                            TezInput tezInput = (TezInput)userFunc.getFunc();
                            tezInput.replaceInput(splittee.getOperatorKey().toString(),
                                    splitter.getOperatorKey().toString());
                            userFunc.getFuncSpec().setCtorArgs(tezInput.getTezInputs());
                        }
                    }
                } catch (VisitorException e) {
                    throw new PlanException(e);
                }

                if (succTezOperator.isUnion()) {
                    int index = succTezOperator.getUnionPredecessors().indexOf(splittee.getOperatorKey());
                    if (index > -1) {
                        succTezOperator.getUnionPredecessors().set(index, splitter.getOperatorKey());
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
        parentOper.UDFs.addAll(subPlanOper.UDFs);
        parentOper.scalars.addAll(subPlanOper.scalars);
        if (subPlanOper.outEdges != null) {
            for (Entry<OperatorKey, TezEdgeDescriptor> entry: subPlanOper.outEdges.entrySet()) {
                parentOper.outEdges.put(entry.getKey(), entry.getValue());
            }
        }
        if (subPlanOper.isSampler()) {
            parentOper.markSampler();
        }
    }
}
