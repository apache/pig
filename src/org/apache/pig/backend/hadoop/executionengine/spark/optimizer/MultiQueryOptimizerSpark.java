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
package org.apache.pig.backend.hadoop.executionengine.spark.optimizer;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSplit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkOpPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkOperPlan;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkOperator;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.ReverseDependencyOrderWalker;
import org.apache.pig.impl.plan.VisitorException;


/**
 * MultiQueryOptimizer for spark
 */
public class MultiQueryOptimizerSpark extends SparkOpPlanVisitor {

    private static final Log LOG = LogFactory.getLog(MultiQueryOptimizerSpark.class);

    private String scope;
    private NodeIdGenerator nig;

    public MultiQueryOptimizerSpark(SparkOperPlan plan) {
        super(plan, new ReverseDependencyOrderWalker<SparkOperator, SparkOperPlan>(plan));
        nig = NodeIdGenerator.getGenerator();
        List<SparkOperator> roots = plan.getRoots();
        scope = roots.get(0).getOperatorKey().getScope();
    }


    @Override
    public void visitSparkOp(SparkOperator sparkOp) throws VisitorException {
        try {
            if (!sparkOp.isSplitter()) {
                return;
            }

            List<SparkOperator> splittees = getPlan().getSuccessors(sparkOp);

            if (splittees == null) {
                return;
            }

            //If the size of predecessors of splittee is more than 1, then not do multiquery optimization
            //@see TestMultiQueryBasic#testMultiQueryWithFJ_2
            for (SparkOperator splittee : splittees) {
                if (getPlan().getPredecessors(splittee).size() > 1) {
                    return;
                }
            }

            if (splittees.size() == 1) {
                // We don't need a POSplit here, we can merge the splittee into spliter
                SparkOperator singleSplitee = splittees.get(0);
                POStore poStore = null;
                PhysicalOperator firstNodeLeaf = sparkOp.physicalPlan.getLeaves().get(0);
                if (firstNodeLeaf instanceof POStore) {
                    poStore = (POStore) firstNodeLeaf;
                }
                PhysicalOperator firstNodeLeafPred = sparkOp.physicalPlan.getPredecessors(firstNodeLeaf).get(0);
                sparkOp.physicalPlan.remove(poStore);  // remove  unnecessary store
                List<PhysicalOperator> firstNodeRoots = singleSplitee.physicalPlan.getRoots();
                sparkOp.physicalPlan.merge(singleSplitee.physicalPlan);
                for (int j = 0; j < firstNodeRoots.size(); j++) {
                    PhysicalOperator firstNodeRoot = firstNodeRoots.get(j);
                    POLoad poLoad = null;
                    if (firstNodeRoot instanceof POLoad && poStore != null) {
                        poLoad = (POLoad) firstNodeRoot;
                        if (poLoad.getLFile().getFileName().equals(poStore.getSFile().getFileName())) {
                            PhysicalOperator firstNodeRootSucc = sparkOp.physicalPlan.getSuccessors(firstNodeRoot).get(0);
                            sparkOp.physicalPlan.remove(poLoad); // remove unnecessary load
                            sparkOp.physicalPlan.forceConnect(firstNodeLeafPred, firstNodeRootSucc);
                        }
                    }
                }
                addSubPlanPropertiesToParent(sparkOp, singleSplitee);
                removeSplittee(getPlan(), sparkOp, singleSplitee);
            } else {
                //If the size of splittee is more than 1, we need create a split which type is POSplit, merge all the physical plans
                // of splittees to the physical plan of split and remove the splittees.
                List<PhysicalOperator> firstNodeLeaves = sparkOp.physicalPlan.getLeaves();
                PhysicalOperator firstNodeLeaf = firstNodeLeaves.size() > 0 ? firstNodeLeaves.get(0) : null;
                POStore poStore = null;
                if (firstNodeLeaf != null && firstNodeLeaf instanceof POStore) {
                    poStore = (POStore) firstNodeLeaf;
                    PhysicalOperator predOfPoStore = sparkOp.physicalPlan.getPredecessors(poStore).get(0);
                    sparkOp.physicalPlan.remove(poStore); // remove  unnecessary store
                    POSplit poSplit = createSplit();
                    ArrayList<SparkOperator> spliteesCopy = new ArrayList
                            <SparkOperator>(splittees);
                    for (SparkOperator splitee : spliteesCopy) {
                        List<PhysicalOperator> rootsOfSplitee = splitee.physicalPlan.getRoots();
                        for (int i = 0; i < rootsOfSplitee.size(); i++) {
                            if (rootsOfSplitee.get(i) instanceof POLoad) {
                                POLoad poLoad = (POLoad) rootsOfSplitee.get(i);
                                if (poLoad.getLFile().getFileName().equals(poStore.getSFile().getFileName())) {
                                    List<PhysicalOperator> successorsOfPoLoad = splitee.physicalPlan.getSuccessors(poLoad);
                                    List<PhysicalOperator> successorofPoLoadsCopy = new ArrayList<PhysicalOperator>(successorsOfPoLoad);
                                    splitee.physicalPlan.remove(poLoad);  // remove  unnecessary load
                                    for (PhysicalOperator successorOfPoLoad : successorofPoLoadsCopy) {
                                        //we store from to relationship in SparkOperator#multiQueryOptimizeConnectionMap
                                        sparkOp.addMultiQueryOptimizeConnectionItem(successorOfPoLoad.getOperatorKey(), predOfPoStore.getOperatorKey());
                                        LOG.debug(String.format("add multiQueryOptimize connection item: to:%s, from:%s for %s",
                                                successorOfPoLoad.toString(), predOfPoStore.getOperatorKey().toString(), splitee.getOperatorKey()));
                                    }
                                    poSplit.addPlan(splitee.physicalPlan);
                                    addSubPlanPropertiesToParent(sparkOp, splitee);
                                    removeSplittee(getPlan(), sparkOp, splitee);
                                }
                            }
                        }
                    }

                    sparkOp.physicalPlan.addAsLeaf(poSplit);
                }
            }
        } catch (PlanException e) {
            throw new VisitorException(e);
        }
    }

    private void removeSplittee(SparkOperPlan plan, SparkOperator splitter,
                                SparkOperator splittee) throws PlanException {
        if (plan.getSuccessors(splittee) != null) {
            List<SparkOperator> succs = new ArrayList();
            succs.addAll(plan.getSuccessors(splittee));
            plan.disconnect(splitter, splittee);
            for (SparkOperator succSparkOperator : succs) {
                plan.disconnect(splittee, succSparkOperator);
                plan.connect(splitter, succSparkOperator);
            }
        }
        getPlan().remove(splittee);
    }

    private POSplit createSplit() {
        return new POSplit(new OperatorKey(scope, nig.getNextNodeId(scope)));
    }

    static public void addSubPlanPropertiesToParent(SparkOperator parentOper, SparkOperator subPlanOper) {
        // Copy only map side properties. For eg: crossKeys.
        // Do not copy reduce side specific properties. For eg: useSecondaryKey, segmentBelow, sortOrder, etc
        if (subPlanOper.getCrossKeys() != null) {
            for (String key : subPlanOper.getCrossKeys()) {
                parentOper.addCrossKey(key);
            }
        }
        parentOper.copyFeatures(subPlanOper, null);

        if (subPlanOper.getRequestedParallelism() > parentOper.getRequestedParallelism()) {
            parentOper.setRequestedParallelism(subPlanOper.getRequestedParallelism());
        }
        subPlanOper.setRequestedParallelismByReference(parentOper);
        parentOper.UDFs.addAll(subPlanOper.UDFs);
        parentOper.scalars.addAll(subPlanOper.scalars);
    }
}
