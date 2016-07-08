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
package org.apache.pig.backend.hadoop.executionengine.spark.optimizer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POGlobalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.backend.hadoop.executionengine.spark.operator.POGlobalRearrangeSpark;
import org.apache.pig.backend.hadoop.executionengine.spark.operator.POJoinGroupSpark;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkOpPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkOperPlan;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkOperator;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;

/**
 * Collapse LocalRearrange,GlobalRearrange,Package to POJoinGroupSpark to reduce unnecessary
 * map operations to optimize join/group. Detail see PIG-4797
 */
public class JoinGroupOptimizerSpark extends SparkOpPlanVisitor {
    private static final Log LOG = LogFactory.getLog(JoinGroupOptimizerSpark.class);

    public JoinGroupOptimizerSpark(SparkOperPlan plan) {
        super(plan, new DependencyOrderWalker<SparkOperator, SparkOperPlan>(plan, true));
    }

    @Override
    public void visitSparkOp(SparkOperator sparkOp) throws VisitorException {
        if (sparkOp.physicalPlan != null) {
            GlobalRearrangeDiscover glrDiscover = new GlobalRearrangeDiscover(sparkOp.physicalPlan);
            glrDiscover.visit();
            List<PhysicalPlan> plans = glrDiscover.getPlansWithJoinAndGroup();
            handlePlans(plans);
        }

    }

    private void handlePlans(List<PhysicalPlan> plans) throws VisitorException {
        for(int i=0;i<plans.size();i++){
            PhysicalPlan planWithJoinAndGroup = plans.get(i);
            POGlobalRearrangeSpark glrSpark = PlanHelper.getPhysicalOperators(planWithJoinAndGroup,POGlobalRearrangeSpark.class).get(0);
            if (verifyJoinOrGroupCase(plans.get(i), glrSpark)) {
                try {
                    restructSparkOp(planWithJoinAndGroup, glrSpark);
                } catch (PlanException e) {
                    throw new RuntimeException("GlobalRearrangeDiscover#visitSparkOp fails: ", e);
                }
            }
        }
    }

    static class GlobalRearrangeDiscover extends PhyPlanVisitor {
        private List<PhysicalPlan> plansWithJoinAndGroup = new ArrayList<PhysicalPlan>();
        public GlobalRearrangeDiscover(PhysicalPlan plan) {
            super(plan, new DepthFirstWalker<PhysicalOperator, PhysicalPlan>(
                    plan));
        }

        @Override
        public void visitGlobalRearrange(POGlobalRearrange glr) throws VisitorException {
            PhysicalPlan currentPlan = this.mCurrentWalker.getPlan();//If there are POSplit, we need traverse the POSplit.getPlans(), so use mCurrentWalker.getPlan()
            if( currentPlan != null) {
                plansWithJoinAndGroup.add(currentPlan);
            }else{
                LOG.info("GlobalRearrangeDiscover#currentPlan is null");
            }

        }

        public List<PhysicalPlan> getPlansWithJoinAndGroup() {
            return plansWithJoinAndGroup;
        }
    }

    //collapse LRA,GRA,PKG to POJoinGroupSpark
    private void restructSparkOp(PhysicalPlan plan,POGlobalRearrangeSpark glaOp) throws PlanException {

        List<PhysicalOperator> predes = plan.getPredecessors(glaOp);
        if (predes != null) {
            List<POLocalRearrange> lraOps = new ArrayList<POLocalRearrange>();
            List<PhysicalOperator> allPredsOfLRA = new ArrayList<PhysicalOperator>();

            //Get the predecessors of POJoinGroupSpark with correct order after JoinOptimizationSpark
            //For other PhysicalOperator, we usually use OperatorPlan#getPredecessor(op) to get predecessors and sort predecessors[JobGraphBuilder#getPredecessors] to
            //get the predecessor with correct order(in common case, PhysicalOperator
            //with small OperatorKey must be executed before that with bigger OperatorKey),but this is not suitable for POJoinGroupSpark
            //Give an example to explain this:
            //original:
            //POLOAD(scope-1)                                POLOAD(scope-2)
            //               \                                   /
            //   POFOREach(scope-3)                              POLocalRearrange(scope-5)
            //                  \                                /
            //              POLocalRearrange(scope-4)       POLocalRearrange(scope-5)
            //                      \                           /
            //                              POGlobalRearrange(scope-6)
            //                                      |
            //                              POPackage(scope-7)
            //after JoinOptimizationSpark:
            //POLOAD(scope-1)                                POLOAD(scope-2)
            //               \                                   /
            //   POFOREach(scope-3)                             /
            //                     \                           /
            //                        POJoinGroupSpark(scope-8)

            //the predecessor of POJoinGroupSpark(scope-8) is POForEach(scope-3) and POLoad(scope-2) because they are
            //the predecessor of POLocalRearrange(scope-4) and POLocalRearrange(scope-5) while we will get
            //will be POLoad(scope-2) and POForEach(scope-3) if use OperatorPlan#getPredecessor(op)to gain predecessors and sort predecessors
            Collections.sort(predes);
            for (PhysicalOperator lra : predes) {
                lraOps.add((POLocalRearrange) lra);
                List<PhysicalOperator> predOfLRAList = plan.getPredecessors(lra);
                if( predOfLRAList != null && predOfLRAList.size() ==1) {
                    PhysicalOperator predOfLRA = predOfLRAList.get(0);
                    plan.disconnect(predOfLRA, lra);
                    allPredsOfLRA.add(predOfLRA);
                }
            }

            POPackage pkgOp = (POPackage) plan.getSuccessors(glaOp).get(0);
            PhysicalOperator pkgSuccessor = plan.getSuccessors(pkgOp).get(0);
            POJoinGroupSpark joinSpark = new POJoinGroupSpark(lraOps, glaOp, pkgOp);
            if(allPredsOfLRA.size()>0) {
                joinSpark.setPredecessors(allPredsOfLRA);
            }
            plan.add(joinSpark);

            for (PhysicalOperator predOfLRA : allPredsOfLRA) {
                plan.connect(predOfLRA, joinSpark);
            }

            plan.disconnect(pkgOp, pkgSuccessor);
            plan.connect(joinSpark, pkgSuccessor);
            for (POLocalRearrange lra : lraOps) {
                plan.remove(lra);
            }
            plan.remove(glaOp);
            plan.remove(pkgOp);
        }
    }

    private boolean verifyJoinOrGroupCase(PhysicalPlan plan, POGlobalRearrangeSpark glaOp) {
        List<PhysicalOperator> lraOps = plan.getPredecessors(glaOp);
        List<PhysicalOperator> pkgOps = plan.getSuccessors(glaOp);
        boolean isAllPredecessorLRA = isAllPredecessorLRA(lraOps);
        boolean isSuccessorPKG = isSuccessorPKG(pkgOps);
        return isAllPredecessorLRA && isSuccessorPKG;
    }

    private boolean isSuccessorPKG(List<PhysicalOperator> pkgOps) {
        boolean result = false;
        if (pkgOps != null && (pkgOps.size() == 1)) {
            if (pkgOps.get(0) instanceof POPackage) {
                result = true;
            }
        } else {
            result = false;
        }


        return result;
    }

    private boolean isAllPredecessorLRA(List<PhysicalOperator> lraOps) {
        boolean result = true;
        if (lraOps != null) {
            for (PhysicalOperator lraOp : lraOps) {
                if (!(lraOp instanceof POLocalRearrange)) {
                    result = false;
                    break;
                }
            }
        } else {
            result = false;
        }

        return result;
    }
}
