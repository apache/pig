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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;

import org.apache.pig.backend.hadoop.executionengine.optimizer.SecondaryKeyOptimizer;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.backend.hadoop.executionengine.spark.operator.POGlobalRearrangeSpark;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkOpPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkOperPlan;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkOperator;
import org.apache.pig.backend.hadoop.executionengine.util.SecondaryKeyOptimizerUtil;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;

/**
 * Secondary key sort optimization for spark mode
 */
public class SecondaryKeyOptimizerSpark extends SparkOpPlanVisitor implements SecondaryKeyOptimizer {
    private static final Log LOG = LogFactory
            .getLog(SecondaryKeyOptimizerSpark.class);

    private int numSortRemoved = 0;
    private int numDistinctChanged = 0;
    private int numUseSecondaryKey = 0;

    public SecondaryKeyOptimizerSpark(SparkOperPlan plan) {
        super(plan, new DepthFirstWalker<SparkOperator, SparkOperPlan>(plan));
    }

    /**
     * Secondary key sort optimization is enabled in group + foreach nested situation, like TestAccumlator#testAccumWithSort
     * After calling SecondaryKeyOptimizerUtil.applySecondaryKeySort, the POSort in the POForeach will be deleted in the spark plan.
     * Sort function can be implemented in secondary key sort even though POSort is deleted in the spark plan.
     *
     * @param sparkOperator
     * @throws VisitorException
     */
    @Override
    public void visitSparkOp(SparkOperator sparkOperator) throws VisitorException {
        List<POLocalRearrange> rearranges = PlanHelper.getPhysicalOperators(sparkOperator.physicalPlan, POLocalRearrange.class);
        if (rearranges.isEmpty()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("No POLocalRearranges found in the spark operator" + sparkOperator.getOperatorKey() + ". Skipping secondary key optimization.");
            }
            return;
        }

        /**
         * When ever POLocalRearrange is encountered in the sparkOperator.physicalPlan,
         * the sub-physicalplan between the previousLR(or root) to currentLR is considered as mapPlan(like what
         * we call in mapreduce) and the sub-physicalplan between the POGlobalRearrange(the successor of currentLR) and
         * nextLR(or leaf) is considered as reducePlan(like what we call in mapreduce).  After mapPlan and reducePlan are got,
         * use SecondaryKeyOptimizerUtil.applySecondaryKeySort(mapPlan,reducePlan) to enable secondary key optimization.
         * SecondaryKeyOptimizerUtil.applySecondaryKeySort will remove POSort in the foreach in the reducePlan or
         * change PODistinct to POSortedDistinct in the foreach in the reducePlan.
         */
        for (POLocalRearrange currentLR : rearranges) {
            PhysicalPlan mapPlan = null;
            PhysicalPlan reducePlan = null;
            try {
                mapPlan = getMapPlan(sparkOperator.physicalPlan, currentLR);
            } catch (PlanException e) {
                throw new RuntimeException(e);
            }
            try {
                reducePlan = getReducePlan(sparkOperator.physicalPlan, currentLR);
            } catch (PlanException e) {
                throw new RuntimeException(e);
            }

            // Current code does not enable secondarykey optimization when join case is encounted
            List<PhysicalOperator> rootsOfReducePlan = reducePlan.getRoots();
            if (rootsOfReducePlan.get(0) instanceof POGlobalRearrangeSpark) {
                PhysicalOperator glr = rootsOfReducePlan.get(0);
                List<PhysicalOperator> predecessors = sparkOperator.physicalPlan.getPredecessors(glr);
                if (predecessors != null && predecessors.size() >= 2) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Current code does not enable secondarykey optimization when  join case is encounted");
                    }
                    return;
                }
            }

            if (mapPlan.getOperator(currentLR.getOperatorKey()) == null) {
                // The POLocalRearrange is sub-plan of a POSplit
                mapPlan = PlanHelper.getLocalRearrangePlanFromSplit(mapPlan, currentLR.getOperatorKey());
            }
            SparkSecondaryKeyOptimizerUtil sparkSecondaryKeyOptUtil = new SparkSecondaryKeyOptimizerUtil();
            SecondaryKeyOptimizerUtil.SecondaryKeyOptimizerInfo info = sparkSecondaryKeyOptUtil.applySecondaryKeySort(mapPlan, reducePlan);
            if (info != null) {
                numSortRemoved += info.getNumSortRemoved();
                numDistinctChanged += info.getNumDistinctChanged();
                numUseSecondaryKey += info.getNumUseSecondaryKey();
            }
        }
    }

    /**
     * Find the MRPlan of the physicalPlan which containing currentLR
     * Backward search all the physicalOperators which precede currentLR until the previous POLocalRearrange
     * is found or the root of physicalPlan is found.
     *
     * @param physicalPlan
     * @param currentLR
     * @return
     * @throws VisitorException
     * @throws PlanException
     */
    private PhysicalPlan getMapPlan(PhysicalPlan physicalPlan, POLocalRearrange currentLR) throws VisitorException, PlanException {
        PhysicalPlan mapPlan = new PhysicalPlan();
        mapPlan.addAsRoot(currentLR);
        List<PhysicalOperator> preList = physicalPlan.getPredecessors(currentLR);
        while (true) {
            if (preList == null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("there is nothing to backward search");
                }
                break;
            }
            if (preList.size() != 1) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("the size of predecessor of currentLR should be 1 but now it is not 1,physicalPlan:" + physicalPlan);
                }
                break;
            }
            PhysicalOperator pre = preList.get(0);
            if (pre instanceof POLocalRearrange) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Finishing to find the mapPlan between preLR and currentLR.");
                }
                break;
            }
            mapPlan.addAsRoot(pre);
            preList = physicalPlan.getPredecessors(pre);

        }
        return mapPlan;
    }

    /**
     * Find the ReducePlan of the physicalPlan which containing currentLR
     * Forward search all the physicalOperators which succeed currentLR until the next POLocalRearrange
     * is found or the leave of physicalPlan is found.
     *
     * @param physicalPlan
     * @param currentLR
     * @return
     * @throws PlanException
     */
    private PhysicalPlan getReducePlan(PhysicalPlan physicalPlan, POLocalRearrange currentLR) throws PlanException {
        PhysicalPlan reducePlan = new PhysicalPlan();
        List<PhysicalOperator> succList = physicalPlan.getSuccessors(currentLR);
        while (true) {
            if (succList == null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("there is nothing to forward search");
                }
                break;
            }
            if (succList.size() != 1) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("the size of successors of currentLR should be 1 but now it is not 1,physicalPlan:" + physicalPlan);
                }
                break;
            }
            PhysicalOperator succ = succList.get(0);
            if (succ instanceof POLocalRearrange) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Finishing to find the ReducePlan between currentLR and netxtLR.");
                }
                break;
            }
            reducePlan.addAsLeaf(succ);
            succList = physicalPlan.getSuccessors(succ);
        }
        return reducePlan;
    }

    @Override
    public int getNumSortRemoved() {
        return numSortRemoved;
    }

    @Override
    public int getNumDistinctChanged() {
        return numDistinctChanged;
    }

    @Override
    public int getNumUseSecondaryKey() {
        return numUseSecondaryKey;
    }
}
