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
package org.apache.pig.backend.hadoop.executionengine.tez;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.hadoop.executionengine.optimizer.SecondaryKeyOptimizer;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.backend.hadoop.executionengine.util.SecondaryKeyOptimizerUtil;
import org.apache.pig.backend.hadoop.executionengine.util.SecondaryKeyOptimizerUtil.SecondaryKeyOptimizerInfo;
import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.VisitorException;

@InterfaceAudience.Private
public class SecondaryKeyOptimizerTez extends TezOpPlanVisitor implements SecondaryKeyOptimizer {

    private static Log log = LogFactory.getLog(SecondaryKeyOptimizerTez.class);

    private int numSortRemoved = 0;
    private int numDistinctChanged = 0;
    private int numUseSecondaryKey = 0;

    public SecondaryKeyOptimizerTez(TezOperPlan plan) {
        super(plan, new DependencyOrderWalker<TezOperator, TezOperPlan>(plan));
    }

    @Override
    public void visitTezOp(TezOperator to) throws VisitorException {

        List<TezOperator> predecessors = mPlan.getPredecessors(to);
        if (predecessors == null) {
            return;
        }

        // Current code does not handle more than one predecessors
        // even though it is possible. The problem is when we 
        // process the first predecessor, we remove the foreach inner
        // operators from the reduce side, and the second predecessor
        // cannot see them
        if (predecessors.size()>1) {
            return;
        }
        TezOperator from = predecessors.get(0);

        List<POLocalRearrangeTez> rearranges = PlanHelper.getPhysicalOperators(from.plan, POLocalRearrangeTez.class);
        if (rearranges.isEmpty()) {
            return;
        }

        POLocalRearrangeTez connectingLR = null;
        PhysicalPlan rearrangePlan = from.plan;
        for (POLocalRearrangeTez lr : rearranges) {
            if (lr.getOutputKey().equals(to.getOperatorKey().toString())) {
                connectingLR = lr;
                break;
            }
        }

        if (connectingLR == null) {
            return;
        }

        // Detected the POLocalRearrange -> POPackage pattern
        TezEdgeDescriptor inEdge = to.inEdges.get(from.getOperatorKey());
        // Only optimize for Cogroup case
        if (from.isGlobalSort()) {
            return;
        }

        // If there is a custom partitioner do not do secondary key optimization.
        if (inEdge.partitionerClass != null) {
            return;
        }

        if (from.plan.getOperator(connectingLR.getOperatorKey()) == null) {
            // The POLocalRearrange is sub-plan of a POSplit
            rearrangePlan = PlanHelper.getLocalRearrangePlanFromSplit(from.plan, connectingLR.getOperatorKey());
        }

        SecondaryKeyOptimizerInfo info = SecondaryKeyOptimizerUtil.applySecondaryKeySort(rearrangePlan, to.plan);
        if (info != null) {
            numSortRemoved += info.getNumSortRemoved();
            numDistinctChanged += info.getNumDistinctChanged();
            numUseSecondaryKey += info.getNumUseSecondaryKey();
            if (info.isUseSecondaryKey()) {
                // Set it on the receiving vertex and the connecting edge.
                to.setUseSecondaryKey(true);
                inEdge.setUseSecondaryKey(true);
                inEdge.setSecondarySortOrder(info.getSecondarySortOrder());
                log.info("Using Secondary Key Optimization in the edge between vertex - "
                        + from.getOperatorKey()
                        + " and vertex - "
                        + to.getOperatorKey());
            }
        }
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
