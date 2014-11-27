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

import java.util.List;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezOpPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezOperPlan;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezOperator;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.operator.POLocalRearrangeTez;
import org.apache.pig.backend.hadoop.executionengine.util.CombinerOptimizerUtil;
import org.apache.pig.impl.plan.CompilationMessageCollector;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.VisitorException;

/**
 * Optimize tez plans to use the combiner where possible.
 */
public class CombinerOptimizer extends TezOpPlanVisitor {
    private CompilationMessageCollector messageCollector = null;
    private boolean doMapAgg;

    public CombinerOptimizer(TezOperPlan plan, boolean doMapAgg) {
        this(plan, doMapAgg, new CompilationMessageCollector());
    }

    public CombinerOptimizer(TezOperPlan plan, boolean doMapAgg,
            CompilationMessageCollector messageCollector) {
        super(plan, new DepthFirstWalker<TezOperator, TezOperPlan>(plan));
        this.messageCollector = messageCollector;
        this.doMapAgg = doMapAgg;
    }

    public CompilationMessageCollector getMessageCollector() {
        return messageCollector;
    }

    @Override
    public void visitTezOp(TezOperator to) throws VisitorException {
        List<POPackage> packages = PlanHelper.getPhysicalOperators(to.plan, POPackage.class);
        if (packages.isEmpty()) {
            return;
        }

        List<TezOperator> predecessors = mPlan.getPredecessors(to);
        if (predecessors == null) {
            return;
        }
        if (to.isCogroup()) {
            return;
        }

        for (TezOperator from : predecessors) {
            List<POLocalRearrangeTez> rearranges = PlanHelper.getPhysicalOperators(from.plan, POLocalRearrangeTez.class);
            if (rearranges.isEmpty()) {
                continue;
            }

            POLocalRearrangeTez connectingLR = null;
            PhysicalPlan rearrangePlan = from.plan;
            for (POLocalRearrangeTez lr : rearranges) {
                if (lr.getOutputKey().equals(to.getOperatorKey().toString())) {
                    connectingLR = lr;
                    break;
                }
            }

            if (from.plan.getOperator(connectingLR.getOperatorKey()) == null) {
                // The POLocalRearrange is sub-plan of a POSplit
                rearrangePlan = PlanHelper.getLocalRearrangePlanFromSplit(from.plan, connectingLR.getOperatorKey());
            }

            // Detected the POLocalRearrange -> POPackage pattern. Let's add
            // combiner if possible.
            PhysicalPlan combinePlan = to.inEdges.get(from.getOperatorKey()).combinePlan;
            CombinerOptimizerUtil.addCombiner(rearrangePlan, to.plan, combinePlan, messageCollector, doMapAgg);

            if(!combinePlan.isEmpty()) {
                // Override the requested parallelism for intermediate reducers
                // when combiners are involved so that there are more tasks doing the combine
                from.setOverrideIntermediateParallelism(true);
            }

        }
    }

}

