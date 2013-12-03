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

import java.util.List;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.backend.hadoop.executionengine.util.CombinerOptimizerUtil;
import org.apache.pig.impl.plan.CompilationMessageCollector;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;

/**
 * Optimize tez plans to use the combiner where possible.
 */
public class CombinerOptimizer extends TezOpPlanVisitor {
    private CompilationMessageCollector messageCollector = null;
    private TezOperPlan parentPlan;
    private boolean doMapAgg;

    public CombinerOptimizer(TezOperPlan plan, boolean doMapAgg) {
        this(plan, doMapAgg, new CompilationMessageCollector());
    }

    public CombinerOptimizer(TezOperPlan plan, boolean doMapAgg,
            CompilationMessageCollector messageCollector) {
        super(plan, new DepthFirstWalker<TezOperator, TezOperPlan>(plan));
        this.messageCollector = messageCollector;
        this.doMapAgg = doMapAgg;
        this.parentPlan = plan;
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

        List<TezOperator> predecessors = parentPlan.getPredecessors(to);
        if (predecessors == null) {
            return;
        }

        for (TezOperator from : predecessors) {
            List<POLocalRearrange> rearranges = PlanHelper.getPhysicalOperators(from.plan, POLocalRearrange.class);
            if (rearranges.isEmpty()) {
                continue;
            }

            // Detected the POLocalRearrange -> POPackage pattern. Let's add
            // combiner if possible.
            PhysicalPlan combinePlan = to.inEdges.get(from.getOperatorKey()).combinePlan;
            // TODO: Right now, CombinerOptimzerUtil doesn't handle a single map
            // plan with multiple POLocalRearrange leaves. i.e. SPLIT + multiple
            // GROUP BY with different keys.
            CombinerOptimizerUtil.addCombiner(from.plan, to.plan, combinePlan, messageCollector, doMapAgg);

            //Replace POLocalRearrange with POLocalRearrangeTez
            if (!combinePlan.isEmpty()) {
                POLocalRearrange lr = (POLocalRearrange) combinePlan.getLeaves().get(0);
                POLocalRearrangeTez lrt = new POLocalRearrangeTez(lr);
                lrt.setOutputKey(to.getOperatorKey().toString());
                try {
                    combinePlan.replace(lr, lrt);
                } catch (PlanException e) {
                    throw new VisitorException(e);
                }
            }
        }
    }
}

