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

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POGlobalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkOpPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkOperPlan;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkOperator;
import org.apache.pig.backend.hadoop.executionengine.util.AccumulatorOptimizerUtil;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.VisitorException;

import java.util.List;

/**
 * A visitor to optimize plans that determines if a vertex plan can run in
 * accumulative mode.
 */
public class AccumulatorOptimizer extends SparkOpPlanVisitor {

    public AccumulatorOptimizer(SparkOperPlan plan) {
        super(plan, new DepthFirstWalker<SparkOperator, SparkOperPlan>(plan));
    }

    @Override
    public void visitSparkOp(SparkOperator sparkOperator) throws
            VisitorException {
        PhysicalPlan plan = sparkOperator.physicalPlan;
        List<PhysicalOperator> pos = plan.getRoots();
        if (pos == null || pos.size() == 0) {
            return;
        }

        List<POGlobalRearrange> glrs = PlanHelper.getPhysicalOperators(plan,
                POGlobalRearrange.class);

        for (POGlobalRearrange glr : glrs) {
            List<PhysicalOperator> successors = plan.getSuccessors(glr);
            AccumulatorOptimizerUtil.addAccumulator(plan, successors);
        }
    }
}