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
package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer;

import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROpPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.util.CombinerOptimizerUtil;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.plan.CompilationMessageCollector;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.VisitorException;

/**
 * Optimize map reduce plans to use the combiner where possible.
 */
public class CombinerOptimizer extends MROpPlanVisitor {
    private CompilationMessageCollector messageCollector = null;
    private boolean doMapAgg;

    public CombinerOptimizer(MROperPlan plan, boolean doMapAgg) {
        this(plan, doMapAgg, new CompilationMessageCollector());
    }

    public CombinerOptimizer(MROperPlan plan, boolean doMapAgg,
            CompilationMessageCollector messageCollector) {
        super(plan, new DepthFirstWalker<MapReduceOper, MROperPlan>(plan));
        this.messageCollector = messageCollector;
        this.doMapAgg = doMapAgg;
    }

    public CompilationMessageCollector getMessageCollector() {
        return messageCollector;
    }

    @Override
    public void visitMROp(MapReduceOper mr) throws VisitorException {
        CombinerOptimizerUtil.addCombiner(mr.mapPlan, mr.reducePlan, mr.combinePlan, messageCollector, doMapAgg);
    }
}
