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

package org.apache.pig.backend.hadoop.executionengine.tez.plan.optimizer;

import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezOpPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezOperPlan;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezOperator;
import org.apache.pig.backend.hadoop.executionengine.util.AccumulatorOptimizerUtil;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.VisitorException;

/**
 * A visitor to optimize plans that determines if a vertex plan can run in
 * accumulative mode.
 */
public class AccumulatorOptimizer extends TezOpPlanVisitor {

    public AccumulatorOptimizer(TezOperPlan plan) {
        super(plan, new DepthFirstWalker<TezOperator, TezOperPlan>(plan));
    }

    @Override
    public void visitTezOp(TezOperator tezOp) throws VisitorException {
        AccumulatorOptimizerUtil.addAccumulator(tezOp.plan, tezOp.plan.getRoots());
    }
}
