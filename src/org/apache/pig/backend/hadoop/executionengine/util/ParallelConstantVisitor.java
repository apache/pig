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
package org.apache.pig.backend.hadoop.executionengine.util;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.ConstantExpression;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.VisitorException;

public class ParallelConstantVisitor extends PhyPlanVisitor {

    private int rp;

    private boolean replaced = false;

    public ParallelConstantVisitor(PhysicalPlan plan, int rp) {
        super(plan, new DepthFirstWalker<PhysicalOperator, PhysicalPlan>(
                plan));
        this.rp = rp;
    }

    @Override
    public void visitConstant(ConstantExpression cnst) throws VisitorException {
        if (cnst.getRequestedParallelism() == -1) {
            Object obj = cnst.getValue();
            if (obj instanceof Integer) {
                if (replaced) {
                    // sample job should have only one ConstantExpression
                    throw new VisitorException("Invalid reduce plan: more " +
                            "than one ConstantExpression found in sampling job");
                }
                cnst.setValue(rp);
                cnst.setRequestedParallelism(rp);
                replaced = true;
            }
        }
    }
}
