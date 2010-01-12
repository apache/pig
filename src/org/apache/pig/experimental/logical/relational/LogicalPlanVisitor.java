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

package org.apache.pig.experimental.logical.relational;

import org.apache.pig.experimental.plan.OperatorPlan;
import org.apache.pig.experimental.plan.PlanVisitor;
import org.apache.pig.experimental.plan.PlanWalker;

/**
 * A visitor for logical plans.
 */
public abstract class LogicalPlanVisitor extends PlanVisitor {

    protected LogicalPlanVisitor(OperatorPlan plan, PlanWalker walker) {
        super(plan, walker);
        
        if (!(plan instanceof LogicalPlan)) {
            throw new RuntimeException(
                "LogicalPlanVisitor expects to visit logical plans");
        }
    }
    
    protected void visitLOLoad(LOLoad load) {
    }

}
