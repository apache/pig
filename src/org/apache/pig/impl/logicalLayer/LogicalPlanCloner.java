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
package org.apache.pig.impl.logicalLayer;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.PlanWalker;
import org.apache.pig.impl.plan.VisitorException;

/**
 * LogicalPlanCloner provides the only mechanism of cloning a logical plan and hence the
 * the logical operators in the plan.
 */
public class LogicalPlanCloner {
    
    private LogicalPlan mPlan;

    /**
     * @param plan logical plan to be cloned
     */
    public LogicalPlanCloner(LogicalPlan plan) {
        mPlan = plan;
    }
    
    public LogicalPlan getClonedPlan() throws CloneNotSupportedException {
        LogicalPlanCloneHelper lpCloneHelper = new LogicalPlanCloneHelper(mPlan, new HashMap<LogicalOperator, LogicalOperator>());
        LogicalPlan clonedPlan = lpCloneHelper.getClonedPlan();
        LogicalPlanCloneHelper.resetState();
        return clonedPlan;
    }
    
}
