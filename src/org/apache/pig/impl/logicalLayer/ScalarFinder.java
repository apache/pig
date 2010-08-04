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

import java.util.HashMap;
import java.util.Map;

import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.VisitorException;

public class ScalarFinder extends LOVisitor {

    Map<LOUserFunc, LogicalPlan> mScalarMap = new HashMap<LOUserFunc, LogicalPlan>();

    /**
     * @param plan
     *            logical plan to query the presence of Scalars
     */
    public ScalarFinder(LogicalPlan plan) {
        super(plan, new DepthFirstWalker<LogicalOperator, LogicalPlan>(plan));
    }

    @Override
    protected void visit(LOUserFunc scalar) throws VisitorException {
        if(scalar.getImplicitReferencedOperator() != null) {
            mScalarMap.put(scalar, mCurrentWalker.getPlan());
        }
    }

    /**
     * @return Map of scalar operators found in the plan
     */
    public Map<LOUserFunc, LogicalPlan> getScalarMap() {
        return mScalarMap;
    }
   
}
