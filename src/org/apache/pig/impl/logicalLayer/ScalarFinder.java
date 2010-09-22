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
import org.apache.pig.impl.util.Pair;

public class ScalarFinder extends LOVisitor {

    // We need to find top level logical operator associated with the scalar,
    // visiting nested plan will not change currentOp; inOp is to make sure
    // we only track top level operator
    private LogicalOperator currentOp;
    
    private boolean inOp = false;
    Map<LOUserFunc, Pair<LogicalPlan, LogicalOperator>> mScalarMap = new HashMap<LOUserFunc, Pair<LogicalPlan, LogicalOperator>>();

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
            mScalarMap.put(scalar, new Pair<LogicalPlan, LogicalOperator>(mCurrentWalker.getPlan(), currentOp));
        }
    }
    
    @Override
    protected void visit(LOFilter op) throws VisitorException {
        if (!inOp) {
            inOp = true;
            currentOp = op;
        }
        super.visit(op);
        inOp = false;
    }

    @Override
    protected void visit(LOForEach op) throws VisitorException {
        if (!inOp) {
            inOp = true;
            currentOp = op;
        }
        super.visit(op);
        inOp = false;
    }
    
    @Override
    protected void visit(LOSplitOutput op) throws VisitorException {
        if (!inOp) {
            inOp = true;
            currentOp = op;
        }
        super.visit(op);
        inOp = false;
    }
    
    @Override
    protected void visit(LOCogroup op) throws VisitorException {
        if (!inOp) {
            inOp = true;
            currentOp = op;
        }
        super.visit(op);
        inOp = false;
    }

    @Override
    protected void visit(LOJoin op) throws VisitorException {
        if (!inOp) {
            inOp = true;
            currentOp = op;
        }
        super.visit(op);
        inOp = false;
    }

    /**
     * @return Map of scalar operators found in the plan
     */
    public Map<LOUserFunc, Pair<LogicalPlan, LogicalOperator>> getScalarMap() {
        return mScalarMap;
    }
   
}
