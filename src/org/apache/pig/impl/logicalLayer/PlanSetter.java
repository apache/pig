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

import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.VisitorException;

/**
 * A visitor to set plans correctly inside logical operators.  When a logical
 * operator is constructed, it is passed the plan value that is currently
 * being used.  However, once a single plan is stitched together, these
 * references are no longer useful.  However, there are a number of places
 * that it is useful to be able to refer to the correct plan.  So this visitor
 * walks the final plan and sets the mCurrentWalker.getPlan() values in LogicalOperator
 * appropriately.  Note that some operators can be in multiple plans (such as
 * inside a foreach).  In this case the mCurrentWalker.getPlan() value will be for one of the
 * plans.  Which one is not guaranteed.
 */
public class PlanSetter extends LOVisitor {

    public PlanSetter(LogicalPlan plan) {
        super(plan, new DepthFirstWalker<LogicalOperator, LogicalPlan>(plan));
    }

    public void visit(LOAdd op) throws VisitorException {
        op.setPlan(mCurrentWalker.getPlan());
    }

    public void visit(LOAnd op) throws VisitorException {
        op.setPlan(mCurrentWalker.getPlan());
    }

    public void visit(LOBinCond op) throws VisitorException {
        op.setPlan(mCurrentWalker.getPlan());
    }

    public void visit(LOCast op) throws VisitorException {
        op.setPlan(mCurrentWalker.getPlan());
    }

    public void visit(LOCogroup op) throws VisitorException {
        op.setPlan(mCurrentWalker.getPlan());
        super.visit(op);
    }
    
    public void visit(LOFRJoin op) throws VisitorException {
        op.setPlan(mCurrentWalker.getPlan());
        super.visit(op);
    }

    public void visit(LOConst op) throws VisitorException {
        op.setPlan(mCurrentWalker.getPlan());
    }

    public void visit(LOCross op) throws VisitorException {
        op.setPlan(mCurrentWalker.getPlan());
    }

    public void visit(LODistinct op) throws VisitorException {
        op.setPlan(mCurrentWalker.getPlan());
    }

    public void visit(LODivide op) throws VisitorException {
        op.setPlan(mCurrentWalker.getPlan());
    }

    public void visit(LOEqual op) throws VisitorException {
        op.setPlan(mCurrentWalker.getPlan());
    }

    public void visit(LOFilter op) throws VisitorException {
        op.setPlan(mCurrentWalker.getPlan());
        super.visit(op);
    }

    public void visit(LOForEach op) throws VisitorException {
        op.setPlan(mCurrentWalker.getPlan());
        super.visit(op);
    }

    public void visit(LOGreaterThan op) throws VisitorException {
        op.setPlan(mCurrentWalker.getPlan());
    }

    public void visit(LOGreaterThanEqual op) throws VisitorException {
        op.setPlan(mCurrentWalker.getPlan());
    }

    public void visit(LOIsNull op) throws VisitorException {
        op.setPlan(mCurrentWalker.getPlan());
    }

    public void visit(LOLesserThan op) throws VisitorException {
        op.setPlan(mCurrentWalker.getPlan());
    }

    public void visit(LOLesserThanEqual op) throws VisitorException {
        op.setPlan(mCurrentWalker.getPlan());
    }

    public void visit(LOLimit op) throws VisitorException {
        op.setPlan(mCurrentWalker.getPlan());
    }

    public void visit(LOLoad op) throws VisitorException {
        op.setPlan(mCurrentWalker.getPlan());
    }

    public void visit(LOMapLookup op) throws VisitorException {
        op.setPlan(mCurrentWalker.getPlan());
    }

    public void visit(LOMod op) throws VisitorException {
        op.setPlan(mCurrentWalker.getPlan());
    }

    public void visit(LOMultiply op) throws VisitorException {
        op.setPlan(mCurrentWalker.getPlan());
    }

    public void visit(LONegative op) throws VisitorException {
        op.setPlan(mCurrentWalker.getPlan());
    }

    public void visit(LONot op) throws VisitorException {
        op.setPlan(mCurrentWalker.getPlan());
    }

    public void visit(LONotEqual op) throws VisitorException {
        op.setPlan(mCurrentWalker.getPlan());
    }

    public void visit(LOOr op) throws VisitorException {
        op.setPlan(mCurrentWalker.getPlan());
    }

    public void visit(LOProject op) throws VisitorException {
        op.setPlan(mCurrentWalker.getPlan());
    }

    public void visit(LORegexp op) throws VisitorException {
        op.setPlan(mCurrentWalker.getPlan());
    }

    public void visit(LOSort op) throws VisitorException {
        op.setPlan(mCurrentWalker.getPlan());
        super.visit(op);
    }

    public void visit(LOSplit op) throws VisitorException {
        op.setPlan(mCurrentWalker.getPlan());
        super.visit(op);
    }

    public void visit(LOSplitOutput op) throws VisitorException {
        op.setPlan(mCurrentWalker.getPlan());
        super.visit(op);
    }

    public void visit(LOStore op) throws VisitorException {
        op.setPlan(mCurrentWalker.getPlan());
    }

    public void visit(LOSubtract op) throws VisitorException {
        op.setPlan(mCurrentWalker.getPlan());
    }

    public void visit(LOUnion op) throws VisitorException {
        op.setPlan(mCurrentWalker.getPlan());
    }

    public void visit(LOUserFunc op) throws VisitorException {
        op.setPlan(mCurrentWalker.getPlan());
    }
    
    public void visit(LOStream op) throws VisitorException {
        op.setPlan(mCurrentWalker.getPlan());
    }

}

