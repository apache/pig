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

package org.apache.pig.newplan.logical.relational;

import java.util.Iterator;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanVisitor;
import org.apache.pig.newplan.PlanWalker;

/**
 * A visitor for logical plans.
 */
public abstract class LogicalRelationalNodesVisitor extends PlanVisitor {

    protected LogicalRelationalNodesVisitor(OperatorPlan plan, PlanWalker walker) throws FrontendException {
        super(plan, walker);

        Iterator<Operator> iter = plan.getOperators();
        while(iter.hasNext()) {
            if (!(iter.next() instanceof LogicalRelationalOperator)) {
                throw new FrontendException("LogicalPlanVisitor can only visit logical plan", 2240);
            }
        }
    }

    public void visit(LOLoad load) throws FrontendException {
    }

    public void visit(LOFilter filter) throws FrontendException {
    }

    public void visit(LOStore store) throws FrontendException {
    }

    public void visit(LOJoin join) throws FrontendException {
    }

    public void visit(LOForEach foreach) throws FrontendException {
    }

    public void visit(LOGenerate gen) throws FrontendException {
    }

    public void visit(LOInnerLoad load) throws FrontendException {
    }

    public void visit(LOCube cube) throws FrontendException {
    }

    public void visit(LOCogroup loCogroup) throws FrontendException {
    }

    public void visit(LOSplit loSplit) throws FrontendException {
    }

    public void visit(LOSplitOutput loSplitOutput) throws FrontendException {
    }

    public void visit(LOUnion loUnion) throws FrontendException {
    }

    public void visit(LOSort loSort) throws FrontendException {
    }

    public void visit(LORank loRank) throws FrontendException{
    }

    public void visit(LODistinct loDistinct) throws FrontendException {
    }

    public void visit(LOLimit loLimit) throws FrontendException {
    }

    public void visit(LOCross loCross) throws FrontendException {
    }

    public void visit(LOStream loStream) throws FrontendException {
    }

    public void visit(LONative nativeMR) throws FrontendException{
    }
}
