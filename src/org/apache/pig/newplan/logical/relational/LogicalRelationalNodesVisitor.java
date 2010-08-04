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

import java.io.IOException;

import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanVisitor;
import org.apache.pig.newplan.PlanWalker;

/**
 * A visitor for logical plans.
 */
public abstract class LogicalRelationalNodesVisitor extends PlanVisitor {

    protected LogicalRelationalNodesVisitor(OperatorPlan plan, PlanWalker walker) {
        super(plan, walker);
        /*
        Iterator<Operator> iter = plan.getOperators();
        while(iter.hasNext()) {
            if (!(iter.next() instanceof LogicalRelationalOperator)) {
                throw new RuntimeException("LogicalPlanVisitor can only visit logical plan");
            }
        }*/
    }
    
    public void visit(LOLoad load) throws IOException {
    }

    public void visit(LOFilter filter) throws IOException {
    }
    
    public void visit(LOStore store) throws IOException {
    }
    
    public void visit(LOJoin join) throws IOException {
    }
    
    public void visit(LOForEach foreach) throws IOException {
    }
    
    public void visit(LOGenerate gen) throws IOException {
    }
    
    public void visit(LOInnerLoad load) throws IOException {
    }

    public void visit(LOCogroup loCogroup) throws IOException {
    }
    
    public void visit(LOSplit loSplit) throws IOException {
    }
    
    public void visit(LOSplitOutput loSplitOutput) throws IOException {
    }
    
    public void visit(LOUnion loUnion) throws IOException {
    }
    
    public void visit(LOSort loSort) throws IOException {
    }
    
    public void visit(LODistinct loDistinct) throws IOException {
    }
    
    public void visit(LOLimit loLimit) throws IOException {
    }
    
    public void visit(LOCross loCross) throws IOException {
    }
    
    public void visit(LOStream loStream) throws IOException {
    }
}
