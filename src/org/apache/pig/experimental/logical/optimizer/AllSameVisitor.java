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

package org.apache.pig.experimental.logical.optimizer;

import java.io.IOException;

import org.apache.pig.experimental.logical.relational.LOFilter;
import org.apache.pig.experimental.logical.relational.LOForEach;
import org.apache.pig.experimental.logical.relational.LOJoin;
import org.apache.pig.experimental.logical.relational.LOLoad;
import org.apache.pig.experimental.logical.relational.LOStore;
import org.apache.pig.experimental.logical.relational.LogicalPlanVisitor;
import org.apache.pig.experimental.logical.relational.LogicalRelationalOperator;
import org.apache.pig.experimental.plan.OperatorPlan;
import org.apache.pig.experimental.plan.PlanWalker;

/**
 * A visitor that walks the logical plan and calls the same method on every
 * type of node.  Subclasses can extend this and implement the execute
 * method, and this method will be called on every node in the graph.
 *
 */
public abstract class AllSameVisitor extends LogicalPlanVisitor {

    /**
     * @param plan OperatorPlan to visit
     * @param walker Walker to use to visit the plan
     */
    public AllSameVisitor(OperatorPlan plan, PlanWalker walker) {
        super(plan, walker);
    }
    
    /**
     * Method to call on every node in the logical plan.
     * @param op Node that is currently being visited.
     */
    abstract protected void execute(LogicalRelationalOperator op) throws IOException;
    
    @Override
    public void visitLOFilter(LOFilter filter) throws IOException {
        execute(filter);
    }

    @Override
    public void visitLOJoin(LOJoin join) throws IOException {
        execute(join);
    }

    @Override
    public void visitLOLoad(LOLoad load) throws IOException {
        execute(load);
    }
    
    @Override
    public void visitLOStore(LOStore store) throws IOException {
        execute(store);
    }
    
    public void visitLOForEach(LOForEach foreach) throws IOException {
        execute(foreach);
    }
}
