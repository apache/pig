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

package org.apache.pig.newplan.logical.optimizer;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanWalker;
import org.apache.pig.newplan.logical.relational.LOCogroup;
import org.apache.pig.newplan.logical.relational.LOCross;
import org.apache.pig.newplan.logical.relational.LOCube;
import org.apache.pig.newplan.logical.relational.LODistinct;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOJoin;
import org.apache.pig.newplan.logical.relational.LOLimit;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LONative;
import org.apache.pig.newplan.logical.relational.LORank;
import org.apache.pig.newplan.logical.relational.LOSort;
import org.apache.pig.newplan.logical.relational.LOSplit;
import org.apache.pig.newplan.logical.relational.LOSplitOutput;
import org.apache.pig.newplan.logical.relational.LOStore;
import org.apache.pig.newplan.logical.relational.LOStream;
import org.apache.pig.newplan.logical.relational.LOUnion;
import org.apache.pig.newplan.logical.relational.LogicalRelationalNodesVisitor;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;

/**
 * A visitor that walks the logical plan and calls the same method on every
 * type of node.  Subclasses can extend this and implement the execute
 * method, and this method will be called on every node in the graph.
 *
 */
public abstract class AllSameRalationalNodesVisitor extends LogicalRelationalNodesVisitor {

    /**
     * @param plan OperatorPlan to visit
     * @param walker Walker to use to visit the plan
     */
    public AllSameRalationalNodesVisitor(OperatorPlan plan, PlanWalker walker) throws FrontendException {
        super(plan, walker);
    }

    /**
     * Method to call on every node in the logical plan.
     * @param op Node that is currently being visited.
     */
    abstract protected void execute(LogicalRelationalOperator op) throws FrontendException;

    @Override
    public void visit(LOFilter filter) throws FrontendException {
        execute(filter);
    }

    @Override
    public void visit(LOJoin join) throws FrontendException {
        execute(join);
    }

    @Override
    public void visit(LOCogroup cg) throws FrontendException {
        execute(cg);
    }

    @Override
    public void visit(LOLoad load) throws FrontendException {
        execute(load);
    }

    @Override
    public void visit(LOStore store) throws FrontendException {
        execute(store);
    }

    @Override
    public void visit(LOForEach foreach) throws FrontendException {
        execute(foreach);
    }

    @Override
    public void visit(LOSplit split) throws FrontendException {
        execute(split);
    }

    @Override
    public void visit(LOSplitOutput splitOutput) throws FrontendException {
        execute(splitOutput);
    }

    @Override
    public void visit(LOUnion union) throws FrontendException {
        execute(union);
    }

    @Override
    public void visit(LOSort sort) throws FrontendException {
        execute(sort);
    }

    @Override
    public void visit(LORank rank) throws FrontendException {
        execute(rank);
    }

    @Override
    public void visit(LODistinct distinct) throws FrontendException {
        execute(distinct);
    }

    @Override
    public void visit(LOCross cross) throws FrontendException {
        execute(cross);
    }

    @Override
    public void visit(LOStream stream) throws FrontendException {
        execute(stream);
    }
    
    @Override
    public void visit(LOLimit limit) throws FrontendException {
        execute(limit);
    }
    
    @Override
    public void visit(LONative loNative) throws FrontendException {
        execute(loNative);
    }
    
    @Override
    public void visit(LOCube cube) throws FrontendException {
        execute(cube);
    }
}
