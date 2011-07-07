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

import java.util.Collection;
import java.util.List;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.newplan.DependencyOrderWalker;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanWalker;
import org.apache.pig.newplan.ReverseDependencyOrderWalker;
import org.apache.pig.newplan.logical.expression.AllSameExpressionVisitor;
import org.apache.pig.newplan.logical.expression.LogicalExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.relational.LOCogroup;
import org.apache.pig.newplan.logical.relational.LOCross;
import org.apache.pig.newplan.logical.relational.LODistinct;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOGenerate;
import org.apache.pig.newplan.logical.relational.LOInnerLoad;
import org.apache.pig.newplan.logical.relational.LOJoin;
import org.apache.pig.newplan.logical.relational.LOLimit;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LOSort;
import org.apache.pig.newplan.logical.relational.LOSplit;
import org.apache.pig.newplan.logical.relational.LOSplitOutput;
import org.apache.pig.newplan.logical.relational.LOStore;
import org.apache.pig.newplan.logical.relational.LOStream;
import org.apache.pig.newplan.logical.relational.LOUnion;
import org.apache.pig.newplan.logical.relational.LogicalRelationalNodesVisitor;

public class UidResetter extends LogicalRelationalNodesVisitor {

    public UidResetter(OperatorPlan plan) throws FrontendException {
        super(plan, new DependencyOrderWalker(plan));
    }

    @Override
    public void visit(LOLoad load) throws FrontendException {
        load.resetUid();
    }

    @Override
    public void visit(LOFilter filter) throws FrontendException {
        filter.resetUid();
        ExpressionUidResetter uidResetter = new ExpressionUidResetter(filter.getFilterPlan());
        uidResetter.visit();
    }
    
    @Override
    public void visit(LOStore store) throws FrontendException {
        store.resetUid();
    }
    
    @Override
    public void visit(LOJoin join) throws FrontendException {
        join.resetUid();
        Collection<LogicalExpressionPlan> joinPlans = join.getExpressionPlanValues();
        for (LogicalExpressionPlan joinPlan : joinPlans) {
            ExpressionUidResetter fsResetter = new ExpressionUidResetter(joinPlan);
            fsResetter.visit();
        }
    }
    
    @Override
    public void visit(LOForEach foreach) throws FrontendException {
        foreach.resetUid();
        OperatorPlan innerPlan = foreach.getInnerPlan();
        PlanWalker newWalker = currentWalker.spawnChildWalker(innerPlan);
        pushWalker(newWalker);
        currentWalker.walk(this);
        popWalker();
    }
    
    @Override
    public void visit(LOGenerate gen) throws FrontendException {
        gen.resetUid();
        List<LogicalExpressionPlan> genPlans = gen.getOutputPlans();
        for (LogicalExpressionPlan genPlan : genPlans) {
            ExpressionUidResetter fsResetter = new ExpressionUidResetter(genPlan);
            fsResetter.visit();
        }
    }
    
    @Override
    public void visit(LOInnerLoad load) throws FrontendException {
        load.resetUid();
        load.getProjection().resetUid();
    }

    @Override
    public void visit(LOCogroup loCogroup) throws FrontendException {
        loCogroup.resetUid();
        MultiMap<Integer, LogicalExpressionPlan> expPlans = loCogroup.getExpressionPlans();
        for (LogicalExpressionPlan expPlan : expPlans.values()) {
            ExpressionUidResetter uidResetter = new ExpressionUidResetter(expPlan);
            uidResetter.visit();
        }
    }
    
    @Override
    public void visit(LOSplit loSplit) throws FrontendException {
        loSplit.resetUid();
    }
    
    @Override
    public void visit(LOSplitOutput loSplitOutput) throws FrontendException {
        loSplitOutput.resetUid();
        ExpressionUidResetter uidResetter = new ExpressionUidResetter(loSplitOutput.getFilterPlan());
        uidResetter.visit();
    }
    
    @Override
    public void visit(LOUnion loUnion) throws FrontendException {
        loUnion.resetUid();
    }
    
    @Override
    public void visit(LOSort loSort) throws FrontendException {
        loSort.resetUid();
        List<LogicalExpressionPlan> sortPlans = loSort.getSortColPlans();
        for (LogicalExpressionPlan sortPlan : sortPlans) {
            ExpressionUidResetter uidResetter = new ExpressionUidResetter(sortPlan);
            uidResetter.visit();
        }
    }
    
    @Override
    public void visit(LODistinct loDistinct) throws FrontendException {
        loDistinct.resetUid();
    }
    
    @Override
    public void visit(LOLimit loLimit) throws FrontendException {
        loLimit.resetUid();
        if (loLimit.getLimitPlan() != null) {
            ExpressionUidResetter uidResetter = new ExpressionUidResetter(
                    loLimit.getLimitPlan());
            uidResetter.visit();
    }
    }
    
    @Override
    public void visit(LOCross loCross) throws FrontendException {
        loCross.resetUid();
    }
    
    @Override
    public void visit(LOStream loStream) throws FrontendException {
        loStream.resetUid();
    }
}

class ExpressionUidResetter extends AllSameExpressionVisitor {
    protected ExpressionUidResetter(OperatorPlan p) throws FrontendException {
        super(p, new ReverseDependencyOrderWalker(p));
    }

    @Override
    protected void execute(LogicalExpression op) throws FrontendException {
        op.resetUid();
    }
}
