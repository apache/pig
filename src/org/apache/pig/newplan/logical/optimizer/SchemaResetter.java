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

public class SchemaResetter extends LogicalRelationalNodesVisitor {

    public SchemaResetter(OperatorPlan plan) throws FrontendException {
        super(plan, new DependencyOrderWalker(plan));
    }

    @Override
    public void visit(LOLoad load) throws FrontendException {
        load.resetSchema();
        load.getSchema();
    }

    @Override
    public void visit(LOFilter filter) throws FrontendException {
        filter.resetSchema();
        FieldSchemaResetter fsResetter = new FieldSchemaResetter(filter.getFilterPlan());
        fsResetter.visit();
        filter.getSchema();
    }
    
    @Override
    public void visit(LOStore store) throws FrontendException {
        store.resetSchema();
        store.getSchema();
    }
    
    @Override
    public void visit(LOJoin join) throws FrontendException {
        join.resetSchema();
        Collection<LogicalExpressionPlan> joinPlans = join.getExpressionPlanValues();
        for (LogicalExpressionPlan joinPlan : joinPlans) {
            FieldSchemaResetter fsResetter = new FieldSchemaResetter(joinPlan);
            fsResetter.visit();
        }
        join.getSchema();
    }
    
    @Override
    public void visit(LOForEach foreach) throws FrontendException {
        foreach.resetSchema();
        OperatorPlan innerPlan = foreach.getInnerPlan();
        PlanWalker newWalker = currentWalker.spawnChildWalker(innerPlan);
        pushWalker(newWalker);
        currentWalker.walk(this);
        popWalker();
        foreach.getSchema();
    }
    
    @Override
    public void visit(LOGenerate gen) throws FrontendException {
        gen.resetSchema();
        List<LogicalExpressionPlan> genPlans = gen.getOutputPlans();
        for (LogicalExpressionPlan genPlan : genPlans) {
            FieldSchemaResetter fsResetter = new FieldSchemaResetter(genPlan);
            fsResetter.visit();
        }
        gen.getSchema();
    }
    
    @Override
    public void visit(LOInnerLoad load) throws FrontendException {
        load.resetSchema();
        load.getProjection().resetFieldSchema();
        load.getSchema();
    }

    @Override
    public void visit(LOCogroup loCogroup) throws FrontendException {
        loCogroup.resetSchema();
        MultiMap<Integer, LogicalExpressionPlan> expPlans = loCogroup.getExpressionPlans();
        for (LogicalExpressionPlan expPlan : expPlans.values()) {
            FieldSchemaResetter fsResetter = new FieldSchemaResetter(expPlan);
            fsResetter.visit();
        }
        loCogroup.getSchema();
    }
    
    @Override
    public void visit(LOSplit loSplit) throws FrontendException {
        loSplit.resetSchema();
        loSplit.getSchema();
    }
    
    @Override
    public void visit(LOSplitOutput loSplitOutput) throws FrontendException {
        loSplitOutput.resetSchema();
        FieldSchemaResetter fsResetter = new FieldSchemaResetter(loSplitOutput.getFilterPlan());
        fsResetter.visit();
        loSplitOutput.getSchema();
    }
    
    @Override
    public void visit(LOUnion loUnion) throws FrontendException {
        loUnion.resetSchema();
        loUnion.getSchema();
    }
    
    @Override
    public void visit(LOSort loSort) throws FrontendException {
        loSort.resetSchema();
        List<LogicalExpressionPlan> sortPlans = loSort.getSortColPlans();
        for (LogicalExpressionPlan sortPlan : sortPlans) {
            FieldSchemaResetter fsResetter = new FieldSchemaResetter(sortPlan);
            fsResetter.visit();
        }
        loSort.getSchema();
    }
    
    @Override
    public void visit(LODistinct loDistinct) throws FrontendException {
        loDistinct.resetSchema();
        loDistinct.getSchema();
    }
    
    @Override
    public void visit(LOLimit loLimit) throws FrontendException {
        loLimit.resetSchema();
        loLimit.getSchema();
    }
    
    @Override
    public void visit(LOCross loCross) throws FrontendException {
        loCross.resetSchema();
        loCross.getSchema();
    }
    
    @Override
    public void visit(LOStream loStream) throws FrontendException {
        loStream.resetSchema();
        loStream.getSchema();
    }
}

class FieldSchemaResetter extends AllSameExpressionVisitor {

    protected FieldSchemaResetter(OperatorPlan p) throws FrontendException {
        super(p, new ReverseDependencyOrderWalker(p));
    }

    @Override
    protected void execute(LogicalExpression op) throws FrontendException {
        op.resetFieldSchema();
        op.getFieldSchema();
    }

}
