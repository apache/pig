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

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanWalker;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.LogicalExpressionVisitor;
import org.apache.pig.newplan.logical.relational.LOCogroup;
import org.apache.pig.newplan.logical.relational.LOCube;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOGenerate;
import org.apache.pig.newplan.logical.relational.LOInnerLoad;
import org.apache.pig.newplan.logical.relational.LOJoin;
import org.apache.pig.newplan.logical.relational.LOLimit;
import org.apache.pig.newplan.logical.relational.LOSort;
import org.apache.pig.newplan.logical.relational.LOSplitOutput;
import org.apache.pig.newplan.logical.relational.LogicalRelationalNodesVisitor;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;

/**
 * A visitor that walks a logical plan and then applies a given
 * LogicalExpressionVisitor to all expressions it encounters.
 *
 */
public abstract class AllExpressionVisitor extends LogicalRelationalNodesVisitor {
    
    protected LogicalRelationalOperator currentOp;

    /**
     * @param plan LogicalPlan to visit
     * @param walker Walker to use to visit the plan.
     */
    public AllExpressionVisitor(OperatorPlan plan,
                                PlanWalker walker) throws FrontendException {
        super(plan, walker);
    }
    
    /**
     * Get a new instance of the expression visitor to apply to 
     * a given expression.
     * @param expr LogicalExpressionPlan that will be visited
     * @return a new LogicalExpressionVisitor for that expression
     */
    abstract protected LogicalExpressionVisitor getVisitor(LogicalExpressionPlan expr) throws FrontendException;
    
    private void visitAll(Collection<LogicalExpressionPlan> lexpPlans) throws FrontendException {
	for (LogicalExpressionPlan plan : lexpPlans) {
	    LogicalExpressionVisitor v = getVisitor(plan);
	    v.visit();
	}
    }
    
    @Override
    public void visit(LOFilter filter) throws FrontendException {
        currentOp = filter;
        LogicalExpressionVisitor v = getVisitor(filter.getFilterPlan());
        v.visit();
    }
    
    @Override
    public void visit(LOLimit limit) throws FrontendException {
        currentOp = limit;
        if (limit.getLimitPlan() != null) {
            LogicalExpressionVisitor v = getVisitor(limit.getLimitPlan());
            v.visit();
        }
    }
 
    @Override
    public void visit(LOJoin join) throws FrontendException {
        currentOp = join;
        visitAll(join.getExpressionPlanValues());
    }
    
    @Override
    public void visit(LOCube cu) throws FrontendException {
	currentOp = cu;
	MultiMap<Integer, LogicalExpressionPlan> expressionPlans = cu.getExpressionPlans();
	for (Integer key : expressionPlans.keySet()) {
	    visitAll(expressionPlans.get(key));
        }
    }
    
    @Override
    public void visit(LOCogroup cg) throws FrontendException {
        currentOp = cg;
        MultiMap<Integer, LogicalExpressionPlan> expressionPlans = cg.getExpressionPlans();
        for( Integer key : expressionPlans.keySet() ) {
            visitAll(expressionPlans.get(key));
        }
    }
    
    @Override
    public void visit(LOForEach foreach) throws FrontendException {
        currentOp = foreach;
        // We have an Inner OperatorPlan in ForEach, so we go ahead
        // and work on that plan
        OperatorPlan innerPlan = foreach.getInnerPlan();
        PlanWalker newWalker = currentWalker.spawnChildWalker(innerPlan);
        pushWalker(newWalker);
        currentWalker.walk(this);
        popWalker();
    }
    
    @Override
    public void visit(LOGenerate gen ) throws FrontendException {
        currentOp = gen;
        visitAll(gen.getOutputPlans());
    }
    
    @Override
    public void visit(LOInnerLoad load) throws FrontendException {
        // the expression in LOInnerLoad contains info relative from LOForEach
        // so use LOForeach as currentOp
        currentOp = load.getLOForEach();
        LogicalExpressionPlan exp = (LogicalExpressionPlan)load.getProjection().getPlan();
       
        LogicalExpressionVisitor v = getVisitor(exp);
        v.visit();       
    }
    
    @Override
    public void visit(LOSplitOutput splitOutput) throws FrontendException {
        currentOp = splitOutput;
        LogicalExpressionVisitor v = getVisitor(splitOutput.getFilterPlan());
        v.visit();
    }
    
    @Override
    public void visit(LOSort sort) throws FrontendException {
        currentOp = sort;
        visitAll(sort.getSortColPlans());
    }
}
