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
package org.apache.pig.newplan.logical;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.pig.impl.logicalLayer.ExpressionOperator;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.LODistinct;
import org.apache.pig.impl.logicalLayer.LOFilter;
import org.apache.pig.impl.logicalLayer.RelationalOperator;
import org.apache.pig.impl.logicalLayer.LOForEach;
import org.apache.pig.impl.logicalLayer.LOLimit;
import org.apache.pig.impl.logicalLayer.LOProject;
import org.apache.pig.impl.logicalLayer.LOSort;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.PlanWalker;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.expression.DereferenceExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.relational.LOGenerate;
import org.apache.pig.newplan.logical.relational.LOInnerLoad;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;


// visitor to translate the inner plan of foreach
// it contains methods to translate all the operators that are allowed 
// in the inner plan of foreach
public class ForeachInnerPlanVisitor extends LogicalExpPlanMigrationVistor {
    private org.apache.pig.newplan.logical.relational.LogicalPlan newInnerPlan;
    private LOForEach oldForeach;
    private org.apache.pig.newplan.logical.relational.LogicalRelationalOperator gen;
    private int inputNo;
    private HashMap<LogicalOperator, LogicalRelationalOperator> innerOpsMap;
    private Map<LogicalExpression, LogicalOperator> scalarAliasMap = new HashMap<LogicalExpression, LogicalOperator>();

    public ForeachInnerPlanVisitor(org.apache.pig.newplan.logical.relational.LOForEach foreach, LOForEach oldForeach, LogicalPlan innerPlan, 
            LogicalPlan oldLogicalPlan, Map<LogicalExpression, LogicalOperator> scalarMap) throws FrontendException {
        super(innerPlan, oldForeach, foreach, oldLogicalPlan, scalarMap);
        newInnerPlan = foreach.getInnerPlan();
        
        // get next inputNo 
        gen = (org.apache.pig.newplan.logical.relational.LogicalRelationalOperator)
            newInnerPlan.getSinks().get(0);
        inputNo = 0;
        List<org.apache.pig.newplan.Operator> suc = newInnerPlan.getPredecessors(gen);
        if (suc != null) {
            inputNo = suc.size();
        }
        
        this.oldForeach = oldForeach;
                    
        innerOpsMap = new HashMap<LogicalOperator, LogicalRelationalOperator>();
        scalarAliasMap = scalarMap;
    }
    
    private void translateInnerPlanConnection(LogicalOperator oldOp, org.apache.pig.newplan.Operator newOp) throws FrontendException {
        List<LogicalOperator> preds = mPlan.getPredecessors(oldOp);
        
        if(preds != null) {
            for(LogicalOperator pred: preds) {
                org.apache.pig.newplan.Operator newPred = innerOpsMap.get(pred);
                if (newPred.getPlan().getSuccessors(newPred)!=null) {
                    org.apache.pig.newplan.Operator newSucc = newOp.getPlan().getSuccessors(newPred).get(0);
                    Pair<Integer, Integer> pair = newOp.getPlan().disconnect(newPred, newSucc);
                    newOp.getPlan().connect(newPred, newOp);
                    newOp.getPlan().connect(newOp, pair.first, newSucc, pair.second);
                }
                else {
                    newOp.getPlan().connect(newPred, newOp);
                }
            }
        }
    }
    
    private LogicalExpressionPlan translateInnerExpressionPlan(LogicalPlan lp, LogicalOperator oldOp, LogicalRelationalOperator op, LogicalPlan outerPlan) throws VisitorException {
        PlanWalker<LogicalOperator, LogicalPlan> childWalker = 
            new DependencyOrderWalker<LogicalOperator, LogicalPlan>(lp);
        
        LogicalExpPlanMigrationVistor childPlanVisitor = new LogicalExpPlanMigrationVistor(lp, oldOp, op, outerPlan, scalarAliasMap);
        
        childWalker.walk(childPlanVisitor);
        return childPlanVisitor.exprPlan;
    }
    
    public void visit(LOProject project) throws VisitorException {
        LogicalOperator op = project.getExpression();
        
        if (op == outerPlan.getPredecessors(oldForeach).get(0)) {
            // if this projection is to get a field from outer plan, change it
            // to LOInnerLoad
            
            LOInnerLoad innerLoad = new LOInnerLoad(newInnerPlan, 
                    (org.apache.pig.newplan.logical.relational.LOForEach)attachedRelationalOp, 
                    project.isStar()?-1:project.getCol());
            
            newInnerPlan.add(innerLoad);
            innerOpsMap.put(project, innerLoad);
            
            // The logical plan part for this foreach plan is done, add ProjectExpression 
            // into expression plan.
                                  
            // The logical plan part is done, add this sub plan under LOGenerate, 
            // and prepare for the expression plan
            newInnerPlan.connect(innerLoad, gen);
            
            ProjectExpression pe = new ProjectExpression(exprPlan, inputNo++, -1, gen);
            exprPlan.add(pe);
            exprOpsMap.put(project, pe);
            try {
                translateInnerPlanConnection(project, pe);
            } catch (FrontendException e) {
                throw new VisitorException(e);
            } 
        }

        // This case occurs when there are two projects one after another
        // These projects in combination project a column (bag) out of a tuple 
        // and then project a column out of this projected bag
        // Here we merge these two projects into one BagDereferenceExpression
        else if( op instanceof LOProject ) {
            LogicalExpression expOper = exprOpsMap.get(op);
            
            if (expOper!=null) {
                // Add the dereference in the plan
                DereferenceExpression dereferenceExp = new DereferenceExpression(
                        exprPlan, project.getProjection());
                exprOpsMap.put(project, dereferenceExp);
                exprPlan.add(dereferenceExp);
                exprPlan.connect(dereferenceExp, expOper);
            }
        } else {
            if (op instanceof RelationalOperator && project.isSendEmptyBagOnEOP()) {
                LogicalOperator currentOp = op;
                while (currentOp instanceof RelationalOperator) {
                    List<LogicalOperator> preds = mPlan.getPredecessors(currentOp);
                    if (preds!=null)
                        currentOp = preds.get(0);
                    else break;
                }
                if (currentOp instanceof ExpressionOperator) {
                    LogicalExpression exp = exprOpsMap.get(currentOp);
                    if (exp!=null)
                        exprOpsMap.put(project, exp);
                }
            }
        }
    }
    
    public void visit(LOSort sort) throws VisitorException {
        List<LogicalPlan> sortPlans = sort.getSortColPlans();
        List<LogicalExpressionPlan> newSortPlans = new ArrayList<LogicalExpressionPlan>();
        
        org.apache.pig.newplan.logical.relational.LOSort newSort = 
            new org.apache.pig.newplan.logical.relational.LOSort(newInnerPlan, 
                    newSortPlans, sort.getAscendingCols(), sort.getUserFunc());
        
        newSort.setAlias(sort.getAlias());
        newSort.setRequestedParallelism(sort.getRequestedParallelism());
        newSort.setLimit(sort.getLimit());
        newInnerPlan.add(newSort);
        innerOpsMap.put(sort, newSort);
        try {
            translateInnerPlanConnection(sort, newSort);
        } catch (FrontendException e) {
            throw new VisitorException(e);
        }
        
        for (LogicalPlan sortPlan : sortPlans) {
            LogicalExpressionPlan newSortPlan = translateInnerExpressionPlan(sortPlan, sort, newSort, mPlan);
            newSortPlans.add(newSortPlan);
        }
    }

    public void visit(LOLimit limit) throws VisitorException {
        org.apache.pig.newplan.logical.relational.LOLimit newLimit = 
            new org.apache.pig.newplan.logical.relational.LOLimit(newInnerPlan, 
                    limit.getLimit());
        
        newLimit.setAlias(limit.getAlias());
        newLimit.setRequestedParallelism(limit.getRequestedParallelism());
        newInnerPlan.add(newLimit);
        innerOpsMap.put(limit, newLimit);
        try {
            translateInnerPlanConnection(limit, newLimit);
        } catch (FrontendException e) {
            throw new VisitorException(e);
        }        
    }
    
    public void visit(LODistinct distinct) throws VisitorException {
        org.apache.pig.newplan.logical.relational.LODistinct newDistinct = 
            new org.apache.pig.newplan.logical.relational.LODistinct(newInnerPlan);
        
        newDistinct.setAlias(distinct.getAlias());
        newDistinct.setRequestedParallelism(distinct.getRequestedParallelism());
        newInnerPlan.add(newDistinct);
        innerOpsMap.put(distinct, newDistinct);
        try {
            translateInnerPlanConnection(distinct, newDistinct);
        } catch (FrontendException e) {
            throw new VisitorException(e);
        }
    }
    
    public void visit(LOFilter filter) throws VisitorException {
        org.apache.pig.newplan.logical.relational.LOFilter newFilter = 
            new org.apache.pig.newplan.logical.relational.LOFilter(newInnerPlan);
        
        newFilter.setAlias(filter.getAlias());
        newFilter.setRequestedParallelism(filter.getRequestedParallelism());
        LogicalExpressionPlan newFilterPlan = translateInnerExpressionPlan(filter.getComparisonPlan(), filter, newFilter, mPlan);
        newFilter.setFilterPlan(newFilterPlan);
        newInnerPlan.add(newFilter);
        innerOpsMap.put(filter, newFilter);
        try {
            translateInnerPlanConnection(filter, newFilter);
        } catch (FrontendException e) {
            throw new VisitorException(e);
        }
    }
    
    public void visit(LOForEach foreach) throws VisitorException {
        org.apache.pig.newplan.logical.relational.LOForEach newForEach = 
            new org.apache.pig.newplan.logical.relational.LOForEach(newInnerPlan);
        
        newForEach.setAlias(foreach.getAlias());
        newForEach.setRequestedParallelism(foreach.getRequestedParallelism());
        
        org.apache.pig.newplan.logical.relational.LogicalPlan newForEachInnerPlan 
            = new org.apache.pig.newplan.logical.relational.LogicalPlan();        
        newForEach.setInnerPlan(newForEachInnerPlan);
        List<LogicalExpressionPlan> expPlans = new ArrayList<LogicalExpressionPlan>();
        boolean[] flattens = new boolean[foreach.getForEachPlans().size()];
        LOGenerate generate = new LOGenerate(newForEachInnerPlan, expPlans, flattens);
        newForEachInnerPlan.add(generate);
        
        for (int i=0;i<foreach.getForEachPlans().size();i++) {
            LogicalPlan innerPlan = foreach.getForEachPlans().get(i);
            // Assume only one project is allowed in this level of foreach
            LOProject project = (LOProject)innerPlan.iterator().next();

            LOInnerLoad innerLoad = new LOInnerLoad(newForEachInnerPlan, 
                    newForEach, project.isStar()?-1:project.getCol());
            newForEachInnerPlan.add(innerLoad);
            newForEachInnerPlan.connect(innerLoad, generate);
            LogicalExpressionPlan expPlan = new LogicalExpressionPlan();
            expPlans.add(expPlan);
            ProjectExpression pe = new ProjectExpression(expPlan, i, -1, generate);
            expPlan.add(pe);
        }
        
        newInnerPlan.add(newForEach);
        innerOpsMap.put(foreach, newForEach);
        try {
            translateInnerPlanConnection(foreach, newForEach);
        } catch (FrontendException e) {
            throw new VisitorException(e);
        }
    }
}