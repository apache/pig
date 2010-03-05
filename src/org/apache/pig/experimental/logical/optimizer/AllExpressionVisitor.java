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
import java.util.Collection;

import org.apache.pig.experimental.logical.expression.LogicalExpressionPlan;
import org.apache.pig.experimental.logical.expression.LogicalExpressionVisitor;
import org.apache.pig.experimental.logical.relational.LOFilter;
import org.apache.pig.experimental.logical.relational.LOForEach;
import org.apache.pig.experimental.logical.relational.LOGenerate;
import org.apache.pig.experimental.logical.relational.LOInnerLoad;
import org.apache.pig.experimental.logical.relational.LOJoin;
import org.apache.pig.experimental.logical.relational.LogicalPlanVisitor;
import org.apache.pig.experimental.logical.relational.LogicalRelationalOperator;
import org.apache.pig.experimental.plan.OperatorPlan;
import org.apache.pig.experimental.plan.PlanWalker;

/**
 * A visitor that walks a logical plan and then applies a given
 * LogicalExpressionVisitor to all expressions it encounters.
 *
 */
public abstract class AllExpressionVisitor extends LogicalPlanVisitor {
    
    protected LogicalExpressionVisitor exprVisitor;
    protected LogicalRelationalOperator currentOp;

    /**
     * @param plan LogicalPlan to visit
     * @param walker Walker to use to visit the plan.
     */
    public AllExpressionVisitor(OperatorPlan plan,
                                PlanWalker walker) {
        super(plan, walker);
    }
    
    /**
     * Get a new instance of the expression visitor to apply to 
     * a given expression.
     * @param expr LogicalExpressionPlan that will be visited
     * @return a new LogicalExpressionVisitor for that expression
     */
    abstract protected LogicalExpressionVisitor getVisitor(LogicalExpressionPlan expr);
    
    @Override
    public void visitLOFilter(LOFilter filter) throws IOException {
        currentOp = filter;
        LogicalExpressionVisitor v = getVisitor(filter.getFilterPlan());
        v.visit();
    }
    
    @Override
    public void visitLOJoin(LOJoin join) throws IOException {
        currentOp = join;
        Collection<LogicalExpressionPlan> c = join.getExpressionPlans();
        for (LogicalExpressionPlan plan : c) {
            LogicalExpressionVisitor v = getVisitor(plan);
            v.visit();
        }
    }
    
    @Override
    public void visitLOForEach(LOForEach foreach) throws IOException {
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
    public void visitLOGenerate(LOGenerate gen ) throws IOException {
        currentOp = gen;
        Collection<LogicalExpressionPlan> plans = gen.getOutputPlans();
        for( LogicalExpressionPlan plan : plans ) {
            LogicalExpressionVisitor v = getVisitor(plan);
            v.visit();
        }
    }
    
    public void visitLOInnerLoad(LOInnerLoad load) throws IOException {
        // the expression in LOInnerLoad contains info relative from LOForEach
        // so use LOForeach as currentOp
        currentOp = load.getLOForEach();
        LogicalExpressionPlan exp = load.getExpression();
       
        LogicalExpressionVisitor v = getVisitor(exp);
        v.visit();       
    }
}
