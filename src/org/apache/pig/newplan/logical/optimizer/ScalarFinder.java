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
import java.util.HashMap;
import java.util.List;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.newplan.DepthFirstWalker;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanWalker;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.LogicalExpressionVisitor;
import org.apache.pig.newplan.logical.expression.UserFuncExpression;
import org.apache.pig.newplan.logical.relational.LOCogroup;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOGenerate;
import org.apache.pig.newplan.logical.relational.LOJoin;
import org.apache.pig.newplan.logical.relational.LOSort;
import org.apache.pig.newplan.logical.relational.LOSplitOutput;
import org.apache.pig.newplan.logical.relational.LogicalRelationalNodesVisitor;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;

public class ScalarFinder extends LogicalRelationalNodesVisitor  {

    private HashMap<UserFuncExpression, LogicalRelationalOperator>  scalars =
       new HashMap<UserFuncExpression, LogicalRelationalOperator>();
    
    public ScalarFinder(OperatorPlan plan)
            throws FrontendException {
        super(plan, new DepthFirstWalker(plan));
    }

    public HashMap<UserFuncExpression, LogicalRelationalOperator> getScalarLOMap(){
        return scalars;
    }
    
    @Override
    public void visit(LOFilter filter) throws FrontendException {
        ScalarFinderInExpPlan sFinder = new ScalarFinderInExpPlan(filter.getFilterPlan(), filter);
        sFinder.visit();
    }
  
    @Override
    public void visit(LOJoin join) throws FrontendException {
        Collection<LogicalExpressionPlan> joinPlans = join.getExpressionPlanValues();
        for (LogicalExpressionPlan joinPlan : joinPlans) {
            ScalarFinderInExpPlan sFinder = new ScalarFinderInExpPlan(joinPlan, join);
            sFinder.visit();
        }
    }
    
    @Override
    public void visit(LOForEach foreach) throws FrontendException {
        OperatorPlan innerPlan = foreach.getInnerPlan();
        PlanWalker newWalker = currentWalker.spawnChildWalker(innerPlan);
        pushWalker(newWalker);
        currentWalker.walk(this);
        popWalker();
    }
    
    @Override
    public void visit(LOGenerate gen) throws FrontendException {
        List<LogicalExpressionPlan> genPlans = gen.getOutputPlans();
        for (LogicalExpressionPlan genPlan : genPlans) {
            ScalarFinderInExpPlan sFinder = new ScalarFinderInExpPlan(genPlan, gen);
            sFinder.visit();
        }
    }
    
    @Override
    public void visit(LOCogroup loCogroup) throws FrontendException {
        MultiMap<Integer, LogicalExpressionPlan> expPlans = loCogroup.getExpressionPlans();
        for (LogicalExpressionPlan expPlan : expPlans.values()) {
            ScalarFinderInExpPlan sFinder = new ScalarFinderInExpPlan(expPlan, loCogroup);
            sFinder.visit();
        }
    }
    
    
    @Override
    public void visit(LOSplitOutput loSplitOutput) throws FrontendException {
        ScalarFinderInExpPlan sFinder =
            new ScalarFinderInExpPlan(loSplitOutput.getFilterPlan(), loSplitOutput);
        sFinder.visit();
    }
    

    @Override
    public void visit(LOSort loSort) throws FrontendException {
        List<LogicalExpressionPlan> sortPlans = loSort.getSortColPlans();
        for (LogicalExpressionPlan sortPlan : sortPlans) {
            ScalarFinderInExpPlan sFinder = new ScalarFinderInExpPlan(sortPlan, loSort);
            sFinder.visit();
        }
    }
    
    
    class ScalarFinderInExpPlan extends LogicalExpressionVisitor{

        
        private LogicalRelationalOperator logicalOp;

        protected ScalarFinderInExpPlan(LogicalExpressionPlan lep, LogicalRelationalOperator lo)
                throws FrontendException {
            super(lep, new DepthFirstWalker(lep));
            this.logicalOp = lo;
        }
        
        public void visit(UserFuncExpression op) throws FrontendException {
            if(op.getImplicitReferencedOperator() != null){
                scalars.put(op, logicalOp);
            }
        }
    }
    
}
