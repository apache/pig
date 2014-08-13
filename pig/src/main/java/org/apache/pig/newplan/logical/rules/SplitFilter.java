/**
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
package org.apache.pig.newplan.logical.rules;

import java.util.Iterator;
import java.util.List;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.OperatorSubPlan;
import org.apache.pig.newplan.logical.expression.AndExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.optimizer.Rule;
import org.apache.pig.newplan.optimizer.Transformer;

public class SplitFilter extends Rule {    

    public SplitFilter(String n) {
        super(n, false);       
    }

    @Override
    public Transformer getNewTransformer() {        
        return new SplitFilterTransformer();
    }

    public class SplitFilterTransformer extends Transformer {
        private OperatorSubPlan subPlan;

        @Override
        public boolean check(OperatorPlan matched) throws FrontendException {
            LOFilter filter = (LOFilter)matched.getSources().get(0);
            LogicalExpressionPlan cond = filter.getFilterPlan();
            LogicalExpression root = (LogicalExpression) cond.getSources().get(0);
            if (root instanceof AndExpression && currentPlan.getSoftLinkPredecessors(filter)==null) {
                return true;
            }
            
            return false;
        }

        @Override
        public void transform(OperatorPlan matched) throws FrontendException {
            subPlan = new OperatorSubPlan(currentPlan);
            
            // split one LOFilter into 2 by "AND"
            LOFilter filter = (LOFilter)matched.getSources().get(0);
            LogicalExpressionPlan cond = filter.getFilterPlan();
            LogicalExpression root = (LogicalExpression) cond.getSources().get(0);
            if (!(root instanceof AndExpression)) {
                return;
            }
            LogicalExpressionPlan op1 = new LogicalExpressionPlan();
            op1.add((LogicalExpression)cond.getSuccessors(root).get(0));
            fillSubPlan(cond, op1, (LogicalExpression)cond.getSuccessors(root).get(0));
            
            LogicalExpressionPlan op2 = new LogicalExpressionPlan();
            op2.add((LogicalExpression)cond.getSuccessors(root).get(1));
            fillSubPlan(cond, op2, (LogicalExpression)cond.getSuccessors(root).get(1));
            
            filter.setFilterPlan(op1);
            LOFilter filter2 = new LOFilter((LogicalPlan)currentPlan, op2);
            currentPlan.add(filter2);
            
            Operator succed = null;
            List<Operator> succeds = currentPlan.getSuccessors(filter);
            if (succeds != null) {
                succed = succeds.get(0);
                subPlan.add(succed);
                currentPlan.insertBetween(filter, filter2, succed);
            } else {
                currentPlan.connect(filter, 0, filter2, 0); 
            }
            
            subPlan.add(filter);
            subPlan.add(filter2);
            Iterator<Operator> iter = filter2.getFilterPlan().getOperators();
            while (iter.hasNext()) {
                Operator oper = iter.next();
                if (oper instanceof ProjectExpression) {
                    ((ProjectExpression)oper).setAttachedRelationalOp(filter2);
                }
            }
        }
        
        @Override
        public OperatorPlan reportChanges() {
            return subPlan;
        }
        
        private void fillSubPlan(OperatorPlan origPlan, 
                OperatorPlan subPlan, Operator startOp) throws FrontendException {
                       
            List<Operator> l = origPlan.getSuccessors(startOp);
            if (l != null) {
                for(Operator le: l) {
                    subPlan.add(le);
                    subPlan.connect(startOp, le);
                    fillSubPlan(origPlan, subPlan, le);
                }            
            }
        }

    }

    @Override
    protected OperatorPlan buildPattern() {        
        // the pattern that this rule looks for
        // is filter
        LogicalPlan plan = new LogicalPlan();      
        LogicalRelationalOperator op2 = new LOFilter(plan);
        plan.add(op2);
        
        return plan;
    }
}

