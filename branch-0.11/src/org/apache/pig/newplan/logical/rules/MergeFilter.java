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

import java.util.ArrayList;
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

public class MergeFilter extends Rule {

    public MergeFilter(String n) {
        super(n, false);       
    }

    @Override
    public Transformer getNewTransformer() {        
        return new MergeFilterTransformer();
    }

    public class MergeFilterTransformer extends Transformer {

        private OperatorSubPlan subPlan;

        @Override
        public boolean check(OperatorPlan matched) throws FrontendException {           
            LOFilter filter = (LOFilter)matched.getSources().get(0);
            List<Operator> succeds = currentPlan.getSuccessors(filter);
            // if this filter is followed by another filter, we should combine them
            if (succeds != null && succeds.size() == 1) {
                if (succeds.get(0) instanceof LOFilter) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public void transform(OperatorPlan matched) throws FrontendException {     
            subPlan = new OperatorSubPlan(currentPlan);
            
            LOFilter filter = (LOFilter)matched.getSources().get(0);

            subPlan.add(filter);
            
            List<Operator> succeds = currentPlan.getSuccessors(filter);
            if (succeds != null && succeds.size()== 1 && (succeds.get(0) instanceof LOFilter)) {
                LOFilter next = (LOFilter)succeds.get(0);
                combineFilterCond(filter, next);
                
                List<Operator> succs = currentPlan.getSuccessors(next);
                if (succs!=null && succs.size()>0) {
                    subPlan.add(succs.get(0));
                }

                // Since we remove next, we need to merge soft link into filter
                List<Operator> nextSoftPreds = null;
                if (currentPlan.getSoftLinkPredecessors(next)!=null) {
                    nextSoftPreds = new ArrayList<Operator>();
                    nextSoftPreds.addAll(currentPlan.getSoftLinkPredecessors(next));
                }
                
                if (nextSoftPreds!=null) {
                    for (Operator softPred : nextSoftPreds) {
                        currentPlan.removeSoftLink(softPred, next);
                        currentPlan.createSoftLink(softPred, filter);
                    }
                }
                currentPlan.removeAndReconnect(next);
            }
            
            Iterator<Operator> iter = filter.getFilterPlan().getOperators();
            while (iter.hasNext()) {
                Operator oper = iter.next();
                if (oper instanceof ProjectExpression) {
                    ((ProjectExpression)oper).setAttachedRelationalOp(filter);
                }
            }
        }        
        
        @Override
        public OperatorPlan reportChanges() {          
            return subPlan;
        }
        
        // combine the condition of two filters. The condition of second filter
        // is added into the condition of first filter with an AND operator.
        private void combineFilterCond(LOFilter f1, LOFilter f2) throws FrontendException {
            LogicalExpressionPlan p1 = f1.getFilterPlan();
            LogicalExpressionPlan p2 = f2.getFilterPlan();
            LogicalExpressionPlan andPlan = new LogicalExpressionPlan();
            
            // add existing operators          
            Iterator<Operator> iter = p1.getOperators();
            while(iter.hasNext()) {
                andPlan.add(iter.next());
            }
            
            iter = p2.getOperators();
            while(iter.hasNext()) {
                andPlan.add(iter.next());
            }
            
            // add all connections
            iter = p1.getOperators();
            while(iter.hasNext()) {
                Operator n = iter.next();
                List<Operator> l = p1.getPredecessors(n);
                if (l != null) {
                    for(Operator op: l) {
                        andPlan.connect(op, n);
                    }
                }
            }
            
            iter = p2.getOperators();
            while(iter.hasNext()) {
                Operator n = iter.next();
                List<Operator> l = p2.getPredecessors(n);
                if (l != null) {
                    for(Operator op: l) {
                        andPlan.connect(op, n);
                    }
                }
            }          
            
            // create an AND
            new AndExpression(andPlan, (LogicalExpression)p1.getSources().get(0), (LogicalExpression)p2.getSources().get(0));          
            
            f1.setFilterPlan(andPlan);
        }

    }

    @Override
    protected OperatorPlan buildPattern() {        
        // the pattern that this rule looks for
        // is filter operator
        LogicalPlan plan = new LogicalPlan();
        LogicalRelationalOperator op = new LOFilter(plan);
        plan.add(op);        
        
        return plan;
    }
}

