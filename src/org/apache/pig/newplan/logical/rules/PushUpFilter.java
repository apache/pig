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

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.OperatorSubPlan;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LOJoin;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.newplan.optimizer.Rule;
import org.apache.pig.newplan.optimizer.Transformer;

public class PushUpFilter extends Rule {
    
    public PushUpFilter(String n) {
        super(n, false);       
    }

    @Override
    public Transformer getNewTransformer() {        
        return new PushUpFilterTransformer();
    }

    public class PushUpFilterTransformer extends Transformer {

        private OperatorSubPlan subPlan;

        @Override
        public boolean check(OperatorPlan matched) throws FrontendException {   
            // check if it is inner join
            LOJoin join = (LOJoin)matched.getSources().get(0);
            boolean[] innerFlags = join.getInnerFlags();
            for(boolean inner: innerFlags) {
                if (!inner){
                    return false;
                }
            }
           
            Operator next = matched.getSinks().get(0);
            while(next != null && next instanceof LOFilter) {
                LOFilter filter = (LOFilter)next;            
                LogicalExpressionPlan filterPlan = filter.getFilterPlan();
                
                // collect all uids used in the filter plan
                Set<Long> uids = new HashSet<Long>();
                Iterator<Operator> iter = filterPlan.getOperators();
                while(iter.hasNext()) {
                    Operator op = iter.next();
                    if (op instanceof ProjectExpression) {
                        long uid = ((ProjectExpression)op).getFieldSchema().uid;
                        uids.add(uid);
                    }
                }
                                
                List<Operator> preds = currentPlan.getPredecessors(join);
                            
                for(int j=0; j<preds.size(); j++) {
                    if (hasAll((LogicalRelationalOperator)preds.get(j), uids)) {                            
                        return true;
                    }
                }                       
             
                // if current filter can not move up, check next filter
                List<Operator> l = currentPlan.getSuccessors(filter);
                if (l != null) {
                    next = l.get(0);
                } else {
                    next = null;
                }
            }
            
            return false;
        }

        @Override
        public void transform(OperatorPlan matched) throws FrontendException {
            subPlan = new OperatorSubPlan(currentPlan);

            LOJoin join = (LOJoin)matched.getSources().get(0);
            subPlan.add(join);     
            
            Operator next = matched.getSinks().get(0);
            while(next != null && next instanceof LOFilter) {
                LOFilter filter = (LOFilter)next;                
                subPlan.add(filter);
                
                LogicalExpressionPlan filterPlan = filter.getFilterPlan();
                
                // collect all uids used in the filter plan
                Set<Long> uids = new HashSet<Long>();
                Iterator<Operator> iter = filterPlan.getOperators();
                while(iter.hasNext()) {
                    Operator op = iter.next();
                    if (op instanceof ProjectExpression) {
                        long uid = ((ProjectExpression)op).getFieldSchema().uid;
                        uids.add(uid);
                    }
                }
                
                // find the farthest predecessor that has all the fields
                LogicalRelationalOperator input = join;
                List<Operator> preds = currentPlan.getPredecessors(input);
                while(preds != null) {                
                    boolean found = false;
                    for(int j=0; j<preds.size(); j++) {
                        if (hasAll((LogicalRelationalOperator)preds.get(j), uids)) {
                            input = (LogicalRelationalOperator)preds.get(j);   
                            subPlan.add(input);
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        break;
                    }
                    preds = currentPlan.getPredecessors(input);
                }
                            
                if (input != join) {                           
                    Operator pred = currentPlan.getPredecessors(filter).get(0);
                    Operator succed = currentPlan.getSuccessors(filter).get(0);
                    subPlan.add(succed);
                    
                    Pair<Integer, Integer> p1 = currentPlan.disconnect(pred, filter);
                    Pair<Integer, Integer> p2 = currentPlan.disconnect(filter, succed);
                    currentPlan.connect(pred, p1.first, succed, p2.second);
                    
                    succed = currentPlan.getSuccessors(input).get(0);
                    Pair<Integer, Integer> p3 = currentPlan.disconnect(input, succed);
                    currentPlan.connect(input, p3.first, filter, 0);
                    currentPlan.connect(filter, 0, succed, p3.second);                                        
                    
                    return;
                }  
                
                List<Operator> l = currentPlan.getSuccessors(filter);
                if (l != null) {
                    next = l.get(0);
                } else {
                    next = null;
                }                         
            }
        }
        
        // check if a relational operator contains all of the specified uids
        private boolean hasAll(LogicalRelationalOperator op, Set<Long> uids) throws FrontendException {
            LogicalSchema schema = op.getSchema();
            for(long uid: uids) {
                if (schema.findField(uid) == -1) {
                    return false;
                }
            }
            
            return true;
        }
           
        @Override
        public OperatorPlan reportChanges() {            
            return subPlan;
        }          

    }

    @Override
    protected OperatorPlan buildPattern() {        
        // the pattern that this rule looks for
        // is join -> filter
        LogicalPlan plan = new LogicalPlan();
        LogicalRelationalOperator op1 = new LOJoin(plan);
        LogicalRelationalOperator op2 = new LOFilter(plan);
        plan.add(op1);
        plan.add(op2);
        plan.connect(op1, op2);
        
        return plan;
    }
}

