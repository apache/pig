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

package org.apache.pig.experimental.logical.rules;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.pig.experimental.logical.expression.LogicalExpressionPlan;
import org.apache.pig.experimental.logical.expression.ProjectExpression;
import org.apache.pig.experimental.logical.relational.LOForEach;
import org.apache.pig.experimental.logical.relational.LOGenerate;
import org.apache.pig.experimental.logical.relational.LOInnerLoad;
import org.apache.pig.experimental.logical.relational.LogicalPlan;
import org.apache.pig.experimental.logical.relational.LogicalRelationalOperator;
import org.apache.pig.experimental.logical.relational.LogicalSchema;
import org.apache.pig.experimental.plan.Operator;
import org.apache.pig.experimental.plan.OperatorPlan;
import org.apache.pig.experimental.plan.OperatorSubPlan;
import org.apache.pig.experimental.plan.optimizer.Transformer;
import org.apache.pig.impl.util.Pair;

public class AddForEach extends WholePlanRule {
    protected static final String REQUIREDCOLS = "AddForEach:RequiredColumns";
    
    public AddForEach(String n) {
        super(n);		
    }

    @Override
    public Transformer getNewTransformer() {
        return new AddForEachTransformer();
    }
    
    public class AddForEachTransformer extends Transformer {
        LogicalRelationalOperator opForAdd;
        OperatorSubPlan subPlan;

        @Override
        public boolean check(OperatorPlan matched) throws IOException {
            Iterator<Operator> iter = matched.getOperators();
            while(iter.hasNext()) {
                LogicalRelationalOperator op = (LogicalRelationalOperator)iter.next();
                if (shouldAdd(op)) {
                    opForAdd = op;
                    return true;
                }
            }
            
            return false;
        }

        @Override
        public OperatorPlan reportChanges() {        	
            return subPlan;
        }

        private void addSuccessors(Operator op) throws IOException {
            subPlan.add(op);
            List<Operator> ll = op.getPlan().getSuccessors(op);
            if (ll != null) {
                for(Operator suc: ll) {
                    addSuccessors(suc);
                }
            }
        }
        
        @Override
        public void transform(OperatorPlan matched) throws IOException {            
            addForeach(opForAdd);
            
            subPlan = new OperatorSubPlan(currentPlan);
            addSuccessors(opForAdd);
        }
        
        @SuppressWarnings("unchecked")
        // check if an LOForEach should be added after the logical operator
        private boolean shouldAdd(LogicalRelationalOperator op) throws IOException {
            if (op instanceof LOForEach) {
                return false;
            }
            Set<Long> output = (Set<Long>)op.getAnnotation(ColumnPruneHelper.OUTPUTUIDS);
            
            if (output == null) {
                return false;
            }
                            
            LogicalSchema s = op.getSchema();
            if (s == null) {
                return false;
            }
                               
            // check if there is already a foreach
            List<Operator> ll = op.getPlan().getSuccessors(op);
            if (ll != null && ll.get(0) instanceof LOForEach) {
                return false;
            }
            
            Set<Integer> cols = new HashSet<Integer>();
            for(long uid: output) {
                int col = s.findField(uid);
                if (col < 0) {
                    throw new RuntimeException("Uid " + uid + " is not in the schema of " + op.getName());
                }
                cols.add(col);
            }
            
            if (cols.size()<s.size()) {
                op.annotate(REQUIREDCOLS, cols);
                return true;
            }
            
            return false;
        }
        
        @SuppressWarnings("unchecked")
        private void addForeach(LogicalRelationalOperator op) throws IOException {            
            LOForEach foreach = new LOForEach(op.getPlan());
            
            // add foreach to the base plan
            LogicalPlan p = (LogicalPlan)op.getPlan();
            p.add(foreach);
            List<Operator> next = p.getSuccessors(op);           
            if (next != null) {
                Operator[] nextArray = next.toArray(new Operator[0]);
                for(Operator n: nextArray) {                  
                    Pair<Integer, Integer> pos = p.disconnect(op, n);           
                    p.connect(foreach, pos.first, n, pos.second);
                }
            }
            
            p.connect(op, foreach);                        
            
            LogicalPlan innerPlan = new LogicalPlan();
            foreach.setInnerPlan(innerPlan);
                      
            // get output columns
            Set<Integer> cols = (Set<Integer>)op.getAnnotation(REQUIREDCOLS);            
            
            // build foreach inner plan
            List<LogicalExpressionPlan> exps = new ArrayList<LogicalExpressionPlan>();            	
            LOGenerate gen = new LOGenerate(innerPlan, exps, new boolean[cols.size()]);
            innerPlan.add(gen);
            
            LogicalSchema schema = op.getSchema();
            for (int i=0, j=0; i<schema.size(); i++) {   
                if (!cols.contains(i)) {
                    continue;
                }
                               
                LOInnerLoad innerLoad = new LOInnerLoad(innerPlan, foreach, i);
                innerLoad.getProjection().setUid(foreach);                    
                innerPlan.add(innerLoad);          
                innerPlan.connect(innerLoad, gen);
                
                LogicalExpressionPlan exp = new LogicalExpressionPlan();
                ProjectExpression prj = new ProjectExpression(exp, schema.getField(i).type, j++, 0);
                prj.setUid(gen);
                exp.add(prj);
                exps.add(exp);                
            }                
           
        }
    }          
}
