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

package org.apache.pig.newplan.logical.rules;

import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.OperatorSubPlan;
import org.apache.pig.newplan.logical.expression.ConstantExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.relational.LOCogroup;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.optimizer.Rule;
import org.apache.pig.newplan.optimizer.Transformer;

/**
 * Rule: If a LOCogroup is 'group all', set the parallelism to 1,
 * or in general - if the group-by expression is just a constant
 * then set parallelism to 1
 * LogicalExpressionSimplifier could be used to convert an expression 
 * with constants into a single ConstantExpression 
 */

public class GroupByConstParallelSetter extends Rule {

    public GroupByConstParallelSetter(String n){
        super(n, false);
    }

    @Override
    public Transformer getNewTransformer() {
        return new GroupAllParallelSetterTransformer();
    }
    private final static Log log = LogFactory.getLog(GroupByConstParallelSetter.class);

    public class GroupAllParallelSetterTransformer extends Transformer {

        @Override
        public boolean check(OperatorPlan matched) throws FrontendException {
            LOCogroup group = (LOCogroup)matched.getSources().get(0);
            MultiMap<Integer, LogicalExpressionPlan> explPlans =
                group.getExpressionPlans();
            //check if the expression plan consist of just a ConstantExpression
            for(LogicalExpressionPlan ep : explPlans.values()){
                Iterator<Operator> op_iter = ep.getOperators();
                if(op_iter.hasNext()){
                    //return false if the ExpressionOperator is not
                    // a ConstantExpression
                    if(! (op_iter.next() instanceof ConstantExpression)){
                        return false;
                    }
                }
                // if there is more than one ExpressionOperator, return false
                if(op_iter.hasNext()){
                    return false;
                }
            }
            return true;
            
        }

        @Override
        public void transform(OperatorPlan plan) throws FrontendException {
            Iterator<Operator> iter = plan.getOperators();
            while (iter.hasNext()) {
                Operator op = iter.next();
                if (op instanceof LOCogroup) {
                    LOCogroup group = (LOCogroup)op;
                    if(group.getRequestedParallelism() > 1){
                        log.warn("Resetting parallism to 1 for the group/cogroup " +
                                group.getAlias() +
                        " as the group by expressions returns a constant");
                    }
                    ((LOCogroup) op).setRequestedParallelism(1);
                }
            }
        }
        
 
        @Override
        public OperatorPlan reportChanges() {
            return new OperatorSubPlan(currentPlan);
        }
    }

    @Override
    protected OperatorPlan buildPattern() {
        LogicalPlan plan = new LogicalPlan();
        LogicalRelationalOperator op = new LOCogroup(plan);
        plan.add(op);
        return plan;
    }
}
