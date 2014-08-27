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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.newplan.BaseOperatorPlan;
import org.apache.pig.newplan.DependencyOrderWalker;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanWalker;
import org.apache.pig.newplan.ReverseDependencyOrderWalkerWOSeenChk;
import org.apache.pig.newplan.logical.expression.AllSameExpressionVisitor;
import org.apache.pig.newplan.logical.expression.ConstantExpression;
import org.apache.pig.newplan.logical.expression.ExpToPhyTranslationVisitor;
import org.apache.pig.newplan.logical.expression.LogicalExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.LogicalExpressionVisitor;
import org.apache.pig.newplan.logical.expression.UserFuncExpression;
import org.apache.pig.newplan.logical.optimizer.AllExpressionVisitor;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.optimizer.Rule;
import org.apache.pig.newplan.optimizer.Transformer;

public abstract class ConstantCalculator extends Rule {
    private List<LogicalRelationalOperator> processedOperators = new ArrayList<LogicalRelationalOperator>();
    private PigContext pc;

    public ConstantCalculator(String n, PigContext pc) {
        super(n, false);
        this.pc = pc;
    }

    @Override
    public Transformer getNewTransformer() {
        return new ConstantCalculatorTransformer(processedOperators, pc);
    }

    public static class ConstantCalculatorTransformer extends Transformer {
        private List<LogicalRelationalOperator> processedOperators = new ArrayList<LogicalRelationalOperator>();
        private OperatorPlan plan;
        private PigContext pc;

        public ConstantCalculatorTransformer(List<LogicalRelationalOperator> processedOperators, PigContext pc) {
            this.processedOperators = processedOperators;
            this.pc = pc;
        }

        @Override
        public boolean check(OperatorPlan matched) throws FrontendException {
            Iterator<Operator> operators = matched.getOperators();
            while (operators.hasNext()) {
                LogicalRelationalOperator operator = (LogicalRelationalOperator)operators.next();
    
                // If the operator is already processed, we quit.
                if (processedOperators.contains(operator)) {
                    continue;
                }

                processedOperators.add(operator);
                return true;
            }
            return false;
        }

        public static class ConstantCalculatorLogicalPlanVisitor extends AllExpressionVisitor {
            private PigContext pc;
            public ConstantCalculatorLogicalPlanVisitor(OperatorPlan plan, PigContext pc) throws FrontendException {
                super(plan, new DependencyOrderWalker(plan));
                this.pc = pc;
            }

            @Override
            protected LogicalExpressionVisitor getVisitor(
                    LogicalExpressionPlan expr) throws FrontendException {
                return new ConstantCalculatorExpressionVisitor(expr, currentOp, pc);
            }
        }

        public static class ConstantCalculatorExpressionVisitor extends AllSameExpressionVisitor {
            private LogicalRelationalOperator currentOp;
            private PigContext pc;
            public ConstantCalculatorExpressionVisitor(OperatorPlan expPlan,
                    LogicalRelationalOperator currentOp, PigContext pc) throws FrontendException {
                super(expPlan, new ReverseDependencyOrderWalkerWOSeenChk(expPlan));
                this.currentOp = currentOp;
                this.pc = pc;
            }

            @Override
            protected void execute(LogicalExpression op) throws FrontendException {
                if (op instanceof UserFuncExpression) {
                    UserFuncExpression udf = (UserFuncExpression)op;
                    if (!udf.getEvalFunc().allowCompileTimeCalculation()) {
                        return;
                    }
                }
                boolean valSet = false;
                Object val = null;
                if (currentWalker.getPlan().getSuccessors(op) != null) {
                    // If has successors and all successors are constant, calculate the constant
                    for (Operator succ : currentWalker.getPlan().getSuccessors(op)) {
                        if (!(succ instanceof ConstantExpression)) {
                            return;
                        }
                    }
                    // All successors are constant, calculate the value
                    OperatorPlan expLogicalPlan = new LogicalExpressionPlan();
                    ((BaseOperatorPlan)currentWalker.getPlan()).moveTree(op, (BaseOperatorPlan)expLogicalPlan);
                    PhysicalPlan expPhysicalPlan = new PhysicalPlan();
                    Map<Operator, PhysicalOperator> logToPhyMap = new HashMap<Operator, PhysicalOperator>();
                    PlanWalker childWalker = new ReverseDependencyOrderWalkerWOSeenChk(expLogicalPlan);

                    // Save the old walker and use childWalker as current Walker
                    pushWalker(childWalker);
                    ExpToPhyTranslationVisitor expTranslationVisitor = new 
                            ExpToPhyTranslationVisitor(expLogicalPlan,
                                    childWalker, currentOp, expPhysicalPlan, logToPhyMap);
                    expTranslationVisitor.visit();
                    popWalker();
                    PhysicalOperator root = expPhysicalPlan.getLeaves().get(0);
                    try {
                        UDFContext.getUDFContext().addJobConf(ConfigurationUtil.toConfiguration(pc.getProperties(), true));
                        val = root.getNext(root.getResultType()).result;
                        UDFContext.getUDFContext().addJobConf(null);
                    } catch (ExecException e) {
                        throw new FrontendException(e);
                    }
                    valSet = true;
                } else if (op instanceof UserFuncExpression) {
                    // If solo UDF, calculate UDF
                    UserFuncExpression udf = (UserFuncExpression)op;
                    try {
                        UDFContext.getUDFContext().addJobConf(ConfigurationUtil.toConfiguration(pc.getProperties(), true));
                        val = udf.getEvalFunc().exec(null);
                        UDFContext.getUDFContext().addJobConf(null);
                    } catch (IOException e) {
                        throw new FrontendException(e);
                    }
                    valSet = true;
                }
                if (valSet) {
                    ConstantExpression constantExpr;
                    constantExpr = new ConstantExpression(currentWalker.getPlan(), val);
                    constantExpr.inheritSchema(op);
                    currentWalker.getPlan().replace(op, constantExpr);
                }
            }
        }

        @Override
        public void transform(OperatorPlan matched) throws FrontendException {
            AllExpressionVisitor expressionVisitor = new ConstantCalculatorLogicalPlanVisitor(matched, pc);
            expressionVisitor.visit();
        }

        @Override
        public OperatorPlan reportChanges() {
            return plan;
        }
    }
}
