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
package org.apache.pig.newplan.logical.visitor;

import org.apache.pig.PigException;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.newplan.DependencyOrderWalker;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanWalker;
import org.apache.pig.newplan.ReverseDependencyOrderWalker;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.LogicalExpressionVisitor;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LOLimit;
import org.apache.pig.newplan.logical.relational.LogicalRelationalNodesVisitor;

/**
 * This validator checks the correctness of use of scalar variables in logical
 * operators. It assesses the validity of the expression by making sure there is
 * no projection in it. Currently it works for Limit and Sample (see PIG-1926)
 * 
 */
public class ScalarVariableValidator extends LogicalRelationalNodesVisitor {
    public static final String ERR_MSG_SCALAR = "Expression in Limit/Sample should be scalar";

    public ScalarVariableValidator(OperatorPlan plan) throws FrontendException {
        super(plan, new DependencyOrderWalker(plan));
    }

    @Override
    public void visit(LOLimit limit) throws FrontendException {
        LogicalExpressionPlan expression = limit.getLimitPlan();
        if (expression != null) {
            ProjectFinder pf = new ProjectFinder(expression,
                    new ReverseDependencyOrderWalker(expression));
            pf.visit();
            if (pf.found()) {
                int errCode = 1131;
                throw new VisitorException(limit, ERR_MSG_SCALAR, errCode,
                        PigException.INPUT);
            }
        }
    }

    @Override
    public void visit(LOFilter filter) throws FrontendException {
        LogicalExpressionPlan expression = filter.getFilterPlan();
        if (expression != null) {
            // if it is a Sample, the expression must be scalar
            if (filter.isSample()) {
                ProjectFinder pf = new ProjectFinder(expression,
                        new ReverseDependencyOrderWalker(expression));
                pf.visit();
                if (pf.found()) {
                    int errCode = 1131;
                    throw new VisitorException(filter, ERR_MSG_SCALAR, errCode,
                            PigException.INPUT);
                }
            }
        }
    }

    public static class ProjectFinder extends LogicalExpressionVisitor {
        private boolean foundProject;

        public boolean found() {
            return foundProject;
        }

        protected ProjectFinder(OperatorPlan p, PlanWalker walker)
                throws FrontendException {
            super(p, walker);
        }

        @Override
        public void visit(ProjectExpression project) {
            foundProject = true;
        }
    }
}
