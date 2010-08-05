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
package org.apache.pig.newplan.logical.expression;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanWalker;

public abstract class AllSameExpressionVisitor extends LogicalExpressionVisitor {

    public AllSameExpressionVisitor(OperatorPlan plan, PlanWalker walker) throws FrontendException {
        super(plan, walker);
    }
    
    /**
     * Method to call on every node in the logical expression plan.
     * @param op Node that is currently being visited.
     */
    abstract protected void execute(LogicalExpression op) throws FrontendException;
    
    @Override
    public void visit(AndExpression andExpr) throws FrontendException {
        execute(andExpr);
    }
    
    @Override
    public void visit(OrExpression exp) throws FrontendException {
        execute(exp);
    }

    @Override
    public void visit(EqualExpression equal) throws FrontendException {
        execute(equal);
    }
    
    @Override
    public void visit(ProjectExpression project) throws FrontendException {
        execute(project);
    }
    
    @Override
    public void visit(ConstantExpression constant) throws FrontendException {
        execute(constant);
    }
    
    @Override
    public void visit(CastExpression cast) throws FrontendException {
        execute(cast);
    }

    @Override
    public void visit(GreaterThanExpression greaterThanExpression) throws FrontendException {
        execute(greaterThanExpression);
    }
    
    @Override
    public void visit(GreaterThanEqualExpression op) throws FrontendException {
        execute(op);
    }

    @Override
    public void visit(LessThanExpression lessThanExpression) throws FrontendException {
        execute(lessThanExpression);
    }
    
    @Override
    public void visit(LessThanEqualExpression op) throws FrontendException {
        execute(op);
    }

    @Override
    public void visit(NotEqualExpression op) throws FrontendException {
        execute(op);
    }

    @Override
    public void visit(NotExpression op) throws FrontendException {
        execute(op);
    }

    @Override
    public void visit(IsNullExpression op) throws FrontendException {
        execute(op);
    }
    
    @Override
    public void visit(NegativeExpression op) throws FrontendException {
        execute(op);
    }
    
    @Override
    public void visit(AddExpression op) throws FrontendException {
        execute(op);
    }
    
    @Override
    public void visit(SubtractExpression op) throws FrontendException {
        execute(op);
    }
    
    @Override
    public void visit(MultiplyExpression op) throws FrontendException {
        execute(op);
    }
    
    @Override
    public void visit(ModExpression op) throws FrontendException {
        execute(op);
    }
    
    @Override
    public void visit(DivideExpression op) throws FrontendException {
        execute(op);
    }

    @Override
    public void visit(MapLookupExpression op) throws FrontendException {
        execute(op);
    }

    @Override
    public void visit(BinCondExpression op) throws FrontendException {
        execute(op);
    }

    @Override
    public void visit(UserFuncExpression op) throws FrontendException {
        execute(op);
    }

    @Override
    public void visit(DereferenceExpression derefenceExpression) throws FrontendException {
        execute(derefenceExpression);
    }

    @Override
    public void visit(RegexExpression op) throws FrontendException {
        execute(op);
    }

}
