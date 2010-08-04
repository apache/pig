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

import java.io.IOException;

import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanWalker;

public abstract class AllSameExpressionVisitor extends LogicalExpressionVisitor {

    public AllSameExpressionVisitor(OperatorPlan plan, PlanWalker walker) {
        super(plan, walker);
    }
    
    /**
     * Method to call on every node in the logical expression plan.
     * @param op Node that is currently being visited.
     */
    abstract protected void execute(LogicalExpression op) throws IOException;
    
    @Override
    public void visit(AndExpression andExpr) throws IOException {
        execute(andExpr);
    }
    
    @Override
    public void visit(OrExpression exp) throws IOException {
        execute(exp);
    }

    @Override
    public void visit(EqualExpression equal) throws IOException {
        execute(equal);
    }
    
    @Override
    public void visit(ProjectExpression project) throws IOException {
        execute(project);
    }
    
    @Override
    public void visit(ConstantExpression constant) throws IOException {
        execute(constant);
    }
    
    @Override
    public void visit(CastExpression cast) throws IOException {
        execute(cast);
    }

    @Override
    public void visit(GreaterThanExpression greaterThanExpression) throws IOException {
        execute(greaterThanExpression);
    }
    
    @Override
    public void visit(GreaterThanEqualExpression op) throws IOException {
        execute(op);
    }

    @Override
    public void visit(LessThanExpression lessThanExpression) throws IOException {
        execute(lessThanExpression);
    }
    
    @Override
    public void visit(LessThanEqualExpression op) throws IOException {
        execute(op);
    }

    @Override
    public void visit(NotEqualExpression op) throws IOException {
        execute(op);
    }

    @Override
    public void visit(NotExpression op) throws IOException {
        execute(op);
    }

    @Override
    public void visit(IsNullExpression op) throws IOException {
        execute(op);
    }
    
    @Override
    public void visit(NegativeExpression op) throws IOException {
        execute(op);
    }
    
    @Override
    public void visit(AddExpression op) throws IOException {
        execute(op);
    }
    
    @Override
    public void visit(SubtractExpression op) throws IOException {
        execute(op);
    }
    
    @Override
    public void visit(MultiplyExpression op) throws IOException {
        execute(op);
    }
    
    @Override
    public void visit(ModExpression op) throws IOException {
        execute(op);
    }
    
    @Override
    public void visit(DivideExpression op) throws IOException {
        execute(op);
    }

    @Override
    public void visit(MapLookupExpression op) throws IOException {
        execute(op);
    }

    @Override
    public void visit(BinCondExpression op) throws IOException {
        execute(op);
    }

    @Override
    public void visit(UserFuncExpression op) throws IOException {
        execute(op);
    }

    @Override
    public void visit(DereferenceExpression derefenceExpression) throws IOException {
        execute(derefenceExpression);
    }

    @Override
    public void visit(RegexExpression op) throws IOException {
        execute(op);
    }

}
