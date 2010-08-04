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
import org.apache.pig.newplan.PlanVisitor;
import org.apache.pig.newplan.PlanWalker;

/**
 * A visitor for expression plans.
 */
public abstract class LogicalExpressionVisitor extends PlanVisitor {

    protected LogicalExpressionVisitor(OperatorPlan p,
                                       PlanWalker walker) {
        super(p, walker);
        
        if (!(plan instanceof LogicalExpressionPlan)) {
            throw new RuntimeException(
                "LogicalExpressionVisitor expects to visit " +
                "expression plans.");
        }
    }
    
    public void visit(AndExpression op) throws IOException {
    }
    
    public void visit(OrExpression op) throws IOException { 
    }

    public void visit(EqualExpression op) throws IOException {
    }
    
    public void visit(ProjectExpression op) throws IOException {
    }
    
    public void visit(ConstantExpression op) throws IOException {
    }
    
    public void visit(CastExpression op) throws IOException {
    }

    public void visit(GreaterThanExpression op) throws IOException {
    }
    
    public void visit(GreaterThanEqualExpression op) throws IOException {
    }

    public void visit(LessThanExpression op) throws IOException { 
    }
    
    public void visit(LessThanEqualExpression op) throws IOException {
    }

    public void visit(NotEqualExpression op) throws IOException { 
    }

    public void visit(NotExpression op ) throws IOException {
    }

    public void visit(IsNullExpression op) throws IOException {
    }
    
    public void visit(NegativeExpression op) throws IOException {
    }
    
    public void visit(AddExpression op) throws IOException {
    }
    
    public void visit(SubtractExpression op) throws IOException {
    }
    
    public void visit(MultiplyExpression op) throws IOException {
    }
    
    public void visit(ModExpression op) throws IOException {
    }
    
    public void visit(DivideExpression op) throws IOException {
    }

    public void visit(MapLookupExpression op) throws IOException {
    }

    public void visit(BinCondExpression op) throws IOException {        
    }

    public void visit(UserFuncExpression op) throws IOException {
    }

    public void visit(DereferenceExpression op) throws IOException {
    }

    public void visit(RegexExpression op) throws IOException {
    }
}
