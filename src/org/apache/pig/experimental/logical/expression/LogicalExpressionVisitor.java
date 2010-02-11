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

package org.apache.pig.experimental.logical.expression;

import java.io.IOException;
import org.apache.pig.experimental.plan.OperatorPlan;
import org.apache.pig.experimental.plan.PlanVisitor;
import org.apache.pig.experimental.plan.PlanWalker;

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
    
    public void visitAnd(AndExpression andExpr) throws IOException {
    }
    
    public void visitOr(OrExpression exp) throws IOException { 
    }

    public void visitEqual(EqualExpression equal) throws IOException {
    }
    
    public void visitProject(ProjectExpression project) throws IOException {
    }
    
    public void visitConstant(ConstantExpression constant) throws IOException {
    }
    
    public void visitCast(CastExpression cast) throws IOException {
    }

    public void visitGreaterThan(GreaterThanExpression greaterThanExpression) throws IOException {
    }
    
    public void visitGreaterThanEqual(GreaterThanEqualExpression op) throws IOException {
    }

    public void visitLessThan(LessThanExpression lessThanExpression) throws IOException { 
    }
    
    public void visitLessThanEqual(LessThanEqualExpression op) throws IOException {
    }
}
