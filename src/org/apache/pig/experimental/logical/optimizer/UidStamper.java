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

package org.apache.pig.experimental.logical.optimizer;

import java.io.IOException;
import java.util.List;

import org.apache.pig.experimental.logical.expression.AddExpression;
import org.apache.pig.experimental.logical.expression.AndExpression;
import org.apache.pig.experimental.logical.expression.BagDereferenceExpression;
import org.apache.pig.experimental.logical.expression.BinCondExpression;
import org.apache.pig.experimental.logical.expression.CastExpression;
import org.apache.pig.experimental.logical.expression.ConstantExpression;
import org.apache.pig.experimental.logical.expression.DivideExpression;
import org.apache.pig.experimental.logical.expression.EqualExpression;
import org.apache.pig.experimental.logical.expression.GreaterThanEqualExpression;
import org.apache.pig.experimental.logical.expression.GreaterThanExpression;
import org.apache.pig.experimental.logical.expression.IsNullExpression;
import org.apache.pig.experimental.logical.expression.LessThanEqualExpression;
import org.apache.pig.experimental.logical.expression.LessThanExpression;
import org.apache.pig.experimental.logical.expression.LogicalExpression;
import org.apache.pig.experimental.logical.expression.LogicalExpressionPlan;
import org.apache.pig.experimental.logical.expression.LogicalExpressionVisitor;
import org.apache.pig.experimental.logical.expression.MapLookupExpression;
import org.apache.pig.experimental.logical.expression.ModExpression;
import org.apache.pig.experimental.logical.expression.MultiplyExpression;
import org.apache.pig.experimental.logical.expression.NegativeExpression;
import org.apache.pig.experimental.logical.expression.NotEqualExpression;
import org.apache.pig.experimental.logical.expression.NotExpression;
import org.apache.pig.experimental.logical.expression.OrExpression;
import org.apache.pig.experimental.logical.expression.ProjectExpression;
import org.apache.pig.experimental.logical.expression.RegexExpression;
import org.apache.pig.experimental.logical.expression.SubtractExpression;
import org.apache.pig.experimental.logical.expression.UserFuncExpression;
import org.apache.pig.experimental.logical.relational.LOLoad;
import org.apache.pig.experimental.logical.relational.LogicalSchema;
import org.apache.pig.experimental.logical.relational.LogicalSchema.LogicalFieldSchema;
import org.apache.pig.experimental.plan.DependencyOrderWalker;
import org.apache.pig.experimental.plan.DepthFirstWalker;
import org.apache.pig.experimental.plan.OperatorPlan;

/**
 * A Visitor to stamp every part of every expression in a tree with a uid.
 */
public class UidStamper extends AllExpressionVisitor {

    /**
     * @param plan LogicalPlan that this stamper will act on.
     */
    public UidStamper(OperatorPlan plan) {
        super(plan, new DependencyOrderWalker(plan));
    }
    
    class ExprUidStamper extends LogicalExpressionVisitor {
        protected ExprUidStamper(OperatorPlan plan) {
            super(plan, new DepthFirstWalker(plan));
        }
        
    
        @Override
        public void visitAnd(AndExpression andExpr) throws IOException {
            andExpr.setUid(currentOp);
        }
        
        @Override
        public void visitOr(OrExpression op) throws IOException {
            op.setUid(currentOp);
        }

        @Override
        public void visitEqual(EqualExpression equal) throws IOException {
            equal.setUid(currentOp);
        }
        
        @Override
        public void visitGreaterThan(GreaterThanExpression greaterThanExpression) throws IOException {
            greaterThanExpression.setUid(currentOp);
        }
        
        @Override
        public void visitGreaterThanEqual(GreaterThanEqualExpression op) throws IOException {
            op.setUid(currentOp);
        }
        
        @Override
        public void visitLessThan(LessThanExpression lessThanExpression) throws IOException {
            lessThanExpression.setUid(currentOp);
        }
        
        @Override
        public void visitLessThanEqual(LessThanEqualExpression op) throws IOException {
            op.setUid(currentOp);
        }
    
        @Override
        public void visitProject(ProjectExpression project) throws IOException {
            project.setUid(currentOp);
        }
        
        @Override
        public void visitMapLookup( MapLookupExpression op ) throws IOException {
            op.setUid(currentOp);
        }
    
        @Override
        public void visitConstant(ConstantExpression constant) throws IOException {
            constant.setUid(currentOp);
        }
        
        @Override
        public void visitCast(CastExpression cast) throws IOException {
            cast.setUid(currentOp);
        }
        
        @Override
        public void visitNotEqual(NotEqualExpression exp) throws IOException { 
            exp.setUid(currentOp);
        }

        @Override
        public void visitNot(NotExpression exp ) throws IOException {
            exp.setUid(currentOp);
        }

        @Override
        public void visitIsNull(IsNullExpression exp) throws IOException {
            exp.setUid(currentOp);
        }
        
        @Override
        public void visitNegative(NegativeExpression exp) throws IOException {
            exp.setUid(currentOp);
        }
        
        @Override
        public void visitAdd(AddExpression exp) throws IOException {
            exp.setUid(currentOp);
        }
        
        @Override
        public void visitSubtract(SubtractExpression exp) throws IOException {
            exp.setUid(currentOp);
        }
       
        @Override
        public void visitMultiply(MultiplyExpression op) throws IOException {
            op.setUid(currentOp);
        }
        
        @Override
        public void visitMod(ModExpression op) throws IOException {
            op.setUid(currentOp);
        }
        
        @Override
        public void visitDivide(DivideExpression op) throws IOException {
            op.setUid(currentOp);
        }
        
        @Override
        public void visitBinCond(BinCondExpression op) throws IOException {
            op.setUid(currentOp);
        }
        
        @Override
        public void visitUserFunc(UserFuncExpression op) throws IOException {
            op.setUid(currentOp);
        }
        
        @Override
        public void visitBagDereference(BagDereferenceExpression op) throws IOException {
            op.setUid(currentOp);
        }
        
        @Override
        public void visitRegex(RegexExpression op) throws IOException {
            op.setUid(currentOp);
        }
    }
    
    /* (non-Javadoc)
     * @see org.apache.pig.experimental.logical.optimizer.AllExpressionVisitor#getVisitor(org.apache.pig.experimental.logical.expression.LogicalExpressionPlan)
     */
    @Override
    protected LogicalExpressionVisitor getVisitor(LogicalExpressionPlan expr) {
        return new ExprUidStamper(expr);
    }

    @Override
    public void visitLOLoad(LOLoad load) throws IOException {
        super.visitLOLoad(load);
        
        LogicalSchema s = load.getSchema();
        stampSchema(s);
    }
    
    private void stampSchema(LogicalSchema s) {
        if (s != null) {
            List<LogicalFieldSchema> l = s.getFields();
            for(LogicalFieldSchema f: l) {
                f.uid = LogicalExpression.getNextUid();
                stampSchema(f.schema);
            }
        }
    }      
}
