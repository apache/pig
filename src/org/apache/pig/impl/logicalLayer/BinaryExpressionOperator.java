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

package org.apache.pig.impl.logicalLayer;

import org.apache.pig.impl.plan.PlanVisitor;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This abstract class represents the logical Binary Expression Operator
 * The binary operator has two operands and an operator. The format of
 * the expression is lhs_operand operator rhs_operand. The operator name
 * is assumed and can be inferred by the class name 
 */

public abstract class BinaryExpressionOperator extends ExpressionOperator {
    private static final long serialVersionUID = 2L;
    private ExpressionOperator mLhsOperand; //left hand side operand
    private ExpressionOperator mRhsOperand; //right hand side operand
    private static Log log = LogFactory.getLog(BinaryExpressionOperator.class);

    /**
     * @param plan
     *            Logical plan this operator is a part of.
     * @param k
     *            Operator key to assign to this node.
     * @param rp
     *            degree of requested parallelism with which to execute this
     *            node.
     * @param lhsOperand
     *            ExpressionOperator the left hand side operand
     * @param rhsOperand
     *            ExpressionOperator the right hand side operand
     */
    public BinaryExpressionOperator(LogicalPlan plan, OperatorKey k, int rp,
            ExpressionOperator lhsOperand, ExpressionOperator rhsOperand) {
        super(plan, k, rp);
        mLhsOperand = lhsOperand;
        mRhsOperand = rhsOperand;
    }

    /**
     * @param plan
     *            Logical plan this operator is a part of.
     * @param k
     *            Operator key to assign to this node.
     * @param lhsOperand
     *            ExpressionOperator the left hand side operand
     * @param rhsOperand
     *            ExpressionOperator the right hand side operand
     */
    public BinaryExpressionOperator(LogicalPlan plan, OperatorKey k,
            ExpressionOperator lhsOperand, ExpressionOperator rhsOperand) {
        super(plan, k);
        mLhsOperand = lhsOperand;
        mRhsOperand = rhsOperand;
    }
    
    public ExpressionOperator getLhsOperand() {
        return mLhsOperand;
    }

    public ExpressionOperator getRhsOperand() {
        return mRhsOperand;
    }
    
    public void setLhsOperand(ExpressionOperator lhs) {
        mLhsOperand = lhs ;
    }

    public void setRhsOperand(ExpressionOperator rhs) {
        mRhsOperand = rhs ;
    }
    
    @Override
    public void visit(LOVisitor v) throws VisitorException {
        v.visit(this);
    }
    
    @Override
    public boolean supportsMultipleInputs() {
        return true;
    }

}
