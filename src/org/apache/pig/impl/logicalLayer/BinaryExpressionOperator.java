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

import java.util.List;

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
    private static Log log = LogFactory.getLog(BinaryExpressionOperator.class);

    /**
     * @param plan
     *            Logical plan this operator is a part of.
     * @param k
     *            Operator key to assign to this node.
     * @param rp
     *            degree of requested parallelism with which to execute this
     *            node.
     */
    public BinaryExpressionOperator(LogicalPlan plan, OperatorKey k, int rp) {
        super(plan, k, rp);
    }

    /**
     * @param plan
     *            Logical plan this operator is a part of.
     * @param k
     *            Operator key to assign to this node.
     */
    public BinaryExpressionOperator(LogicalPlan plan, OperatorKey k) {
        super(plan, k);
    }
    
    public ExpressionOperator getLhsOperand() {
        List<LogicalOperator>preds = getPlan().getPredecessors(this);
        if(preds == null)
            return null;
        return (ExpressionOperator)preds.get(0);
    }

    public ExpressionOperator getRhsOperand() {
        List<LogicalOperator>preds = getPlan().getPredecessors(this);
        if(preds == null)
            return null;
        return (ExpressionOperator)preds.get(1);
    }
        
    @Override
    public void visit(LOVisitor v) throws VisitorException {
        v.visit(this);
    }
    
    @Override
    public boolean supportsMultipleInputs() {
        return true;
    }

    /**
     * @see org.apache.pig.impl.logicalLayer.ExpressionOperator#clone()
     * Do not use the clone method directly. Operators are cloned when logical plans
     * are cloned using {@link LogicalPlanCloner}
     */
    @Override
    protected Object clone() throws CloneNotSupportedException {
        BinaryExpressionOperator binExOpClone = (BinaryExpressionOperator)super.clone();
        return binExOpClone;
    }

}
