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

import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanVisitor;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.data.DataType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This abstract class represents the logical Unary Expression Operator The
 * unary operator has an operand and an operator. The format of the expression
 * is operator operand. The operator is implicit and not recorded in the class
 */
public abstract class UnaryExpressionOperator extends ExpressionOperator {
    private static final long serialVersionUID = 2L;
    private static Log log = LogFactory.getLog(UnaryExpressionOperator.class);

    /**
     * @param plan
     *            Logical plan this operator is a part of.
     * @param k
     *            Operator key to assign to this node.
     * @param rp
     *            degree of requested parallelism with which to execute this
     *            node.
     */
    public UnaryExpressionOperator(LogicalPlan plan, OperatorKey k, int rp) {
        super(plan, k, rp);
    }

    /**
     * @param plan
     *            Logical plan this operator is a part of.
     * @param k
     *            Operator key to assign to this node.
     */
    public UnaryExpressionOperator(LogicalPlan plan, OperatorKey k) {
        super(plan, k);

    }
    
    public ExpressionOperator getOperand() {
        List<LogicalOperator>preds = getPlan().getPredecessors(this);
        if(preds == null)
            return null;
        return (ExpressionOperator)preds.get(0);
    }

    @Override
    public void visit(LOVisitor v) throws VisitorException {
        v.visit(this);
    }

    @Override
    public boolean supportsMultipleInputs() {
        return false;
    }

    /**
     * @see org.apache.pig.impl.logicalLayer.ExpressionOperator#clone()
     * Do not use the clone method directly. Operators are cloned when logical plans
     * are cloned using {@link LogicalPlanCloner}
     */
    @Override
    protected Object clone() throws CloneNotSupportedException {
        UnaryExpressionOperator unExOpClone = (UnaryExpressionOperator)super.clone();
        return unExOpClone;
    }

}
