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
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanVisitor;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.newplan.logical.relational.LogicalSchema.LogicalFieldSchema;
import org.apache.pig.parser.SourceLocation;

public class BinCondExpression extends LogicalExpression {

    /**
     * Will add this operator to the plan and connect it to the
     * left and right hand side operators and the condition operator
     * @param plan plan this operator is part of
     * @param lhs expression on its left hand side
     * @param rhs expression on its right hand side
     */
    public BinCondExpression(OperatorPlan plan,
                            LogicalExpression condition,
                            LogicalExpression lhs,
                            LogicalExpression rhs) {
        super("BinCond", plan);
        plan.add(this);
        plan.connect(this, condition);
        plan.connect(this, lhs);
        plan.connect(this, rhs);
    }

    /**
     * Returns the operator which handles this condition
     * @return expression which handles the condition
     * @throws FrontendException
     */
    public LogicalExpression getCondition() throws FrontendException {
        return (LogicalExpression)plan.getSuccessors(this).get(0);
    }

    /**
     * Get the left hand side of this expression.
     * @return expression on the left hand side
     * @throws FrontendException
     */
    public LogicalExpression getLhs() throws FrontendException {
        return (LogicalExpression)plan.getSuccessors(this).get(1);
    }

    /**
     * Get the right hand side of this expression.
     * @return expression on the right hand side
     * @throws FrontendException
     */
    public LogicalExpression getRhs() throws FrontendException {
        return (LogicalExpression)plan.getSuccessors(this).get(2);
    }

    /**
     * @link org.apache.pig.newplan.Operator#accept(org.apache.pig.newplan.PlanVisitor)
     */
    @Override
    public void accept(PlanVisitor v) throws FrontendException {
        if (!(v instanceof LogicalExpressionVisitor)) {
            throw new FrontendException("Expected LogicalExpressionVisitor", 2222);
        }
        ((LogicalExpressionVisitor)v).visit(this);
    }

    @Override
    public boolean isEqual(Operator other) throws FrontendException {
        if (other != null && other instanceof BinCondExpression) {
            BinCondExpression ao = (BinCondExpression)other;
            return ao.getCondition().isEqual(getCondition()) &&
            ao.getLhs().isEqual(getLhs()) && ao.getRhs().isEqual(getRhs());
        } else {
            return false;
        }
    }

    @Override
    public LogicalSchema.LogicalFieldSchema getFieldSchema() throws FrontendException {
        if (fieldSchema!=null)
            return fieldSchema;

        //TypeCheckingExpVisitor will ensure that lhs and rhs have same schema
        LogicalFieldSchema argFs = getLhs().getFieldSchema();
        fieldSchema = argFs.deepCopy();
        fieldSchema.resetUid();

        uidOnlyFieldSchema = fieldSchema.mergeUid(uidOnlyFieldSchema);
        return fieldSchema;
    }

    @Override
    public LogicalExpression deepCopy(LogicalExpressionPlan lgExpPlan) throws FrontendException {
        LogicalExpression copy = new BinCondExpression(
                lgExpPlan,
                this.getCondition().deepCopy(lgExpPlan),
                this.getLhs().deepCopy(lgExpPlan),
                this.getRhs().deepCopy(lgExpPlan) );
        copy.setLocation( new SourceLocation( location ) );
        return copy;
    }
}
