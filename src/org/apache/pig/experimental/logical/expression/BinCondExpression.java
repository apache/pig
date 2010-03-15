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

import org.apache.pig.experimental.plan.Operator;
import org.apache.pig.experimental.plan.OperatorPlan;
import org.apache.pig.experimental.plan.PlanVisitor;

public class BinCondExpression extends LogicalExpression {

    /**
     * Will add this operator to the plan and connect it to the 
     * left and right hand side operators and the condition operator
     * @param plan plan this operator is part of
     * @param b Datatype of this expression
     * @param lhs expression on its left hand side
     * @param rhs expression on its right hand side
     */
    public BinCondExpression(OperatorPlan plan,
                            byte b,
                            LogicalExpression condition,
                            LogicalExpression lhs,
                            LogicalExpression rhs) {
        super("BinCond", plan, b);
        plan.add(this);
        plan.connect(this, condition);
        plan.connect(this, lhs);
        plan.connect(this, rhs);
    }
    
    /**
     * Returns the operator which handles this condition
     * @return expression which handles the condition
     * @throws IOException
     */
    public LogicalExpression getCondition() throws IOException {
        return (LogicalExpression)plan.getSuccessors(this).get(0);
    }

    /**
     * Get the left hand side of this expression.
     * @return expression on the left hand side
     * @throws IOException 
     */
    public LogicalExpression getLhs() throws IOException {
        return (LogicalExpression)plan.getSuccessors(this).get(1);        
    }

    /**
     * Get the right hand side of this expression.
     * @return expression on the right hand side
     * @throws IOException 
     */
    public LogicalExpression getRhs() throws IOException {
        return (LogicalExpression)plan.getSuccessors(this).get(2);
    }

    /**
     * @link org.apache.pig.experimental.plan.Operator#accept(org.apache.pig.experimental.plan.PlanVisitor)
     */
    @Override
    public void accept(PlanVisitor v) throws IOException {
        if (!(v instanceof LogicalExpressionVisitor)) {
            throw new IOException("Expected LogicalExpressionVisitor");
        }
        ((LogicalExpressionVisitor)v).visitBinCond(this);
    }
    
    @Override
    public boolean isEqual(Operator other) {
        if (other != null && other instanceof BinCondExpression) {
            BinCondExpression ao = (BinCondExpression)other;
            try {
                return ao.getCondition().isEqual(getCondition()) && 
                ao.getLhs().isEqual(getLhs()) && ao.getRhs().isEqual(getRhs());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            return false;
        }
    }

}
