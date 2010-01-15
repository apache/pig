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

import org.apache.pig.experimental.plan.OperatorPlan;
import org.apache.pig.experimental.plan.PlanVisitor;

/**
 * A constant
 *
 */
public class ConstantExpression extends ColumnExpression {
    
    // Stupid Java needs a union
    Object val;
    
    /**
     * Adds expression to the plan 
     * @param plan LogicalExpressionPlan this constant is a part of.
     * @param type type of the constant.  This could be determined dynamically,
     * but it would require a long chain of instanceofs, and the parser will 
     * already know the type, so there's no reason to take the performance hit.
     * @param val Value of this constant.
     */
    public ConstantExpression(OperatorPlan plan, byte type, Object val) {
        super("Constant", plan, type);
        this.val = val;
        plan.add(this);
    }

    /**
     * @link org.apache.pig.experimental.plan.Operator#accept(org.apache.pig.experimental.plan.PlanVisitor)
     */
    @Override
    public void accept(PlanVisitor v) {
        if (!(v instanceof LogicalExpressionVisitor)) {
            throw new RuntimeException("Expected LogicalExpressionVisitor");
        }
        ((LogicalExpressionVisitor)v).visitConstant(this);

    }

    /**
     * Get the value of this constant.
     * @return value of the constant
     */
    public Object getValue() {
        return val;
    }

}
