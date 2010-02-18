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

/**
 * Superclass for all binary expressions
 *
 */
public abstract class BinaryExpression extends LogicalExpression {
    
    /**
     * Will add this operator to the plan and connect it to the 
     * left and right hand side operators.
     * @param name of the operator
     * @param plan plan this operator is part of
     * @param b Datatype of this expression
     * @param lhs expression on its left hand side
     * @param rhs expression on its right hand side
     */
    public BinaryExpression(String name,
                            OperatorPlan plan,
                            byte b,
                            LogicalExpression lhs,
                            LogicalExpression rhs) {
        super(name, plan, b);
        plan.add(this);
        plan.connect(this, lhs);
        plan.connect(this, rhs);
    }

    /**
     * Get the left hand side of this binary expression.
     * @return expression on the left hand side
     * @throws IOException 
     */
    public LogicalExpression getLhs() throws IOException {
        return (LogicalExpression)plan.getSuccessors(this).get(0);        
    }

    /**
     * Get the right hand side of this binary expression.
     * @return expression on the right hand side
     * @throws IOException 
     */
    public LogicalExpression getRhs() throws IOException {
        return (LogicalExpression)plan.getSuccessors(this).get(1);
        
    }
}
