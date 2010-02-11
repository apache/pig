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

import org.apache.pig.FuncSpec;
import org.apache.pig.experimental.logical.relational.LOFilter;
import org.apache.pig.experimental.plan.Operator;
import org.apache.pig.experimental.plan.OperatorPlan;
import org.apache.pig.experimental.plan.PlanVisitor;

public class CastExpression extends UnaryExpression {
    private FuncSpec castFunc;

    public CastExpression(OperatorPlan plan, byte b, LogicalExpression exp) {
        super("Cast", plan, b, exp);		
    }

    @Override
    public void accept(PlanVisitor v) throws IOException {
        if (!(v instanceof LogicalExpressionVisitor)) {
            throw new IOException("Expected LogicalExpressionVisitor");
        }
        ((LogicalExpressionVisitor)v).visitCast(this);
    }

    /**
     * Set the <code>FuncSpec</code> that performs the casting functionality
     * @param spec the <code>FuncSpec</code> that does the casting
     */
    public void setFuncSpec(FuncSpec spec) {
        castFunc = spec;
    }
    
    /**
     * Get the <code>FuncSpec</code> that performs the casting functionality
     * @return the <code>FuncSpec</code> that does the casting
     */
    public FuncSpec getFuncSpec() {
        return castFunc;
    }

    @Override
    public boolean isEqual(Operator other) {
        if (other != null && other instanceof CastExpression) { 
            CastExpression of = (CastExpression)other;
            try {
                return plan.isEqual(of.plan) && getExpression().isEqual( of.getExpression() );
            } catch (IOException e) {
                return false;
            }
        } else {
            return false;
        }
    }
}
