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

public class NotExpression extends UnaryExpression {

    public NotExpression(OperatorPlan plan, byte b, LogicalExpression exp) {
        super("Not", plan, b, exp);        
    }

    @Override
    public void accept(PlanVisitor v) throws IOException {
        if (!(v instanceof LogicalExpressionVisitor)) {
            throw new IOException("Expected LogicalExpressionVisitor");
        }
        ((LogicalExpressionVisitor)v).visitNot(this);
    }

    @Override
    public boolean isEqual(Operator other) {
        if (other != null && other instanceof NotExpression) { 
            NotExpression of = (NotExpression)other;
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
