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

import org.apache.pig.FuncSpec;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanVisitor;
import org.apache.pig.newplan.logical.relational.LogicalSchema;

public class CastExpression extends UnaryExpression {
    private FuncSpec castFunc;
    private LogicalSchema.LogicalFieldSchema castSchema;

    public CastExpression(OperatorPlan plan, LogicalExpression exp, LogicalSchema.LogicalFieldSchema fs) {
        super("Cast", plan, exp);
        castSchema = fs;
    }

    @Override
    public void accept(PlanVisitor v) throws FrontendException {
        if (!(v instanceof LogicalExpressionVisitor)) {
            throw new FrontendException("Expected LogicalExpressionVisitor", 2222);
        }
        ((LogicalExpressionVisitor)v).visit(this);
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
    public boolean isEqual(Operator other) throws FrontendException {
        if (other != null && other instanceof CastExpression) { 
            CastExpression of = (CastExpression)other;
            return plan.isEqual(of.plan) && getExpression().isEqual( of.getExpression() );
        } else {
            return false;
        }
    }
    
    @Override
    public LogicalSchema.LogicalFieldSchema getFieldSchema() throws FrontendException {
        if (fieldSchema!=null)
            return fieldSchema;
        fieldSchema = new LogicalSchema.LogicalFieldSchema(null, castSchema.schema, castSchema.type);
        uidOnlyFieldSchema = fieldSchema.mergeUid(uidOnlyFieldSchema);
        // Bring back the top level uid, this is not changed
        LogicalExpression exp = (LogicalExpression)plan.getSuccessors(this).get(0);
        fieldSchema.uid = exp.getFieldSchema().uid;
        return fieldSchema;
    }
}
