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

import org.apache.pig.PigException;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanVisitor;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.data.DataType;

public class LOConst extends ExpressionOperator {

    // Cast has an expression that has to be converted to a specified type

    private static final long serialVersionUID = 2L;
    private Object mValue;

    /**
     * 
     * @param plan
     *            Logical plan this operator is a part of.
     * @param k
     *            Operator key to assign to this node.
     * @param value
     *            the value of the constant
     */
    public LOConst(LogicalPlan plan, OperatorKey k, Object value) {
        super(plan, k);
        mValue = value;
    }// End Constructor LOConst

    public Object getValue() {
        return mValue;
    }

    @Override
    public void visit(LOVisitor v) throws VisitorException {
        v.visit(this);
    }

    @Override
    public Schema getSchema() {
        return mSchema;
    }

    @Override
    public Schema.FieldSchema getFieldSchema() throws FrontendException {
        if(!mIsFieldSchemaComputed) {
            try {
                mFieldSchema = DataType.determineFieldSchema(mValue);
                mIsFieldSchemaComputed = true;
            } catch (Exception e) {
                mFieldSchema = null;
                mIsFieldSchemaComputed = false;
                int errCode = 1015;
                String msg = "Error determining fieldschema of constant: " + this;
                throw new FrontendException(msg, errCode, PigException.INPUT, false, null, e);
            }
        }
        return mFieldSchema;
    }

    @Override
    public String name() {
        return "Const " + mKey.scope + "-" + mKey.id + "( " + mValue + " )";
    }

    @Override
    public boolean supportsMultipleInputs() {
        return false;
    }

    // This allows us to assign a constant to an alias
    @Override
    public boolean supportsMultipleOutputs() {
        return true;
    }

    /**
     * @see org.apache.pig.impl.logicalLayer.ExpressionOperator#clone()
     * Do not use the clone method directly. Operators are cloned when logical plans
     * are cloned using {@link LogicalPlanCloner}
     */
    @Override
    protected Object clone() throws CloneNotSupportedException {
        LOConst clone = (LOConst)super.clone();
        return clone;
    }

}
