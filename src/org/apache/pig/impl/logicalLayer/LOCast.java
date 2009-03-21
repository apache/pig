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

import org.apache.pig.FuncSpec;
import org.apache.pig.LoadFunc;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanVisitor;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.data.DataType;

public class LOCast extends ExpressionOperator {

    // Cast has an expression that has to be converted to a specified type

    private static final long serialVersionUID = 2L;
    private FuncSpec mLoadFuncSpec = null;

    /**
     * 
     * @param plan
     *            Logical plan this operator is a part of.
     * @param k
     *            Operator key to assign to this node.
     * @param type
     *            the type to which the expression is cast
     */
    public LOCast(LogicalPlan plan, OperatorKey k, byte type) {
        super(plan, k);
        mType = type;
    }// End Constructor LOCast

    public ExpressionOperator getExpression() {
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
    public Schema getSchema() {
        return mSchema;
    }

    @Override
    public Schema.FieldSchema getFieldSchema() throws FrontendException {
        if(!mIsFieldSchemaComputed) {
            mFieldSchema = new Schema.FieldSchema(null, mType);
            mIsFieldSchemaComputed = true;
        }
        return mFieldSchema;
    }

    @Override
    public String name() {
        return "Cast " + mKey.scope + "-" + mKey.id;
    }

    @Override
    public boolean supportsMultipleInputs() {
        return false;
    }

    public FuncSpec getLoadFuncSpec() {
        return mLoadFuncSpec;
    }

    public void setLoadFuncSpec(FuncSpec loadFuncSpec) {
        mLoadFuncSpec = loadFuncSpec;
    }

    /**
     * @see org.apache.pig.impl.logicalLayer.ExpressionOperator#clone()
     * Do not use the clone method directly. Operators are cloned when logical plans
     * are cloned using {@link LogicalPlanCloner}
     */
    @Override
    protected Object clone() throws CloneNotSupportedException {
        LOCast clone = (LOCast)super.clone();
        if(mLoadFuncSpec != null) {
            clone.mLoadFuncSpec = mLoadFuncSpec.clone();
        }
        return clone;
    }

}
