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

import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.plan.PlanVisitor;
import org.apache.pig.impl.logicalLayer.schema.Schema;

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
    public String name() {
        return "Const " + mKey.scope + "-" + mKey.id;
    }

    @Override
    public boolean supportsMultipleInputs() {
        return false;
    }

}
