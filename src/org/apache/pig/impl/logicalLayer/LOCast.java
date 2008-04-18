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

public class LOCast extends ExpressionOperator {

    // Cast has an expression that has to be converted to a specified type

    private static final long serialVersionUID = 2L;
    private ExpressionOperator mExpr;

    /**
     * 
     * @param plan
     *            Logical plan this operator is a part of.
     * @param k
     *            Operator key to assign to this node.
     * @param rp
     *            degree of requested parallelism with which to execute this
     *            node.
     * @param expr
     *            the expression whose type has to be cast
     * @param type
     *            the type to which the expression is cast
     */
    public LOCast(LogicalPlan plan, OperatorKey k, int rp,
            ExpressionOperator expr, byte type) {
        super(plan, k, rp);
        mExpr = expr;
        mType = type;
    }// End Constructor LOCast

    public ExpressionOperator getExpression() {
        return mExpr;
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
        return "Cast " + mKey.scope + "-" + mKey.id;
    }

    @Override
    public boolean supportsMultipleInputs() {
        return false;
    }

}
