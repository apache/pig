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

import java.io.IOException;

import org.apache.pig.impl.logicalLayer.parser.ParseException;
import org.apache.pig.impl.plan.PlanVisitor;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class LOBinCond extends ExpressionOperator {

    // BinCond has a conditional expression and two nested queries.
    // If the conditional expression evaluates to true the first nested query
    // is executed else the second nested query is executed

    private static final long serialVersionUID = 2L;
    private ExpressionOperator mCond;
    private ExpressionOperator mLhsOp;
    private ExpressionOperator mRhsOp;

    /**
     * 
     * @param plan
     *            Logical plan this operator is a part of.
     * @param k
     *            Operator key to assign to this node.
     * @param rp
     *            degree of requested parallelism with which to execute this
     *            node.
     * @param cond
     *            ExpressionOperator the expression specifying condition
     * @param lhsOp
     *            ExpressionOperator query to be executed when condition is true
     * @param rhsOp
     *            ExpressionOperator query to be executed when condition is
     *            false
     */
    public LOBinCond(LogicalPlan plan, OperatorKey k, int rp,
            ExpressionOperator cond, ExpressionOperator lhsOp,
            ExpressionOperator rhsOp) {
        super(plan, k, rp);
        mCond = cond;
        mLhsOp = lhsOp;
        mRhsOp = rhsOp;

    }// End Constructor LOBinCond

    public ExpressionOperator getCond() {
        return mCond;
    }

    public ExpressionOperator getLhsOp() {
        return mLhsOp;
    }

    public ExpressionOperator getRhsOp() {
        return mRhsOp;
    }

    @Override
    public void visit(LOVisitor v) throws ParseException {
        v.visit(this);
    }

    @Override
    public Schema getSchema() throws IOException {
        if (!mIsSchemaComputed && (null == mSchema)) {
            try {
                mSchema = mLhsOp.getSchema();
                mIsSchemaComputed = true;
            } catch (IOException ioe) {
                mSchema = null;
                mIsSchemaComputed = false;
                throw ioe;
            }
        }
        return mSchema;
    }

    @Override
    public String name() {
        return "BinCond " + mKey.scope + "-" + mKey.id;
    }

    @Override
    public boolean supportsMultipleInputs() {
        return true;
    }

}
