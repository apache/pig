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

import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanVisitor;
import org.apache.pig.impl.plan.VisitorException;

public class LOBinCond extends ExpressionOperator {

    // BinCond has a conditional expression and two nested queries.
    // If the conditional expression evaluates to true the first nested query
    // is executed else the second nested query is executed

    private static final long serialVersionUID = 2L;
 
    /**
     * 
     * @param plan
     *            Logical plan this operator is a part of.
     * @param k
     *            Operator key to assign to this node.
     */
    public LOBinCond(LogicalPlan plan, OperatorKey k) {
        super(plan, k);
    }// End Constructor LOBinCond

    public ExpressionOperator getCond() {
        List<LogicalOperator>preds = getPlan().getPredecessors(this);
        if(preds == null)
            return null;
        return (ExpressionOperator)preds.get(0);
    }

    public ExpressionOperator getLhsOp() {
        List<LogicalOperator>preds = getPlan().getPredecessors(this);
        if(preds == null)
            return null;
        return (ExpressionOperator)preds.get(1);
    }

    public ExpressionOperator getRhsOp() {
        List<LogicalOperator>preds = getPlan().getPredecessors(this);
        if(preds == null)
            return null;
        return (ExpressionOperator)preds.get(2);
    }
    
    
    @Override
    public void visit(LOVisitor v) throws VisitorException {
        v.visit(this);
    }

	@Override
	public Schema getSchema() throws FrontendException {
		return mSchema;
	}

    @Override
    public Schema.FieldSchema getFieldSchema() throws FrontendException {
		//We need a check of LHS and RHS schemas
        //The type checker perform this task
        if (!mIsFieldSchemaComputed) {
            try {
                mFieldSchema = getLhsOp().getFieldSchema();
                mIsFieldSchemaComputed = true;
            } catch (FrontendException fee) {
                mFieldSchema = null;
                mIsFieldSchemaComputed = false;
                throw fee;
            }
        }
        return mFieldSchema;
    }

    @Override
    public String name() {
        return "BinCond " + mKey.scope + "-" + mKey.id;
    }

    @Override
    public boolean supportsMultipleInputs() {
        return true;
    }

    /**
     * @see org.apache.pig.impl.logicalLayer.ExpressionOperator#clone()
     * Do not use the clone method directly. Operators are cloned when logical plans
     * are cloned using {@link LogicalPlanCloner}
     */
    @Override
    protected Object clone() throws CloneNotSupportedException {
        LOBinCond clone = (LOBinCond)super.clone();
        return clone;
    }

}
