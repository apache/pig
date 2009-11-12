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
package org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators;

import java.util.ArrayList;
import java.util.List;

import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.ExpressionOperator;
import org.apache.pig.impl.plan.VisitorException;

public abstract class UnaryExpressionOperator extends ExpressionOperator {

    ExpressionOperator expr;
    private transient List<ExpressionOperator> child;
    
    public UnaryExpressionOperator(OperatorKey k, int rp) {
        super(k, rp);
        
    }

    public UnaryExpressionOperator(OperatorKey k) {
        super(k);
        
    }

    @Override
    public boolean supportsMultipleInputs() {
        // TODO Auto-generated method stub
        return false;
    }

    /**
     * Set the contained expression to the be the input value.
     */
    public void setInputAsExpr(PhysicalPlan plan) {
        expr = (ExpressionOperator)plan.getPredecessors(this).get(0);
    }

    /**
     * Set the contained expression explicitly.  This is mostly for testing.
     * @param e Expression to contain.
     */
    public void setExpr(ExpressionOperator e) {
        expr = e;
    }

    /**
     * Get the contained expression.
     * @return contained expression.
     */
    public ExpressionOperator getExpr() { 
        return expr;
    }

    protected void cloneHelper(UnaryExpressionOperator op) {
        // Don't clone this, as it is just a reference to something already in
        // the plan.
        expr = op.expr;
        resultType = op.getResultType();
    }

    /**
     * Get child expression of this expression
     */
    @Override
    public List<ExpressionOperator> getChildExpressions() {
        if (child == null) {
            child = new ArrayList<ExpressionOperator>();		
            child.add(expr);		
        }
        
        return child;		
    }

}
