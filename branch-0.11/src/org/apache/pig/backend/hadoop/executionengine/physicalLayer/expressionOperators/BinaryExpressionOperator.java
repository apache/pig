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

import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.util.IdentityHashSet;

/**
 * A base class for all Binary expression operators.
 * Supports the lhs and rhs operators which are used
 * to fetch the inputs and apply the appropriate operation
 * with the appropriate type.
 *
 */
public abstract class BinaryExpressionOperator extends ExpressionOperator {
    private static final long serialVersionUID = 1L;
    
    protected ExpressionOperator lhs;
    protected ExpressionOperator rhs;
    private transient List<ExpressionOperator> child;
    
    public BinaryExpressionOperator(OperatorKey k) {
        this(k,-1);
    }

    public BinaryExpressionOperator(OperatorKey k, int rp) {
        super(k, rp);
    }

    public ExpressionOperator getLhs() {
        return lhs;
    }
    
    /**
     * Get the child expressions of this expression
     */
    public List<ExpressionOperator> getChildExpressions() {
        if (child == null) {
            child = new ArrayList<ExpressionOperator>();    	
            child.add(lhs);    	
            child.add(rhs);
        }
        return child;
    }
    
    @Override
    public boolean supportsMultipleInputs() {
        return true;
    }

    public void setLhs(ExpressionOperator lhs) {
        this.lhs = lhs;
    }

    public ExpressionOperator getRhs() {
        return rhs;
    }

    public void setRhs(ExpressionOperator rhs) {
        this.rhs = rhs;
    }

    protected void cloneHelper(BinaryExpressionOperator op) {
        // Don't clone these, as they are just references to things already in
        // the plan.
        lhs = op.lhs;
        rhs = op.rhs;
        super.cloneHelper(op);
    }
    
    @Override
    public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
        return null;
    }
    }
