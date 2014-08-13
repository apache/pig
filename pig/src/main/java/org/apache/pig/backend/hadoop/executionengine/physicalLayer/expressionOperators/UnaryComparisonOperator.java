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

import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.util.IdentityHashSet;

/**
 * This is a base class for all unary comparison operators. Supports the
 * use of operand type instead of result type as the result type is
 * always boolean.
 * 
 */
public abstract class UnaryComparisonOperator extends UnaryExpressionOperator
        implements ComparisonOperator {
    //The result type for comparison operators is always
    //Boolean. So the plans evaluating these should consider
    //the type of the operands instead of the result.
    //The result will be comunicated using the Status object.
    //This is a slight abuse of the status object.
    protected byte operandType;
    
    public UnaryComparisonOperator(OperatorKey k) {
        this(k,-1);
    }

    public UnaryComparisonOperator(OperatorKey k, int rp) {
        super(k, rp);
    }

    public byte getOperandType() {
        return operandType;
    }

    public void setOperandType(byte operandType) {
        this.operandType = operandType;
    }
    
    @Override
    public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
        if(illustrator != null) {
            illustrator.setSubExpResult(eqClassIndex == 0);
        }
        return null;
    }
}
