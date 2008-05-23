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
package org.apache.pig.impl.physicalLayer.expressionOperators;

import org.apache.pig.impl.logicalLayer.OperatorKey;

/**
 * This is a base class for all comparison operators. Supports the
 * use of operand type instead of result type as the result type is
 * always boolean.
 * 
 * All comparison operators fetch the lhs and rhs operands and compare
 * them for each type using different comparison methods based on what
 * comparison is being implemented.
 *
 */
public abstract class ComparisonOperator extends BinaryExpressionOperator {
    //The result type for comparison operators is always
    //Boolean. So the plans evaluating these should consider
    //the type of the operands instead of the result.
    //The result will be comunicated using the Status object.
    //This is a slight abuse of the status object.
    protected byte operandType;
    
    public ComparisonOperator(OperatorKey k) {
        this(k,-1);
    }

    public ComparisonOperator(OperatorKey k, int rp) {
        super(k, rp);
    }

    public byte getOperandType() {
        return operandType;
    }

    public void setOperandType(byte operandType) {
        this.operandType = operandType;
    }
}
