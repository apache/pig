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

package org.apache.pig.experimental.logical.expression;

import org.apache.pig.experimental.plan.Operator;
import org.apache.pig.experimental.plan.OperatorPlan;

/**
 * Logical representation of expression operators.  Expression operators have
 * a data type and a uid.  Uid is a unique id for each expression. 
 *
 */
public abstract class LogicalExpression extends Operator {
    
    protected byte type;
    protected long uid;

    /**
     * 
     * @param name of the operator
     * @param plan LogicalExpressionPlan this is part of
     * @param b datatype of this expression
     */
    public LogicalExpression(String name, OperatorPlan plan, byte b) {
        super(name, plan);
        type = b;
    }
    
    /**
     * Get the data type for this expression.
     * @return data type, one of the static bytes of DataType
     */
    public byte getType() {
        return type;
    }
    
    /**
     * Get the unique identifier for this expression
     * @return unique identifier
     */
    public long getUid() {
        return uid;
    }
    
    /**
     * Set the unique identify for this expression
     * @param uid unique identifier
     */
    public void setUid(long uid) {
       this.uid = uid; 
    }

}
