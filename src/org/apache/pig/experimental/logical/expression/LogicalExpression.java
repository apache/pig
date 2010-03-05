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

import java.io.IOException;

import org.apache.pig.data.DataType;
import org.apache.pig.experimental.logical.relational.LogicalRelationalOperator;
import org.apache.pig.experimental.plan.Operator;
import org.apache.pig.experimental.plan.OperatorPlan;

/**
 * Logical representation of expression operators.  Expression operators have
 * a data type and a uid.  Uid is a unique id for each expression. 
 *
 */
public abstract class LogicalExpression extends Operator {
    
    static long nextUid = 1;
    protected byte type;
    protected long uid = -1;

    static public long getNextUid() {
        return nextUid++;
    }
    
    // used for junit test, should not be called elsewhere
    static public void resetNextUid() {
        nextUid = 1;
    }
    
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
        if (uid == -1) {
            throw new RuntimeException("getUid called before uid set");
        }
        return uid;
    }
    
    /**
     * Set the uid.  For most expressions this will get a new uid.
     * ProjectExpression needs to override this and find its uid from its
     * predecessor.
     * @param currentOp Current LogicalRelationalOperator that this expression operator
     * is attached to.  Passed so that projection operators can determine their uid.
     * @throws IOException
     */
    public void setUid(LogicalRelationalOperator currentOp) throws IOException {
        uid = getNextUid();
    }
    
    /**
     * Hard code the uid.  This should only be used in testing, never in real
     * code.
     * @param uid value to set uid to
     */
    public void neverUseForRealSetUid(long uid) {
        this.uid = uid;
    }
    
    public String toString() {
        StringBuilder msg = new StringBuilder();

        msg.append("(Name: " + name + " Type: " + DataType.findTypeName(type) + " Uid: " + uid + ")");

        return msg.toString();
    }
}
