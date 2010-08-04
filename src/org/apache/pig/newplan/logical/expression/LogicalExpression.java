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

package org.apache.pig.newplan.logical.expression;

import java.io.IOException;

import org.apache.pig.data.DataType;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.newplan.logical.relational.LogicalSchema.LogicalFieldSchema;

/**
 * Logical representation of expression operators.  Expression operators have
 * a data type and a uid.  Uid is a unique id for each expression. 
 *
 */
public abstract class LogicalExpression extends Operator {
    
    static long nextUid = 1;
    protected LogicalSchema.LogicalFieldSchema fieldSchema;
    protected LogicalSchema.LogicalFieldSchema uidOnlyFieldSchema;

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
     */
    public LogicalExpression(String name, OperatorPlan plan) {
        super(name, plan);
    }
    
    abstract public LogicalSchema.LogicalFieldSchema getFieldSchema() throws IOException;
    
    public void resetFieldSchema() {
        fieldSchema = null;
    }
    
    /**
     * Get the data type for this expression.
     * @return data type, one of the static bytes of DataType
     */
    public byte getType() {
        try {
            if (getFieldSchema()!=null)
                return getFieldSchema().type;
        } catch (IOException e) {
        }
        return DataType.UNKNOWN;
    }
    
    public String toString() {
        StringBuilder msg = new StringBuilder();
        try {
            msg.append("(Name: " + name + " Type: ");
            if (fieldSchema!=null)
                msg.append(DataType.findTypeName(getFieldSchema().type));
            else
                msg.append("null");
            msg.append(" Uid: ");
            if (fieldSchema!=null)
                msg.append(getFieldSchema().uid);
            else
                msg.append("null");
            msg.append(")");
        } catch (IOException e) {
            e.printStackTrace();
        }

        return msg.toString();
    }
    
    public void neverUseForRealSetFieldSchema(LogicalFieldSchema fs) throws IOException {
        fieldSchema = fs;
        uidOnlyFieldSchema = fieldSchema.mergeUid(uidOnlyFieldSchema);
    }
}
