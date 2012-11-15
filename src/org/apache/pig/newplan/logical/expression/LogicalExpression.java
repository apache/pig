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

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
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

    /**
     * This is a convenience method to avoid the side-effectful nature of getFieldSchema().
     * It simply returns whether or not fieldSchema is currently null.
     */
    public boolean hasFieldSchema() {
        return fieldSchema != null;
    }

    /**
     * Get the field schema for the output of this expression operator.  This does
     * not merely return the field schema variable.  If schema is not yet set, this
     * will attempt to construct it.  Therefore it is abstract since each
     * operator will need to construct its field schema differently.
     * @return the FieldSchema
     * @throws FrontendException
     */
    abstract public LogicalSchema.LogicalFieldSchema getFieldSchema() throws FrontendException;

    public void resetFieldSchema() {
        fieldSchema = null;
    }

    /**
     * Get the data type for this expression.
     * @return data type, one of the static bytes of DataType
     */
    public byte getType() throws FrontendException {
        if (getFieldSchema()!=null && getFieldSchema().type!=DataType.NULL)
            return getFieldSchema().type;
        return DataType.BYTEARRAY;
    }

    public String toString() {
        StringBuilder msg = new StringBuilder();
        msg.append("(Name: " + name + " Type: ");
        if (fieldSchema!=null)
            msg.append(DataType.findTypeName(fieldSchema.type));
        else
            msg.append("null");
        msg.append(" Uid: ");
        if (fieldSchema!=null)
            msg.append(fieldSchema.uid);
        else
            msg.append("null");
        msg.append(")");

        return msg.toString();
    }

    public void neverUseForRealSetFieldSchema(LogicalFieldSchema fs) throws FrontendException {
        fieldSchema = fs;
        uidOnlyFieldSchema = fieldSchema.mergeUid(uidOnlyFieldSchema);
    }


    /**
     * Create the deep copy of this expression and add that into the passed
     * LogicalExpressionPlan Return the copy of this expression with updated
     * logical expression plan.
     * @param lgExpPlan LogicalExpressionPlan in which this expression will be added.
     * @return LogicalExpression with its own logical expression plan.
     * @throws IOException.
     */
    abstract public LogicalExpression deepCopy(LogicalExpressionPlan lgExpPlan) throws FrontendException;

    /**
     * Erase all cached uid, regenerate uid when we regenerating schema.
     * This process currently only used in ImplicitSplitInsert, which will
     * insert split and invalidate some uids in plan
     */
    public void resetUid() {
        uidOnlyFieldSchema = null;
    }
}