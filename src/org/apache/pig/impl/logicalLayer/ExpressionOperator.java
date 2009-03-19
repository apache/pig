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
import java.util.ArrayList;

import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.parser.ParseException;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class ExpressionOperator extends LogicalOperator {

    private static final long serialVersionUID = 2L;
    private static Log log = LogFactory.getLog(ExpressionOperator.class);
    protected boolean mIsFieldSchemaComputed = false;
    protected Schema.FieldSchema mFieldSchema = null;

    /**
     * @param plan
     *            Logical plan this operator is a part of.
     * @param k
     *            Operator key to assign to this node.
     * @param rp
     *            degree of requested parallelism with which to execute this
     *            node.
     */
    public ExpressionOperator(LogicalPlan plan, OperatorKey k, int rp) {
        super(plan, k, rp);
    }

    /**
     * @param plan
     *            Logical plan this operator is a part of.
     * @param k
     *            Operator key to assign to this node.
     */
    public ExpressionOperator(LogicalPlan plan, OperatorKey k) {
        super(plan, k);
    }

    
    @Override
    public boolean supportsMultipleOutputs() {
        return true;
    }

    @Override
    public Schema getSchema() throws FrontendException{
        return mSchema;
    }

    // Default implementation just get type info from mType
    public Schema.FieldSchema getFieldSchema() throws FrontendException {
        Schema.FieldSchema fs = new Schema.FieldSchema(null, mType) ;
        return fs ;
    }

    /**
     * Set the output schema for this operator. If a schema already exists, an
     * attempt will be made to reconcile it with this new schema.
     * 
     * @param fs
     *            FieldSchema to set.
     * @throws FrontendException
     *             if there is already a schema and the existing schema cannot
     *             be reconciled with this new schema.
     */
    public void setFieldSchema(Schema.FieldSchema fs) throws FrontendException {
        mFieldSchema = fs;
        setAlias(fs.alias);
        setType(fs.type);
        mIsFieldSchemaComputed = true;
    }

    /**
     * Unset the field schema as if it had not been calculated.  This is used
     * by anyone who reorganizes the tree and needs to have schemas
     * recalculated.
     */
    public void unsetFieldSchema() {
        mIsFieldSchemaComputed = false;
        mFieldSchema = null;
    }
    
    public Schema.FieldSchema regenerateFieldSchema() throws FrontendException {
        unsetFieldSchema();
        return getFieldSchema();
    }

    void setFieldSchemaComputed(boolean b) {
        mIsFieldSchemaComputed = b;
    }

    boolean getFieldSchemaComputed() {
        return mIsFieldSchemaComputed;
    }

    @Override
    public byte getType() {                         
        // Called to make sure we've constructed the field schema before trying
        // to read it.
        try {
            getFieldSchema();
        } catch (FrontendException fe) {
            return DataType.UNKNOWN;
        }

        if (mFieldSchema != null){
            return mFieldSchema.type ;
        }
        else {
            return DataType.UNKNOWN ;
        }
    }

    /**
     * @see org.apache.pig.impl.logicalLayer.LogicalOperator#clone()
     * Do not use the clone method directly. Operators are cloned when logical plans
     * are cloned using {@link LogicalPlanCloner}
     */
    @Override
    protected Object clone() throws CloneNotSupportedException {
        ExpressionOperator exOpClone = (ExpressionOperator)super.clone();
        if(mFieldSchema != null)
            exOpClone.mFieldSchema = this.mFieldSchema.clone();
        return exOpClone;
    }

}

