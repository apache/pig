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

import java.util.ArrayList;
import java.util.List;
import java.io.IOException;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.parser.ParseException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.Operator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * Parent for all Logical operators.
 */
abstract public class LogicalOperator extends Operator<LOVisitor> {
    private static final long serialVersionUID = 2L;

    /**
     * Schema that defines the output of this operator.
     */
    protected Schema mSchema = null;

    /**
     * A boolean variable to remember if the schema has been computed
     */
    protected boolean mIsSchemaComputed = false;

    /**
     * Datatype of this output of this operator. Operators start out with data
     * type set to UNKNOWN, and have it set for them by the type checker.
     */
    protected byte mType = DataType.UNKNOWN;

    /**
     * Requested level of parallelism for this operation.
     */
    protected int mRequestedParallelism;

    /**
     * Name of the record set that results from this operator.
     */
    protected String mAlias;

    /**
     * Logical plan that this operator is a part of.
     */
    protected LogicalPlan mPlan;

    /**
     * A boolean variable to remember if input has to be flattened Used only in
     * the context of generate
     */
    //private boolean mIsFlatten = false;
    
    private static Log log = LogFactory.getLog(LogicalOperator.class);

    /**
     * Equivalent to LogicalOperator(k, 0).
     * 
     * @param plan
     *            Logical plan this operator is a part of.
     * @param k
     *            Operator key to assign to this node.
     */
    public LogicalOperator(LogicalPlan plan, OperatorKey k) {
        this(plan, k, -1);
    }

    /**
     * @param plan
     *            Logical plan this operator is a part of.
     * @param -
     *            k Operator key to assign to this node.
     * @param =
     *            rp degree of requested parallelism with which to execute this
     *            node.
     */
    public LogicalOperator(LogicalPlan plan, OperatorKey k, int rp) {
        super(k);
        mPlan = plan;
        mRequestedParallelism = rp;
    }

    /**
     * Get the operator key for this operator.
     */
    public OperatorKey getOperatorKey() {
        return mKey;
    }

    /**
     * Set the output schema for this operator. If a schema already exists, an
     * attempt will be made to reconcile it with this new schema.
     * 
     * @param schema
     *            Schema to set.
     * @throws ParseException
     *             if there is already a schema and the existing schema cannot
     *             be reconciled with this new schema.
     */
    public final void setSchema(Schema schema) throws ParseException {
        // In general, operators don't generate their schema until they're
        // asked, so ask them to do it.
        try {
            getSchema();
        } catch (FrontendException ioe) {
            // It's fine, it just means we don't have a schema yet.
        }
        if (mSchema == null) {
            log.debug("Operator schema is null; Setting it to new schema");
            mSchema = schema;
        } else {
            log.debug("Reconciling schema");
            log.debug("mSchema: " + mSchema + " schema: " + schema);
            mSchema.reconcile(schema);
        }
    }

    /**
     * Directly force the schema without reconcilation
     * Please use with great care
     * @param schema
     */
    public void forceSchema(Schema schema) {
        this.mSchema = schema;
    }


    /**
     * Get a copy of the schema for the output of this operator.
     */
    public abstract Schema getSchema() throws FrontendException;

    /**
     * Set the type of this operator. This should only be called by the type
     * checking routines.
     * 
     * @param type -
     *            Type to set this operator to.
     */
    final public void setType(byte t) {
        mType = t;
    }

    /**
     * Get the type of this operator.
     */
    public byte getType() {
        return mType;
    }

    public String getAlias() {
        return mAlias;
    }

    public void setAlias(String newAlias) {
        mAlias = newAlias;
    }

    public int getRequestedParallelism() {
        return mRequestedParallelism;
    }

    public void setRequestedParallelism(int newRequestedParallelism) {
        mRequestedParallelism = newRequestedParallelism;
    }

    @Override
    public String toString() {
        StringBuffer msg = new StringBuffer();

        msg.append("(Name: " + name() + " Operator Key: " + mKey + ")");

        return msg.toString();
    }

    /**
     * Given a schema, reconcile it with our existing schema.
     * 
     * @param schema
     *            Schema to reconcile with the existing.
     * @throws ParseException
     *             if the reconciliation is not possible.
     */
    protected void reconcileSchema(Schema schema) throws ParseException {
        if (mSchema == null) {
            mSchema = schema;
            return;
        }

        // TODO
    }

    /**
     * Visit this node with the provided visitor. This should only be called by
     * the visitor class itself, never directly.
     * 
     * @param v
     *            Visitor to visit with.
     * @throws VisitException
     *             if the visitor has a problem.
     */
    public abstract void visit(LOVisitor v) throws VisitorException;

	/*
    public boolean isFlatten() {
        return mIsFlatten;
    }

    public void setFlatten(boolean b) {
        mIsFlatten = b;
    }
	*/
    public LogicalPlan getPlan() {
        return mPlan ;
    }


    /***
     * IMPORTANT:
     * This method is only used for unit testing purpose.
     */
    public void setSchemaComputed(boolean computed) {
       mIsSchemaComputed = computed ;   
    }
}
