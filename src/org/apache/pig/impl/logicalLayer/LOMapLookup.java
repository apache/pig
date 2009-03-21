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

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.parser.ParseException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanVisitor;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LOMapLookup extends ExpressionOperator {
    private static final long serialVersionUID = 2L;

    /**
     * The key to lookup along with the type and schema corresponding to the
     * type and schema of the value linked to the key
     */
    private Object mMapKey;
    private byte mValueType;
    private Schema mValueSchema;
    private static Log log = LogFactory.getLog(LOMapLookup.class);

    /**
     * 
     * @param plan
     *            Logical plan this operator is a part of.
     * @param key
     *            Operator key to assign to this node.
     * @param mapKey
     *            key to look up in the map. The key is of atomic type
     * @param valueType
     *            type of the value corresponding to the key
     * @param valueSchema
     *            schema of the value if the type is tuple
     */
    public LOMapLookup(LogicalPlan plan, OperatorKey key,
            Object mapKey, byte valueType, Schema valueSchema)
            throws ParseException {
        super(plan, key);

        if (!DataType.isAtomic(DataType.findType(mapKey))) {
            throw new ParseException("Map key " + mapKey.toString()
                    + " is not atomic");
        }
        mMapKey = mapKey;
        mValueType = valueType;
        mValueSchema = valueSchema;
        mType = mValueType;
    }

    public ExpressionOperator getMap() {
        List<LogicalOperator>preds = getPlan().getPredecessors(this);
        if(preds == null)
            return null;
        return (ExpressionOperator)preds.get(0);
    }

    public Object getLookUpKey() {
        return mMapKey;
    }

    public byte getValueType() {
        return mValueType;
    }

    @Override
    public String name() {
        return "MapLookup " + mKey.scope + "-" + mKey.id;
    }

    @Override
    public boolean supportsMultipleInputs() {
        return false;
    }

    @Override
    public Schema getSchema() {
        return mSchema;
    }

    @Override
    public Schema.FieldSchema getFieldSchema() throws FrontendException {
        if (!mIsFieldSchemaComputed) {
            if (DataType.isSchemaType(mValueType)) {
                mFieldSchema = new Schema.FieldSchema(null, mValueSchema, mValueType);
            } else {
                mFieldSchema = new Schema.FieldSchema(null, mValueType);
            }
            ExpressionOperator map = getMap();
            mFieldSchema.setParent(map.getFieldSchema().canonicalName, map);

            mIsFieldSchemaComputed = true;
        }
        return mFieldSchema;
    }

    @Override
    public void visit(LOVisitor v) throws VisitorException {
        v.visit(this);
    }

    /**
     * @see org.apache.pig.impl.logicalLayer.ExpressionOperator#clone()
     * Do not use the clone method directly. Operators are cloned when logical plans
     * are cloned using {@link LogicalPlanCloner}
     */
    @Override
    protected Object clone() throws CloneNotSupportedException {
        LOMapLookup clone = (LOMapLookup)super.clone();
        
        // deep copy project specific attributes
        if(mValueSchema != null) {
            clone.mValueSchema = mValueSchema.clone();
        }

        return clone;
    }

}
