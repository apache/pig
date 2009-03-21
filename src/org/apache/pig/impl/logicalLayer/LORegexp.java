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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanVisitor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LORegexp extends BinaryExpressionOperator {
    private static final long serialVersionUID = 2L;

    /**
     * The expression and the column to be projected.
     */
    private static Log log = LogFactory.getLog(LORegexp.class);

    /**
     * 
     * @param plan
     *            Logical plan this operator is a part of.
     * @param key
     *            Operator key to assign to this node.
     */
    public LORegexp(LogicalPlan plan, OperatorKey key) {
        super(plan, key);
    }

    public ExpressionOperator getOperand() {
        return getLhsOperand();
    }

    public String getRegexp() {
        ExpressionOperator op = getRhsOperand();
        if (!(op instanceof LOConst)) {
            throw new RuntimeException(
                "Regular expression patterns must be a constant.");
        }
        Object o = ((LOConst)op).getValue();
        // better be a string
        if (!(o instanceof String)) {
            throw new RuntimeException(
                "Regular expression patterns must be a string.");
        }

        return (String)o;
    }
    
    @Override
    public String name() {
        return "Regexp " + mKey.scope + "-" + mKey.id;
    }

    @Override
    public boolean supportsMultipleInputs() {
        return true;
    }

    @Override
    public Schema getSchema() {
        return mSchema;
    }

    @Override
    public Schema.FieldSchema getFieldSchema() throws FrontendException {
        if(!mIsFieldSchemaComputed) {
            mFieldSchema = new Schema.FieldSchema(null, DataType.BOOLEAN);
            mFieldSchema.setParent(getLhsOperand().getFieldSchema().canonicalName, getLhsOperand());
            mFieldSchema.setParent(getRhsOperand().getFieldSchema().canonicalName, getRhsOperand());
            mIsFieldSchemaComputed = true;
        }
        return mFieldSchema;
    }

    @Override
    public void visit(LOVisitor v) throws VisitorException {
        v.visit(this);
    }


}
