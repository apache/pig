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
import org.apache.pig.impl.plan.PlanVisitor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LORegexp extends ExpressionOperator {
    private static final long serialVersionUID = 2L;

    /**
     * The expression and the column to be projected.
     */
    private ExpressionOperator mOperand;
    private String mRegexp;
    private static Log log = LogFactory.getLog(LORegexp.class);

    /**
     * 
     * @param plan
     *            Logical plan this operator is a part of.
     * @param k
     *            Operator key to assign to this node.
     * @param exp
     *            the expression which might contain the column to project
     * @param projection
     *            the list of columns to project
     */
    public LORegexp(LogicalPlan plan, OperatorKey key,
            ExpressionOperator operand, String regexp) {
        super(plan, key);
        mOperand = operand;
        mRegexp = regexp;
    }

    public ExpressionOperator getOperand() {
        return mOperand;
    }

    public void setOperand(ExpressionOperator op) {
        mOperand = op ;
    }

    public String getRegexp() {
        return mRegexp;
    }
    
    @Override
    public String name() {
        return "Regexp " + mKey.scope + "-" + mKey.id;
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
    public Schema.FieldSchema getFieldSchema() {
        return mFieldSchema;
    }

    @Override
    public void visit(LOVisitor v) throws VisitorException {
        v.visit(this);
    }

    @Override
    public byte getType() {
        return DataType.BOOLEAN ;
    }

}
