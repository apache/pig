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
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanVisitor;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.data.DataType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LOFilter extends LogicalOperator {

    private static final long serialVersionUID = 2L;
    private LogicalOperator mInput;
    private LogicalPlan mComparisonPlan;
    private static Log log = LogFactory.getLog(LOFilter.class);

    /**
     * 
     * @param plan
     *            Logical plan this operator is a part of.
     * @param k
     *            Operator key to assign to this node.
     * @param cond
     *            the filter condition
     * @param input
     *            the input that needs filtering
     */

    public LOFilter(LogicalPlan plan, OperatorKey k,
            LogicalPlan comparisonPlan, LogicalOperator input) {
        super(plan, k);
        mComparisonPlan = comparisonPlan;
        mInput = input;
    }

    public LogicalOperator getInput() {
        return mInput;
    }

    public LogicalPlan getComparisonPlan() {
        return mComparisonPlan;
    }

    @Override
    public Schema getSchema() throws FrontendException {
        if (!mIsSchemaComputed && (null == mSchema)) {
            ArrayList<Schema.FieldSchema> fss = new ArrayList<Schema.FieldSchema>();
            try {
                if(mInput instanceof ExpressionOperator) {
                    fss.add(((ExpressionOperator)mInput).getFieldSchema());
                    mSchema = new Schema(fss);
                } else {
                    mSchema = mInput.getSchema();
                }
                mIsSchemaComputed = true;
            } catch (FrontendException ioe) {
                mSchema = null;
                mIsSchemaComputed = false;
                throw ioe;
            }
        }
        return mSchema;
    }

    @Override
    public String name() {
        return "Filter " + mKey.scope + "-" + mKey.id;
    }

    @Override
    public boolean supportsMultipleInputs() {
        return false;
    }

    @Override
    public boolean supportsMultipleOutputs() {
        return false;
    }

    @Override
    public void visit(LOVisitor v) throws VisitorException {
        v.visit(this);
    }

    @Override
    public byte getType() {
        return DataType.BAG ;
    }

}
