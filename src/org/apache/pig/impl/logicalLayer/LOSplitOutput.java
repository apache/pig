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
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.data.DataType;


public class LOSplitOutput extends LogicalOperator {
    private static final long serialVersionUID = 2L;

    protected int mIndex;
    private LogicalPlan mCondPlan;
    
    /**
     * @param plan
     *            LogicalPlan this operator is a part of.
     * @param key
     *            OperatorKey for this operator
     * @param outputs
     *            list of aliases that are the output of the split
     * @param conditions
     *            list of conditions for the split
     */
    public LOSplitOutput(LogicalPlan plan, OperatorKey key, int index, LogicalPlan condPlan) {
        super(plan, key);
        this.mIndex = index;
        this.mCondPlan = condPlan;
    }

    public LogicalPlan getConditionPlan() {
        return mCondPlan;
    }
    
    @Override
    public String name() {
        return "SplitOutput " + mKey.scope + "-" + mKey.id;
    }

    @Override
    public Schema getSchema() throws FrontendException{
        if (!mIsSchemaComputed && (null == mSchema)) {
            // get our parent's schema
            Collection<LogicalOperator> s = mPlan.getPredecessors(this);
            try {
                LogicalOperator op = s.iterator().next();
                if (null == op) {
                    throw new FrontendException("Could not find operator in plan");
                }
                mSchema = s.iterator().next().getSchema();
                mIsSchemaComputed = true;
            } catch (FrontendException fe) {
                mSchema = null;
                mIsSchemaComputed = false;
                throw fe;
            }
        }
        return mSchema;
    }

    public void visit(LOVisitor v) throws VisitorException{
        v.visit(this);
    }

    @Override
    public boolean supportsMultipleInputs() {
        return false;
    }

    @Override
    public boolean supportsMultipleOutputs() {
        return false;
    }

    public int getReadFrom() {
        return mIndex;
    }

    public byte getType() {
        return DataType.BAG ;
    }
}
