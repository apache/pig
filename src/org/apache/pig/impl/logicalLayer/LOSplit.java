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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.pig.PigException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanVisitor;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.data.DataType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LOSplit extends LogicalOperator {
    private static final long serialVersionUID = 2L;

    private ArrayList<LogicalOperator> mOutputs;
    private static Log log = LogFactory.getLog(LOSplit.class);

    /**
     * @param plan
     *            LogicalPlan this operator is a part of.
     * @param key
     *            OperatorKey for this operator
     * @param outputs
     *            list of aliases that are the output of the split
     */
    public LOSplit(LogicalPlan plan, OperatorKey key,
            ArrayList<LogicalOperator> outputs) {
        super(plan, key);
        mOutputs = outputs;
    }

    public List<LogicalOperator> getOutputs() {
        return mOutputs;
    }

    public void setOutputs(ArrayList<LogicalOperator> outputs) {
        mOutputs = outputs;
    }

    public void addOutput(LogicalOperator lOp) {
        mOutputs.add(lOp);
    }

    @Override
    public String name() {
        return "Split " + mKey.scope + "-" + mKey.id;
    }

    @Override
    public Schema getSchema() throws FrontendException {
        if (!mIsSchemaComputed) {
            // get our parent's schema
            Collection<LogicalOperator> s = mPlan.getPredecessors(this);
            try {
                LogicalOperator op = s.iterator().next();
                if (null == op) {
                    int errCode = 1006;
                    String msg = "Could not find operator in plan";
                    throw new FrontendException(msg, errCode, PigException.INPUT, false, null);
                }
                mSchema = s.iterator().next().getSchema();
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
    public boolean supportsMultipleInputs() {
        return false;
    }

    @Override
    public boolean supportsMultipleOutputs() {
        return true;
    }

    public void visit(LOVisitor v) throws VisitorException {
        v.visit(this);
    }

    @Override
    public byte getType() {
        return DataType.BAG;
    }
    
    /**
     * @see org.apache.pig.impl.logicalLayer.LogicalOperator#clone()
     * Do not use the clone method directly. Operators are cloned when logical plans
     * are cloned using {@link LogicalPlanCloner}
     */
    @Override
    protected Object clone() throws CloneNotSupportedException {
        LOSplit splitClone = (LOSplit)super.clone();
        return splitClone;
    }
}
