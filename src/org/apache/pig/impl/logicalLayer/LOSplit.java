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

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.plan.PlanVisitor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LOSplit extends LogicalOperator {
    private static final long serialVersionUID = 2L;

    private Map<String, LogicalPlan> mCondPlans;
    private ArrayList<LogicalOperator> mOutputs;
    private static Log log = LogFactory.getLog(LOSplit.class);

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
    public LOSplit(LogicalPlan plan, OperatorKey key,
            ArrayList<LogicalOperator> outputs,
            Map<String, LogicalPlan> condPlans) {
        super(plan, key);
        mOutputs = outputs;
        mCondPlans = condPlans;
    }

    public List<LogicalOperator> getOutputs() {
        return mOutputs;
    }

    public Collection<LogicalPlan> getConditionPlans() {
        return mCondPlans.values();
    }

    public Set<String> getOutputAliases() {
        return mCondPlans.keySet();
    }

    public void addOutputAlias(String output, LogicalPlan cond) {
        mCondPlans.put(output, cond);
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
}
