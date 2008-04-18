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
import java.io.IOException;
import java.util.Set;

import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.PlanVisitor;

public class LOSplit extends LogicalOperator {
    private static final long serialVersionUID = 2L;

    private Map<String, ExpressionOperator> mOutputs;

    /**
     * @param plan
     *            LogicalPlan this operator is a part of.
     * @param key
     *            OperatorKey for this operator
     * @param rp
     *            Requested level of parallelism to be used in the sort.
     * @param aliases
     *            list of aliases that are the output of the split
     * @param conditions
     *            list of conditions for the split
     */
    public LOSplit(LogicalPlan plan, OperatorKey key, int rp,
            Map<String, ExpressionOperator> outputs) {
        super(plan, key, rp);
        mOutputs = outputs;
    }

    public Collection<ExpressionOperator> getConditions() {
        return mOutputs.values();
    }

    public Set<String> getOutputAliases() {
        return mOutputs.keySet();
    }

    public void addOutputAlias(String output, ExpressionOperator cond) {
        mOutputs.put(output, cond);
    }

    @Override
    public String name() {
        return "Split " + mKey.scope + "-" + mKey.id;
    }

    @Override
    public Schema getSchema() throws IOException {
        if (!mIsSchemaComputed && (null == mSchema)) {
            // get our parent's schema
            Collection<LogicalOperator> s = mPlan.getSuccessors(this);
            try {
                LogicalOperator op = s.iterator().next();
                if (null == op) {
                    throw new IOException("Could not find operator in plan");
                }
                mSchema = s.iterator().next().getSchema();
                mIsSchemaComputed = true;
            } catch (IOException ioe) {
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
