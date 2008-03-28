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

import java.util.Collection;
import java.util.List;
import java.io.IOException;

import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.PlanVisitor;
import org.apache.pig.impl.logicalLayer.parser.ParseException;

public class LODistinct extends LogicalOperator {

    private static final long serialVersionUID = 2L;

    /**
     * 
     * @param plan
     *            Logical plan this operator is a part of.
     * @param k
     *            Operator key to assign to this node.
     * @param rp
     *            degree of requested parallelism with which to execute this
     *            node.
     */
    public LODistinct(LogicalPlan plan, OperatorKey k, int rp) {

        super(plan, k, rp);
    }

    @Override
    public Schema getSchema() throws IOException {
        if (!mIsSchemaComputed && (null == mSchema)) {
            // Get the schema of the parent
            Collection<LogicalOperator> s = mPlan.getSuccessors(this);
            try {
                LogicalOperator op = s.iterator().next();
                if(null == op) {
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
    public String name() {
        return "Distinct " + mKey.scope + "-" + mKey.id;
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
    public void visit(LOVisitor v) throws ParseException {
        v.visit(this);
    }

}
