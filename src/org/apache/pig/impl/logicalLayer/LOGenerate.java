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

public class LOGenerate extends LogicalOperator {
    private static final long serialVersionUID = 2L;

    /**
     * The projection list of this generate.
     */
    private ArrayList<ExpressionOperator> mProjections;

    /**
     * 
     * @param plan
     *            Logical plan this operator is a part of.
     * @param k
     *            Operator key to assign to this node.
     * @param rp
     *            degree of requested parallelism with which to execute this
     *            node.
     * @param projections
     *            the projection list of the generate
     */
    public LOGenerate(LogicalPlan plan, OperatorKey key, int rp,
            ArrayList<ExpressionOperator> projections) {
        super(plan, key, rp);
        mProjections = projections;
    }

    public List<ExpressionOperator> getProjections() {
        return mProjections;
    }

    @Override
    public String name() {
        return "Generate " + mKey.scope + "-" + mKey.id;
    }

    @Override
    public boolean supportsMultipleInputs() {
        return true;
    }

    @Override
    public boolean supportsMultipleOutputs() {
        return false;
    }

    @Override
    public Schema getSchema() throws IOException {
        if (mSchema == null) {
            List<Schema.FieldSchema> fss = new ArrayList<Schema.FieldSchema>(
                    mProjections.size());
            for (ExpressionOperator op : mProjections) {
                String opAlias = op.getAlias();
                if (op.getType() == DataType.TUPLE) {
                    try {
                        fss.add(new Schema.FieldSchema(opAlias, op.getSchema()));
                    } catch (IOException ioe) {
                        mSchema = null;
                        mIsSchemaComputed = false;
                        throw ioe;
                    }
                } else {
                    fss.add(new Schema.FieldSchema(opAlias, op.getType()));
                }
            }
            mSchema = new Schema(fss);
            mIsSchemaComputed = true;
        }
        return mSchema;
    }

    @Override
    public void visit(LOVisitor v) throws VisitorException {
        v.visit(this);
    }

}
