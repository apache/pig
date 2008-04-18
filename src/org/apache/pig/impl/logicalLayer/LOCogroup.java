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
import java.util.Map;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.PlanVisitor;

public class LOCogroup extends LogicalOperator {
    private static final long serialVersionUID = 2L;

    private ArrayList<String> mInputs;
    private ArrayList<ExpressionOperator> mGroupByCols;

    /**
     * 
     * @param plan
     *            LogicalPlan this operator is a part of.
     * @param key
     *            OperatorKey for this operator
     * @param rp
     *            Requested level of parallelism to be used
     * @param groupByCols
     *            the group by columns
     */
    public LOCogroup(LogicalPlan plan, OperatorKey k, int rp,
            ArrayList<String> inputs, ArrayList<ExpressionOperator> groupByCols) {
        super(plan, k, rp);
        mInputs = inputs;
        mGroupByCols = groupByCols;
    }

    public List<String> getInputs() {
        return mInputs;
    }

    public List<ExpressionOperator> getGroupByCols() {
        return mGroupByCols;
    }

    @Override
    public String name() {
        return "CoGroup " + mKey.scope + "-" + mKey.id;
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
        // TODO create schema
        /**
         * Dumping my understanding of how the schema of a Group/CoGroup will
         * look. The first field of the resulting tuple will have the alias
         * 'group'. The schema for this field is a union of the group by columns
         * for each input. The subsequent fields in the output tuple will have
         * the alias of the input as the alias for a bag that contains the
         * tuples from the input that match the grouping criterion
         */
        if (null == mSchema) {
            List<Schema.FieldSchema> fss = new ArrayList<Schema.FieldSchema>(
                    mGroupByCols.size() + 1);
            // one more to account for the "group"
            // the alias of the first field is group and hence the
            // string "group"
            // TODO The type of the field named "group" requires
            // type promotion and the like
            fss.add(new Schema.FieldSchema("group", null));
            for (String input : mInputs) {
                fss.add(new Schema.FieldSchema(input, DataType.BAG));
            }
            mIsSchemaComputed = true;
        }
        return mSchema;
    }

    @Override
    public void visit(LOVisitor v) throws VisitorException {
        v.visit(this);
    }

}
