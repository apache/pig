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
import java.util.List;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.parser.ParseException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.PlanVisitor;

public class LOGenerate extends LogicalOperator {
    private static final long serialVersionUID = 2L;

    /**
     * The projection list of this generate.
     */
    private ArrayList<LogicalOperator> mProjections;

    public LOGenerate(LogicalPlan plan, OperatorKey key) {
        super(plan, key);
    }

    @Override
    public String name() {
        return "Generate " + mKey.scope + "-" + mKey.id;
    }

    @Override
    public String typeName() {
        return "LOGenerate";
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
    public Schema getSchema() {
        if (mSchema == null) {
            List<Schema.FieldSchema> fss =
                new ArrayList<Schema.FieldSchema>(mProjections.size());
            for (LogicalOperator op : mProjections) {
                if (op.getType() == DataType.TUPLE) {
                    fss.add(new Schema.FieldSchema(null, op.getSchema()));
                } else {
                    fss.add(new Schema.FieldSchema(null, op.getType()));
                }
            }
            mSchema = new Schema(fss);
        }
        return mSchema;
    }

    @Override
    public void visit(PlanVisitor v) throws ParseException {
        if (!(v instanceof LOVisitor)) {
            throw new RuntimeException("You can only visit LogicalOperators "
                + "with an LOVisitor!");
        }
        ((LOVisitor)v).visitGenerate(this);
    }

    public List<LogicalOperator> getProjections() {
        return mProjections;
    }

}
