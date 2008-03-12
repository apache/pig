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

import org.apache.pig.impl.logicalLayer.parser.ParseException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.PlanVisitor;

public class LOEval extends LogicalOperator {
    private static final long serialVersionUID = 2L;

    private LOGenerate mGen = null;

    public LOEval(LogicalPlan plan, OperatorKey key) {
        super(plan, key);
    }

    public void setGenerate(LOGenerate gen) {
        mGen = gen;
    }

    @Override
    public String name() {
        return "Eval " + mKey.scope + "-" + mKey.id;
    }

    @Override
    public String typeName() {
        return "LOEval";
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
            // Ask the generate at the end of the plan for its schema,
            // and return that.
            mSchema = mGen.getSchema();
            }
        return mSchema;
    }

    @Override
    public void visit(PlanVisitor v) throws ParseException {
        if (!(v instanceof LOVisitor)) {
            throw new RuntimeException("You can only visit LogicalOperators "
                + "with an LOVisitor!");
        }
        ((LOVisitor)v).visitEval(this);
    }

}
