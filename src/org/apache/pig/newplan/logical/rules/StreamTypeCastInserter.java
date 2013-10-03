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
package org.apache.pig.newplan.logical.rules;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LOStream;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.newplan.logical.relational.LogicalSchema.LogicalFieldSchema;

public class StreamTypeCastInserter extends TypeCastInserter {

    public StreamTypeCastInserter(String n) {
        super(n);
    }

    @Override
    protected OperatorPlan buildPattern() {
        LogicalPlan plan = new LogicalPlan();
        plan.add(new LOStream(plan, null, null, null));
        return plan;
    }

    protected LogicalSchema determineSchema(LogicalRelationalOperator op)
            throws FrontendException {
        LogicalSchema determinedSchema = new LogicalSchema();
        for (int i = 0; i < op.getSchema().size(); i++) {
            determinedSchema.addField(new LogicalFieldSchema(
                    null,
                    null,
                    DataType.BYTEARRAY));
        }
        return determinedSchema;
    }

    @Override
    protected void markCastInserted(LogicalRelationalOperator op) {
        ((LOStream)op).setCastState(LOStream.CastState.INSERTED);
    }
    
    @Override
    protected void markCastNoNeed(LogicalRelationalOperator op) {
        ((LOStream)op).setCastState(LOStream.CastState.NONEED);
    }

    @Override
    protected boolean isCastAdjusted(LogicalRelationalOperator op) {
        return ((LOStream)op).isCastAdjusted();
    }
}
