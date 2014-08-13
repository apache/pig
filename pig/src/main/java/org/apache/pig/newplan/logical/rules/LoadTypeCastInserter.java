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

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.logical.relational.LogicalSchema;

public class LoadTypeCastInserter extends TypeCastInserter {

    public LoadTypeCastInserter(String n) {
        super(n);
    }

    @Override
    protected OperatorPlan buildPattern() {
        LogicalPlan plan = new LogicalPlan();
        plan.add(new LOLoad(null, plan));
        return plan;
    }

    /**
     * if we are inserting casts in a load and if the loader implements
     * determineSchema(), insert casts only where necessary Note that in this
     * case, the data coming out of the loader is not a BYTEARRAY but is
     * whatever determineSchema() says it is.
     */
    protected LogicalSchema determineSchema(LogicalRelationalOperator op)
            throws FrontendException {
        return ((LOLoad) op).getDeterminedSchema();
    }

    @Override
    protected void markCastInserted(LogicalRelationalOperator op) {
        ((LOLoad)op).setCastState(LOLoad.CastState.INSERTED);
    }
    
    @Override
    protected void markCastNoNeed(LogicalRelationalOperator op) {
        ((LOLoad)op).setCastState(LOLoad.CastState.NONEED);
    }

    @Override
    protected boolean isCastAdjusted(LogicalRelationalOperator op) {
        return ((LOLoad)op).isCastAdjusted();
    }
}
