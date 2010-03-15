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

package org.apache.pig.experimental.logical.relational;

import java.io.IOException;
import java.util.List;

import org.apache.pig.experimental.plan.Operator;
import org.apache.pig.experimental.plan.OperatorPlan;
import org.apache.pig.experimental.plan.PlanVisitor;

public class LOUnion extends LogicalRelationalOperator {

    public LOUnion(OperatorPlan plan) {
        super("LOUnion", plan);
    }    
    @Override
    public LogicalSchema getSchema() {
        List<Operator> inputs = null;
        try {
            inputs = plan.getPredecessors(this);
        }catch(Exception e) {
            throw new RuntimeException("Unable to get predecessor of LOUnion.", e);
        }
        
        // If any predecessor's schema is null, or length of predecessor's schema does not match,
        // then the schema for union is null
        int length = -1;
        for (Operator input : inputs) {
            LogicalRelationalOperator op = (LogicalRelationalOperator)input;
            if (op.getSchema()==null)
                return null;
            if (length==-1)
                op.getSchema().size();
            else {
                if (op.getSchema().size()!=length)
                    return null;
            }
        }
        
        // Check if all predecessor's schema are compatible.
        // TODO: Migrate all existing schema merging rules
        LogicalSchema mergedSchema = ((LogicalRelationalOperator)inputs.get(0)).getSchema();
        for (int i=1;i<inputs.size();i++) {
            LogicalSchema otherSchema = ((LogicalRelationalOperator)inputs.get(i)).getSchema();
            if (!mergedSchema.isEqual(otherSchema))
                return null;
        }
        return mergedSchema;
    }

    @Override
    public void accept(PlanVisitor v) throws IOException {
        if (!(v instanceof LogicalPlanVisitor)) {
            throw new IOException("Expected LogicalPlanVisitor");
        }
        ((LogicalPlanVisitor)v).visitLOUnion(this);
    }

    @Override
    public boolean isEqual(Operator other) {
        if (other != null && other instanceof LOUnion) { 
            return checkEquality((LOUnion)other);
        } else {
            return false;
        }
    }

}
