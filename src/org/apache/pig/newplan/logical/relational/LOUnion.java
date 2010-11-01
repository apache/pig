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

package org.apache.pig.newplan.logical.relational;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanVisitor;
import org.apache.pig.newplan.logical.expression.LogicalExpression;

public class LOUnion extends LogicalRelationalOperator {

    // uid mapping from output uid to input uid
    private List<Pair<Long, Long>> uidMapping = new ArrayList<Pair<Long, Long>>();
    
    public LOUnion(OperatorPlan plan) {
        super("LOUnion", plan);
    }    
    @Override
    public LogicalSchema getSchema() throws FrontendException {
        if (schema != null) {
            return schema;
        }
        List<Operator> inputs = null;
        inputs = plan.getPredecessors(this);
        
        // If any predecessor's schema is null, then the schema for union is null
        for (Operator input : inputs) {
            LogicalRelationalOperator op = (LogicalRelationalOperator)input;
            if (op.getSchema()==null)
                return null;
        }
        
        LogicalSchema s0 = ((LogicalRelationalOperator)inputs.get(0)).getSchema();
        if (inputs.size()==1)
            return s0;
        LogicalSchema s1 = ((LogicalRelationalOperator)inputs.get(1)).getSchema();
        LogicalSchema mergedSchema = LogicalSchema.merge(s0, s1);
        
        // Merge schema
        for (int i=2;i<inputs.size();i++) {
            LogicalSchema otherSchema = ((LogicalRelationalOperator)inputs.get(i)).getSchema();
            if (mergedSchema==null || otherSchema==null)
                return null;
            mergedSchema = LogicalSchema.merge(mergedSchema, otherSchema);
            if (mergedSchema == null)
                return null;
        }
        
        // Bring back cached uid if any; otherwise, cache uid generated
        for (int i=0;i<s0.size();i++)
        {
            LogicalSchema.LogicalFieldSchema fs = mergedSchema.getField(i);
            long uid = -1;
            for (Pair<Long, Long> pair : uidMapping) {
                if (pair.second==s0.getField(i).uid) {
                    uid = pair.first;
                    break;
                }
            }
            if (uid==-1) {
                uid = LogicalExpression.getNextUid();
                for (Operator input : inputs) {
                    long inputUid = ((LogicalRelationalOperator)input).getSchema().getField(i).uid;
                    uidMapping.add(new Pair<Long, Long>(uid, inputUid));
                }
            }

            fs.uid = uid;
        }
        schema = mergedSchema;
        return schema;
    }

    @Override
    public void accept(PlanVisitor v) throws FrontendException {
        if (!(v instanceof LogicalRelationalNodesVisitor)) {
            throw new FrontendException("Expected LogicalPlanVisitor", 2223);
        }
        ((LogicalRelationalNodesVisitor)v).visit(this);
    }

    @Override
    public boolean isEqual(Operator other) throws FrontendException {
        if (other != null && other instanceof LOUnion) { 
            return checkEquality((LOUnion)other);
        } else {
            return false;
        }
    }

    // Get input uids mapping to the output uid
    public Set<Long> getInputUids(long uid) {
        Set<Long> result = new HashSet<Long>();
        for (Pair<Long, Long> pair : uidMapping) {
            if (pair.first==uid)
                result.add(pair.second);
        }
        return result;
    }
    
    @Override
    public void resetUid() {
        uidMapping = new ArrayList<Pair<Long, Long>>();
    }
}
