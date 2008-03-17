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
import java.util.Map;

import org.apache.pig.impl.eval.cond.Cond;
import org.apache.pig.impl.logicalLayer.schema.TupleSchema;

public class LOSplit extends LogicalOperator {
    private static final long serialVersionUID = 1L;

      List<Cond> conditions = new ArrayList<Cond>();

    public LOSplit(Map<OperatorKey, LogicalOperator> opTable,
                   String scope, 
                   long id, 
                   OperatorKey input) {
        super(opTable, scope, id, input);
    }
    
    public void addCond(Cond cond) {
        conditions.add(cond);
    }

    @Override
    public int getOutputType() {
        return opTable.get(getInputs().get(0)).getOutputType();
    }

    public ArrayList<Cond> getConditions() {
        return new ArrayList<Cond> (conditions);
    }

    @Override
    public TupleSchema outputSchema() {
        return opTable.get(getInputs().get(0)).outputSchema().copy();
    }

    @Override
    public String name() {
        StringBuilder sb = new StringBuilder();
        sb.append("Split ");
        sb.append(scope);
        sb.append("-");
        sb.append(id);
        return sb.toString();
    }
    
    public void visit(LOVisitor v) {
        v.visitSplit(this);
    }

}
