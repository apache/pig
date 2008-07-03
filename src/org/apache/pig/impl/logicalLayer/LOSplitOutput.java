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
import java.util.Map;

import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.schema.TupleSchema;


public class LOSplitOutput extends LogicalOperator {
    private static final long serialVersionUID = 1L;

    protected int index;
    
    public LOSplitOutput(Map<OperatorKey, LogicalOperator> opTable,
                         String scope,
                         long id,
                         OperatorKey splitOpInput,
                         int index) {
        super(opTable, scope, id, splitOpInput);
        this.index = index;
    }
    
    @Override
    public String toString() {
        StringBuilder result = new StringBuilder(super.toString());
        result.append(" (index: ");
        result.append(index);
        result.append(')');
        return result.toString();
    }


    @Override
    public String name() {
        StringBuilder sb = new StringBuilder();
        sb.append("SplitOutput ");
        sb.append(scope);
        sb.append("-");
        sb.append(id);
        return sb.toString();
    }

    @Override
    public TupleSchema outputSchema() {
        return opTable.get(getInputs().get(0)).outputSchema();
    }

    @Override
    public int getOutputType() {
        return opTable.get(getInputs().get(0)).getOutputType();
    }

    public void visit(LOVisitor v) {
        v.visitSplitOutput(this);
    }

    public int getReadFrom() {
        return index;
    }
}