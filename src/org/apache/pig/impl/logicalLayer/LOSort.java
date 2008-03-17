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

import java.util.Map;

import org.apache.pig.impl.eval.EvalSpec;
import org.apache.pig.impl.logicalLayer.schema.TupleSchema;


public class LOSort extends LogicalOperator {
    private static final long serialVersionUID = 1L;
    private EvalSpec sortSpec;


    protected EvalSpec spec;

    public EvalSpec getSpec() {
        return spec;
    }
    
    public LOSort(Map<OperatorKey, LogicalOperator> opTable,
                  String scope, 
                  long id, 
                  OperatorKey input, 
                  EvalSpec sortSpec) {
        super(opTable, scope, id, input);
        this.sortSpec = sortSpec;
        getOutputType();
    }

    @Override
    public String name() {
        StringBuilder sb = new StringBuilder();
        sb.append("SORT ");
        sb.append(scope);
        sb.append("-");
        sb.append(id);
        return sb.toString();
    }

    @Override
    public String arguments() {
        return sortSpec.toString();
    }

    @Override
    public int getOutputType() {
        switch (opTable.get(getInputs().get(0)).getOutputType()) {
        case FIXED:
            return FIXED;
        default:
            throw new RuntimeException
                ("Blocking operator such as sort cannot handle streaming input");
        }
    }

    @Override
    public TupleSchema outputSchema() {
        if (schema == null)
            schema = opTable.get(getInputs().get(0)).outputSchema().copy();

        schema.setAlias(alias);
        return schema;

    }

    public EvalSpec getSortSpec() {
        return sortSpec;
    }

    public void visit(LOVisitor v) {
        v.visitSort(this);
    }

}
