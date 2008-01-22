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

import org.apache.pig.impl.eval.EvalSpec;
import org.apache.pig.impl.logicalLayer.schema.TupleSchema;



public class LOEval extends LogicalOperator {
    private static final long serialVersionUID = 1L;

    protected EvalSpec spec;

    public LOEval(LogicalOperator input, EvalSpec specIn) {
        super(input);
        spec = specIn;
        getOutputType();
    }

    @Override
    public String name() {
        return "Foreach";
    }

    @Override
    public String arguments() {
        return spec.toString();
    }

    @Override
    public TupleSchema outputSchema() {
        if (schema == null) {
            //System.out.println("LOEval input: " + inputs[0].outputSchema());
            //System.out.println("LOEval spec: " + spec);
            schema =
                (TupleSchema) spec.getOutputSchemaForPipe(getInputs().get(0).
                                                          outputSchema());

            //System.out.println("LOEval output: " + schema);
        }
        schema.setAlias(alias);
        return schema;
    }

    @Override
    public int getOutputType() {
        switch (getInputs().get(0).getOutputType()) {
        case FIXED:
            return FIXED;
        case MONOTONE:
        case AMENDABLE:
            return MONOTONE;
        default:
            throw new RuntimeException("Wrong type of input to EVAL");
        }
    }

    @Override
    public List<String> getFuncs() {
        List<String> funcs = super.getFuncs();
        funcs.addAll(spec.getFuncs());
        return funcs;
    }

    public EvalSpec getSpec() {
        return spec;
    }

	public void visit(LOVisitor v) {
		v.visitEval(this);
	}
}
