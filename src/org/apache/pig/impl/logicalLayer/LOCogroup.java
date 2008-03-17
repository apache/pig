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

import org.apache.pig.data.Datum;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.eval.EvalSpec;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.TupleSchema;



public class LOCogroup extends LogicalOperator {
    private static final long serialVersionUID = 1L;

    protected ArrayList<EvalSpec> specs;

    public LOCogroup(Map<OperatorKey, LogicalOperator> opTable,
                     String scope,
                     long id,
                     List<OperatorKey> inputs,
                     ArrayList<EvalSpec> specs) {
        super(opTable, scope, id, inputs);
        this.specs = specs;
        getOutputType();
    }

    @Override
    public String name() {
        StringBuilder sb = new StringBuilder();
        if (inputs.size() == 1) {
            sb.append("Group ");
        } else {
            sb.append("CoGroup ");
        }
        sb.append(scope);
        sb.append("-");
        sb.append(id);
        return sb.toString();
    }
    @Override
    public String arguments() {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < specs.size(); i++) {
            sb.append(specs.get(i));
            if (i + 1 < specs.size())
                sb.append(", ");
        }

        return sb.toString();
    }

    public static Datum[] getGroupAndTuple(Datum d) {
        if (!(d instanceof Tuple)) {
            throw new RuntimeException
                ("Internal Error: Evaluation of group expression did not return a tuple");
        }
        Tuple output = (Tuple) d;
        if (output.arity() < 2) {
            throw new RuntimeException
                ("Internal Error: Evaluation of group expression returned a tuple with <2 fields");
        }

        Datum[]groupAndTuple = new Datum[2];
        if (output.arity() == 2) {
            groupAndTuple[0] = output.getField(0);
            groupAndTuple[1] = output.getField(1);
        } else {
            Tuple group = new Tuple();
            for (int j = 0; j < output.arity() - 1; j++) {
                group.appendField(output.getField(j));
            }
            groupAndTuple[0] = group;
            groupAndTuple[1] = output.getField(output.arity() - 1);
        }
        return groupAndTuple;
    }

    @Override
    public TupleSchema outputSchema() {
        if (schema == null) {
            schema = new TupleSchema();


            Schema groupElementSchema =
                specs.get(0).getOutputSchemaForPipe(opTable.get(getInputs().get(0)).
                                                    outputSchema());
            if (groupElementSchema == null) {
                groupElementSchema = new TupleSchema();
                groupElementSchema.setAlias("group");
            } else {

                if (!(groupElementSchema instanceof TupleSchema))
                    throw new RuntimeException
                        ("Internal Error: Schema of group expression was atomic");
                List<Schema> fields =
                    ((TupleSchema) groupElementSchema).getFields();

                if (fields.size() < 2)
                    throw new RuntimeException
                        ("Internal Error: Schema of group expression retured <2 fields");

                if (fields.size() == 2) {
                    groupElementSchema = fields.get(0);
                    groupElementSchema.removeAllAliases();
                    groupElementSchema.setAlias("group");
                } else {
                    groupElementSchema = new TupleSchema();
                    groupElementSchema.setAlias("group");

                    for (int i = 0; i < fields.size() - 1; i++) {
                        ((TupleSchema) groupElementSchema).add(fields.get(i));
                    }
                }

            }

            schema.add(groupElementSchema);

          for (OperatorKey key: getInputs()) {
              LogicalOperator lo = opTable.get(key);
                TupleSchema inputSchema = lo.outputSchema();
                if (inputSchema == null)
                    inputSchema = new TupleSchema();
                schema.add(inputSchema);
            }
        }

        schema.setAlias(alias);
        return schema;
    }

    @Override
    public int getOutputType() {
        int outputType = FIXED;
        for (int i = 0; i < getInputs().size(); i++) {
            switch (opTable.get(getInputs().get(i)).getOutputType()) {
            case FIXED:
                continue;
            case MONOTONE:
                outputType = AMENDABLE;
                break;
            case AMENDABLE:
            default:
                throw new RuntimeException
                    ("Can't feed a cogroup into another in the streaming case");
            }
        }
        return outputType;
    }

    @Override
    public List<String> getFuncs() {
        List<String> funcs = super.getFuncs();
      for (EvalSpec spec:specs) {
            funcs.addAll(spec.getFuncs());
        }
        return funcs;
    }

    public ArrayList<EvalSpec> getSpecs() {
        return specs;
    }

    public void visit(LOVisitor v) {
        v.visitCogroup(this);
    }

}
