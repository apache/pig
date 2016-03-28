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

import java.util.ArrayList;
import java.util.List;

import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.streaming.StreamingCommand;
import org.apache.pig.impl.streaming.StreamingCommand.HandleSpec;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.expression.CastExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOGenerate;
import org.apache.pig.newplan.logical.relational.LOInnerLoad;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LOStream;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.newplan.optimizer.Rule;
import org.apache.pig.newplan.optimizer.Transformer;

public abstract class TypeCastInserter extends Rule {

    public TypeCastInserter(String n) {
        super(n, true);
    }

    protected abstract LogicalSchema determineSchema(LogicalRelationalOperator op) throws FrontendException;

    @Override
    public Transformer getNewTransformer() {
        return new TypeCastInserterTransformer();
    }

    public class TypeCastInserterTransformer extends Transformer {
        @Override
        public boolean check(OperatorPlan matched) throws FrontendException {
            LogicalRelationalOperator op = (LogicalRelationalOperator)matched.getSources().get(0);
            LogicalSchema s = op.getSchema();
            if (s == null) return false;

            // only process each node once
            if (isCastAdjusted(op)) return false;

            if (op instanceof LOLoad) {
                if (((LOLoad)op).getScriptSchema()==null) return false;
            }
            else {
                if (((LOStream)op).getScriptSchema()==null) return false;
            }

            // Now that we've narrowed it down to an operation that *can* have casts added,
            // (because the user specified some types which might not match the data) let's
            // see if they're actually needed:
            LogicalSchema determinedSchema = determineSchema(op);
            if(determinedSchema == null || determinedSchema.size() != s.size()) {
                // we don't know what the data looks like, but the user has specified
                // that they want a certain number of fields loaded.
                return true;
            }
            if(atLeastOneCastNeeded(determinedSchema, s)) {
                return true;
            }

            return false;
        }

        private boolean atLeastOneCastNeeded(LogicalSchema determinedSchema, LogicalSchema s) {
            for (int i = 0; i < s.size(); i++) {
                LogicalSchema.LogicalFieldSchema fs = s.getField(i);
                if (fs.type != DataType.BYTEARRAY && !fs.isEqual(determinedSchema.getField(i))) {
                    // we have to cast this field from the default BYTEARRAY type to
                    // whatever the user specified in the 'AS' clause of the LOAD
                    // statement (the fs.type).
                    return true;
                }
            }
            return false;
        }

        @Override
        public void transform(OperatorPlan matched) throws FrontendException {
            LogicalRelationalOperator op = (LogicalRelationalOperator)matched.getSources().get(0);
            LogicalSchema s = op.getSchema();
            LogicalSchema determinedSchema = determineSchema(op);

            if (currentPlan.getSuccessors(op) == null) {
                // the output of this LOAD's not going anywhere, so we don't need
                // to bother about tidying up the output
                return;
            }

            // For every field, build a logical plan.  If the field has a type
            // other than byte array, then the plan will be cast(project).  Else
            // it will just be project.
            LogicalPlan innerPlan = new LogicalPlan();

            LOForEach foreach = new LOForEach(currentPlan);
            foreach.setInnerPlan(innerPlan);
            foreach.setAlias(op.getAlias());
            // Insert the foreach into the plan and patch up the plan.
            Operator next = currentPlan.getSuccessors(op).get(0);
            currentPlan.insertBetween(op, foreach, next);

            List<LogicalExpressionPlan> exps = new ArrayList<LogicalExpressionPlan>();
            LOGenerate gen = new LOGenerate(innerPlan, exps, new boolean[s.size()]);
            innerPlan.add(gen);

            for (int i = 0; i < s.size(); i++) {
                LogicalSchema.LogicalFieldSchema fs = s.getField(i);

                LOInnerLoad innerLoad = new LOInnerLoad(innerPlan, foreach, i);
                innerPlan.add(innerLoad);
                innerPlan.connect(innerLoad, gen);

                LogicalExpressionPlan exp = new LogicalExpressionPlan();

                ProjectExpression prj = new ProjectExpression(exp, i, -1, gen);
                exp.add(prj);

                if (fs.type != DataType.BYTEARRAY && (determinedSchema == null || (!fs.isEqual(determinedSchema.getField(i))))) {
                    // Either no schema was determined by loader OR the type
                    // from the "determinedSchema" is different
                    // from the type specified - so we need to cast
                    CastExpression cast = new CastExpression(exp, prj, new LogicalSchema.LogicalFieldSchema(fs));
                    exp.add(cast);
                    FuncSpec loadFuncSpec = null;
                    if(op instanceof LOLoad) {
                        loadFuncSpec = ((LOLoad)op).getFileSpec().getFuncSpec();
                    } else if (op instanceof LOStream) {
                        StreamingCommand command = ((LOStream)op).getStreamingCommand();
                        HandleSpec streamOutputSpec = command.getOutputSpec();
                        loadFuncSpec = new FuncSpec(streamOutputSpec.getSpec());
                    } else {
                        String msg = "TypeCastInserter invoked with an invalid operator class name: " + innerPlan.getClass().getSimpleName();
                        throw new FrontendException(msg, 2242);
                    }
                    cast.setFuncSpec(loadFuncSpec);
                }
                exps.add(exp);
            }
            markCastInserted(op);
        }

        @Override
        public OperatorPlan reportChanges() {
            return currentPlan;
        }
    }

    protected abstract void markCastInserted(LogicalRelationalOperator op);

    protected abstract void markCastNoNeed(LogicalRelationalOperator op);

    protected abstract boolean isCastAdjusted(LogicalRelationalOperator op);
}
