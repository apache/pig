/**
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

package org.apache.pig.newplan.logical.visitor;

import java.util.ArrayList;
import java.util.List;

import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.builtin.IdentityColumn;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.newplan.DependencyOrderWalker;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.OperatorSubPlan;
import org.apache.pig.newplan.logical.expression.LogicalExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.expression.UserFuncExpression;
import org.apache.pig.newplan.logical.optimizer.SchemaResetter;
import org.apache.pig.newplan.logical.optimizer.UidResetter;
import org.apache.pig.newplan.logical.relational.LOCross;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOGenerate;
import org.apache.pig.newplan.logical.relational.LOInnerLoad;
import org.apache.pig.newplan.logical.relational.LOJoin;
import org.apache.pig.newplan.logical.relational.LOSort;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalNodesVisitor;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.newplan.logical.relational.LogicalSchema.LogicalFieldSchema;

public class DuplicateForEachColumnRewriteVisitor
             extends LogicalRelationalNodesVisitor {

    public DuplicateForEachColumnRewriteVisitor (OperatorPlan plan)
        throws FrontendException {
        super(plan, new DependencyOrderWalker(plan));
    }

    @Override
    public void visit(LOForEach foreach) throws FrontendException {
        List <LogicalExpressionPlan> expPlansToInsertIdentity
                                = findAllExpPlansToInsertIdentity(foreach);
        if( expPlansToInsertIdentity.isEmpty() ) {
          return;
        }


        // Following section is moved from
        // newplan/logical/rules/DuplicateForEachColumnRewrite.transform()

        for (LogicalExpressionPlan expPlan : expPlansToInsertIdentity) {
            LogicalExpression oldRoot = (LogicalExpression)expPlan.getSources().get(0);
            UserFuncExpression userFuncExpression = new UserFuncExpression(expPlan, new FuncSpec(IdentityColumn.class.getName()));
            expPlan.connect(userFuncExpression, oldRoot);
        }
        expPlansToInsertIdentity.clear();

        // Since we adjust the uid layout, clear all cached uids
        UidResetter uidResetter = new UidResetter(plan);
        uidResetter.visit();

        // Manually regenerate schema
        // skip duplicate uid check in schema as it would be fixed in
        // only portion of the plan
        SchemaResetter schemaResetter = new SchemaResetter(plan, true);
        schemaResetter.visit();
    }

    // moved from newplan/logical/rules/DuplicateForEachColumnRewrite.check()
    private List<LogicalExpressionPlan> findAllExpPlansToInsertIdentity(LOForEach foreach) throws FrontendException {

        LOGenerate gen = (LOGenerate)foreach.getInnerPlan().getSinks().get(0);

        List<LogicalExpressionPlan> expPlans = gen.getOutputPlans();
        boolean[] flattens = gen.getFlattenFlags();

        List<LogicalExpressionPlan> expPlansToInsertIdentity = new ArrayList<LogicalExpressionPlan>();
        List<Long> uidSeen = new ArrayList<Long>();

        for (int i=0;i<expPlans.size();i++) {
            LogicalExpressionPlan expPlan = expPlans.get(i);
            boolean flatten = flattens[i];
            LogicalExpression exp = (LogicalExpression)expPlan.getSources().get(0);
            if (exp.getFieldSchema()!=null) {
                if (flatten && (exp.getFieldSchema().type == DataType.BAG || exp.getFieldSchema().type == DataType.TUPLE)) {
                    List<LogicalFieldSchema> innerFieldSchemas = null;
                    if (exp.getFieldSchema().type == DataType.BAG) {
                        if (exp.getFieldSchema().schema!=null) {
                            if (exp.getFieldSchema().type == DataType.BAG) {
                                //  assert(fieldSchema.schema.size() == 1 && fieldSchema.schema.getField(0).type == DataType.TUPLE)
                                if (exp.getFieldSchema().schema.getField(0).schema!=null)
                                    innerFieldSchemas = exp.getFieldSchema().schema.getField(0).schema.getFields();
                            } else {
                                if (exp.getFieldSchema().schema!=null)
                                    innerFieldSchemas = exp.getFieldSchema().schema.getFields();
                            }
                        }
                    }
                    else { // DataType.TUPLE
                        if (exp.getFieldSchema().schema!=null)
                            innerFieldSchemas = exp.getFieldSchema().schema.getFields();
                    }
                    if (innerFieldSchemas != null) {
                        for (LogicalFieldSchema innerFieldSchema : innerFieldSchemas) {
                            long uid = innerFieldSchema.uid;
                            if (checkAndAdd(uid, uidSeen)) {
                                // Seen before
                                expPlansToInsertIdentity.add(expPlan);
                                break;
                            }
                        }
                    }
                }
                else {
                    long uid = exp.getFieldSchema().uid;
                    if (checkAndAdd(uid, uidSeen)) {
                        // Seen before
                        expPlansToInsertIdentity.add(expPlan);
                    }
                }
            }
        }
        return expPlansToInsertIdentity;
    }

    private boolean checkAndAdd(long uid, List<Long> uidSeen) {
        if (uidSeen.contains(uid))
            return true;
        uidSeen.add(uid);
        return false;
    }

}

