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
package org.apache.pig.newplan.logical.visitor;

import java.util.ArrayList;
import java.util.List;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.DependencyOrderWalker;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.expression.CastExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOGenerate;
import org.apache.pig.newplan.logical.relational.LOInnerLoad;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalNodesVisitor;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.newplan.logical.relational.LogicalSchema.LogicalFieldSchema;

public class ForEachUserSchemaVisitor extends LogicalRelationalNodesVisitor {
    public ForEachUserSchemaVisitor(OperatorPlan plan) throws FrontendException {
        super(plan, new DependencyOrderWalker(plan));
    }

    private static LogicalSchema replaceNullByteArraySchema(
                         LogicalSchema originalSchema,
                         LogicalSchema userSchema) throws FrontendException {
        if( originalSchema == null && userSchema == null ) {
            return null;
        } else if ( originalSchema == null ) {
            return userSchema.deepCopy();
        } else if ( userSchema == null ) {
            return originalSchema.deepCopy();
        }

        LogicalSchema replacedSchema = new LogicalSchema();
        for (int i=0;i<originalSchema.size();i++) {
            LogicalFieldSchema replacedFS = replaceNullByteArrayFieldSchema(originalSchema.getField(i), userSchema.getField(i));
            replacedSchema.addField(replacedFS);
        }
        return replacedSchema;
    }

    private static LogicalFieldSchema replaceNullByteArrayFieldSchema(
                         LogicalFieldSchema originalFS,
                         LogicalFieldSchema userFS) throws FrontendException {
        if( originalFS == null && userFS == null ) {
            return null;
        } else if ( originalFS == null ) {
            return userFS.deepCopy();
        } else if ( userFS == null ) {
            return originalFS.deepCopy();
        }
        if ( originalFS.type==DataType.NULL
            || originalFS.type==DataType.BYTEARRAY ) {
            return userFS.deepCopy();
        } else if ( userFS.type==DataType.NULL
            || userFS.type==DataType.BYTEARRAY ) {
            // Use originalFS schema but keep the alias from userFS
            return new LogicalFieldSchema(userFS.alias, originalFS.schema,  originalFS.type);
        }

        if ( !DataType.isSchemaType(originalFS.type) ) {
            return userFS.deepCopy();
        } else {
            LogicalSchema replacedSchema = replaceNullByteArraySchema(originalFS.schema, userFS.schema);
            return new LogicalFieldSchema(userFS.alias, replacedSchema, userFS.type);
        }
    }

    private static boolean hasOnlyNullOrByteArraySchema (LogicalFieldSchema fs) {
        if( DataType.isSchemaType(fs.type) ) {
            if( fs.schema != null ) {
                for (LogicalFieldSchema sub_fs : fs.schema.getFields() ) {
                    if( !hasOnlyNullOrByteArraySchema(sub_fs)  ) {
                        return false;
                    }
                }
            }
        } else if( fs.type != DataType.NULL && fs.type != DataType.BYTEARRAY )  {
            return false;
        }
        return true;
    }

    @Override
    public void visit(LOForEach foreach) throws FrontendException {
        LOGenerate generate = (LOGenerate)foreach.getInnerPlan().getSinks().get(0);
        List<LogicalSchema> mExpSchemas = generate.getExpSchemas();
        List<LogicalSchema> mUserDefinedSchemas = generate.getUserDefinedSchema();

        // Skip if no way to figure out schema (usually both expression schema and
        // user defined schema are null)
        if (foreach.getSchema()==null) {
            return;
        }

        if (mUserDefinedSchemas==null) {
            return;
        }

        boolean hasUserDefinedSchema = false;
        for (LogicalSchema mUserDefinedSchema : mUserDefinedSchemas) {
            if (mUserDefinedSchema!=null) {
                hasUserDefinedSchema = true;
                break;
            }
        }

        if (!hasUserDefinedSchema) {
            return;
        }

        if (mExpSchemas.size()!=mUserDefinedSchemas.size()) {
            throw new FrontendException("Size mismatch: Get " + mExpSchemas.size() +
                    " mExpSchemas, but " + mUserDefinedSchemas.size() + " mUserDefinedSchemas",
                    0, generate.getLocation());
        }

        LogicalPlan innerPlan = new LogicalPlan();
        LOForEach casterForEach = new LOForEach(plan);
        casterForEach.setInnerPlan(innerPlan);
        casterForEach.setAlias(foreach.getAlias());

        List<LogicalExpressionPlan> exps = new ArrayList<LogicalExpressionPlan>();
        LOGenerate gen = new LOGenerate(innerPlan, exps, null);
        innerPlan.add(gen);

        int index = 0;
        boolean needCast = false;
        for(int i=0;i<mExpSchemas.size();i++) {
            LogicalSchema mExpSchema = mExpSchemas.get(i);
            LogicalSchema mUserDefinedSchema = mUserDefinedSchemas.get(i);

            // Use user defined schema to cast, this is the prevailing use case
            if (mExpSchema==null) {
                for (LogicalFieldSchema fs : mUserDefinedSchema.getFields()) {
                    if (hasOnlyNullOrByteArraySchema(fs)) {
                        addToExps(casterForEach, innerPlan, gen, exps, index, false, null);
                    } else {
                        addToExps(casterForEach, innerPlan, gen, exps, index, true, fs);
                        needCast = true;
                    }
                    index++;
                }
                continue;
            }

            // No user defined schema, no need to cast
            if (mUserDefinedSchema==null) {
                for (int j=0;j<mExpSchema.size();j++) {
                    addToExps(casterForEach, innerPlan, gen, exps, index, false, null);
                    index++;
                }
                continue;
            }

            // Expression has schema, but user also define schema, need cast only
            // when there is a mismatch
            if (mExpSchema.size()!=mUserDefinedSchema.size()) {
                throw new FrontendException("Size mismatch: Cannot cast " + mExpSchema.size() +
                        " fields to " + mUserDefinedSchema.size(), 0, foreach.getLocation());
            }

            LogicalSchema replacedSchema = replaceNullByteArraySchema(mExpSchema,mUserDefinedSchema);
            for (int j=0;j<mExpSchema.size();j++) {
                LogicalFieldSchema mExpFieldSchema = mExpSchema.getField(j);
                LogicalFieldSchema mUserDefinedFieldSchema = replacedSchema.getField(j);

                if (hasOnlyNullOrByteArraySchema(mUserDefinedFieldSchema) ||
                    LogicalFieldSchema.typeMatch(mExpFieldSchema, mUserDefinedFieldSchema)) {
                    addToExps(casterForEach, innerPlan, gen, exps, index, false, null);
                } else {
                    addToExps(casterForEach, innerPlan, gen, exps, index, true, mUserDefinedFieldSchema);
                    needCast = true;
                }
                index++;
            }
        }

        gen.setFlattenFlags(new boolean[index]);
        if (needCast) {
            // Insert the casterForEach into the plan and patch up the plan.
            List <Operator> successorOps = plan.getSuccessors(foreach);
            if (successorOps != null && successorOps.size() > 0){
                Operator next = plan.getSuccessors(foreach).get(0);
                plan.insertBetween(foreach, casterForEach, next);
            }else{
                plan.add(casterForEach);
                plan.connect(foreach,casterForEach);
            }

            // Since the explict cast is now inserted after the original foreach,
            // throwing away the user defined "types" but keeping the user
            // defined names from the original foreach.
            // 'generate' (LOGenerate) still holds the reference to this
            // mUserDefinedSchemas
            for( LogicalSchema mUserDefinedSchema : mUserDefinedSchemas ) {
                resetTypeToNull( mUserDefinedSchema );
            }

            // Given mUserDefinedSchema was changed, we should drop the cached schema
            foreach.resetSchema();
            generate.resetSchema();
        }
    }

    private void resetTypeToNull (LogicalSchema s1) {
        if( s1 != null ) {
            for (LogicalFieldSchema fs : s1.getFields()) {
                if( DataType.isSchemaType(fs.type) ) {
                    resetTypeToNull(fs.schema);
                } else {
                    fs.type = DataType.NULL;
                }
            }
        }
    }

    private void addToExps(LOForEach casterForEach, LogicalPlan innerPlan, LOGenerate gen,
            List<LogicalExpressionPlan> exps, int index, boolean needCaster, LogicalFieldSchema fs) {

        LOInnerLoad innerLoad = new LOInnerLoad(innerPlan, casterForEach, index);
        innerPlan.add(innerLoad);
        innerPlan.connect(innerLoad, gen);

        LogicalExpressionPlan exp = new LogicalExpressionPlan();

        ProjectExpression prj = new ProjectExpression(exp, index, 0, gen);
        exp.add(prj);

        if (needCaster) {
            CastExpression cast = new CastExpression(exp, prj, new LogicalSchema.LogicalFieldSchema(fs));
            exp.add(cast);
        }
        exps.add(exp);
    }
}
