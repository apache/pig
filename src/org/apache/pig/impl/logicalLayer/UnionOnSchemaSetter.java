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

import org.apache.pig.PigException;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.SchemaMergeException;
import org.apache.pig.impl.logicalLayer.validators.LogicalPlanValidationExecutor;
import org.apache.pig.impl.logicalLayer.validators.UnionOnSchemaSetException;
import org.apache.pig.impl.plan.CompilationMessageCollector;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanValidationException;

/**
 * A visitor that modifies the logical plan (if necessary) for union-onschema
 * functionality. It runs logical plan validator so that the correct schema
 * of its inputs is available. It inserts foreach statements in its input
 * if the input operator schema does not match the schema created by 
 * merging all input schemas 
 * 
 */
public class UnionOnSchemaSetter extends LOVisitor {

    private PigContext pigContext;


    public UnionOnSchemaSetter(LogicalPlan plan, PigContext pigContext) {
        super(plan, new DependencyOrderWalker<LogicalOperator, LogicalPlan>(plan));
        this.pigContext = pigContext;
    }


    public void visit(LOUnion loUnion) throws PlanValidationException, UnionOnSchemaSetException {
        if(!loUnion.isOnSchema()) {
            //Not union-onschema, nothing to be done
            return;
        }
        // run through validator first on inputs so that the schemas have the right
        //types for columns. It will run TypeCheckingValidator as well.
        // The compilation messages will be logged when validation is
        // done from PigServer, so not doing it here
        CompilationMessageCollector collector = new CompilationMessageCollector() ;
        boolean isBeforeOptimizer = true;


        LogicalPlanValidationExecutor validator = 
            new LogicalPlanValidationExecutor(mPlan, pigContext, isBeforeOptimizer);
        validator.validate(mPlan, collector);
        List<LogicalOperator> preds = mPlan.getPredecessors(loUnion);

        //validate each input schema, and collect them in the ArrayList
        ArrayList<Schema> schemas = new ArrayList<Schema>(preds.size());
        for(LogicalOperator lop : preds){
            Schema sch;
            try {
                sch = lop.getSchema();
            } catch (FrontendException e) {
                throw new UnionOnSchemaSetException("Error getting schema from logical operator");
            }
            if(sch == null)                     
            {                         
                String msg = "Schema of relation " + lop.getAlias()
                + " is null." 
                + " UNION ONSCHEMA cannot be used with relations that"
                + " have null schema.";
                throw new UnionOnSchemaSetException(msg, 1116, PigException.INPUT);
            }
            for(Schema.FieldSchema fs : sch.getFields()){
                if(fs.alias == null){
                    String msg = "Schema of relation " + lop.getAlias()
                    + " has a null fieldschema for column(s). Schema :" +
                    sch;
                    throw new UnionOnSchemaSetException(msg, 1116, PigException.INPUT);
                }
            }
            schemas.add(sch);
        }
        
        //create the merged schema
        Schema mergedSchema ;
        try {
            mergedSchema = Schema.mergeSchemasByAlias(schemas);   
        }catch(SchemaMergeException e)                 {
            String msg = "Error merging schemas for union operator : "
                + e.getMessage();
            throw new UnionOnSchemaSetException(msg, 1116, PigException.INPUT, e);
        }


        //create a user defined schema list for use in LOForeach
        // using merged schema
        ArrayList<Schema> mergedSchemaList = new ArrayList<Schema>();
        for(Schema.FieldSchema fs : mergedSchema.getFields()){
            // Use NULL datatype because the type will be set by the TypeChecking
            // visitors
            mergedSchemaList.add(
                    new Schema(new Schema.FieldSchema(fs.alias, DataType.NULL))
            );
        }

        // add a foreach for inputs that don't match mergedSchema, projecting
        // null for columns that don't exist in the input
        for(LogicalOperator lop : preds)                 
        {                     
            try {
                if(! lop.getSchema().equals(mergedSchema))
                {
                    //the mergedSchema is different from this operators schema
                    // so add a foreach to project columns appropriately
                    int mergeSchSz = mergedSchema.size();
                    ArrayList<LogicalPlan> generatePlans =
                        new ArrayList<LogicalPlan>(mergeSchSz);
                    ArrayList<Boolean> flattenList =
                        new ArrayList<Boolean>(mergeSchSz);

                    String scope = loUnion.getOperatorKey().getScope();
                    for(Schema.FieldSchema fs : mergedSchema.getFields()) { 
                        LogicalPlan projectPlan = new LogicalPlan();
                        Schema inpSchema = lop.getSchema();
                        flattenList.add(Boolean.FALSE);

                        int inpPos = inpSchema.getPositionSubName(fs.alias);

                        LogicalOperator columnProj = null;
                        boolean isCastNeeded = false;
                        if(inpPos == -1){   
                            //the column is not present in schema of this input,
                            // so project null
                            columnProj =
                                new LOConst(mPlan, getNextId(scope), null);
                            // cast is necessary if the type in schema is
                            // not a BYTEARRAY
                            if(fs.type != DataType.BYTEARRAY){
                                isCastNeeded = true;
                            }
                        }else {
                            //project the column from input
                            columnProj = 
                                new LOProject(projectPlan,
                                        new OperatorKey(
                                                scope, 
                                                NodeIdGenerator.getGenerator().getNextNodeId(scope)
                                        ),
                                        lop, inpPos
                                );

                            //cast is needed if types are different.    
                            //compatibility of types has already been checked
                            //during creation of mergedSchema
                            Schema.FieldSchema inpFs = inpSchema.getFieldSubNameMatch(fs.alias);
                            if(inpFs.type != fs.type)
                                isCastNeeded = true;
                        }
                        projectPlan.add(columnProj);

                        //add a LOCast if necessary
                        if(isCastNeeded){
                            LOCast loCast = new LOCast(
                                    projectPlan,
                                    getNextId(scope),
                                    fs.type
                            );
                            loCast.setFieldSchema(fs);
                            projectPlan.add(loCast);
                            projectPlan.connect(columnProj, loCast);
                        }
                        generatePlans.add(projectPlan);

                    }
                    LogicalOperator foreach = new LOForEach(
                            mPlan,
                            getNextId(scope),
                            generatePlans, flattenList,
                            mergedSchemaList
                    );
                    mPlan.add(foreach);
                    mPlan.insertBetween(lop, foreach, loUnion);
                }
            } 
            catch (FrontendException e) {
                String msg = "Error adding union operator " + loUnion.getAlias()
                   + ":" + e.getMessage();
                UnionOnSchemaSetException pe = new UnionOnSchemaSetException(msg);
                pe.initCause(e);
                throw pe;  
            }
            
        }
        
    }


    private OperatorKey getNextId(String scope) {
        return new OperatorKey(
                scope, 
                NodeIdGenerator.getGenerator().getNextNodeId(scope)
        );
    }

}
