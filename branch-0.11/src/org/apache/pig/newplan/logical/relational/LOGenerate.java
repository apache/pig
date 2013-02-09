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
import java.util.List;
import java.util.Map;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanVisitor;
import org.apache.pig.newplan.logical.expression.LogicalExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.relational.LogicalSchema.LogicalFieldSchema;

public class LOGenerate extends LogicalRelationalOperator {
     private List<LogicalExpressionPlan> outputPlans;
     private boolean[] flattenFlags;
     private List<LogicalSchema> mUserDefinedSchema = null;
     private List<LogicalSchema> outputPlanSchemas = null;
     // If LOGenerate generate new uid, cache it here.
     // This happens when expression plan does not have complete schema, however,
     // user give complete schema in ForEach statement in script
     private List<LogicalSchema> uidOnlySchemas = null;

    public LOGenerate(OperatorPlan plan, List<LogicalExpressionPlan> ps, boolean[] flatten) {
        this( plan );
        outputPlans = ps;
        flattenFlags = flatten;
    }
    
    public void setOutputPlans(List<LogicalExpressionPlan> plans) {
        this.outputPlans = plans;
    }
    
    public LOGenerate(OperatorPlan plan) {
        super( "LOGenerate", plan );
    }

    @Override
    public LogicalSchema getSchema() throws FrontendException {
        if (schema != null) {
            return schema;
        }
        
        if (uidOnlySchemas == null) {
            uidOnlySchemas = new ArrayList<LogicalSchema>();
            for (int i=0;i<outputPlans.size();i++)
                uidOnlySchemas.add(null);
        }
        
        schema = new LogicalSchema();
        outputPlanSchemas = new ArrayList<LogicalSchema>();
        
        for(int i=0; i<outputPlans.size(); i++) {
            LogicalExpression exp = (LogicalExpression)outputPlans.get(i).getSources().get(0);
            
            LogicalSchema mUserDefinedSchemaCopy = null;
            if (mUserDefinedSchema!=null && mUserDefinedSchema.get(i)!=null) {
                mUserDefinedSchemaCopy = new LogicalSchema();
                for (LogicalSchema.LogicalFieldSchema fs : mUserDefinedSchema.get(i).getFields()) {
                    mUserDefinedSchemaCopy.addField(fs.deepCopy());
                }
            }
            
            LogicalFieldSchema fieldSchema = null;
            
            // schema of the expression after flatten
            LogicalSchema expSchema = null;
            
            if (exp.getFieldSchema()!=null) {
            
                fieldSchema = exp.getFieldSchema().deepCopy();
                
                expSchema = new LogicalSchema();
                if ((fieldSchema.type != DataType.TUPLE && fieldSchema.type != DataType.BAG)||!flattenFlags[i]) {
                    // if type is primitive, just add to schema
                    if (fieldSchema!=null)
                        expSchema.addField(fieldSchema);
                    else
                        expSchema = null;
                } else {
                    // if bag/tuple don't have inner schema, after flatten, we don't have schema for the entire operator
                    if (fieldSchema.schema==null) {
                        expSchema = null;
                    }
                    else {
                     // if we come here, we get a BAG/Tuple with flatten, extract inner schema of the tuple as expSchema
                        List<LogicalSchema.LogicalFieldSchema> innerFieldSchemas = new ArrayList<LogicalSchema.LogicalFieldSchema>();
                        if (flattenFlags[i]) {
                            if (fieldSchema.type == DataType.BAG) {
                                // if it is bag, get the schema of tuples
                                if (fieldSchema.schema!=null) {
                                    if (fieldSchema.schema.getField(0).schema!=null)
                                        innerFieldSchemas = fieldSchema.schema.getField(0).schema.getFields();
                                    for (LogicalSchema.LogicalFieldSchema fs : innerFieldSchemas) {
                                        fs.alias = fs.alias == null ? null : fieldSchema.alias + "::" + fs.alias;
                                    }
                                }
                            } else { // DataType.TUPLE
                                innerFieldSchemas = fieldSchema.schema.getFields();
                                for (LogicalSchema.LogicalFieldSchema fs : innerFieldSchemas) {
                                    fs.alias = fs.alias == null ? null : fieldSchema.alias + "::" + fs.alias;
                                }
                            }
                            
                            for (LogicalSchema.LogicalFieldSchema fs : innerFieldSchemas)
                                expSchema.addField(fs);
                        }
                        else
                            expSchema.addField(fieldSchema);
                    }
                }
            }
            
            // Merge with user defined schema
            if (expSchema!=null && expSchema.size()==0)
                expSchema = null;
            LogicalSchema planSchema = new LogicalSchema();
            if (mUserDefinedSchemaCopy!=null) {
                LogicalSchema mergedSchema = new LogicalSchema();
                // merge with userDefinedSchema
                if (expSchema==null) {
                    // Use user defined schema
                    for (LogicalFieldSchema fs : mUserDefinedSchemaCopy.getFields()) {
                        fs.stampFieldSchema();
                        mergedSchema.addField(new LogicalFieldSchema(fs));
                    }
                    if(mergedSchema.size() == 1 && mergedSchema.getField(0).type == DataType.NULL){
                        //this is the use case where a new alias has been specified by user
                        mergedSchema.getField(0).type = DataType.BYTEARRAY;
                    }
                
                } else {

                    // Merge uid with the exp field schema
                    mergedSchema = LogicalSchema.merge(mUserDefinedSchemaCopy, expSchema, LogicalSchema.MergeMode.LoadForEach);
                    if (mergedSchema==null) {
                        throw new FrontendException(this, "Cannot merge (" + expSchema.toString(false) + 
                                ") with user defined schema (" + mUserDefinedSchemaCopy.toString(false) + ")", 1117);
                    }
                    mergedSchema.mergeUid(expSchema);

                }
                for (LogicalFieldSchema fs : mergedSchema.getFields())
                    planSchema.addField(fs);
            } else {
                // if any plan do not have schema, the whole LOGenerate do not have schema
                if (expSchema==null) {
                    planSchema = null;
                }
                else {
                    // Merge schema for the plan
                    for (LogicalFieldSchema fs : expSchema.getFields())
                        planSchema.addField(fs);
                }
            }
            
            if (planSchema==null) {
                schema = null;
                break;
            }
            for (LogicalFieldSchema fs : planSchema.getFields())
                schema.addField(fs);
            
            // If the schema is generated by user defined schema, keep uid
            if (expSchema==null) {
                LogicalSchema uidOnlySchema = planSchema.mergeUid(uidOnlySchemas.get(i));
                uidOnlySchemas.set(i, uidOnlySchema);
            }
            outputPlanSchemas.add(planSchema);
        }
        if (schema==null || schema.size()==0) {
            schema = null;
            outputPlanSchemas = null;
        }
        return schema;
    }

    public List<LogicalExpressionPlan> getOutputPlans() {
        return outputPlans;
    }
    
    public boolean[] getFlattenFlags() {
        return flattenFlags;
    }
    
    public void setFlattenFlags(boolean[] flatten) {
        flattenFlags = flatten;
    }
    
    @Override
    public boolean isEqual(Operator other) throws FrontendException {
        if (!(other instanceof LOGenerate)) {
            return false;
        }
        
        List<LogicalExpressionPlan> otherPlan = ((LOGenerate)other).getOutputPlans();
        boolean[] fs = ((LOGenerate)other).getFlattenFlags();
        
        if (outputPlans.size() != otherPlan.size()) {
            return false;
        }
        
        for(int i=0; i<outputPlans.size(); i++) {
            if (flattenFlags[i] != fs[i]) {
                return false;
            }
            
            if (!outputPlans.get(i).isEqual(otherPlan.get(i))) {
                return false;
            }
        }
        
        return true;
    }
  
    @Override
    public void accept(PlanVisitor v) throws FrontendException {
         if (!(v instanceof LogicalRelationalNodesVisitor)) {
             throw new FrontendException("Expected LogicalPlanVisitor", 2223);
         }
         ((LogicalRelationalNodesVisitor)v).visit(this);
    }
    
    @Override
    public String toString() {
        StringBuilder msg = new StringBuilder();

        if (alias!=null) {
            msg.append(alias + ": ");
        }
        msg.append("(Name: " + name + "[");
        for (int i=0;i<flattenFlags.length;i++) {
            msg.append(flattenFlags[i]);
            if (i!=flattenFlags.length-1)
                msg.append(",");
        }
        msg.append("] Schema: ");
        if (schema!=null)
            msg.append(schema);
        else
            msg.append("null");
        msg.append(")");
        if (annotations!=null) {
            for (Map.Entry<String, Object> entry : annotations.entrySet()) {
                msg.append(entry);
            }
        }
        return msg.toString();
    }
    
    public List<LogicalSchema> getUserDefinedSchema() {
        return mUserDefinedSchema;
    }

    public void setUserDefinedSchema(List<LogicalSchema> userDefinedSchema) {
        mUserDefinedSchema = userDefinedSchema;
    }
    
    /**
     * Get the output schema corresponding to each input expression plan
     * @return list of output schemas
     */
    public List<LogicalSchema> getOutputPlanSchemas() {
        return outputPlanSchemas;
    }
    
    public void setOutputPlanSchemas(List<LogicalSchema> outputPlanSchemas) {
        this.outputPlanSchemas = outputPlanSchemas;
    }
    
    public List<LogicalSchema> getUidOnlySchemas() {
        return uidOnlySchemas;
    }
    
    public void setUidOnlySchemas(List<LogicalSchema> uidOnlySchemas) {
        this.uidOnlySchemas = uidOnlySchemas;
    }
    
    @Override
    public void resetUid() {
        this.uidOnlySchemas = null;
    }
    
    @Override
    public void resetSchema(){
        super.resetSchema();
        outputPlanSchemas = null;
    }
}
