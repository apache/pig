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
package org.apache.pig.experimental.logical.relational;

import java.io.IOException;
import java.util.List;

import org.apache.pig.data.DataType;
import org.apache.pig.experimental.logical.expression.LogicalExpression;
import org.apache.pig.experimental.logical.expression.LogicalExpressionPlan;
import org.apache.pig.experimental.logical.expression.ProjectExpression;
import org.apache.pig.experimental.logical.relational.LogicalSchema.LogicalFieldSchema;
import org.apache.pig.experimental.plan.Operator;
import org.apache.pig.experimental.plan.OperatorPlan;
import org.apache.pig.experimental.plan.PlanVisitor;

public class LOGenerate extends LogicalRelationalOperator {
     private List<LogicalExpressionPlan> outputPlans;
     private boolean[] flattenFlags;

    public LOGenerate(OperatorPlan plan, List<LogicalExpressionPlan> ps, boolean[] flatten) {
        super("LOGenerate", plan);
        outputPlans = ps;
        flattenFlags = flatten;
    }

    @Override
    public LogicalSchema getSchema() {
        if (schema != null) {
            return schema;
        }
        
        schema = new LogicalSchema();
        
        for(int i=0; i<outputPlans.size(); i++) {
            LogicalExpression exp = (LogicalExpression)outputPlans.get(i).getSources().get(0);
            byte t = exp.getType();
            LogicalSchema fieldSchema = null;
            String alias = null;
            
            // for tuple and bag type, if there is projection, calculate schema of this field
            if (exp instanceof ProjectExpression) {                
                LogicalRelationalOperator op = null;
                try{
                    op = ((ProjectExpression)exp).findReferent(this);
                }catch(Exception e) {
                    throw new RuntimeException(e);
                }
                LogicalSchema s = op.getSchema();
                if (s != null) {
                    if (((ProjectExpression)exp).isProjectStar()) {
                        for(LogicalFieldSchema f: s.getFields()) {
                            schema.addField(f);
                        }
                        continue;
                    }
                    
                    fieldSchema = s.getField(((ProjectExpression)exp).getColNum()).schema;
                    alias = s.getField(((ProjectExpression)exp).getColNum()).alias;
                }
            }
            
            // if type is primitive, just add to schema
            if (t != DataType.TUPLE && t != DataType.BAG) {
                LogicalFieldSchema f = new LogicalSchema.LogicalFieldSchema(alias, fieldSchema, t, exp.getUid());                
                schema.addField(f);
                continue;
            }
            
            // if flatten is set, set schema of tuple field to this schema
            if (flattenFlags[i]) {
                if (t == DataType.BAG) {
                    // if it is bag of tuples, get the schema of tuples
                    if (fieldSchema != null && fieldSchema.size() == 1 
                        && fieldSchema.getField(0).type == DataType.TUPLE) {
                        
                        fieldSchema = fieldSchema.getField(0).schema;
                    }else {
                        fieldSchema = null;
                    }
                }
                
                if (fieldSchema != null) {
                    List<LogicalFieldSchema> ll = fieldSchema.getFields();
                    for(LogicalFieldSchema f: ll) {
                        LogicalFieldSchema nf = new LogicalSchema.LogicalFieldSchema(alias+"::"+f.alias, f.schema, f.type, f.uid); 
                        schema.addField(nf);
                    }                               
                } else {
                    schema = null;
                    break;
                }
            } else {
                 LogicalFieldSchema f = new LogicalSchema.LogicalFieldSchema(alias, fieldSchema, t, exp.getUid());                 
                 schema.addField(f);  
            }                                                      
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
    public boolean isEqual(Operator other) {
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
    public void accept(PlanVisitor v) throws IOException {
         if (!(v instanceof LogicalPlanVisitor)) {
                throw new IOException("Expected LogicalPlanVisitor");
            }
            ((LogicalPlanVisitor)v).visitLOGenerate(this);
    }

    @Override
    public String toString() {
        StringBuilder msg = new StringBuilder();

        msg.append("(Name: " + name + " Schema: " + getSchema() + ")");

        return msg.toString();
    }
}
