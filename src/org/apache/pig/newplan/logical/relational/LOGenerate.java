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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.data.DataType;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanVisitor;
import org.apache.pig.newplan.logical.expression.LogicalExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.relational.LogicalSchema.LogicalFieldSchema;

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
            
            LogicalFieldSchema fieldSchema = null;
            try {
                fieldSchema = exp.getFieldSchema().deepCopy();
            } catch (IOException e) {
                return null;
            }
            
            if (fieldSchema.type != DataType.TUPLE && fieldSchema.type != DataType.BAG) {
                // if type is primitive, just add to schema
                schema.addField(fieldSchema);
                continue;
            } else {
                // if flatten is set, set schema of tuple field to this schema
                List<LogicalSchema.LogicalFieldSchema> innerFieldSchemas = new ArrayList<LogicalSchema.LogicalFieldSchema>();
                if (flattenFlags[i]) {
                    if (fieldSchema.type == DataType.BAG) {
                        // if it is bag of tuples, get the schema of tuples
                        if (fieldSchema.schema.isTwoLevelAccessRequired()) {
                            //  assert(fieldSchema.schema.size() == 1 && fieldSchema.schema.getField(0).type == DataType.TUPLE)
                            innerFieldSchemas = fieldSchema.schema.getField(0).schema.getFields();
                        } else {
                            innerFieldSchemas = fieldSchema.schema.getFields();
                        }
                        for (LogicalSchema.LogicalFieldSchema fs : innerFieldSchemas) {
                            fs.alias = fieldSchema.alias + "::" + fs.alias;
                        }
                    } else { // DataType.TUPLE
                        innerFieldSchemas = fieldSchema.schema.getFields();
                        for (LogicalSchema.LogicalFieldSchema fs : innerFieldSchemas) {
                            fs.alias = fieldSchema.alias + "::" + fs.alias;
                        }
                    }
                    
                    
                    for (LogicalSchema.LogicalFieldSchema fs : innerFieldSchemas)
                        schema.addField(fs);
                }
                else
                    schema.addField(fieldSchema);
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
         if (!(v instanceof LogicalRelationalNodesVisitor)) {
                throw new IOException("Expected LogicalPlanVisitor");
            }
            ((LogicalRelationalNodesVisitor)v).visit(this);
    }
}
