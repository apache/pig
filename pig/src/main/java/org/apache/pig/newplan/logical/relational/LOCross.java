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

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.PlanVisitor;

public class LOCross extends LogicalRelationalOperator {
    
    private static final long serialVersionUID = 2L;
    //private static Log log = LogFactory.getLog(LOFilter.class);
    
    protected boolean nested = false;
        
    public LOCross(LogicalPlan plan) {
        super("LOCross", plan);       
    }

    public boolean isNested() {
        return nested;
    }

    public void setNested(boolean nested) {
        this.nested = nested;
    }

    @Override
    public LogicalSchema getSchema() throws FrontendException {        
        // if schema is calculated before, just return
        if (schema != null) {
            return schema;
        }
        
        List<Operator> inputs = null;
        inputs = plan.getPredecessors(this);
        if (inputs == null) {
            return null;
        }
        
        List<LogicalSchema.LogicalFieldSchema> fss = new ArrayList<LogicalSchema.LogicalFieldSchema>();
        
        for (Operator op : inputs) {
            LogicalSchema inputSchema = ((LogicalRelationalOperator)op).getSchema();
            // the schema of one input is unknown, so the join schema is unknown, just return 
            if (inputSchema == null) {
                schema = null;
                return schema;
            }
                               
            for (int i=0; i<inputSchema.size(); i++) {
                 LogicalSchema.LogicalFieldSchema fs = inputSchema.getField(i);
                 LogicalSchema.LogicalFieldSchema newFS = null;
                 if(fs.alias != null) {                    
                     newFS = new LogicalSchema.LogicalFieldSchema(((LogicalRelationalOperator)op).getAlias()+"::"+fs.alias ,fs.schema, fs.type, fs.uid);                    
                 } else {
                     newFS = new LogicalSchema.LogicalFieldSchema(fs.alias, fs.schema, fs.type, fs.uid);
                 }                       
                 fss.add(newFS);                 
            }            
        }        

        if (nested) {
            LogicalRelationalOperator.fixDuplicateUids(fss);
        }

        schema = new LogicalSchema();
        for(LogicalSchema.LogicalFieldSchema fieldSchema: fss) {
            schema.addField(fieldSchema);
        }         
        
        return schema;
    }   
    
    @Override
    public void accept(PlanVisitor v) throws FrontendException {
        if (!(v instanceof LogicalRelationalNodesVisitor)) {
            throw new FrontendException("Expected LogicalPlanVisitor", 2223);
        }
        ((LogicalRelationalNodesVisitor)v).visit(this);
    }
    
    @Override
    public boolean isEqual(Operator other) throws FrontendException {
        if (other != null && other instanceof LOCross) {
            return checkEquality((LogicalRelationalOperator)other);
        } else {
            return false;
        }
    }
    
    public List<Operator>  getInputs() {
        return plan.getPredecessors(this);
    }
}
