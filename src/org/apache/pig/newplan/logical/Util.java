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
package org.apache.pig.newplan.logical;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOGenerate;
import org.apache.pig.newplan.logical.relational.LOInnerLoad;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.logical.relational.LogicalSchema;

public class Util {
    public static LogicalSchema translateSchema(Schema schema) {       
        if (schema == null) {
            return null;
        }
        
        LogicalSchema s2 = new LogicalSchema();
        List<Schema.FieldSchema> ll = schema.getFields();
        for (Schema.FieldSchema f: ll) {
            LogicalSchema.LogicalFieldSchema f2 = 
                new LogicalSchema.LogicalFieldSchema(f.alias, translateSchema(f.schema), f.type);
                       
            s2.addField(f2);
        }
        
        return s2;
    }
    
    public static LogicalSchema.LogicalFieldSchema translateFieldSchema(Schema.FieldSchema fs) {      
        LogicalSchema newSchema = null;
        if (fs.schema!=null) {
            newSchema = translateSchema(fs.schema);
        }
        
        LogicalSchema.LogicalFieldSchema newFs = new LogicalSchema.LogicalFieldSchema(fs.alias, newSchema, fs.type);
        return newFs;
    }
    
    /**
     * This function translates the new LogicalSchema into old Schema format required
     * by PhysicalOperators
     * @param schema LogicalSchema to be converted to Schema
     * @return Schema that is converted from LogicalSchema
     * @throws FrontendException 
     */
    public static Schema translateSchema(LogicalSchema schema) {       
        if (schema == null) {
            return null;
        }
        
        Schema s2 = new Schema();
        List<LogicalSchema.LogicalFieldSchema> ll = schema.getFields();
        for (LogicalSchema.LogicalFieldSchema f: ll) {
            Schema.FieldSchema f2 = null;
            try {
                f2 = new Schema.FieldSchema(f.alias, translateSchema(f.schema), f.type);
                f2.canonicalName = ((Long)f.uid).toString();
                s2.add(f2);
            } catch (FrontendException e) {
            }
        }
        
        return s2;
    }
    
    /**
     * If schema argument has fields where a bag does not contain a tuple schema,
     * it inserts a tuple schema. It does so for all inner levels.
     * eg bag({int}) => bag({(int)}) 
     * @param sch
     * @return modified schema
     * @throws FrontendException 
     */
    public static Schema fixSchemaAddTupleInBag(Schema sch) throws FrontendException{
        LogicalSchema logSch = translateSchema(sch);
        logSch.normalize();
        return translateSchema(logSch);
    }


    public static Schema.FieldSchema translateFieldSchema(LogicalSchema.LogicalFieldSchema fs) {
        if(fs == null)
            return null;
        Schema newSchema = null;
        if (fs.schema!=null) {
            newSchema = translateSchema(fs.schema);
        }
        
        Schema.FieldSchema newFs = null;
        try {
            newFs = new Schema.FieldSchema(null, newSchema, fs.type);
        } catch (FrontendException e) {
        }
        return newFs;
    }
    
    public static LOForEach addForEachAfter(LogicalPlan plan, LogicalRelationalOperator op, int branch,
            Set<Integer> columnsToDrop) throws FrontendException {
        LOForEach foreach = new LOForEach(plan);
        
        plan.add(foreach);
        List<Operator> next = plan.getSuccessors(op);
        if (next != null) {
            LogicalRelationalOperator nextOp = (LogicalRelationalOperator)next.get(branch);
            plan.insertBetween(op, foreach, nextOp);
            foreach.setAlias(op.getAlias());
        }
        else {
            plan.connect(op, foreach);
        }
        
        LogicalPlan innerPlan = new LogicalPlan();
        foreach.setInnerPlan(innerPlan);
        
        LogicalSchema schema = op.getSchema();
        
        // build foreach inner plan
        List<LogicalExpressionPlan> exps = new ArrayList<LogicalExpressionPlan>();
        LOGenerate gen = new LOGenerate(innerPlan, exps, new boolean[schema.size()-columnsToDrop.size()]);
        innerPlan.add(gen);
        
        for (int i=0, j=0; i<schema.size(); i++) {
            if (columnsToDrop.contains(i)) {
                continue;
            }
            
            LOInnerLoad innerLoad = new LOInnerLoad(innerPlan, foreach, i);
            innerPlan.add(innerLoad);
            innerPlan.connect(innerLoad, gen);
            
            LogicalExpressionPlan exp = new LogicalExpressionPlan();
            ProjectExpression prj = new ProjectExpression(exp, j++, -1, gen);
            exp.add(prj);
            exps.add(exp);
        }
        return foreach;
    }
}
