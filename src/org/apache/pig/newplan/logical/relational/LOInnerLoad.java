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

import org.apache.pig.data.DataType;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanVisitor;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.relational.LogicalSchema.LogicalFieldSchema;

/**
 * Operator to map the data into the inner plan of LOForEach
 * It can only be used in the inner plan of LOForEach
 *
 */
public class LOInnerLoad extends LogicalRelationalOperator {    
    private ProjectExpression prj; 
    private LOForEach foreach;
    private boolean sourceIsBag = false;

    public LOInnerLoad(OperatorPlan plan, LOForEach foreach, int colNum) {
        super("LOInnerLoad", plan);        
        
        // store column number as a ProjectExpression in a plan 
        // to be able to dynamically adjust column number during optimization
        LogicalExpressionPlan exp = new LogicalExpressionPlan();
        
        // we don't care about type, so set to -1
        prj = new ProjectExpression(exp, 0, colNum, foreach);
        this.foreach = foreach;
    }

    @Override
    public LogicalSchema getSchema() {
        if (schema!=null)
            return schema;
        
        try {
            if (prj.getFieldSchema()!=null) {
                schema = new LogicalSchema();
                if (prj.getFieldSchema().type==DataType.BAG && prj.getFieldSchema().schema.isTwoLevelAccessRequired()) {
                    LogicalFieldSchema tupleSchema = prj.getFieldSchema().schema.getField(0);
                    for (int i=0;i<tupleSchema.schema.size();i++)
                        schema.addField(tupleSchema.schema.getField(i));
                    sourceIsBag = true;
                    alias = prj.getFieldSchema().alias;
                }
                else if (prj.getFieldSchema().type==DataType.BAG){
                    for (int i=0;i<prj.getFieldSchema().schema.size();i++)
                        schema.addField(prj.getFieldSchema().schema.getField(i));
                    sourceIsBag = true;
                    alias = prj.getFieldSchema().alias;
                }
                else {
                    schema.addField(prj.getFieldSchema());
                }
            }
        } catch (IOException e) {
            // TODO
        }
        return schema;
    }
    
    public ProjectExpression getProjection() {
        return prj;
    }

    @Override
    public boolean isEqual(Operator other) {
        if (!(other instanceof LOInnerLoad)) {
            return false;
        }
        
        return (getColNum() == ((LOInnerLoad)other).getColNum());
    }    
    
    @Override
    public void accept(PlanVisitor v) throws IOException {
         if (!(v instanceof LogicalRelationalNodesVisitor)) {
             throw new IOException("Expected LogicalPlanVisitor");
         }
         ((LogicalRelationalNodesVisitor)v).visit(this);
    }

    public int getColNum() {
        return prj.getColNum();
    }
    
    /**
     * Get the LOForEach operator that contains this operator as part of inner plan
     * @return the LOForEach operator
     */
    public LOForEach getLOForEach() {
        return foreach;
    }
    
    public boolean sourceIsBag() {
        return sourceIsBag;
    }
}
