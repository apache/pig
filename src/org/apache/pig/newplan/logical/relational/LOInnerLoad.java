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

import java.util.Map;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
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
    public LogicalSchema getSchema() throws FrontendException {
        if (schema!=null)
            return schema;
        
        if (prj.findReferent().getSchema()!=null) {
            if (prj.getFieldSchema()!=null) {
                if (prj.getFieldSchema().type==DataType.BAG && prj.getFieldSchema().schema!=null &&
                        prj.getFieldSchema().schema.isTwoLevelAccessRequired()) {
                    schema = new LogicalSchema();
                    LogicalFieldSchema tupleSchema = prj.getFieldSchema().schema.getField(0);
                    for (int i=0;i<tupleSchema.schema.size();i++)
                        schema.addField(tupleSchema.schema.getField(i));
                    sourceIsBag = true;
                    alias = prj.getFieldSchema().alias;
                }
                else if (prj.getFieldSchema().type==DataType.BAG){
                    sourceIsBag = true;
                    alias = prj.getFieldSchema().alias;
                    if (prj.getFieldSchema().schema!=null) {
                        schema = new LogicalSchema();
                        for (int i=0;i<prj.getFieldSchema().schema.size();i++)
                            schema.addField(prj.getFieldSchema().schema.getField(i));
                    }
                    
                }
                else {
                    schema = new LogicalSchema();
                    schema.addField(prj.getFieldSchema());
                }
            }
        }
        return schema;
    }
    
    public ProjectExpression getProjection() {
        return prj;
    }

    @Override
    public boolean isEqual(Operator other) throws FrontendException {
        if (!(other instanceof LOInnerLoad)) {
            return false;
        }
        
        return (getColNum() == ((LOInnerLoad)other).getColNum());
    }    
    
    @Override
    public void accept(PlanVisitor v) throws FrontendException {
         if (!(v instanceof LogicalRelationalNodesVisitor)) {
             throw new FrontendException("Expected LogicalPlanVisitor", 2223);
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
    
    public String toString() {
        StringBuilder msg = new StringBuilder();

        if (alias!=null) {
            msg.append(alias + ": ");
        }
        msg.append("(Name: " + name);
        msg.append("[");
        if (getProjection().getColNum()==-1)
            msg.append("*");
        else
            msg.append(getProjection().getColNum());
        msg.append("]");
        msg.append(" Schema: ");
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
}
