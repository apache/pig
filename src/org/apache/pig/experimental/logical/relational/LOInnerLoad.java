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

import org.apache.pig.experimental.logical.expression.LogicalExpressionPlan;
import org.apache.pig.experimental.logical.expression.ProjectExpression;
import org.apache.pig.experimental.plan.Operator;
import org.apache.pig.experimental.plan.OperatorPlan;
import org.apache.pig.experimental.plan.PlanVisitor;

/**
 * Operator to map the data into the inner plan of LOForEach
 * It can only be used in the inner plan of LOForEach
 *
 */
public class LOInnerLoad extends LogicalRelationalOperator {    
    private ProjectExpression prj; 
    private LOForEach foreach;

    public LOInnerLoad(OperatorPlan plan, LOForEach foreach, int colNum) {
        super("LOInnerLoad", plan);        
        
        // store column number as a ProjectExpression in a plan 
        // to be able to dynamically adjust column number during optimization
        LogicalExpressionPlan exp = new LogicalExpressionPlan();
        
        // we don't care about type, so set to -1
        prj = new ProjectExpression(exp, (byte)-1, 0, colNum);
        this.foreach = foreach;
    }

    @Override
    public LogicalSchema getSchema() {
        if (schema != null) {
            return schema;
        }
        
        LogicalPlan p = (LogicalPlan)foreach.getPlan();
        try {
            LogicalRelationalOperator op = (LogicalRelationalOperator)p.getPredecessors(foreach).get(0);
            LogicalSchema s = op.getSchema();
            if (s != null) {
                if (prj.isProjectStar()) {
                    schema = s;
                } else {
                    schema = new LogicalSchema();                  
                    schema.addField(s.getField(getColNum()));           
                }
            }            
            
            if ( schema != null && schema.size() == 0) {
                schema = null;
            }
        }catch(Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
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
         if (!(v instanceof LogicalPlanVisitor)) {
                throw new IOException("Expected LogicalPlanVisitor");
            }
            ((LogicalPlanVisitor)v).visitLOInnerLoad(this);
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
}
