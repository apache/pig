/**
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

//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
import org.apache.pig.experimental.logical.expression.LogicalExpressionPlan;
import org.apache.pig.experimental.plan.Operator;
import org.apache.pig.experimental.plan.PlanVisitor;

public class LOFilter extends LogicalRelationalOperator {

    private static final long serialVersionUID = 2L;
    private LogicalExpressionPlan filterPlan;
    //private static Log log = LogFactory.getLog(LOFilter.class);

        
    public LOFilter(LogicalPlan plan) {
        super("LOFilter", plan);       
    }

    public LOFilter(LogicalPlan plan, LogicalExpressionPlan filterPlan) {
        super("LOFilter", plan);
        this.filterPlan = filterPlan;
    }
    
    public LogicalExpressionPlan getFilterPlan() {
        return filterPlan;
    }
    
    public void setFilterPlan(LogicalExpressionPlan filterPlan) {
        this.filterPlan = filterPlan;
    }
    
    @Override
    public LogicalSchema getSchema() {        
        LogicalRelationalOperator input = null;
        try {
            input = (LogicalRelationalOperator)plan.getPredecessors(this).get(0);
        }catch(Exception e) {
            throw new RuntimeException("Unable to get predecessor of LOFilter.", e);
        }
        
        schema = input.getSchema();        
        return schema;
    }   
    
    @Override
    public void accept(PlanVisitor v) throws IOException {
        if (!(v instanceof LogicalPlanVisitor)) {
            throw new IOException("Expected LogicalPlanVisitor");
        }
        ((LogicalPlanVisitor)v).visitLOFilter(this);
    }
    
    @Override
    public boolean isEqual(Operator other) {
        if (other != null && other instanceof LOFilter) { 
            LOFilter of = (LOFilter)other;
            return filterPlan.isEqual(of.filterPlan) && checkEquality(of);
        } else {
            return false;
        }
    }
}

