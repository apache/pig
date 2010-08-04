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

import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.PlanVisitor;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;

public class LOSplitOutput extends LogicalRelationalOperator {
    private LogicalExpressionPlan filterPlan;
    public LOSplitOutput(LogicalPlan plan) {
        super("LOSplitOutput", plan);       
    }
    
    public LOSplitOutput(LogicalPlan plan, LogicalExpressionPlan filterPlan) {
        super("LOSplitOutput", plan);
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
            throw new RuntimeException("Unable to get predecessor of LOSplit.", e);
        }
        
        return input.getSchema();
    }   
    
    @Override
    public void accept(PlanVisitor v) throws IOException {
        if (!(v instanceof LogicalRelationalNodesVisitor)) {
            throw new IOException("Expected LogicalPlanVisitor");
        }
        ((LogicalRelationalNodesVisitor)v).visit(this);
    }
    
    @Override
    public boolean isEqual(Operator other) {
        if (other != null && other instanceof LOSplitOutput) { 
            LOSplitOutput os = (LOSplitOutput)other;
            return filterPlan.isEqual(os.filterPlan) && checkEquality(os);
        } else {
            return false;
        }
    }
}
