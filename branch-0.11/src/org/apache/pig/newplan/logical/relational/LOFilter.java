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
package org.apache.pig.newplan.logical.relational;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.PlanVisitor;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;

public class LOFilter extends LogicalRelationalOperator {

    private static final long serialVersionUID = 2L;
    private LogicalExpressionPlan filterPlan;
    private boolean isSample;
        
    public LOFilter(LogicalPlan plan) {
        super("LOFilter", plan);       
    }

    public LOFilter(LogicalPlan plan, LogicalExpressionPlan filterPlan) {
        super("LOFilter", plan);
        this.filterPlan = filterPlan;
    }
    
    public LOFilter(LogicalPlan plan, boolean sample) {
        this(plan);
        isSample = sample;
    }

    public LOFilter(LogicalPlan plan, LogicalExpressionPlan filterPlan, boolean sample) {
        this(plan, filterPlan);
        isSample = sample;
    }
    
    public LogicalExpressionPlan getFilterPlan() {
        return filterPlan;
    }
    
    public void setFilterPlan(LogicalExpressionPlan filterPlan) {
        this.filterPlan = filterPlan;
    }
    
    public boolean isSample() {
        return isSample;
    }
    
    @Override
    public LogicalSchema getSchema() throws FrontendException {
        if (schema!=null)
            return schema;
        
        LogicalRelationalOperator input = null;
        input = (LogicalRelationalOperator)plan.getPredecessors(this).get(0);
        
        schema = input.getSchema();
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
        if (other != null && other instanceof LOFilter) { 
            LOFilter of = (LOFilter)other;
            return filterPlan.isEqual(of.filterPlan) && checkEquality(of);
        } else {
            return false;
        }
    }
    
    public Operator getInput(LogicalPlan plan) {
        return plan.getPredecessors(this).get(0);
    }
}

