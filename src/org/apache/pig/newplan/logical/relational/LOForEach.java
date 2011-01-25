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
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanVisitor;
import org.apache.pig.newplan.ReverseDependencyOrderWalker;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.optimizer.AllSameRalationalNodesVisitor;

public class LOForEach extends LogicalRelationalOperator {

    private static final long serialVersionUID = 2L;

    private LogicalPlan innerPlan;
      
    public LOForEach(OperatorPlan plan) {
        super("LOForEach", plan);		
    }

    public LogicalPlan getInnerPlan() {
        return innerPlan;
    }
    
    public void setInnerPlan(LogicalPlan p) {
        innerPlan = p;
    }
    
    @Override
    public boolean isEqual(Operator other) throws FrontendException {
        if (!(other instanceof LOForEach)) {
            return false;
        }
        
        return innerPlan.isEqual(((LOForEach)other).innerPlan);
    }
       
    @Override
    public LogicalSchema getSchema() throws FrontendException {
        List<Operator> ll = innerPlan.getSinks();
        if (ll != null) {
            schema = ((LogicalRelationalOperator)ll.get(0)).getSchema();
        }
        
        return schema;
    }

    @Override
    public void accept(PlanVisitor v) throws FrontendException {
        if (!(v instanceof LogicalRelationalNodesVisitor)) {
            throw new FrontendException("Expected LogicalPlanVisitor", 2222);
        }
        ((LogicalRelationalNodesVisitor)v).visit(this);
    }
    
    public static List<LOInnerLoad> findReacheableInnerLoadFromBoundaryProject(ProjectExpression project) throws FrontendException {
        LogicalRelationalOperator referred = project.findReferent();
        List<Operator> srcs = referred.getPlan().getSources();
        List<LOInnerLoad> innerLoads = new ArrayList<LOInnerLoad>();
        for (Operator src:srcs) {
            if (src instanceof LOInnerLoad) {
                Operator succ = src;
                while (succ!=null) {
                    if (succ==referred)
                        innerLoads.add((LOInnerLoad)src);
                    if (referred.getPlan().getSuccessors(succ)==null)
                        break;
                    succ = referred.getPlan().getSuccessors(succ).get(0);
                }
            }
        }
        return innerLoads;
    }
    
    public LogicalSchema dumpNestedSchema(String alias, String nestedAlias) throws FrontendException {
        NestedRelationalOperatorFinder opFinder = new NestedRelationalOperatorFinder(innerPlan, nestedAlias);
        opFinder.visit();
        
        if (opFinder.getMatchedOperator()!=null) {
            LogicalSchema nestedSc = opFinder.getMatchedOperator().getSchema();
            return nestedSc;
        }
        return null;
    }
    
    private static class NestedRelationalOperatorFinder extends AllSameRalationalNodesVisitor {
        String aliasOfOperator;
        LogicalRelationalOperator opFound = null;
        public NestedRelationalOperatorFinder(LogicalPlan plan, String alias) throws FrontendException {
            super(plan, new ReverseDependencyOrderWalker(plan));
            aliasOfOperator = alias;
        }
        public LogicalRelationalOperator getMatchedOperator() {
            return opFound;
        }
        @Override
        public void execute(LogicalRelationalOperator op) throws FrontendException {
            if (op.getAlias()!=null && op.getAlias().equals(aliasOfOperator))
                opFound = op;
        }
    }
}
