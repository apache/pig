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
package org.apache.pig.impl.logicalLayer;

import java.util.Collection;
import java.util.List;
import java.util.Iterator;
import java.util.Set;
import java.util.Map;
import java.util.ArrayList;

import org.apache.commons.el.RelationalOperator;
import org.apache.pig.impl.plan.PlanVisitor;
import org.apache.pig.impl.plan.PlanWalker;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.logicalLayer.parser.ParseException;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.data.DataType;

/**
 * A visitor to remove redundant operators in a plan
 */
public class RemoveRedundantOperators extends
        LOVisitor {

    public RemoveRedundantOperators(LogicalPlan plan) {
        super(plan, new DependencyOrderWalker<LogicalOperator, LogicalPlan>(plan));
    }

    /**
     * 
     * @param project
     *            the logical project operator that has to be visited
     * @throws VisitorException
     */
    protected void visit(LOProject project) throws VisitorException {
        LogicalPlan currentPlan = (LogicalPlan)mCurrentWalker.getPlan();
        
        //if the project is a project(*) and if there are predecessors 
        // and successors that are
        //1. both relational operators OR
        //2. both expression operators
        //then the project(*) can be removed and the input and outputs
        // short circuited, i.e. directly connected
        if(project.isStar()) {

            List<LogicalOperator> prSuccessors = 
                    currentPlan.getSuccessors(project);
            
            List<LogicalOperator> prPredecessors = 
                    currentPlan.getPredecessors(project);
            
            if( ((prSuccessors != null) && (prSuccessors.size() > 0)) 
                    /* prPredecessors.size() == 1 for project(*) */
                    && ((prPredecessors != null) && (prPredecessors.size() == 1)) ){
                
                LogicalOperator pred =  prPredecessors.get(0);
                
                
                //check if either all pred and succ oper are ExpressionOperator
                // or if all of them are relationalOperators (ie != ExpressionOperator)
                boolean allExpressionOp = true;
                boolean allRelationalOp = true;
                if(pred instanceof ExpressionOperator)
                    allRelationalOp = false;
                else 
                    allExpressionOp = false;
                
                for(LogicalOperator op: prSuccessors){
                    if (op instanceof ExpressionOperator) 
                        allRelationalOp = false;
                    else 
                        allExpressionOp = false;
                    
                    if(allExpressionOp == false && allRelationalOp == false)
                        break;
                }
                
                // remove project if either condition is met
                if(allExpressionOp == true || allRelationalOp == true){
                    try{
                        currentPlan.removeAndReconnectMultiSucc(project);
                        patchInputReference(pred, project, prSuccessors);
                    }catch (PlanException pe){
                        String msg = new String("Error while removing redundant project in plan");
                        throw new VisitorException(msg,pe);
                    }
                }
                
            }
        }       
    }
    
    
    private void patchInputReference(LogicalOperator pred, LogicalOperator current, List<LogicalOperator> succs) {
        for(LogicalOperator n : succs){
            // special handling of LOProject because its getExpression() does
            // need not be same as getPredecessors(LOProject)
            if(n instanceof LOProject){
                LOProject lop = (LOProject)n;
                if(current == lop.getExpression()){
                    lop.setExpression((LogicalOperator)pred);
                }
            }
        }
    }
    
    
}
