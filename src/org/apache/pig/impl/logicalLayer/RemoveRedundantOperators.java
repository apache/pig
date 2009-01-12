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

import java.util.List;
import java.util.Iterator;
import java.util.Set;
import java.util.Map;
import java.util.ArrayList;

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
        
        //if the project is a project(*) and if there are predecessors and successors that are
        //1. both relational operators OR
        //2. both expression operators
        //then the project(*) can be removed and the input and outputs short circuited, i.e. directly connected
        if(project.isStar()) {

            List<LogicalOperator> projectSuccessors = currentPlan.getSuccessors(project);
            List<LogicalOperator> projectPredecessors = currentPlan.getPredecessors(project);

            if(((projectSuccessors != null) && (projectSuccessors.size() > 0)) 
                && ((projectPredecessors != null) && (projectPredecessors.size() > 0))) {

                //Making copies to avoid concurrent modification exceptions
                List<LogicalOperator> successors = new ArrayList(currentPlan.getSuccessors(project));
                List<LogicalOperator> predecessors = new ArrayList(currentPlan.getPredecessors(project));

                //if the project(*) cannot be removed
                boolean removeProject = true;

                for(LogicalOperator projectPred: predecessors) {
                    for(LogicalOperator projectSucc: successors) {
                        if (((projectPred instanceof ExpressionOperator) && (projectSucc instanceof ExpressionOperator))
                            || (!(projectPred instanceof ExpressionOperator) && !(projectSucc instanceof ExpressionOperator))) {
                            try {
                                currentPlan.disconnect(projectPred, project);
                                currentPlan.disconnect(project, projectSucc);
                                currentPlan.connect(projectPred, projectSucc);
                                patchInputReference(projectSucc, project, projectPred);
                            } catch (PlanException pe) {
                                throw new VisitorException(pe.getMessage(), pe);
                            }
                        } else {
                            removeProject = false;
                        }
                    }
                }
                if(removeProject) {
                    currentPlan.remove(project);
                }
            }
        }
    }

    private void patchInputReference(LogicalOperator op, LogicalOperator prevInput, LogicalOperator newInput) {
        //TODO
        //Using reference comparison here as operators do not have equals() method yet
        //Depending on the successor of prevInput, fix the referenes to point to newInput
        if(op instanceof BinaryExpressionOperator) {
            BinaryExpressionOperator binOp = (BinaryExpressionOperator)op;
            if(prevInput == binOp.getLhsOperand()) {
                binOp.setLhsOperand((ExpressionOperator)newInput);
            } else if(prevInput == binOp.getRhsOperand()) {
                binOp.setRhsOperand((ExpressionOperator)newInput);
            }
        } else if (op instanceof UnaryExpressionOperator) {
            UnaryExpressionOperator uniOp = (UnaryExpressionOperator)op;
            if(prevInput == uniOp.getOperand()) {
                uniOp.setOperand((ExpressionOperator)newInput);
            }
        } else if (op instanceof LOBinCond) {
            LOBinCond binCond = (LOBinCond)op;
            if(prevInput == binCond.getLhsOp()) {
                binCond.setLhsOp((ExpressionOperator)newInput);
            } else if(prevInput == binCond.getRhsOp()) {
                binCond.setRhsOp((ExpressionOperator)newInput);
            } else if(prevInput == binCond.getCond()) {
                binCond.setCond((ExpressionOperator)newInput);
            }
        } else if (op instanceof LOCast) {
            LOCast cast = (LOCast)op;
            if(prevInput == cast.getExpression()) {
                cast.setExpression((ExpressionOperator)newInput);
            }
        } else if (op instanceof LOMapLookup) {
            LOMapLookup map = (LOMapLookup)op;
            if(prevInput == map.getMap()) {
                map.setMap((ExpressionOperator)newInput);
            }
        } else if (op instanceof LOUserFunc) {
            LOUserFunc userFunc = (LOUserFunc)op;
            List<ExpressionOperator> args = userFunc.getArguments();
            ArrayList<ExpressionOperator> newArgs = new ArrayList<ExpressionOperator>(args.size());
            for(ExpressionOperator expOp: args) {
                if(prevInput == expOp) {
                    newArgs.add((ExpressionOperator)newInput);
                } else {
                    newArgs.add(expOp);
                }
            }
            userFunc.setArguments(newArgs);
        } else if (op instanceof LOProject) {
            LOProject proj = (LOProject)op;
            if(prevInput == proj.getExpression()) {
                proj.setExpression(newInput);
            }
        }
    }

}
