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
import java.util.List;

import org.apache.pig.experimental.plan.BaseOperatorPlan;
import org.apache.pig.experimental.plan.Operator;
import org.apache.pig.experimental.plan.OperatorPlan;

/**
 * LogicalPlan is the logical view of relational operations Pig will execute 
 * for a given script.  Note that it contains only realtional operations.
 * All expressions will be contained in LogicalExpressionPlans inside
 * each relational operator.  LogicalPlan provides operations for
 * removing and adding LogicalRelationalOperators.  These will handle doing
 * all of the necessary add, remove, connect, and disconnect calls in
 * OperatorPlan.  They will not handle patching up individual relational
 * operators.  That will be handle by the various Patchers.
 *
 */
public class LogicalPlan extends BaseOperatorPlan {
    
    /**
     * Add a relational operation to the plan.
     * @param before operator that will be before the new operator.  This
     * operator should already be in the plan.  If before is null then
     * the new operator will be a root.
     * @param newOper new operator to add.  This operator should not already
     * be in the plan.
     * @param after operator  that will be after the new operator.  This
     * operator should already be in the plan.  If after is null, then the
     * new operator will be a root.
     * @throws IOException if add is already in the plan, or before or after
     * are not in the plan.
     */
    public void add(LogicalRelationalOperator before,
                    LogicalRelationalOperator newOper,
                    LogicalRelationalOperator after) throws IOException {
        doAdd(before, newOper, after);
    }
   
    /**
     * Add a relational operation with multiple outputs to the plan.
     * @param before operators that will be before the new operator.  These
     * operator should already be in the plan.
     * @param newOper new operator to add.  This operator should not already
     * be in the plan.
     * @param after operator  that will be after the new operator.  This
     * operator should already be in the plan.  If after is null, then the
     * new operator will be a root.
     * @throws IOException if add is already in the plan, or before or after
     * are not in the plan.
     */
    public void add(List<LogicalRelationalOperator> before,
                    LogicalRelationalOperator newOper,
                    LogicalRelationalOperator after) throws IOException {
        doAdd(null, newOper, after);
        
        for (LogicalRelationalOperator op : before) {
            checkIn(op);
            connect(op, newOper);
        }
    }
    
    /**
     * Add a relational operation with multiple inputs to the plan.
     * @param before operator that will be before the new operator.  This
     * operator should already be in the plan.  If before is null then
     * the new operator will be a root.
     * @param newOper new operator to add.  This operator should not already
     * be in the plan.
     * @param after operators that will be after the new operator.  These
     * operator should already be in the plan.
     * @throws IOException if add is already in the plan, or before or after
     * are not in the plan.
     */
    public void add(LogicalRelationalOperator before,
                    LogicalRelationalOperator newOper,
                    List<LogicalRelationalOperator> after) throws IOException {
        doAdd(before, newOper, null);
        
        for (LogicalRelationalOperator op : after) {
            checkIn(op);
            connect(newOper, op);
        }
    }
    
    /**
     * Add a relational operation to the plan when the caller wants to control
     * how the nodes are connected in the graph.
     * @param before operator that will be before the new operator.  This
     * operator should already be in the plan.  before should not be null.
     * the new operator will be a root.
     * @param beforeToPos Position in before's edges to connect newOper at.
     * @param beforeFromPos Position in newOps's edges to connect before at.
     * @param newOper new operator to add.  This operator should not already
     * be in the plan.
     * @param afterToPos Position in after's edges to connect newOper at.
     * @param afterFromPos Position in newOps's edges to connect after at.
     * @param after operator  that will be after the new operator.  This
     * operator should already be in the plan.  If after is null, then the
     * new operator will be a root.
     * @throws IOException if add is already in the plan, or before or after
     * are not in the plan.
     */
    public void add(LogicalRelationalOperator before,
                    int beforeToPos,
                    int beforeFromPos,
                    LogicalRelationalOperator newOper,
                    int afterToPos,
                    int afterFromPos,
                    LogicalRelationalOperator after) throws IOException {
        if (before != null) checkIn(before);
        if (after != null) checkIn(after);
        checkNotIn(newOper);
        
        add(newOper);
        if (before != null) connect(before, beforeToPos, newOper, beforeFromPos);
        if (after != null) connect(newOper, afterToPos, after, afterFromPos);
        
    }
    
    /**
     * Remove an operator from the logical plan.  This call will take care
     * of disconnecting the operator, connecting the predecessor(s) and 
     * successor(s) and patching up the plan. 
     * @param op operator to be removed.
     * @throws IOException If the operator is not in the plan.
     */
    public void removeLogical(LogicalRelationalOperator op) throws IOException {
        
        checkIn(op);
        List<Operator> pred = getPredecessors(op);
        List<Operator> succ = getSuccessors(op);
        int predSz = pred.size();
        int succSz = succ.size();
        if (predSz > 1 && succSz > 1) {
            // Don't have a clue what to do here.  We shouldn't have any
            // operators that have multiple inputs and multiple outputs.
            throw new IOException("Attempt to remove a node with multiple "
                + "inputs and outputs!");
        }
        
        // Disconnect and remove the given node.
        for (Operator p : pred) {
            disconnect(p, op);
        }
        for (Operator s : succ) {
            disconnect(op, s);
        }
        remove(op);
        
        // Now reconnect the before and after
        if (predSz > 1 && succSz == 1) {
            for (Operator p : pred) {
                connect(p, succ.get(0));
            }
        } else if (predSz == 1 && succSz >= 1) {
            for (Operator s : succ) {
                connect(pred.get(0), s);
            }
        }
        
    }
    
    private void doAdd(LogicalRelationalOperator before,
                       LogicalRelationalOperator newOper,
                       LogicalRelationalOperator after) throws IOException {
        if (before != null) checkIn(before);
        if (after != null) checkIn(after);
        checkNotIn(newOper);
        
        add(newOper);
        if (before != null) connect(before, newOper);
        if (after != null) connect(newOper, after);
    }
    
    private void checkIn(LogicalRelationalOperator op) throws IOException {
        if (!ops.contains(op)) {
            throw new IOException("Attempt to use operator " + op.getName() + 
                " which is not in the plan.");
        }
    }
    
     private void checkNotIn(LogicalRelationalOperator op) throws IOException {
        if (ops.contains(op)) {
            throw new IOException("Attempt to add operator " + op.getName() + 
                " which is already in the plan.");
        }
    }
        
}
