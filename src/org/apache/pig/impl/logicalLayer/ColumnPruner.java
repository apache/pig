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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.pig.PigException;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanWalker;
import org.apache.pig.impl.plan.RequiredFields;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.plan.ProjectionMap.Column;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.impl.logicalLayer.RelationalOperator;

public class ColumnPruner extends LOVisitor {
    private Map<LogicalOperator, List<Pair<Integer,Integer>>> prunedColumnsMap;
    LogicalPlan plan;
    
    public ColumnPruner(LogicalPlan plan, LogicalOperator op, List<Pair<Integer, Integer>> prunedColumns, 
            PlanWalker<LogicalOperator, LogicalPlan> walker) {
        super(plan, walker);
        prunedColumnsMap = new HashMap<LogicalOperator, List<Pair<Integer,Integer>>>();
        prunedColumnsMap.put(op, prunedColumns);
        this.plan = plan;
    }

    protected void prune(RelationalOperator lOp) throws VisitorException {
        List<LogicalOperator> predecessors = plan.getPredecessors(lOp);
        if (predecessors==null)
        {
            int errCode = 2187;
            throw new VisitorException("Cannot get predessors", errCode, PigException.BUG);
        }
        List<Pair<Integer, Integer>> columnsPruned = new ArrayList<Pair<Integer, Integer>>();
        List<Pair<Integer, Integer>> columnsToPrune = new ArrayList<Pair<Integer, Integer>>();
        for (int i=0;i<predecessors.size();i++)
        {
            RelationalOperator predecessor = (RelationalOperator)predecessors.get(i);
            if (prunedColumnsMap.containsKey(predecessor))
            {
                List<Pair<Integer, Integer>> predColumnsToPrune = prunedColumnsMap.get(predecessor);
                if (predColumnsToPrune!=null)
                {
                    for (int j=0;j<predColumnsToPrune.size();j++)
                    {
                        predColumnsToPrune.get(j).first = i;
                    }
                    columnsPruned.addAll(predColumnsToPrune);
                }
            }
        }
    
        try {
            if (lOp.getSchema()==null)
            {
                int errCode = 2189;
                throw new VisitorException("Expect schema", errCode, PigException.BUG);
            }
            
            // For every input column, check if it is pruned
            nextOutput:for (int i=0;i<lOp.getSchema().size();i++)
            {
                List<RequiredFields> relevantFieldsList = lOp.getRelevantInputs(0, i);
                
                // Check if this output do not need any inputs, if so, it is a constant field.
                // Since we never prune a constant field, so we continue without pruning 
                boolean needNoInputs = true;
                
                if (relevantFieldsList==null)
                    needNoInputs = true;
                else
                {
                    for (RequiredFields relevantFields: relevantFieldsList)
                    {
                        if (relevantFields!=null && !relevantFields.needNoFields())
                            needNoInputs = false;
                    }
                }
                
                if (needNoInputs)
                    continue;
                
                boolean allPruned = true;
                
                // For LOUnion, we treat it differently. LOUnion is the only operator that cannot be pruned independently.
                // For every pruned input column, we will prune. LOUnion (Contrary to other operators, unless all relevant
                // fields are pruned, we then prune the output field. Inside LOUnion, we have a counter, the output columns 
                // is actually pruned only after all corresponding input columns have been pruned
                if (lOp instanceof LOUnion)
                {
                    allPruned = false;
                    checkAllPrunedUnion: for (RequiredFields relevantFields: relevantFieldsList)
                    {
                        for (Pair<Integer, Integer> relevantField: relevantFields.getFields())
                        {
                            if (columnsPruned.contains(relevantField))
                            {
                                allPruned = true;
                                break checkAllPrunedUnion;
                            }
                        }
                    }
                }
                // For LOCogroup, one output can be pruned if all its relevant input are pruned except for "key" fields 
                else if (lOp instanceof LOCogroup)
                {
                    List<RequiredFields> requiredFieldsList = lOp.getRequiredFields();
                    boolean sawInputPruned = false;
                    for (Pair<Integer, Integer> column : columnsPruned)
                    {
                        if (column.first == i-1)  // Saw at least one input pruned
                        {
                            sawInputPruned = true;
                            // Further check if requiredFields of the LOCogroup contains these fields.
                            // If not, we can safely prune this output column
                            if (requiredFieldsList.get(i-1).getFields().contains(column))
                            {
                                allPruned = false;
                                break;
                            }
                        }
                    }
                    if (!sawInputPruned)
                        allPruned = false;
                }
                else
                {
                    nextRelevantFields:for (RequiredFields relevantFields: relevantFieldsList)
                    {
                        if (relevantFields==null)
                            continue;
                        
                        if (relevantFields.needAllFields())
                        {
                            allPruned = false;
                            break;
                        }
                        if (relevantFields.needNoFields())
                            continue;
                        for (Pair<Integer, Integer> relevantField: relevantFields.getFields())
                        {
                            if (relevantField==null)
                                continue;
                            
                            if (lOp instanceof LOUnion)
                            {
                                if (columnsPruned.contains(relevantField))
                                    break nextRelevantFields;
                            }
                            else if (!columnsPruned.contains(relevantField))
                            {
                                allPruned = false;
                                break nextRelevantFields;
                            }
                        }
                    }
                }
                if (allPruned)
                    columnsToPrune.add(new Pair<Integer, Integer>(0, i));
            }
    
            if (columnsPruned.size()!=0)
            {
                MultiMap<Integer, Column> mappedFields = new MultiMap<Integer, Column>();
                List<Column> columns = new ArrayList<Column>();
                columns.add(new Column(new Pair<Integer, Integer>(0, 0)));
                mappedFields.put(0, columns);
                LogicalOperator nextOp = lOp;
                if (lOp instanceof LOCogroup)
                {
                    ArrayList<Boolean> flattenList = new ArrayList<Boolean>();
                    ArrayList<LogicalPlan> generatingPlans = new ArrayList<LogicalPlan>();
                    String scope = lOp.getOperatorKey().scope;
                    for (int i=0;i<=predecessors.size();i++) {
                        if (!columnsToPrune.contains(new Pair<Integer, Integer>(0, i)))
                        {
                            LogicalPlan projectPlan = new LogicalPlan();
                            LogicalOperator projectInput = lOp;
                            ExpressionOperator column = new LOProject(projectPlan, new OperatorKey(scope, NodeIdGenerator.getGenerator().getNextNodeId(scope)), projectInput, i);
                            flattenList.add(false);
                            projectPlan.add(column);
                            generatingPlans.add(projectPlan);
                        }
                        columns = new ArrayList<Column>();
                        columns.add(new Column(new Pair<Integer, Integer>(0, i+1)));
                        mappedFields.put(i+1, columns);
                    }
                    LOForEach forEach = new LOForEach(mPlan, new OperatorKey(scope, NodeIdGenerator.getGenerator().getNextNodeId(scope)), generatingPlans, flattenList);
                    LogicalOperator succ = mPlan.getSuccessors(lOp).get(0);
                    mPlan.add(forEach);
                    // Since the successor has not been pruned yet, so we cannot rewire directly because
                    // rewire has the assumption that predecessor and successor is in consistent
                    // state. The way we do the rewire is kind of hacky. We give a fake projection map in the 
                    // new node to fool rewire
                    mPlan.doInsertBetween(lOp, forEach, succ, false);
                    forEach.getProjectionMap().setMappedFields(mappedFields);
                    succ.rewire(lOp, 0, forEach, false);
                    nextOp = forEach;
                }
                if (lOp.pruneColumns(columnsPruned))
                    prunedColumnsMap.put(nextOp, columnsToPrune);
            }
        } catch (FrontendException e) {
            int errCode = 2188;
            throw new VisitorException("Cannot prune columns for "+lOp, errCode, PigException.BUG, e);
        }
    }
    
    protected void visit(LOCogroup cogroup) throws VisitorException {
        prune(cogroup);
    }
    
    protected void visit(LOCross cross) throws VisitorException {
        prune(cross);
    }
    
    protected void visit(LODistinct distinct) throws VisitorException {
        prune(distinct);
    }
    
    protected void visit(LOFilter filter) throws VisitorException {
        prune(filter);
    }
    
    protected void visit(LOForEach foreach) throws VisitorException {
        prune(foreach);
    }
    
    protected void visit(LOJoin join) throws VisitorException {
        prune(join);
    }
    
    protected void visit(LOLimit limit) throws VisitorException {
        prune(limit);
    }
    
    protected void visit(LOSort sort) throws VisitorException {
        prune(sort);
    }
    
    protected void visit(LOSplit split) throws VisitorException {
        prune(split);
    }
    
    protected void visit(LOSplitOutput splitoutput) throws VisitorException {
        prune(splitoutput);
    }
    
    protected void visit(LOStore store) throws VisitorException {
        return;
    }
    
    protected void visit(LOStream stream) throws VisitorException {
        return;
    }
    
    protected void visit(LOUnion union) throws VisitorException {
        prune(union);
        return;
    }
    
    protected void visit(LOLoad lOp) throws VisitorException {
        return;
    }
}
