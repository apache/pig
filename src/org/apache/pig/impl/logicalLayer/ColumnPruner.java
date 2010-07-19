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
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.RequiredFields;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.plan.optimizer.OptimizerException;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.impl.logicalLayer.RelationalOperator;

public class ColumnPruner extends LOVisitor {
    private Map<LogicalOperator, List<Pair<Integer,Integer>>> prunedColumnsMap;
    LogicalPlan plan;
    
    public ColumnPruner(LogicalPlan plan) {
        super(plan, new DependencyOrderWalker<LogicalOperator, LogicalPlan>(plan));
        prunedColumnsMap = new HashMap<LogicalOperator, List<Pair<Integer,Integer>>>();
        this.plan = plan;
    }

    public void addPruneMap(LogicalOperator op, List<Pair<Integer,Integer>> prunedColumns) {
        prunedColumnsMap.put(op, prunedColumns);
    }
    
    public boolean isEmpty() {
        return prunedColumnsMap.isEmpty();
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
            for (int i=0;i<lOp.getSchema().size();i++)
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
                
                boolean columnPruned = false;
                
                // For LOCogroup, one output can be pruned if all its relevant input are pruned except for "key" fields 
                if (lOp instanceof LOCogroup)
                {
                    List<RequiredFields> requiredFieldsList = lOp.getRequiredFields();
                    for (Pair<Integer, Integer> column : columnsPruned)
                    {
                        if (column.first == i-1)  // Saw at least one input pruned
                        {
                            if (requiredFieldsList.get(i-1).getFields().contains(column))
                            {
                                columnPruned = true;
                                break;
                            }
                        }
                    }
                }
                else
                {
                    // If we see any of the relevant field of this column get pruned, 
                    // then we prune this column for this operator
                    for (RequiredFields relevantFields: relevantFieldsList)
                    {
                        if (relevantFields == null)
                            continue;
                        if (relevantFields.getNeedAllFields())
                            break;
                        for (Pair<Integer, Integer> relevantField: relevantFields.getFields())
                        {
                            // If any of the input column is pruned, prune this output column
                            if (columnsPruned.contains(relevantField))
                            {
                                columnPruned = true;
                                break;
                            }
                        }
                    }
                }
                if (columnPruned)
                    columnsToPrune.add(new Pair<Integer, Integer>(0, i));
            }
    
            LogicalOperator currentOp = lOp;
            
            // If it is LOCogroup, insert foreach to mimic pruning, because we have no way to prune
            // LOCogroup output only by pruning the inputs
            if (columnsPruned.size()!=0 && lOp instanceof LOCogroup)
            {
                List<Integer> columnsToProject = new ArrayList<Integer>();
                for (int i=0;i<=predecessors.size();i++) {
                    if (!columnsToPrune.contains(new Pair<Integer, Integer>(0, i)))
                        columnsToProject.add(i);
                }                
                currentOp = lOp.insertPlainForEachAfter(columnsToProject);
            }
            
            if (!columnsPruned.isEmpty()&&lOp.pruneColumns(columnsPruned)) {
                prunedColumnsMap.put(currentOp, columnsToPrune);
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
        // The only case we should skip foreach is when this is the foreach
        // inserted after LOLoad to mimic pruning, then we put the prunedColumns entry
        // for that foreach, and we do not need to further visit this foreach here
        if (!prunedColumnsMap.containsKey(foreach))
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
