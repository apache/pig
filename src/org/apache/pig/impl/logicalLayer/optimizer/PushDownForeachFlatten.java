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

package org.apache.pig.impl.logicalLayer.optimizer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.pig.impl.logicalLayer.CastFinder;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.LOCross;
import org.apache.pig.impl.logicalLayer.LOForEach;
import org.apache.pig.impl.logicalLayer.LOJoin;
import org.apache.pig.impl.logicalLayer.LOProject;
import org.apache.pig.impl.logicalLayer.LOSort;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.logicalLayer.RelationalOperator;
import org.apache.pig.impl.logicalLayer.UDFFinder;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.ProjectionMap;
import org.apache.pig.impl.plan.RequiredFields;
import org.apache.pig.impl.plan.OperatorPlan.IndexHelper;
import org.apache.pig.impl.plan.ProjectionMap.Column;
import org.apache.pig.impl.plan.optimizer.OptimizerException;
import org.apache.pig.PigException;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.impl.util.Pair;

/**
 * A visitor to discover if a foreach with flatten(s) can be pushed as low down the tree as
 * possible.
 */
public class PushDownForeachFlatten extends LogicalTransformer {

    // boolean to remember if the foreach has to be swapped
    private boolean mSwap = false;

    // boolean to remember if the foreach has to be cloned and pushed into one
    // of the foreach's successor's outputs
    private boolean mInsertBetween = false;
    
    // map of flattened column to its new position in the output
    Map<Integer, Integer> mFlattenedColumnReMap = null;

    public PushDownForeachFlatten(LogicalPlan plan) {
        super(plan);
    }

    /**
     * 
     * @return true if the foreach has to swapped; false otherwise
     */
    public boolean getSwap() {
        return mSwap;
    }

    /**
     * 
     * @return true if the foreach has to be inserted after its successor; false
     *         otherwise
     */
    public boolean getInsertBetween() {
        return mInsertBetween;
    }
    
    /**
     * 
     * @return a map of old column position in the foreach to the column
     *         position in foreach's successor
     */
    public Map<Integer, Integer> getFlattenedColumnMap() {
        return mFlattenedColumnReMap;
    }

    @Override
    public boolean check(List<LogicalOperator> nodes) throws OptimizerException {
        try {
            LOForEach foreach = (LOForEach) getOperator(nodes);
            
            Pair<Boolean, List<Integer>> flattenResult = foreach.hasFlatten();
            boolean flattened = flattenResult.first;
            List<Integer> flattenedColumns = flattenResult.second;
            Set<Integer> flattenedColumnSet = (flattenedColumns == null? null: new HashSet<Integer>(flattenedColumns));

            if(!flattened) {
                return false;
            }
            
            if(flattenedColumns == null || flattenedColumns.size() == 0) {
                return false;
            }
            
            ProjectionMap foreachProjectionMap = foreach.getProjectionMap();
            
            if(foreachProjectionMap == null) {
                return false;
            }
            
            List<Integer> foreachAddedFields = foreachProjectionMap.getAddedFields();
            if(foreachAddedFields != null) {
                Set<Integer> foreachAddedFieldsSet = new HashSet<Integer>(foreachAddedFields);
                flattenedColumnSet.removeAll(foreachAddedFieldsSet);
            }
            
            if(flattenedColumnSet.size() == 0) {
                return false;
            }
            
            for(LogicalPlan foreachPlan: foreach.getForEachPlans()) {
                UDFFinder udfFinder = new UDFFinder(foreachPlan);
                udfFinder.visit();
    
                // if any of the foreach's inner plans contain a UDF then return false
                if (udfFinder.foundAnyUDF()) {
                    return false;
                }
                
                CastFinder castFinder = new CastFinder(foreachPlan);
                castFinder.visit();

                // TODO
                // if any of the foreach's inner plans contain a cast then return false
                // in the future the cast should be moved appropriately
                if (castFinder.foundAnyCast()) {
                    return false;
                }
            }

            List<LogicalOperator> successors = (mPlan.getSuccessors(foreach) == null ? null
                    : new ArrayList<LogicalOperator>(mPlan
                            .getSuccessors(foreach)));

            // if the foreach has no successors or more than one successor
            // return false
            if (successors == null || successors.size() == 0 || successors.size() > 1) {
                return false;
            }

            LogicalOperator successor = successors.get(0);

            List<LogicalOperator> peers = (mPlan.getPredecessors(successor) == null ? null
                    : new ArrayList<LogicalOperator>(mPlan.getPredecessors(successor)));
            
            // check if any of the foreach's peers is a foreach flatten
            // if so then this rule does not apply
            if (peers != null){
                for(LogicalOperator peer: peers) {
                    if(!peer.equals(foreach)) {
                        if(peer instanceof LOForEach) {
                            LOForEach peerForeach = (LOForEach)peer;
                            if(peerForeach.hasFlatten().first) {
                                return false;
                            }
                        }
                    }
                }
            }
            
            IndexHelper<LogicalOperator> indexHelper = new IndexHelper<LogicalOperator>(peers);
            Integer foreachPosition = indexHelper.getIndex(foreach);
            
            // Check if flattened fields is required by successor, if so, don't optimize
            List<RequiredFields> requiredFieldsList = ((RelationalOperator)successor).getRequiredFields();
            RequiredFields requiredFields = requiredFieldsList.get(foreachPosition.intValue());
            
            MultiMap<Integer, Column> foreachMappedFields = foreachProjectionMap.getMappedFields();
            
            if (requiredFields.getFields()!=null) {
                for (Pair<Integer, Integer> pair : requiredFields.getFields()) {
                    Collection<Column> columns = foreachMappedFields.get(pair.second);
                    if (columns!=null) {
                        for (Column column : columns) {
                            Pair<Integer, Integer> foreachInputColumn = column.getInputColumn();
                            if (foreach.isInputFlattened(foreachInputColumn.second))
                                return false;
                        }
                    }
                }
            }
            
            // the foreach with flatten can be swapped with an order by
            // as the order by will have lesser number of records to sort
            // also the sort does not alter the records that are processed
            
            // the foreach with flatten can be pushed down a cross or a join
            // for the same reason. In this case the foreach has to be first
            // unflattened and then a new foreach has to be inserted after
            // the cross or join. In both cross and foreach the actual columns
            // from the foreach are not altered but positions might be changed
            
            // in the case of union the column is transformed and as a result
            // the foreach flatten cannot be pushed down
            
            // for distinct the output before flattening and the output
            // after flattening might be different. For example, consider
            // {(1), (1)}. Distinct of this bag is still {(1), (1)}.
            // distinct(flatten({(1), (1)})) is (1). However,
            // flatten(distinct({(1), (1)})) is (1), (1)
            
            // in both cases correctness is not affected
            if(successor instanceof LOSort) {
                LOSort sort = (LOSort) successor;
                RequiredFields sortRequiredField = sort.getRequiredFields().get(0);
                
                if(sortRequiredField.getNeedAllFields()) {
                    return false;
                }
                
                List<Pair<Integer, Integer>> sortInputs = sortRequiredField.getFields();
                Set<Integer> requiredInputs = new HashSet<Integer>(); 
                for(Pair<Integer, Integer> pair: sortInputs) {
                    requiredInputs.add(pair.second);
                }
                
                requiredInputs.retainAll(flattenedColumnSet);
                // the intersection of the sort's required inputs
                // and the flattened columns in the foreach should
                // be null, i.e., the size of required inputs == 0
                if(requiredInputs.size() != 0) {
                    return false;
                }
                
                mSwap = true;
                return true;
            } else if (successor instanceof LOCross
                    || successor instanceof LOJoin) {
                
                List<LogicalOperator> children = mPlan.getSuccessors(successor);
                
                if(children == null || children.size() > 1) {
                    return false;
                }
                
                ProjectionMap succProjectionMap = successor.getProjectionMap();
                
                if(succProjectionMap == null) {
                    return false;
                }
                
                MultiMap<Integer, ProjectionMap.Column> mappedFields = succProjectionMap.getMappedFields();
                
                if(mappedFields == null) {
                    return false;
                }

                if(mFlattenedColumnReMap == null) {
                    mFlattenedColumnReMap = new HashMap<Integer, Integer>();
                }

                // initialize the map
                for(Integer key: flattenedColumnSet) {
                    mFlattenedColumnReMap.put(key, Integer.MAX_VALUE);
                }
                
                // for each output column find the corresponding input that matches the foreach's position
                // for each input column in the foreach check if the output column is a mapping of the flattened column
                // due to flattenning multiple output columns could be generated from the same input column
                // find the first or the lowest column that is a result of the 
                for(Integer key: mappedFields.keySet()) {
                    List<ProjectionMap.Column> columns = mappedFields.get(key);
                    for(ProjectionMap.Column column: columns) {
                        Pair<Integer, Integer> inputColumn = column.getInputColumn();
                        
                        // check if the input column number is the same as the
                        // position of foreach in the list of predecessors
                        if(foreachPosition.equals(inputColumn.first)) {
                            if(flattenedColumnSet.contains(inputColumn.second)) {
                                // check if the output column, i.e., key is the
                                // least column number seen till date
                                if(key < mFlattenedColumnReMap.get(inputColumn.second)) {
                                    mFlattenedColumnReMap.put(inputColumn.second, key);
                                }
                            }
                        }
                    }
                }
                
                // check if any of the flattened columns is not remapped
                for(Integer key: mFlattenedColumnReMap.keySet()) {
                    if(mFlattenedColumnReMap.get(key).equals(Integer.MAX_VALUE)) {
                        return false;
                    }
                }
                
                mInsertBetween = true;
                return true;
            }
            
            return false;

        } catch (OptimizerException oe) {
            throw oe;
        } catch (Exception e) {
            int errCode = 2152;
            String msg = "Internal error while trying to check if foreach with flatten can be pushed down.";
            throw new OptimizerException(msg, errCode, PigException.BUG, e);
        }
    }

    private LogicalOperator getOperator(List<LogicalOperator> nodes)
            throws FrontendException {
        if ((nodes == null) || (nodes.size() <= 0)) {
            int errCode = 2052;
            String msg = "Internal error. Cannot retrieve operator from null or empty list.";
            throw new OptimizerException(msg, errCode, PigException.BUG);
        }

        LogicalOperator lo = nodes.get(0);
        if (lo == null || !(lo instanceof LOForEach)) {
            // we should never be called with any other operator class name
            int errCode = 2005;
            String msg = "Expected " + LOForEach.class.getSimpleName()
                    + ", got "
                    + (lo == null ? lo : lo.getClass().getSimpleName());
            throw new OptimizerException(msg, errCode, PigException.INPUT);
        } else {
            return lo;
        }

    }

    @Override
    public void transform(List<LogicalOperator> nodes)
            throws OptimizerException {
        try {
            LOForEach foreach = (LOForEach) getOperator(nodes);
            LogicalOperator successor = mPlan.getSuccessors(foreach).get(0);
            if (mSwap) {
                mPlan.swap(successor, foreach);
            } else if (mInsertBetween) {
                // mark the flattened columns as not flattened in the foreach
                // create a new foreach operator that projects each column of the
                // successor. Mark the remapped flattened columns as flattened
                // in the new foreach operator
                
                if(mFlattenedColumnReMap == null) {
                    int errCode = 2153;
                    String msg = "Internal error. The mapping for the flattened columns is empty";
                    throw new OptimizerException(msg, errCode, PigException.BUG);
                }
                
                // set flatten to false for all columns in the mapping
                
                ArrayList<Boolean> flattenList = (ArrayList<Boolean>)foreach.getFlatten();                
                for(Integer key: mFlattenedColumnReMap.keySet()) {
                    flattenList.set(key, false);
                }
                
                // rebuild schemas of the foreach and the successor after the foreach modification
                foreach.regenerateSchema();
                successor.regenerateSchema();
                
                Schema successorSchema = successor.getSchema();
                
                if(successorSchema == null) {
                    int errCode = 2154;
                    String msg = "Internal error. Schema of successor cannot be null for pushing down foreach with flatten.";
                    throw new OptimizerException(msg, errCode, PigException.BUG);
                }
                
                flattenList = new ArrayList<Boolean>();
                
                ArrayList<LogicalPlan> foreachInnerPlans = new ArrayList<LogicalPlan>();
                
                for(int i = 0; i < successorSchema.size(); ++i) {
                    LogicalPlan innerPlan = new LogicalPlan();
                    LOProject project = new LOProject(innerPlan, OperatorKey
                            .genOpKey(foreach.getOperatorKey().scope),
                            successor, i);
                    innerPlan.add(project);
                    foreachInnerPlans.add(innerPlan);
                    flattenList.add(false);
                }
                
                // set the flattened remapped column to true
                for(Integer key: mFlattenedColumnReMap.keySet()) {
                    Integer value = mFlattenedColumnReMap.get(key);
                    flattenList.set(value, true);
                }            
                
                
                LOForEach newForeach = new LOForEach(mPlan, OperatorKey
                        .genOpKey(foreach.getOperatorKey().scope), foreachInnerPlans,
                        flattenList);
                
                // add the new foreach to the plan
                mPlan.add(newForeach);
                
                // insert the new foreach between the successor and the successor's successor
                mPlan.insertBetween(successor, newForeach, mPlan.getSuccessors(successor).get(0));             
            }
        } catch (OptimizerException oe) {
            throw oe;
        } catch (Exception e) {
            int errCode = 2155;
            String msg = "Internal error while pushing foreach with flatten down.";
            throw new OptimizerException(msg, errCode, PigException.BUG, e);
        }
    }

    @Override
    public void reset() {
        mInsertBetween = false;
        mSwap = false;
        mFlattenedColumnReMap = null;
    }

}
