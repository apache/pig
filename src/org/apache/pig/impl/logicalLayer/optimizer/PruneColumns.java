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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadPushDown;
import org.apache.pig.PigException;
import org.apache.pig.LoadPushDown.RequiredField;
import org.apache.pig.LoadPushDown.RequiredFieldList;
import org.apache.pig.LoadPushDown.RequiredFieldResponse;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.ColumnPruner;
import org.apache.pig.impl.logicalLayer.ExpressionOperator;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.LOCast;
import org.apache.pig.impl.logicalLayer.LOCogroup;
import org.apache.pig.impl.logicalLayer.LOCross;
import org.apache.pig.impl.logicalLayer.LODistinct;
import org.apache.pig.impl.logicalLayer.LOFilter;
import org.apache.pig.impl.logicalLayer.LOForEach;
import org.apache.pig.impl.logicalLayer.LOJoin;
import org.apache.pig.impl.logicalLayer.LOLoad;
import org.apache.pig.impl.logicalLayer.LOMapLookup;
import org.apache.pig.impl.logicalLayer.LOProject;
import org.apache.pig.impl.logicalLayer.LOSort;
import org.apache.pig.impl.logicalLayer.LOSplit;
import org.apache.pig.impl.logicalLayer.LOSplitOutput;
import org.apache.pig.impl.logicalLayer.LOStore;
import org.apache.pig.impl.logicalLayer.LOStream;
import org.apache.pig.impl.logicalLayer.LOUnion;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.logicalLayer.RelationalOperator;
import org.apache.pig.impl.logicalLayer.TopLevelProjectFinder;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.MapKeysInfo;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.RequiredFields;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.plan.ProjectionMap.Column;
import org.apache.pig.impl.plan.optimizer.OptimizerException;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.impl.util.Pair;

class RequiredInfo {
    List<RequiredFields> requiredFieldsList;

    RequiredInfo(List<RequiredFields> requiredFieldsList)
    {
        this.requiredFieldsList = requiredFieldsList;
    }
}

public class PruneColumns extends LogicalTransformer {
    private boolean safeToPrune = true;
    private static Log log = LogFactory.getLog(PruneColumns.class);
    Map<RelationalOperator, RequiredInfo> cachedRequiredInfo = new HashMap<RelationalOperator, RequiredInfo>();
    private Map<LOLoad, RequiredFields> prunedLoaderColumnsMap = new HashMap<LOLoad, RequiredFields>();
    ColumnPruner pruner;
    public PruneColumns(LogicalPlan plan) {
        super(plan);
        pruner = new ColumnPruner(plan);
    }

    @Override
    public boolean check(List<LogicalOperator> nodes) throws OptimizerException {
        if((nodes == null) || (nodes.size() <= 0)) {
            int errCode = 2177;
            String msg = "Cannot retrieve operator from null or empty list.";
            throw new OptimizerException(msg, errCode, PigException.BUG);
        }
        
        try {
            LogicalOperator lo = nodes.get(0);
            if (lo == null) {
                int errCode = 2178;
                String msg = "The matching node from the optimizor framework is null";
                throw new OptimizerException(msg, errCode, PigException.BUG);
            }
            if ((lo instanceof LOForEach||lo instanceof LOSplit)&&lo.getSchema()!=null)
                return true;
            return false;
        } catch (Exception e) {
            int errCode = 2179;
            String msg = "Error while performing checks to prune columns.";
            throw new OptimizerException(msg, errCode, PigException.BUG, e);
        }
    }
    
    @Override
    // transform will pick every LOForEach and LOSplit
    public void transform(List<LogicalOperator> nodes)
            throws OptimizerException {
        if((nodes == null) || (nodes.size() <= 0)) {
            int errCode = 2177;
            String msg = "Cannot retrieve operator from null or empty list.";
            throw new OptimizerException(msg, errCode, PigException.BUG);
        }
        try {
            LogicalOperator lo = nodes.get(0);
            if (lo == null || !(lo instanceof LOForEach || lo instanceof LOSplit)) {
                int errCode = 2178;
                String msg = "Expected " + LOForEach.class.getSimpleName() + " or " + LOSplit.class.getSimpleName();
                throw new OptimizerException(msg, errCode, PigException.BUG);
            }

            // Check if we have saved requiredInfo, if so, we will use that as required output fields for that operator;
            // Otherwise means we require every output field
            RequiredInfo requiredOutputInfo = cachedRequiredInfo.get(lo);

            if (requiredOutputInfo==null)
            {
                List<RequiredFields> requiredOutputFieldsList = new ArrayList<RequiredFields>();
                List<LogicalOperator> successors = mPlan.getSuccessors(lo);
                if (successors==null)
                {
                    requiredOutputFieldsList.add(new RequiredFields(true));
                }
                else
                {
                    // The only case requiredOutputFieldsList more than 1 element is when the current 
                    // operator is LOSplit
                    for (int i=0;i<successors.size();i++)
                    {
                        requiredOutputFieldsList.add(new RequiredFields(true));
                    }
                }
                requiredOutputInfo = new RequiredInfo(requiredOutputFieldsList);
            }
            processNode(lo, requiredOutputInfo);
        } catch (OptimizerException oe) {
            throw oe;
        } catch (Exception e) {
            int errCode = 2181;
            String msg = "Unable to prune columns.";
            throw new OptimizerException(msg, errCode, PigException.BUG, e);
        }
    }

    // We recursively collect required fields from forEach from bottom to top, until one of the following conditions occurs:
    //   1. If we see another LOForEach, we simply stop because optimizor will pick that foreach later and start from there
    //   2. If we see LOStore, LOStream, LODistinct, we stop, LOStore, LOStream, LODistinct require all fields, we cannot push upward
    //   3. If we see LOLoad, we set required fields and stop, LOLoad suppose to read only required fields
    //   4. If we see LOSplit, we save requiredInfo and quit. optimizor will pick that split after all its successors are visited
    // For all other operators, we recursively call processNode for all its parents
    // 
    // Inside processNode, we will collect required input columns from required output columns. Required input/output columns 
    //     also include required map keys referred by the logical plan beneath. Required input columns come from two sources:
    //   1. Relevant input fields of required output fields
    //   2. Required input fields of the logic operator
    //
    // lo: logical operator to process
    // requiredOutputFields: requiredFieldsList below this operator
    public void processNode(LogicalOperator lo, RequiredInfo requiredOutputInfo) throws OptimizerException
    {
        try
        {
            if (!safeToPrune)
                return;
            if (!(lo instanceof RelationalOperator))
            {
                int errCode = 2182;
                String msg = "Only relational operator can be used in column prune optimization.";
                throw new OptimizerException(msg, errCode, PigException.BUG);
            }
            if (lo.getSchema()==null)
            {
                safeToPrune = false;
                return;
            }
            RelationalOperator rlo = (RelationalOperator)lo;
            List<LogicalOperator> predecessors = (mPlan.getPredecessors(rlo) == null ? null
                    : new ArrayList<LogicalOperator>(mPlan.getPredecessors(rlo)));
            
            // Now we have collected required output fields of LOLoad (include requried map keys).
            // We need to push these into the loader
            if (rlo instanceof LOLoad)
            {
                // LOLoad has only one output
                RequiredFields loaderRequiredFields = requiredOutputInfo.requiredFieldsList.get(0);
                prunedLoaderColumnsMap.put((LOLoad)rlo, loaderRequiredFields);
                return;
            }
            
            // If the predecessor is one of LOStore/LOStream/LODistinct, we stop to trace up.
            // We require all input fields. We stop processing here. The optimizer will
            // pick the next ForEach and start processing from there
            if (rlo instanceof LOStore || rlo instanceof LOStream || rlo instanceof LODistinct) {
                return;
            }
            
            // merge requiredOutputFields and process the predecessor
            if (rlo instanceof LOSplit)
            {
                List<RequiredFields> requiredInputFieldsList = new ArrayList<RequiredFields>();
                RequiredFields requiredFields = new RequiredFields(false);
                for (int i=0;i<mPlan.getSuccessors(rlo).size();i++)
                {
                    RequiredFields rf = null;
                    try {
                        rf = requiredOutputInfo.requiredFieldsList.get(i);
                    } catch (Exception e) {
                    }
                    if (rf!=null)
                    {
                        rf.reIndex(0);
                        requiredFields.merge(rf);
                    } else {
                        // need all fields
                        List<Pair<Integer, Integer>> l = new ArrayList<Pair<Integer, Integer>>();
                        for (int j=0;j<rlo.getSchema().size();j++)
                            l.add(new Pair<Integer, Integer>(0, j));
                        rf = new RequiredFields(l);
                        requiredFields.merge(rf);
                        break;
                    }
                }
                requiredInputFieldsList.add(requiredFields);
                if (predecessors.get(0) instanceof LOForEach || predecessors.get(0) instanceof LOSplit)
                    cachedRequiredInfo.put((RelationalOperator)predecessors.get(0), new RequiredInfo(requiredInputFieldsList));
                else
                    processNode(predecessors.get(0), new RequiredInfo(requiredInputFieldsList));
                return;
            }
            
            // Initialize requiredInputFieldsList
            List<RequiredFields> requiredInputFieldsList = new ArrayList<RequiredFields>(); 
            for (int i=0;i<predecessors.size();i++)
                requiredInputFieldsList.add(null);
            
            // Map required output columns to required input columns.
            // We also collect required output map keys into input map keys.
            // Since we have already processed Split, so every remaining operator
            // have only one element in requiredOutputFieldList, so we get the first 
            // element and process
            RequiredFields requiredOutputFields = requiredOutputInfo.requiredFieldsList.get(0);
            
            // needAllFields means we require every individual output column and all map keys of that output.
            // We convert needAllFields to individual fields here to facilitate further processing
            if (requiredOutputFields.needAllFields())
            {
                List<Pair<Integer, Integer>> outputList = new ArrayList<Pair<Integer, Integer>>(); 
                for (int j=0;j<rlo.getSchema().size();j++)
                    outputList.add(new Pair<Integer, Integer>(0, j));
                requiredOutputFields = new RequiredFields(outputList);
                for (int i=0;i<requiredOutputFields.size();i++)
                    requiredOutputFields.setMapKeysInfo(i, new MapKeysInfo(true));
            }
            
            if (requiredOutputFields.getFields()==null)
            {
                int errCode = 2184;
                String msg = "Fields list inside RequiredFields is null.";
                throw new OptimizerException(msg, errCode, PigException.BUG);
            }
            
            for (int i=0;i<requiredOutputFields.size();i++)
            {
                Pair<Integer, Integer> requiredOutputField = requiredOutputFields.getField(i);
                MapKeysInfo outputMapKeysInfo = requiredOutputFields.getMapKeysInfo(i);

                List<RequiredFields> relevantFieldsList = rlo.getRelevantInputs(requiredOutputField.first, requiredOutputField.second);
                
                // We do not have any relevant input fields for this output, continue to next output 
                if (relevantFieldsList==null)
                    continue;
                
                for (int j=0;j<relevantFieldsList.size();j++)
                {
                    RequiredFields relevantFields = relevantFieldsList.get(j);
                    
                    if (relevantFields!=null && relevantFields.needAllFields())
                    {
                        requiredInputFieldsList.set(j, new RequiredFields(true));
                        continue;
                    }
                    
                    // Mapping output map keys to input map keys
                    if (rlo instanceof LOCogroup)
                    {
                        if (j!=0 && relevantFields!=null && !relevantFields.needAllFields())
                        {
                            for (Pair<Integer, Integer> pair : relevantFields.getFields())
                                relevantFields.setMapKeysInfo(pair.first, pair.second, 
                                        new MapKeysInfo(true));
                        }
                    }
                    else if (rlo instanceof LOForEach)
                    {
                        // Relay map keys from output to input
                        LogicalPlan forEachPlan = ((LOForEach)rlo).getRelevantPlan(requiredOutputField.second);
                        if (relevantFields.getFields()!=null && relevantFields.getFields().size()!=0)
                        {
                            int index = ((LOForEach)rlo).getForEachPlans().indexOf(forEachPlan);
                            // We check if the field get flattened, if it does, then we do not relay output map keys to input map keys.
                            // There are two situations:
                            // 1. input column is tuple, bag, or other simple type, there is no concept of map key, so we do not relay
                            // 2. input column is map, flatten does not do anything, we can still relay
                            boolean nonflatten = false;
                            if (!((LOForEach)rlo).getFlatten().get(index))
                            {
                                nonflatten = true;
                            }
                            else
                            {
                                // Foreach plan is flattened, check if there is only one input for this foreach plan 
                                // and input schema for that input is not map, if so, it is a dummy flatten 
                                if (forEachPlan.getRoots().size()==1 && forEachPlan.getRoots().get(0) instanceof LOProject)
                                {
                                    LOProject loProj = (LOProject)forEachPlan.getRoots().get(0);
                                    if (loProj.getExpression().getSchema()!=null &&
                                            loProj.getExpression().getSchema().getField(loProj.getCol()).type!=DataType.BAG)
                                        nonflatten = true;
                                }
                            }
                            if (nonflatten && outputMapKeysInfo!=null && isSimpleProjectCast(forEachPlan))
                            {
                                Pair<Integer, Integer> inputColumn = relevantFields.getFields().get(0);
                                relevantFields.setMapKeysInfo(inputColumn.first, inputColumn.second, outputMapKeysInfo);
                            }
                        }
                        
                        // Collect required map keys in foreach plan here.
                        // This is the only logical operator that we collect map keys 
                        // which are introduced by the operator here.
                        // For all other logical operators, it is attached to required fields
                        // of that logical operator, will process in required fields processing
                        // section
                        for (Pair<Integer, Integer> relevantField : relevantFields.getFields())
                        {
                            MapKeysInfo mapKeysInfo = getMapKeysInPlan(forEachPlan, relevantField.second);
                            relevantFields.mergeMapKeysInfo(0, relevantField.second, mapKeysInfo);
                        }
                    }
                    else
                    {
                        // For all other logical operators, we have one output column mapping to one or more input column.
                        // We copy the output map keys from the output column to the according input column
                        if (relevantFields!=null && relevantFields.getFields()!=null && outputMapKeysInfo!=null)
                        {
                            for (Pair<Integer, Integer> pair : relevantFields.getFields())
                                relevantFields.setMapKeysInfo(pair.first, pair.second, 
                                        outputMapKeysInfo);
                        }
                    }
                    
                    // Now we aggregate the input columns of this output column to the required input columns
                    if (requiredInputFieldsList.get(j)==null)
                        requiredInputFieldsList.set(j, relevantFields);
                    else
                    {
                        requiredInputFieldsList.get(j).merge(relevantFields);
                    }
                }
            }
            
            // Merge with required input fields of this logical operator.
            // RequiredInputFields come from two sources, one is mapping from required output to input, 
            // the other is from the operator itself. Here we use getRequiredFields to get the second part,
            // and merge with the first part
            List<RequiredFields> requiredFieldsListOfLOOp;
            
            // For LOForEach, requiredFields all flattened fields. Even the flattened fields get pruned, 
            // it may expand the number of rows in the result. So flattened fields shall not be pruned.
            // LOForEach.getRequiredFields does not give the required fields. RequiredFields means that field
            // is required by all the outputs. The pipeline does not work correctly without that field. 
            // LOForEach.getRequiredFields give all the input fields referred in the LOForEach statement, but those
            // fields can still be pruned (which means, not required)
            // Eg:
            // B = foreach A generate a0, a1, a2+a3;
            // LOForEach.getRequiredFields gives (a0, a1, a2, a3);
            // However, a2,a3 can be pruned if we do not need the a2+a3 for LOForEach.
            // So here, we do not use LOForEach.getRequiredFields, instead, any flattened fields are required fields
            if (rlo instanceof LOForEach) {
                List<Pair<Integer, Integer>> flattenedInputs = new ArrayList<Pair<Integer, Integer>>();
                for (int i=0;i<rlo.getSchema().size();i++) {
                    if (((LOForEach)rlo).isInputFlattened(i)) {
                        flattenedInputs.add(new Pair<Integer, Integer>(0, i));
                    }
                }
                if (!flattenedInputs.isEmpty()) {
                    requiredFieldsListOfLOOp = new ArrayList<RequiredFields>();
                    requiredFieldsListOfLOOp.add(new RequiredFields(flattenedInputs));
                }
                else
                    requiredFieldsListOfLOOp = null;
            }
            // For LOCross/LOUnion, actually we do not require any field here
            else if (rlo instanceof LOCross || rlo instanceof LOUnion)
                requiredFieldsListOfLOOp = null;
            else
                requiredFieldsListOfLOOp = rlo.getRequiredFields();
            
            if (requiredFieldsListOfLOOp!=null)
            {
                for (int i=0;i<requiredFieldsListOfLOOp.size();i++)
                {
                    RequiredFields requiredFieldsOfLOOp = requiredFieldsListOfLOOp.get(i);
                    if (requiredInputFieldsList.get(i)==null)
                        requiredInputFieldsList.set(i, requiredFieldsOfLOOp);
                    else
                    {
                        requiredInputFieldsList.get(i).merge(requiredFieldsOfLOOp);
                    }
                }
                
                // Collect required map keys of this operator
                // Cases are:
                // 1. Single predecessor: LOFilter, LOSplitOutput, LOSort
                // 2. Multiple predecessors: LOJoin
                // 3. LOForEach do not have operator-wise required fields, we
                //    have already processed it
                // 4. LOCogroup require all map keys (even if we cogroup by a0#'k1', a0 itself will be in bag a
                //    and we have no way to figure out which keys are referenced for a0. So we do not process it and
                //    simply require all map keys)
                // 5. Other operators do not have required fields, no need to process
                if (rlo instanceof LOFilter || rlo instanceof LOSplitOutput || rlo instanceof LOSort)
                {
                    List<LogicalPlan> innerPlans = new ArrayList<LogicalPlan>();
                    if (rlo instanceof LOFilter)
                    {
                        innerPlans.add(((LOFilter)rlo).getComparisonPlan());
                    }
                    else if (rlo instanceof LOSplitOutput)
                    {
                        innerPlans.add(((LOSplitOutput)rlo).getConditionPlan());
                    }
                    else if (rlo instanceof LOSort)
                    {
                        innerPlans.addAll(((LOSort)rlo).getSortColPlans());
                    }
                    for (LogicalPlan p : innerPlans)
                    {
                        for (RequiredFields rf : requiredFieldsListOfLOOp)
                        {
                            if (rf.getFields()==null)
                                continue;
                            for (Pair<Integer, Integer> pair : rf.getFields())
                            {
                                MapKeysInfo mapKeysInfo = getMapKeysInPlan(p, pair.second);
                                if (mapKeysInfo!=null && !mapKeysInfo.needAllKeys() && mapKeysInfo.getKeys()!=null)
                                    requiredInputFieldsList.get(0).mergeMapKeysInfo(0, pair.second, 
                                            mapKeysInfo);
                            }
                        }
                    }
                }
                else if (rlo instanceof LOJoin)
                {
                    for (int i=0;i<predecessors.size();i++)
                    {
                        Collection<LogicalPlan> joinPlans = ((LOJoin)rlo).getJoinPlans().get(predecessors.get(i));
                        if (joinPlans==null)
                            continue;
                        for (LogicalPlan p : joinPlans)
                        {
                            RequiredFields rf = requiredFieldsListOfLOOp.get(i);
                            if (rf.getFields()==null)
                                continue;
                            for (Pair<Integer, Integer> pair : rf.getFields())
                            {
                                MapKeysInfo mapKeysInfo = getMapKeysInPlan(p, pair.second);
                                if (mapKeysInfo!=null && !mapKeysInfo.needAllKeys() && mapKeysInfo.getKeys()!=null)
                                    requiredInputFieldsList.get(i).mergeMapKeysInfo(i, pair.second, 
                                            mapKeysInfo);
                            }
                        }
                    }
                }
            }
            
            // Now we finish the current logical operator, we need to process next logical operator. There are two cases:
            // 1. If the predecessor is LOForEach or LOSplit, we put requiredOutputFieldsList into cache and exit, the optimizer
            //    will invoke transform() on LOForEach or LOSplit and continue to process
            // 2. If the predecessor is otherwise, we then recursively collect required fields for the predecessor
            for (int i=0;i<predecessors.size();i++)
            {
            	RelationalOperator predecessor = (RelationalOperator)predecessors.get(i);
                
                List<RequiredFields> newRequiredOutputFieldsList = new ArrayList<RequiredFields>();
            	
                // In this optimization, we only prune columns and do not change structure of logical plan
                // So if we do not require anything from the input, we change it to require the first field
                if (requiredInputFieldsList.get(i)==null || requiredInputFieldsList.get(i).getNeedNoFields())
                {
                    List<Pair<Integer, Integer>> dummyFields = new ArrayList<Pair<Integer, Integer>>();
                    dummyFields.add(new Pair<Integer, Integer>(i, 0));
                    requiredInputFieldsList.set(i, new RequiredFields(dummyFields));
                }
                // For all logical operator with one output, reindex the output to 0
                if (!(predecessor instanceof LOSplit))
                {
                    if (requiredInputFieldsList.get(i)!=null)
                        requiredInputFieldsList.get(i).reIndex(0);
                    newRequiredOutputFieldsList.add(requiredInputFieldsList.get(i));
                }
            	if (predecessor instanceof LOForEach)
            	{
            	    cachedRequiredInfo.put(predecessor, new RequiredInfo(newRequiredOutputFieldsList));
            	    continue;
            	}
            	
            	if (predecessor instanceof LOSplit)
            	{
            	    int outputIndex = mPlan.getSuccessors(predecessor).indexOf(rlo);
            	    if (outputIndex==-1)
            	    {
            	        int errCode = 2186;
                        String msg = "Cannot locate node from successor";
                        throw new OptimizerException(msg, errCode, PigException.BUG);
            	    }
            	    if (requiredInputFieldsList.get(i)!=null)
                    {
            	        requiredInputFieldsList.get(i).reIndex(outputIndex);
                    }
            	    if (cachedRequiredInfo.containsKey(predecessor))
            	        newRequiredOutputFieldsList = cachedRequiredInfo.get(predecessor).requiredFieldsList;
        	        
            	    while (newRequiredOutputFieldsList.size()<=outputIndex)
            	        newRequiredOutputFieldsList.add(null);
        	        newRequiredOutputFieldsList.set(outputIndex, requiredInputFieldsList.get(i));

            	    cachedRequiredInfo.put(predecessor, new RequiredInfo(newRequiredOutputFieldsList));
            	    continue;
            	}
            	processNode(predecessors.get(i), new RequiredInfo(newRequiredOutputFieldsList));
            }
        } catch (FrontendException e) {
            int errCode = 2211;
            String msg = "Unable to prune columns when processing node " + lo;
            throw new OptimizerException(msg, errCode, PigException.BUG, e);
        }
    }
    
    // Get map keys in an inner plan of a particular input. 
    private MapKeysInfo getMapKeysInPlan(LogicalPlan plan, int column) throws OptimizerException
    {
        // Determine if this foreach/cogroup plan can relate map keys from output columns to input columns.
        // For criteria of this, see the comment of method isMapKeyRelayableInInnerPlan
        // If this is true, the reference of the column here does not actually used by the logical operator:
        // eg: B = foreach A generate a0;
        //   We relay map key of a0 to B.$0. Appearance of a0 on its own here does not mean we need all map keys of a0
        //   So once we see this situation, we stop collecting required map keys of this logical operator
        if (isSimpleProjectCast(plan))
            return null;
        
        boolean requireAll = false;
        List<String> mapKeys = null;
        TopLevelProjectFinder projectFinder = new TopLevelProjectFinder(plan);
        try {
            projectFinder.visit();
        } catch (VisitorException ve) {
            int errCode = 2200;
            String msg = "Error getting top level project ";
            throw new OptimizerException(msg, errCode, PigException.BUG, ve);
        }
        for (LOProject project : projectFinder.getProjectSet())
        {
            if (!project.isStar() && project.getCol()==column)  // LOProject for that column 
            {
                List<LogicalOperator> successors = plan.getSuccessors(project);
                // If there are LOCast(s) in the middle (can only be cast to map, otherwise, there will not be maplookup below)
                // it is fine, we can ignore LOCast and continue to look for LOMapLookup 
                while (successors!=null && successors.size()==1 && successors.get(0) instanceof LOCast)
                {
                    LOCast cast = (LOCast)successors.get(0);
                    successors = plan.getSuccessors(cast);
                }
                if (successors!=null && successors.size()==1 && successors.get(0) instanceof LOMapLookup)
                {
                    LOMapLookup loMapLookup = (LOMapLookup)successors.get(0);
                    if (loMapLookup.getLookUpKey()!=null)
                    {
                        if (mapKeys==null)
                            mapKeys = new ArrayList<String>();
                        if (!mapKeys.contains(loMapLookup.getLookUpKey()))
                            mapKeys.add(loMapLookup.getLookUpKey());
                    }
                    requireAll = false;
                }
                else
                {
                    requireAll = true;
                }
            }
        }
        return new MapKeysInfo(requireAll, mapKeys);
    }
    
    // Figure if we need to relay output map keys to input map keys. It is used for inner plan for LOForEach
    // and groupByPlan for LOCogroup
    // There are several cases:
    // 1. UDF, we cannot figure out how each input field is used in UDF, so for each input field,
    //    we require everything
    // 2. Map constant, which do not requires any data input
    // 3. BinCond (B = foreach A generate a0==0?a1:a2; when a1, a2 is map)
    //    This situation is complex. Two branches (a1, a2) is relayed independently, if it relays, then we cannot 
    //    collect map keys of that branches as the required operator map keys.
    //    However, it is unlikely that user will refer a map key of that output, we simply say we need all map keys of 
    //    both map inputs
    // 4. Cast (B = foreach A generate a0 as (map[]);)
    //    The only cases we can cast a field to a map is the input field is a map already or the field is a byte array
    //    If the input field is a byte array, then the input field do not have concept of map keys. So we do not need
    //    to figure out the map key for this input. If the input field is a map, then this cast is just a 1 to 1 mapping.
    //    It is case 5
    // 5. 1 to 1 mapping (B = foreach A generate a0;)
    // 6. Map resolution (B = foreach A generate a0#'key1' as b0)
    //    Since we only trace one level map keys, so relay no key (since all the key in the following script refer to b0#'k'
    //    actually refer to a0#'key1'#'k', we do not relay second level map key 'k'), but require 'key1' for a0
    // 
    // Based on the above observation, the algorithm to map output map keys to input map keys are:
    // 1. If a output column map to multiple input column, then we do not relay this output
    // 2. Find top level project for that plan, we collect map key only when we have only one input 
    //    associate with it
    // 3. Check if the predecessor of the project is null, if not, stop relaying input map keys
    // 4. Check the successors of that project, if it is not a null or cast, stop relaying input map keys
    // The qualifying logical plan should takes one project as root, optionally followed by one or more casts:
    //
    //      Project
    //         |
    //       Cast*
    private boolean isSimpleProjectCast(LogicalPlan innerPlan) throws OptimizerException
    {
        TopLevelProjectFinder projectFinder = new TopLevelProjectFinder(innerPlan);
        
        try {
            projectFinder.visit();
        } catch (VisitorException ve) {
            throw new OptimizerException();
        }
        boolean relayingMapKeys = false;
        if (projectFinder.getProjectSet()!=null && projectFinder.getProjectSet().size()==1)
        {
            LOProject project = projectFinder.getProjectSet().iterator().next();
            if (innerPlan.getPredecessors(project)==null)
            {
                relayingMapKeys = true;
                LogicalOperator pred = project;
                while (innerPlan.getSuccessors(pred)!=null)
                {
                    if (innerPlan.getSuccessors(pred).size()!=1)
                        return false;
                    if (!(innerPlan.getSuccessors(pred).get(0) instanceof LOCast))
                    {
                        return false;
                    }
                    pred = innerPlan.getSuccessors(pred).get(0);
                }
            }
            if (relayingMapKeys)
                return true;
        }
        return false;
    }
    
    // Prune fields of LOLoad, and use ColumePruner to prune all the downstream logical operators
    private void pruneLoader(LOLoad load, RequiredFields loaderRequiredFields) throws FrontendException
    {
        RequiredFieldList requiredFieldList = new RequiredFieldList();

        if (loaderRequiredFields==null || loaderRequiredFields.needAllFields())
            return;
        Schema loadSchema = load.getSchema();
        for (int i=0;i<loaderRequiredFields.size();i++)
        {
            Pair<Integer, Integer> pair = loaderRequiredFields.getField(i);
            MapKeysInfo mapKeysInfo = loaderRequiredFields.getMapKeysInfo(i);
            RequiredField requiredField = new RequiredField();
            requiredField.setIndex(pair.second);
            requiredField.setAlias(loadSchema.getField(pair.second).alias);
            requiredField.setType(loadSchema.getField(pair.second).type);
            if (mapKeysInfo!=null && !mapKeysInfo.needAllKeys())
            {
                List<RequiredField> subFieldList = new ArrayList<RequiredField>();
                for (String key : mapKeysInfo.getKeys())
                {
                    RequiredField mapKeyField = new RequiredField();
                    mapKeyField.setIndex(-1);
                    mapKeyField.setType(DataType.UNKNOWN);
                    mapKeyField.setAlias(key);
                    subFieldList.add(mapKeyField);
                }
                requiredField.setSubFields(subFieldList);
            }
            // Sort requiredFieldList, loader expect required field list sorted by index
            int j=0;
            while (requiredFieldList.getFields().size()>j && requiredFieldList.getFields().get(j).getIndex()<pair.second)
                j++;
            requiredFieldList.getFields().add(j, requiredField);
        }
        
        boolean[] columnRequired = new boolean[load.getSchema().size()];
        RequiredFieldResponse response = null;
        try {
            response = load.pushProjection(requiredFieldList);    
            
        } catch (FrontendException e) {
            log.warn("fieldsToRead on "+load+" throw an exception, skip it");
        }
        
        // If the request is not granted, probably the loader support position prune only,
        // and do not prune map key pruning (such as PigStorage). Drop all map keys (means 
        // we do not prune map keys) and try again
        if (response==null || !response.getRequiredFieldResponse())
        {
            for (RequiredField rf : requiredFieldList.getFields())
            {
                if (rf.getType() == DataType.MAP)
                    rf.setSubFields(null);
            }
            try {
                response = load.pushProjection(requiredFieldList);    
            } catch (FrontendException e) {
                log.warn("fieldsToRead on "+load+" throw an exception, skip it");
            }
        }
        
        // Loader does not support column pruning, insert foreach
        LogicalOperator forEach = null;
        if (response==null || !response.getRequiredFieldResponse())
        {
            List<Integer> columnsToProject = new ArrayList<Integer>();
            for (RequiredField rf : requiredFieldList.getFields())
                columnsToProject.add(rf.getIndex());
            
            forEach = load.insertPlainForEachAfter(columnsToProject);
        }
        
        // Begin to prune
        for (Pair<Integer, Integer> pair: loaderRequiredFields.getFields())
            columnRequired[pair.second] = true;
        
        List<Pair<Integer, Integer>> pruneList = new ArrayList<Pair<Integer, Integer>>();
        for (int i=0;i<columnRequired.length;i++)
        {
            if (!columnRequired[i])
                pruneList.add(new Pair<Integer, Integer>(0, i));
        }

        StringBuffer message = new StringBuffer();
        if (pruneList.size()!=0)
        {
            if (forEach == null)
                pruner.addPruneMap(load, pruneList);
            else
                pruner.addPruneMap(forEach, pruneList);

            message.append("Columns pruned for " + load.getAlias() + ": ");
            for (int i=0;i<pruneList.size();i++)
            {
                message.append("$"+pruneList.get(i).second);
                if (i!=pruneList.size()-1)
                    message.append(", ");
            }
            log.info(message);
        }
        else
            log.info("No column pruned for " + load.getAlias());
        message = new StringBuffer();;
        for (RequiredField rf : requiredFieldList.getFields())
        {
            if (rf.getSubFields()!=null)
            {
                message.append("Map key required for " + load.getAlias()+": ");
                if (rf.getIndex()!=-1)
                    message.append("$"+rf.getIndex());
                else
                    message.append(rf.getAlias());
                message.append("->[");
                for (int i=0;i<rf.getSubFields().size();i++)
                {
                    RequiredField keyrf = rf.getSubFields().get(i);
                    message.append(keyrf);
                    if (i!=rf.getSubFields().size()-1)
                        message.append(",");
                }
                message.append("] ");
            }
        }
        if (message.length()!=0)
            log.info(message);
        else
            log.info("No map keys pruned for " + load.getAlias());
    }
    
    public void prune() throws OptimizerException {
        try {
            if (!safeToPrune)
                return;
            
            for (LOLoad load : prunedLoaderColumnsMap.keySet())
                pruneLoader(load, prunedLoaderColumnsMap.get(load));
            
            if (!pruner.isEmpty())
                pruner.visit();
        }
        catch (FrontendException e) {
            int errCode = 2212;
            String msg = "Unable to prune plan";
            throw new OptimizerException(msg, errCode, PigException.BUG, e);
        }
    }    
}
