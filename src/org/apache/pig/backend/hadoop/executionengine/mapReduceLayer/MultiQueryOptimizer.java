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
package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROpPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSplit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.ReverseDependencyOrderWalker;
import org.apache.pig.impl.plan.VisitorException;

/** 
 * An optimizer that merges all or part splittee MapReduceOpers into 
 * splitter MapReduceOper. The merge can produce a MROperPlan that has 
 * fewer MapReduceOpers than MapReduceOpers in the original MROperPlan.
 * 
 * The MRCompler generates multiple MapReduceOpers whenever it encounters 
 * a split operator and connects the single splitter MapReduceOper to 
 * one or more splittee MapReduceOpers using store/load operators:  
 *  
 *     ---- POStore (in splitter) -... ----
 *     |        |    ...    |
 *     |        |    ...    |
 *    POLoad  POLoad ...  POLoad (in splittees)
 *      |        |           |
 *      
 *  This optimizer merges those MapReduceOpers by replacing POLoad/POStore 
 *  combination with POSplit operator.    
 */
class MultiQueryOptimizer extends MROpPlanVisitor {
    
    private Log log = LogFactory.getLog(getClass());
    
    private NodeIdGenerator nig;
    
    private String scope;
    
    MultiQueryOptimizer(MROperPlan plan) {
        super(plan, new ReverseDependencyOrderWalker<MapReduceOper, MROperPlan>(plan));
        nig = NodeIdGenerator.getGenerator();
        List<MapReduceOper> roots = plan.getRoots();
        scope = roots.get(0).getOperatorKey().getScope();
    }

    @Override
    public void visitMROp(MapReduceOper mr) throws VisitorException {
       
        if (!mr.isSplitter()) {
            return;
        }
       
        // first classify all the splittees
        List<MapReduceOper> mappers = new ArrayList<MapReduceOper>();
        List<MapReduceOper> multiLoadMROpers = new ArrayList<MapReduceOper>();
        List<MapReduceOper> mapReducers = new ArrayList<MapReduceOper>();
                    
        List<MapReduceOper> successors = getPlan().getSuccessors(mr);
        for (MapReduceOper successor : successors) {
            if (isMapOnly(successor)) {
                if (isSingleLoadMapperPlan(successor.mapPlan)) {                    
                    mappers.add(successor);
                } else {                    
                    multiLoadMROpers.add(successor);
                }
            } else {
                if (isSingleLoadMapperPlan(successor.mapPlan)) {                     
                    mapReducers.add(successor);
                } else {                    
                    multiLoadMROpers.add(successor);
                }
            }                
        }
              
        // case 1: exactly one splittee and it's map-only
        if (mappers.size() == 1 && mapReducers.size() == 0 
                && multiLoadMROpers.size() == 0 ) {            
            mergeOnlyMapperSplittee(mappers.get(0), mr);
            return;              
        }
        
        // case 2: exactly one splittee and it has reducer
        if (isMapOnly(mr) && mapReducers.size() == 1 
                && mappers.size() == 0 && multiLoadMROpers.size() == 0) {            
            mergeOnlyMapReduceSplittee(mapReducers.get(0), mr);
            return;
        } 
        
        PhysicalPlan splitterPl = isMapOnly(mr) ? mr.mapPlan : mr.reducePlan;                            
        POStore storeOp = (POStore)splitterPl.getLeaves().get(0); 
        
        POSplit splitOp = null;
        
        // case 3: multiple splittees and at least one of them is map-only
        if (mappers.size() > 0) {                
            splitOp = getSplit();                
            mergeAllMapOnlySplittees(mappers, mr, splitOp);
        }            
               
        boolean splitterMapOnly = isMapOnly(mr);
        
        // case 4: multiple splittees and at least one of them has reducer  
        if (splitterMapOnly && mapReducers.size() > 0) {
           
            // pick one to merge, prefer one that has a combiner
            MapReduceOper mapReducer= mapReducers.get(0);
            for (MapReduceOper mro : mapReducers) {
                if (!mro.combinePlan.isEmpty()) {
                    mapReducer = mro;
                    break;
                }
            }
              
            PhysicalOperator leaf = splitterPl.getLeaves().get(0);
                                                            
            splitOp = (leaf instanceof POStore) ? 
                    getSplit() : (POSplit)leaf;
                    
            mergeSingleMapReduceSplittee(mapReducer, mr, splitOp);                
        }
        
        // finally, add original store to the split operator 
        // if there is splittee that hasn't been merged
        if (splitOp != null 
                && ((multiLoadMROpers.size() > 0)
                        || (mapReducers.size() > 1) 
                        || (!splitterMapOnly && mapReducers.size() > 0))) {

            PhysicalPlan storePlan = new PhysicalPlan();
            try {
                storePlan.addAsLeaf(storeOp);
                splitOp.addPlan(storePlan);
            } catch (PlanException e) {
                throw new VisitorException(e);
            }                               
        }
    }                
   
    private void mergeOneMapPart(MapReduceOper mapper, MapReduceOper splitter)
    throws VisitorException {
        PhysicalPlan splitterPl = isMapOnly(splitter) ? splitter.mapPlan : splitter.reducePlan;
        POStore storeOp = (POStore)splitterPl.getLeaves().get(0);
        List<PhysicalOperator> storePreds = splitterPl.getPredecessors(storeOp);           
        
        PhysicalPlan pl = mapper.mapPlan;
        PhysicalOperator load = pl.getRoots().get(0);               
        pl.remove(load);
                        
        // make a copy before removing the store operator
        List<PhysicalOperator> predsCopy = new ArrayList<PhysicalOperator>(storePreds);
        splitterPl.remove(storeOp);                
        
        try {
            splitterPl.merge(pl);
        } catch (PlanException e) {
            throw new VisitorException(e);
        }                
        
        // connect two plans   
        List<PhysicalOperator> roots = pl.getRoots();
        for (PhysicalOperator pred : predsCopy) {
            for (PhysicalOperator root : roots) {
                try {
                    splitterPl.connect(pred, root);
                } catch (PlanException e) {
                    throw new VisitorException(e);
                }
            }
        }
    }

    private void mergeOnlyMapperSplittee(MapReduceOper mapper, MapReduceOper splitter) 
    throws VisitorException {
        mergeOneMapPart(mapper, splitter);       
        removeAndReconnect(mapper, splitter);
    }
    
    private void mergeOnlyMapReduceSplittee(MapReduceOper mapReducer, MapReduceOper splitter)
    throws VisitorException {
        mergeOneMapPart(mapReducer, splitter);

        splitter.setMapDone(true);
        splitter.reducePlan = mapReducer.reducePlan;
        splitter.setReduceDone(true);

        removeAndReconnect(mapReducer, splitter);          
    }
    
    private void mergeAllMapOnlySplittees(List<MapReduceOper> mappers, 
            MapReduceOper splitter, POSplit splitOp) throws VisitorException {
        
        PhysicalPlan splitterPl = isMapOnly(splitter) ? 
                splitter.mapPlan : splitter.reducePlan;                            
        PhysicalOperator storeOp = splitterPl.getLeaves().get(0);
        List<PhysicalOperator> storePreds = splitterPl.getPredecessors(storeOp);  

        // merge splitee's map plans into nested plan of 
        // the splitter operator
        for (MapReduceOper mapper : mappers) {                                
            PhysicalPlan pl = mapper.mapPlan;
            PhysicalOperator load = pl.getRoots().get(0);
            pl.remove(load);
            try {
                splitOp.addPlan(pl);
            } catch (PlanException e) {
                throw new VisitorException(e);
            }
        }
                           
        // replace store operator in the splitter with split operator
        splitOp.setInputs(storePreds);
        try {
            splitterPl.replace(storeOp, splitOp);;
        } catch (PlanException e) {
            throw new VisitorException(e);
        }    
        
        // remove all the map-only splittees from the MROperPlan
        for (MapReduceOper mapper : mappers) {
            removeAndReconnect(mapper, splitter);                  
        }
    }
    
    private void mergeSingleMapReduceSplittee(MapReduceOper mapReduce, 
            MapReduceOper splitter, POSplit splitOp) throws VisitorException {
        
        PhysicalPlan splitterPl = splitter.mapPlan;
        PhysicalOperator leaf = splitterPl.getLeaves().get(0);
        PhysicalOperator storeOp = splitterPl.getLeaves().get(0);
        List<PhysicalOperator> storePreds = splitterPl.getPredecessors(storeOp);  
                        
        PhysicalPlan pl = mapReduce.mapPlan;
        PhysicalOperator load = pl.getRoots().get(0);
        pl.remove(load);
        try {
            splitOp.addPlan(pl);
        } catch (PlanException e) {
            throw new VisitorException(e);
        }
                              
        splitter.setMapDone(true);
        splitter.reducePlan = mapReduce.reducePlan;
        splitter.setReduceDone(true);
        splitter.combinePlan = mapReduce.combinePlan;
                
        // replace store operator in the splitter with split operator
        if (leaf instanceof POStore) {                            
            splitOp.setInputs(storePreds);
            try {
                splitterPl.replace(storeOp, splitOp);;
            } catch (PlanException e) {
                throw new VisitorException(e);
            }  
        }
        
        removeAndReconnect(mapReduce, splitter);           
    }
    
    /**
     * Removes the specified MR operator from the plan after the merge. 
     * Connects its predecessors and successors to the merged MR operator
     * @param mr the MR operator to remove
     * @param newMR the MR operator to be connected to the predecessors and 
     *              the successors of the removed operator
     * @throws VisitorException if connect operation fails
     */
    private void removeAndReconnect(MapReduceOper mr, MapReduceOper newMR) throws VisitorException {
        List<MapReduceOper> mapperSuccs = getPlan().getSuccessors(mr);
        List<MapReduceOper> mapperPreds = getPlan().getPredecessors(mr);
        
        // make a copy before removing operator
        ArrayList<MapReduceOper> succsCopy = null;
        ArrayList<MapReduceOper> predsCopy = null;
        if (mapperSuccs != null) {
            succsCopy = new ArrayList<MapReduceOper>(mapperSuccs);
        }
        if (mapperPreds != null) {
            predsCopy = new ArrayList<MapReduceOper>(mapperPreds);
        }
        getPlan().remove(mr);   
        
        // reconnect the mapper's successors
        if (succsCopy != null) {
            for (MapReduceOper succ : succsCopy) {
                try {                   
                    getPlan().connect(newMR, succ);
                } catch (PlanException e) {
                    throw new VisitorException(e);
                }
            }
        }
        
        // reconnect the mapper's predecessors
        if (predsCopy != null) {
            for (MapReduceOper pred : predsCopy) {
                if (newMR.getOperatorKey().equals(pred.getOperatorKey())) {
                    continue;
                }
                try {                    
                    getPlan().connect(pred, newMR);
                } catch (PlanException e) {
                    throw new VisitorException(e);
                }
            }
        }     
        
        mergeMROperProperties(mr, newMR);
    }
    
    private void mergeMROperProperties(MapReduceOper from, MapReduceOper to) {

        if (from.isStreamInMap()) {
            to.setStreamInMap(true);
        }

        if (from.isStreamInReduce()) {
            to.setStreamInReduce(true);
        }
        
        if (from.getRequestedParallelism() > to.getRequestedParallelism()) {
            to.requestedParallelism = from.requestedParallelism;
        }

        if (!from.UDFs.isEmpty()) {
            to.UDFs.addAll(from.UDFs);
        }

        if (from.needsDistinctCombiner()) {
            to.setNeedsDistinctCombiner(true);
        }

        if (to.mapKeyType == DataType.UNKNOWN) {
            to.mapKeyType = from.mapKeyType;
        }
    }

    private boolean isMapOnly(MapReduceOper mr) {
        return mr.reducePlan.isEmpty();
    }
    
    private boolean isSingleLoadMapperPlan(PhysicalPlan pl) {
        List<PhysicalOperator> roots = pl.getRoots();
        return (roots.size() == 1);
    }
    
    private POSplit getSplit(){
        POSplit sp = new POSplit(new OperatorKey(scope, nig.getNextNodeId(scope)));
        return sp;
    } 
}
