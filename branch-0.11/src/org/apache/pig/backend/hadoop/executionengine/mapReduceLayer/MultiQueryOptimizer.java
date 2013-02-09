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
import java.util.Map;
import java.util.Set;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROpPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.PODemux;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POMultiQueryPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSplit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.io.PigNullableWritable;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.ReverseDependencyOrderWalker;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.plan.optimizer.OptimizerException;
import org.apache.pig.impl.util.Pair;


/** 
 * An optimizer that merges all or part splittee MapReduceOpers into 
 * splitter MapReduceOper. 
 * <p>
 * The merge can produce a MROperPlan that has 
 * fewer MapReduceOpers than MapReduceOpers in the original MROperPlan.
 * <p>
 * The MRCompler generates multiple MapReduceOpers whenever it encounters 
 * a split operator and connects the single splitter MapReduceOper to 
 * one or more splittee MapReduceOpers using store/load operators:  
 * <p>
 *     ---- POStore (in splitter) -... ----
 *     |        |    ...    |
 *     |        |    ...    |
 *    POLoad  POLoad ...  POLoad (in splittees)
 *      |        |           |
 * <p>   
 *  This optimizer merges those MapReduceOpers by replacing POLoad/POStore 
 *  combination with POSplit operator.    
 */
class MultiQueryOptimizer extends MROpPlanVisitor {
    
    private Log log = LogFactory.getLog(getClass());
    
    private NodeIdGenerator nig;
    
    private String scope;
    
    private boolean inIllustrator = false;
    
    MultiQueryOptimizer(MROperPlan plan, boolean inIllustrator) {
        super(plan, new ReverseDependencyOrderWalker<MapReduceOper, MROperPlan>(plan));
        nig = NodeIdGenerator.getGenerator();
        List<MapReduceOper> roots = plan.getRoots();
        scope = roots.get(0).getOperatorKey().getScope();
        this.inIllustrator = inIllustrator;
        log.info("MR plan size before optimization: " + plan.size());
    }

    @Override
    public void visit() throws VisitorException {
        super.visit();
        
        log.info("MR plan size after optimization: " + mPlan.size());
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
            if (successor.getUseSecondaryKey()) {
                log.debug("Splittee " + successor.getOperatorKey().getId()
                        + " uses secondary key, do not merge it");
                continue;
            }
            if (isMapOnly(successor)) {
                if (isSingleLoadMapperPlan(successor.mapPlan)
                        && isSinglePredecessor(successor)) {                    
                    mappers.add(successor);                
                } else {                    
                    multiLoadMROpers.add(successor);
                }
            } else {
                if (isSingleLoadMapperPlan(successor.mapPlan)
                        && isSinglePredecessor(successor)) {                     
                    mapReducers.add(successor);                  
                } else {                    
                    multiLoadMROpers.add(successor);                      
                }
            }                
        }
                  
        int numSplittees = successors.size();
        
        // case 1: exactly one splittee and it's map-only
        if (mappers.size() == 1 && numSplittees == 1) {    
            mergeOnlyMapperSplittee(mappers.get(0), mr);
            
            log.info("Merged the only map-only splittee.");
            
            return;              
        }
        
        // case 2: exactly one splittee and it has reducer
        if (isMapOnly(mr) && mapReducers.size() == 1 && numSplittees == 1) {            
            mergeOnlyMapReduceSplittee(mapReducers.get(0), mr);
            
            log.info("Merged the only map-reduce splittee.");
            
            return;
        } 
                
        int numMerges = 0;
        
        PhysicalPlan splitterPl = isMapOnly(mr) ? mr.mapPlan : mr.reducePlan;                            
        POStore storeOp = (POStore)splitterPl.getLeaves().get(0); 
        
        POSplit splitOp = null;
        
        // case 3: multiple splittees and at least one of them is map-only
        if (mappers.size() > 0) {                
            splitOp = getSplit();  
            int n = mergeAllMapOnlySplittees(mappers, mr, splitOp);            
            
            log.info("Merged " + n + " map-only splittees.");
            
            numMerges += n;   
        }            
        
        if (mapReducers.size() > 0) {
            
            boolean isMapOnly = isMapOnly(mr);
            int merged = 0;
            
            // case 4: multiple splittees and at least one of them has reducer  
            //         and the splitter is map-only   
            if (isMapOnly) {
                         
                PhysicalOperator leaf = splitterPl.getLeaves().get(0);
                                                            
                splitOp = (leaf instanceof POStore) ? getSplit() : (POSplit)leaf;
                    
                merged = mergeMapReduceSplittees(mapReducers, mr, splitOp);  
            }
            
            // case 5: multiple splittees and at least one of them has reducer
            //         and splitter has reducer
            else {
                
                merged = mergeMapReduceSplittees(mapReducers, mr);  
                
            }
            
            log.info("Merged " + merged + " map-reduce splittees.");
            
            numMerges += merged;      
        }
        
        // Finally, add original store to the split operator 
        // if there is splittee that hasn't been merged into the splitter
        if (splitOp != null 
                && (numMerges < numSplittees)) {

            PhysicalPlan storePlan = new PhysicalPlan();
            try {
                storePlan.addAsLeaf(storeOp);
                splitOp.addPlan(storePlan);
            } catch (PlanException e) {
                int errCode = 2129;
                String msg = "Internal Error. Unable to add store to the split plan for optimization.";
                throw new OptimizerException(msg, errCode, PigException.BUG, e);
            }    
        }

        // case 6: special diamond case with trivial MR operator at the head
        if (numMerges == 0 && isDiamondMROper(mr)) {
            int merged = mergeDiamondMROper(mr, getPlan().getSuccessors(mr));
            log.info("Merged " + merged + " diamond splitter.");
            numMerges += merged;    
        }
        
        log.info("Merged " + numMerges + " out of total " 
                + (numSplittees +1) + " MR operators.");
    }                
    
    private boolean isDiamondMROper(MapReduceOper mr) {
        
        // We'll remove this mr as part of diamond query optimization
        // only if this mr is a trivial one, that is, it's plan
        // has either two operators (load followed by store) or three operators 
        // (the operator between the load and store must be a foreach,
        // introduced by casting operation).
        // 
        // We won't optimize in other cases where there're more operators
        // in the plan. Otherwise those operators world run multiple times 
        // in the successor MR operators which may not give better
        // performance.
        boolean rtn = false;
        if (isMapOnly(mr)) {
            PhysicalPlan pl = mr.mapPlan;
            if (pl.size() == 2 || pl.size() == 3) {               
                PhysicalOperator root = pl.getRoots().get(0);
                PhysicalOperator leaf = pl.getLeaves().get(0);
                if (root instanceof POLoad && leaf instanceof POStore) {
                    if (pl.size() == 3) {
                        PhysicalOperator mid = pl.getSuccessors(root).get(0);
                        if (mid instanceof POForEach) {
                            rtn = true;
                        }                      
                    } else {
                        rtn = true;
                    }
                }
            }
        }
        return rtn;
    }
    
    private int mergeDiamondMROper(MapReduceOper mr, List<MapReduceOper> succs) 
        throws VisitorException {
       
        // Only consider the cases where all inputs of the splittees are 
        // from the splitter
        for (MapReduceOper succ : succs) {
            List<MapReduceOper> preds = getPlan().getPredecessors(succ);
            if (preds.size() != 1) {
                return 0;
            }
        }
        
        // first, remove the store operator from the splitter
        PhysicalPlan pl = mr.mapPlan;
        PhysicalOperator leaf = mr.mapPlan.getLeaves().get(0);
        pl.remove(leaf);
        
        POStore store = (POStore)leaf;
        String ofile = store.getSFile().getFileName();
        
        // then connect the remaining map plan to the successor of
        // each root (load) operator of the splittee
        for (MapReduceOper succ : succs) {
            List<PhysicalOperator> roots = succ.mapPlan.getRoots();
            ArrayList<PhysicalOperator> rootsCopy = 
                new ArrayList<PhysicalOperator>(roots);
            for (PhysicalOperator op : rootsCopy) {
                POLoad load = (POLoad)op;
                String ifile = load.getLFile().getFileName();
                if (ofile.compareTo(ifile) != 0) {
                    continue;
                }
                PhysicalOperator opSucc = succ.mapPlan.getSuccessors(op).get(0);
                PhysicalPlan clone = null;
                try {
                    if (inIllustrator)
                        pl.setOpMap(succ.phyToMRMap);
                    clone = pl.clone();
                    if (inIllustrator)
                        pl.resetOpMap();
                } catch (CloneNotSupportedException e) {
                    int errCode = 2127;
                    String msg = "Internal Error: Cloning of plan failed for optimization.";
                    throw new OptimizerException(msg, errCode, PigException.BUG, e);
                }
                succ.mapPlan.remove(op);
                
                if (inIllustrator) {
                    // need to remove the LOAD since data from load on temporary files can't be handled by illustrator
                    for (Iterator<PhysicalOperator> it = pl.iterator(); it.hasNext(); )
                    {
                        PhysicalOperator po = it.next();
                        if (po instanceof POLoad)
                            succ.phyToMRMap.removeKey(po);
                    }
                }
                
                while (!clone.isEmpty()) {
                    PhysicalOperator oper = clone.getLeaves().get(0);
                    clone.remove(oper);
                    succ.mapPlan.add(oper);
                    try {
                        succ.mapPlan.connect(oper, opSucc);
                        opSucc = oper;
                    } catch (PlanException e) {
                        int errCode = 2131;
                        String msg = "Internal Error. Unable to connect split plan for optimization.";
                        throw new OptimizerException(msg, errCode, PigException.BUG, e);
                    }                
                }
            }
            
            // PIG-2069: LoadFunc jar does not ship to backend in MultiQuery case
            if (!mr.UDFs.isEmpty()) {
                succ.UDFs.addAll(mr.UDFs);
            }
        }
        
        // finally, remove the splitter from the MR plan
        List<MapReduceOper> mrPreds = getPlan().getPredecessors(mr);
        if (mrPreds != null) {
            for (MapReduceOper pred : mrPreds) {
                for (MapReduceOper succ : succs) {
                    try {
                        getPlan().connect(pred, succ);
                    } catch (PlanException e) {
                        int errCode = 2131;
                        String msg = "Internal Error. Unable to connect split plan for optimization.";
                        throw new OptimizerException(msg, errCode, PigException.BUG, e);
                    }
                }
            }
        }
        
        getPlan().remove(mr);
        
        return 1;
    }
    
    private void mergeOneMapPart(MapReduceOper mapper, MapReduceOper splitter)
    throws VisitorException {
        PhysicalPlan splitterPl = isMapOnly(splitter) ? 
                splitter.mapPlan : splitter.reducePlan;
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
            int errCode = 2130;
            String msg = "Internal Error. Unable to merge split plans for optimization.";
            throw new OptimizerException(msg, errCode, PigException.BUG, e);
        }                
        
        // connect two plans   
        List<PhysicalOperator> roots = pl.getRoots();
        for (PhysicalOperator pred : predsCopy) {
            for (PhysicalOperator root : roots) {
                try {
                    splitterPl.connect(pred, root);
                } catch (PlanException e) {
                    int errCode = 2131;
                    String msg = "Internal Error. Unable to connect split plan for optimization.";
                    throw new OptimizerException(msg, errCode, PigException.BUG, e);

                }
            }
        }
    }

    private void mergeOnlyMapperSplittee(MapReduceOper mapper, 
            MapReduceOper splitter) throws VisitorException {
        mergeOneMapPart(mapper, splitter);       
        removeAndReconnect(mapper, splitter);
    }
    
    private void mergeOnlyMapReduceSplittee(MapReduceOper mapReducer, 
            MapReduceOper splitter) throws VisitorException {
        mergeOneMapPart(mapReducer, splitter);

        splitter.setMapDone(true);
        splitter.reducePlan = mapReducer.reducePlan;
        splitter.setReduceDone(true);

        removeAndReconnect(mapReducer, splitter);          
    }
    
    private int mergeAllMapOnlySplittees(List<MapReduceOper> mappers, 
            MapReduceOper splitter, POSplit splitOp) throws VisitorException {
        
        PhysicalPlan splitterPl = isMapOnly(splitter) ? 
                splitter.mapPlan : splitter.reducePlan;                            
        PhysicalOperator storeOp = splitterPl.getLeaves().get(0);
        List<PhysicalOperator> storePreds = splitterPl.getPredecessors(storeOp);  

        // merge splitee's map plans into nested plan of 
        // the split operator
        for (MapReduceOper mapper : mappers) {                                
            PhysicalPlan pl = mapper.mapPlan;
            PhysicalOperator load = pl.getRoots().get(0);
            pl.remove(load);                   
            splitOp.addPlan(pl);
        }
                           
        // replace store operator in the splitter with split operator
        splitOp.setInputs(storePreds);    
        try {
            splitterPl.replace(storeOp, splitOp);;
        } catch (PlanException e) {
            int errCode = 2132;
            String msg = "Internal Error. Unable to replace store with split operator for optimization.";
            throw new OptimizerException(msg, errCode, PigException.BUG, e);
        }    
        
        // remove all the map-only splittees from the MROperPlan
        for (MapReduceOper mapper : mappers) {
            removeAndReconnect(mapper, splitter);                  
        }
        
        return mappers.size();
    }
    
    private boolean isSplitteeMergeable(MapReduceOper splittee) {
        
        // cannot be global sort or limit after sort, they are 
        // using a different partitioner
        if (splittee.isGlobalSort() || splittee.isLimitAfterSort()) {
            log.info("Cannot merge this splittee: " +
            		"it is global sort or limit after sort");
            return false;
        }
        
        // check the plan leaf: only merge local rearrange or split
        PhysicalOperator leaf = splittee.mapPlan.getLeaves().get(0);
        if (!(leaf instanceof POLocalRearrange) && 
                ! (leaf instanceof POSplit)) {
            log.info("Cannot merge this splittee: " +
            		"its map plan doesn't end with LR or Split operator: " 
                    + leaf.getClass().getName());
            return false;
        }
           
        // cannot have distinct combiner, it uses a different combiner
        if (splittee.needsDistinctCombiner()) {
            log.info("Cannot merge this splittee: " +
            		"it has distinct combiner.");
            return false;           
        }
        
        return true;
    }
       
    private List<MapReduceOper> getMergeList(MapReduceOper splitter,
            List<MapReduceOper> mapReducers) {
        List<MapReduceOper> mergeNoCmbList = new ArrayList<MapReduceOper>();
        List<MapReduceOper> mergeCmbList = new ArrayList<MapReduceOper>();
        List<MapReduceOper> mergeDistList = new ArrayList<MapReduceOper>();
       
        for (MapReduceOper mrOp : mapReducers) {
            if (isSplitteeMergeable(mrOp)) {
                if (mrOp.combinePlan.isEmpty()) {
                    mergeNoCmbList.add(mrOp);
                } else {
                    mergeCmbList.add(mrOp);
                } 
            } else if (splitter.reducePlan.isEmpty()
                    || splitter.needsDistinctCombiner()) {                
                if (mrOp.needsDistinctCombiner()) {                    
                    mergeDistList.add(mrOp);
                }
            }
        }    
        
        int max = Math.max(mergeNoCmbList.size(), mergeCmbList.size());
        max = Math.max(max, mergeDistList.size());
        
        if (max == mergeDistList.size()) return mergeDistList;
        else if (max == mergeNoCmbList.size()) return mergeNoCmbList;
        else return mergeCmbList;                        
    }
    
    private int mergeMapReduceSplittees(List<MapReduceOper> mapReducers, 
            MapReduceOper splitter, POSplit splitOp) throws VisitorException {
                
        List<MapReduceOper> mergeList = getMergeList(splitter, mapReducers);
    
        if (mergeList.size() <= 1) {

            // chose one to merge, prefer the one with a combiner
            MapReduceOper mapReducer = mapReducers.get(0);
            for (MapReduceOper mro : mapReducers) {
                if (!mro.combinePlan.isEmpty()) {
                    mapReducer = mro;
                    break;
                }
            }
            mergeList.clear();
            mergeList.add(mapReducer);
        } 
                         
        if (mergeList.size() == 1) {
            mergeSingleMapReduceSplittee(mergeList.get(0), splitter, splitOp);
        } else {                                   
            mergeAllMapReduceSplittees(mergeList, splitter, splitOp);
        }
        
        return mergeList.size();
    }

    private int mergeMapReduceSplittees(List<MapReduceOper> mapReducers, 
            MapReduceOper splitter) throws VisitorException {

        // In this case the splitter has non-empty reducer so we can't merge 
        // MR splittees into the splitter. What we'll do is to merge multiple 
        // splittees (if exists) into a new MR operator and connect it to the splitter.
        
        List<MapReduceOper> mergeList = getMergeList(splitter, mapReducers);
    
        if (mergeList.size() <= 1) {
            // nothing to merge, just return
            return  0;
        } 
                         
        MapReduceOper mrOper = getMROper();

        MapReduceOper splittee = mergeList.get(0);
        PhysicalPlan pl = splittee.mapPlan;
        POLoad load = (POLoad)pl.getRoots().get(0);
        
        mrOper.mapPlan.add(load);
       
        // add a dummy store operator, it'll be replaced by the split operator later.
        try {
            mrOper.mapPlan.addAsLeaf(getStore());
        } catch (PlanException e) {                   
            int errCode = 2137;
            String msg = "Internal Error. Unable to add store to the plan as leaf for optimization.";
            throw new OptimizerException(msg, errCode, PigException.BUG, e);
        }

        // connect the new MR operator to the splitter
        try {
            getPlan().add(mrOper);
            getPlan().connect(splitter, mrOper);
        } catch (PlanException e) {
            int errCode = 2133;
            String msg = "Internal Error. Unable to connect splitter with successors for optimization.";
            throw new OptimizerException(msg, errCode, PigException.BUG, e);
        }

        // merger the splittees into the new MR operator
        mergeAllMapReduceSplittees(mergeList, mrOper, getSplit());
        
        return (mergeList.size() - 1);
    }
    
    private boolean hasSameMapKeyType(List<MapReduceOper> splittees) {
        boolean sameKeyType = true;
        for (MapReduceOper outer : splittees) {
            for (MapReduceOper inner : splittees) {
                if (inner.mapKeyType != outer.mapKeyType) {
                    sameKeyType = false;
                    break;
                }
            }
            if (!sameKeyType) break;
        }      
 
        return sameKeyType;
    }
    
    private int setIndexOnLRInSplit(int initial, POSplit splitOp, boolean sameKeyType)
            throws VisitorException {
        int index = initial;
        
        List<PhysicalPlan> pls = splitOp.getPlans();
        for (PhysicalPlan pl : pls) {
            PhysicalOperator leaf = pl.getLeaves().get(0);
            if (leaf instanceof POLocalRearrange) {
                POLocalRearrange lr = (POLocalRearrange)leaf;
                try {
                    lr.setMultiQueryIndex(index++); 
                } catch (ExecException e) {                    
                    int errCode = 2136;
                    String msg = "Internal Error. Unable to set multi-query index for optimization.";
                    throw new OptimizerException(msg, errCode, PigException.BUG, e);                   
                }
                
                // change the map key type to tuple when 
                // multiple splittees have different map key types
                if (!sameKeyType) {
                    lr.setKeyType(DataType.TUPLE);
                }
            } else if (leaf instanceof POSplit) {
                POSplit spl = (POSplit)leaf;
                index = setIndexOnLRInSplit(index, spl, sameKeyType);
            }
        }

        return index;
    }
    
    private int mergeOneMapPlanWithIndex(PhysicalPlan pl, POSplit splitOp, 
            int index, boolean sameKeyType) throws VisitorException {        
        PhysicalOperator load = pl.getRoots().get(0);
        pl.remove(load);
                
        int curIndex = index;
        
        PhysicalOperator leaf = pl.getLeaves().get(0);
        if (leaf instanceof POLocalRearrange) {
            POLocalRearrange lr = (POLocalRearrange)leaf;
            try {
                lr.setMultiQueryIndex(curIndex++);  
            } catch (ExecException e) {                                      
                int errCode = 2136;
                String msg = "Internal Error. Unable to set multi-query index for optimization.";
                throw new OptimizerException(msg, errCode, PigException.BUG, e);
            }
            
            // change the map key type to tuple when 
            // multiple splittees have different map key types
            if (!sameKeyType) {
                lr.setKeyType(DataType.TUPLE);
            }
        } else if (leaf instanceof POSplit) {
            // if the map plan that we are trying to merge
            // has a split, we need to update the indices of
            // the POLocalRearrange operators in the inner plans
            // of the split to be a continuation of the index
            // number sequence we are currently at.
            // So for example, if we we are in the MapRedOper
            // we are currently processing, if the index is currently
            // at 1 (meaning index 0 was used for a map plan
            // merged earlier), then we want the POLocalRearrange
            // operators in the split to have indices 1, 2 ...
            // essentially we are flattening the index numbers
            // across all POLocalRearranges in all merged map plans
            // including nested ones in POSplit
            POSplit spl = (POSplit)leaf;
            curIndex = setIndexOnLRInSplit(index, spl, sameKeyType);
        }
                    
        splitOp.addPlan(pl);
        
        // return the updated index after setting index
        // on all POLocalRearranges including ones
        // in inner plans of any POSplit operators
        return curIndex;
    }
        
    private void mergeOneReducePlanWithIndex(PhysicalPlan from, 
            PhysicalPlan to, int initial, int current, byte mapKeyType) throws VisitorException {                    
        POPackage pk = (POPackage)from.getRoots().get(0);
        from.remove(pk);
 
        if(!(pk instanceof POMultiQueryPackage)){
            // XXX the index of the original keyInfo map is always 0,
            // we need to shift the index so that the lookups works
            // with the new indexed key
            addShiftedKeyInfoIndex(initial, pk); 
        }
         
        int total = current - initial;
        
        POMultiQueryPackage pkg = (POMultiQueryPackage)to.getRoots().get(0);        
        int pkCount = 0;
        if (pk instanceof POMultiQueryPackage) {
            List<POPackage> pkgs = ((POMultiQueryPackage)pk).getPackages();
            for (POPackage p : pkgs) {
                pkg.addPackage(p);
                pkCount++;
            }
            pkg.addIsKeyWrappedList(((POMultiQueryPackage)pk).getIsKeyWrappedList());
            addShiftedKeyInfoIndex(initial, current, (POMultiQueryPackage)pk);
        } else {
            pkg.addPackage(pk, mapKeyType);
            pkCount = 1;
        }
        
        if (pkCount != total) {
            int errCode = 2146;
            String msg = "Internal Error. Inconsistency in key index found during optimization.";
            throw new OptimizerException(msg, errCode, PigException.BUG);
        }
        
        PODemux demux = (PODemux)to.getLeaves().get(0);
        int plCount = 0;
        PhysicalOperator root = from.getRoots().get(0);
        if (root instanceof PODemux) {
            // flattening the inner plans of the demux operator.
            // This is based on the fact that if a plan has a demux
            // operator, then it's the only operator in the plan.
            List<PhysicalPlan> pls = ((PODemux)root).getPlans();
            for (PhysicalPlan pl : pls) {
                demux.addPlan(pl);
                plCount++;
            }
        } else {
            demux.addPlan(from);
            plCount = 1;
        }
        
        if (plCount != total) {
            int errCode = 2146;
            String msg = "Internal Error. Inconsistency in key index found during optimization.";
            throw new OptimizerException(msg, errCode, PigException.BUG);
        }

        if (pkg.isSameMapKeyType()) {
            pkg.setKeyType(pk.getKeyType());
        } else {
            pkg.setKeyType(DataType.TUPLE);
        }            
    }
    
    private void addShiftedKeyInfoIndex(int index, POPackage pkg) throws OptimizerException {
        /**
         * we only do multi query optimization for single input MROpers
         * Hence originally the keyInfo would have had only index 0. As
         * we merge MROpers into parent MROpers we add entries for the
         * multiquery based index (ORed with multi query bit mask). These additions
         * would mean we have many entries in the keyInfo while really it should
         * only have one since there is only one input that the package would
         * be processing and hence only one index. So each time we add an entry
         * for a new shifted index, we should clean up keyInfo so that it has only one entry
         * - the valid entry at that point. The "value" in the keyInfo map for the new
         * addition should be the same as the "value" in the existing Entry. After
         * addition, we should remove the older entry
         */
        Map<Integer, Pair<Boolean, Map<Integer, Integer>>> keyInfo = pkg.getKeyInfo();
        byte newIndex = (byte)(index | PigNullableWritable.mqFlag);
        
        Set<Integer> existingIndices = keyInfo.keySet();
        if(existingIndices.size() != 1) {
            // we always maintain one entry in the keyinfo
            // which is the valid entry at the given stage of
            // multi query optimization
            int errCode = 2146;
            String msg = "Internal Error. Inconsistency in key index found during optimization.";
            throw new OptimizerException(msg, errCode, PigException.BUG);
        }
        int existingIndex = existingIndices.iterator().next();
        keyInfo.put(Integer.valueOf(newIndex), keyInfo.get(existingIndex));
        
        // clean up the old entry so we only keep
        // the valid entry around - if we did something wrong while
        // setting this up, we will fail at runtime which is better
        // than doing something wrong and giving incorrect results!
        if(newIndex != existingIndex) {
            keyInfo.remove(existingIndex);
        }
    }
    
    /**
     * @param initialIndex
     * @param onePastEndIndex
     * @param mpkg
     * @throws OptimizerException 
     */
    private int addShiftedKeyInfoIndex(int initialIndex, int onePastEndIndex,
            POMultiQueryPackage mpkg) throws OptimizerException {
        
        List<POPackage> pkgs = mpkg.getPackages();
        // if we have lesser pkgs than (onePastEndIndex - initialIndex)
        // its because one or more of the pkgs is a POMultiQueryPackage which
        // internally has packages.
        int numIndices = (onePastEndIndex - initialIndex);
        int end = numIndices;
        if(numIndices > pkgs.size()) {
            end = pkgs.size();
        } else if (numIndices < pkgs.size()) {
            int errCode = 2146;
            String msg = "Internal Error. Inconsistency in key index found during optimization.";
            throw new OptimizerException(msg, errCode, PigException.BUG);
        }
        int i = 0;
        int curIndex = initialIndex;
        while (i < end) {
            POPackage pkg = pkgs.get(i);
            addShiftedKeyInfoIndex(curIndex, pkg);
            curIndex++;
            i++;
        }
        return curIndex; // could be used in a caller who recursively called this function
        
    }

    private void mergeOneCombinePlanWithIndex(PhysicalPlan from,
            PhysicalPlan to, int initial, int current, byte mapKeyType) throws VisitorException {
        POPackage cpk = (POPackage)from.getRoots().get(0);
        from.remove(cpk);
        
        PODemux demux = (PODemux)to.getLeaves().get(0);
                
        POMultiQueryPackage pkg = (POMultiQueryPackage)to.getRoots().get(0);
        
        boolean isSameKeyType = pkg.isSameMapKeyType();
        
        // if current > initial + 1, it means we had
        // a split in the map of the MROper we are trying to
        // merge. In that case we would have changed the indices
        // of the POLocalRearranges in the split to be in the
        // range initial to current. To handle key, value pairs
        // coming out of those POLocalRearranges, we add
        // the Packages in the 'from' POMultiQueryPackage (in this case, 
        // it has to be a POMultiQueryPackage since we had
        // a POSplit in the map) to the 'to' POMultiQueryPackage. 
        // These Packages would have correct positions in the package 
        // list and would be able to handle the outputs from the different
        // POLocalRearranges.
        int total = current - initial;
        int pkCount = 0;
        if (cpk instanceof POMultiQueryPackage) {
            List<POPackage> pkgs = ((POMultiQueryPackage)cpk).getPackages();
            for (POPackage p : pkgs) {
                pkg.addPackage(p);
                if (!isSameKeyType) {
                    p.setKeyType(DataType.TUPLE);
                }
                pkCount++;
            }
        } else {
            pkg.addPackage(cpk);
            pkCount = 1;
        }

        pkg.setSameMapKeyType(isSameKeyType);
        
        if (pkCount != total) {
            int errCode = 2146;
            String msg = "Internal Error. Inconsistency in key index found during optimization.";
            throw new OptimizerException(msg, errCode, PigException.BUG);
        }

        // all packages should have the same key type
        if (!isSameKeyType) {
            cpk.setKeyType(DataType.TUPLE);          
        } 
        
        pkg.setKeyType(cpk.getKeyType());
        
        // See comment above for why we flatten the Packages
        // in the from plan - for the same reason, we flatten
        // the inner plans of Demux operator now.
        int plCount = 0;
        PhysicalOperator leaf = from.getLeaves().get(0);
        if (leaf instanceof PODemux) {
            List<PhysicalPlan> pls = ((PODemux)leaf).getPlans();
            for (PhysicalPlan pl : pls) {
                demux.addPlan(pl);
                POLocalRearrange lr = (POLocalRearrange)pl.getLeaves().get(0);
                try {
                    lr.setMultiQueryIndex(initial + plCount++);            
                } catch (ExecException e) {                                        
                    int errCode = 2136;
                    String msg = "Internal Error. Unable to set multi-query index for optimization.";
                    throw new OptimizerException(msg, errCode, PigException.BUG, e);
                }
                
                // change the map key type to tuple when 
                // multiple splittees have different map key types
                if (!isSameKeyType) {
                    lr.setKeyType(DataType.TUPLE);
                }
            }
        } else {
            demux.addPlan(from);
            POLocalRearrange lr = (POLocalRearrange)from.getLeaves().get(0);
            try {
                lr.setMultiQueryIndex(initial + plCount++);            
            } catch (ExecException e) {                                        
                int errCode = 2136;
                String msg = "Internal Error. Unable to set multi-query index for optimization.";
                throw new OptimizerException(msg, errCode, PigException.BUG, e);
            }
                
            // change the map key type to tuple when 
            // multiple splittees have different map key types
            if (!isSameKeyType) {
                lr.setKeyType(DataType.TUPLE);
            }
        }
        
        if (plCount != total) {
            int errCode = 2146;
            String msg = "Internal Error. Inconsistency in key index found during optimization.";
            throw new OptimizerException(msg, errCode, PigException.BUG);
        }
    }
    
    private boolean needCombiner(List<MapReduceOper> mapReducers) {
        boolean needCombiner = false;
        for (MapReduceOper mrOp : mapReducers) {
            if (!mrOp.combinePlan.isEmpty()) {
                needCombiner = true;
                break;
            }
        }
        return needCombiner;
    }
       
    private PhysicalPlan createDemuxPlan(boolean sameKeyType, boolean isCombiner) 
        throws VisitorException {
        PODemux demux = getDemux(isCombiner);
        POMultiQueryPackage pkg= getMultiQueryPackage(sameKeyType, isCombiner);
        
        PhysicalPlan pl = new PhysicalPlan();
        pl.add(pkg);
        try {
            pl.addAsLeaf(demux);
        } catch (PlanException e) {                   
            int errCode = 2137;
            String msg = "Internal Error. Unable to add demux to the plan as leaf for optimization.";
            throw new OptimizerException(msg, errCode, PigException.BUG, e);
        }
        return pl;
    }

    private void mergeAllMapReduceSplittees(List<MapReduceOper> mergeList, 
            MapReduceOper splitter, POSplit splitOp) throws VisitorException {
       
        boolean sameKeyType = hasSameMapKeyType(mergeList);
        
        log.debug("Splittees have the same key type: " + sameKeyType);
        
        // create a new reduce plan that will be the container
        // for the multiple reducer plans of the MROpers in the mergeList
        PhysicalPlan redPl = createDemuxPlan(sameKeyType, false);
        
        // create a new combine plan that will be the container
        // for the multiple combiner plans of the MROpers in the mergeList                
        PhysicalPlan comPl = needCombiner(mergeList) ? 
                createDemuxPlan(sameKeyType, true) : null;

        log.debug("Splittees have combiner: " + (comPl != null));
                
        int index = 0;             
        
        for (MapReduceOper mrOp : mergeList) {

            // merge the map plan - this will recursively
            // set index on all POLocalRearranges encountered
            // including ones in inner plans of any POSplit
            // operators. Hence the index returned could be
            // > index + 1
            int incIndex = mergeOneMapPlanWithIndex(
                    mrOp.mapPlan, splitOp, index, sameKeyType);
                        
            // merge the combiner plan
            if (comPl != null) {
                if (!mrOp.combinePlan.isEmpty()) {                    
                    mergeOneCombinePlanWithIndex(
                            mrOp.combinePlan, comPl, index, incIndex, mrOp.mapKeyType);
                } else {         
                    int errCode = 2141;
                    String msg = "Internal Error. Cannot merge non-combiner with combiners for optimization.";
                    throw new OptimizerException(msg, errCode, PigException.BUG);
                }
            }
            
            // merge the reducer plan
            mergeOneReducePlanWithIndex(
                    mrOp.reducePlan, redPl, index, incIndex, mrOp.mapKeyType);
           
            index = incIndex;
            
            log.info("Merged MR job " + mrOp.getOperatorKey().getId() 
                    + " into MR job " + splitter.getOperatorKey().getId());
        }

        PhysicalPlan splitterPl = splitter.mapPlan;
        PhysicalOperator leaf = splitterPl.getLeaves().get(0);
        PhysicalOperator storeOp = splitterPl.getLeaves().get(0);
        List<PhysicalOperator> storePreds = splitterPl.getPredecessors(storeOp);  
 
        // replace store operator in the splitter with split operator
        if (leaf instanceof POStore) {                            
            splitOp.setInputs(storePreds);
            try {
                splitterPl.replace(storeOp, splitOp);;
            } catch (PlanException e) {                   
                int errCode = 2132;
                String msg = "Internal Error. Unable to replace store with split operator for optimization.";
                throw new OptimizerException(msg, errCode, PigException.BUG, e);
            }
        }     
        
        splitter.setMapDone(true);
        splitter.reducePlan = redPl;
        splitter.setReduceDone(true);
           
        if (comPl != null) {
            splitter.combinePlan = comPl;        
        }
        
        for (MapReduceOper mrOp : mergeList) {
            removeAndReconnect(mrOp, splitter);
        }
       
        splitter.mapKeyType = sameKeyType ?
                mergeList.get(0).mapKeyType : DataType.TUPLE;         
                
        log.info("Requested parallelism of splitter: " 
                + splitter.getRequestedParallelism());               
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
        
        splitOp.addPlan(pl);
                              
        splitter.setMapDone(true);
        splitter.reducePlan = mapReduce.reducePlan;
        splitter.setReduceDone(true);
        splitter.combinePlan = mapReduce.combinePlan;
        splitter.customPartitioner = mapReduce.customPartitioner;
                
        // replace store operator in the splitter with split operator
        if (leaf instanceof POStore) {                            
            splitOp.setInputs(storePreds);
            try {
                splitterPl.replace(storeOp, splitOp);;
            } catch (PlanException e) {
                int errCode = 2132;
                String msg = "Internal Error. Unable to replace store with split operator for optimization.";
                throw new OptimizerException(msg, errCode, PigException.BUG, e);
            }  
        }
        
        removeAndReconnect(mapReduce, splitter);           
    }
    
    /**
     * Removes the specified MR operator from the plan after the merge. 
     * Connects its predecessors and successors to the merged MR operator
     * 
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
                    int errCode = 2133;
                    String msg = "Internal Error. Unable to connect map plan with successors for optimization.";
                    throw new OptimizerException(msg, errCode, PigException.BUG, e);
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
                    int errCode = 2134;
                    String msg = "Internal Error. Unable to connect map plan with predecessors for optimization.";
                    throw new OptimizerException(msg, errCode, PigException.BUG, e);
                }
            }
        }     
        
        mergeMROperProperties(mr, newMR);
    }
    
    private void mergeMROperProperties(MapReduceOper from, MapReduceOper to) {

        if (from.isEndOfAllInputSetInMap()) {
            to.setEndOfAllInputInMap(true);
        }

        if (from.isEndOfAllInputSetInReduce()) {
            to.setEndOfAllInputInReduce(true);
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
        return (pl.getRoots().size() == 1);
    }
    
    private boolean isSinglePredecessor(MapReduceOper mr) {
        return (getPlan().getPredecessors(mr).size() == 1);
    }
    
    private POSplit getSplit(){
        return new POSplit(new OperatorKey(scope, nig.getNextNodeId(scope)));
    } 
 
    private MapReduceOper getMROper(){
        return new MapReduceOper(new OperatorKey(scope, nig.getNextNodeId(scope)));
    } 
   
    private POStore getStore(){
        return new POStore(new OperatorKey(scope, nig.getNextNodeId(scope)));
    } 
     
    private PODemux getDemux(boolean inCombiner){
        PODemux demux = new PODemux(new OperatorKey(scope, nig.getNextNodeId(scope)));
        demux.setInCombiner(inCombiner);
        return demux;
    } 
    
    private POMultiQueryPackage getMultiQueryPackage(boolean sameMapKeyType, boolean inCombiner){
        POMultiQueryPackage pkg =  
            new POMultiQueryPackage(new OperatorKey(scope, nig.getNextNodeId(scope)));
        pkg.setInCombiner(inCombiner);
        pkg.setSameMapKeyType(sameMapKeyType);
        return pkg;
    }   
}
