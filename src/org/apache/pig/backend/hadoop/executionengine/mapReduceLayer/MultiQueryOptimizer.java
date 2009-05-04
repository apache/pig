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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROpPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.PODemux;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POMultiQueryPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSplit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.ReverseDependencyOrderWalker;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.impl.plan.optimizer.OptimizerException;
import org.apache.pig.PigException;


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
    
    MultiQueryOptimizer(MROperPlan plan) {
        super(plan, new ReverseDependencyOrderWalker<MapReduceOper, MROperPlan>(plan));
        nig = NodeIdGenerator.getGenerator();
        List<MapReduceOper> roots = plan.getRoots();
        scope = roots.get(0).getOperatorKey().getScope();
        
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
            
            log.info("Merged the only map-only splittee.");
            
            return;              
        }
        
        // case 2: exactly one splittee and it has reducer
        if (isMapOnly(mr) && mapReducers.size() == 1 
                && mappers.size() == 0 && multiLoadMROpers.size() == 0) {            
            mergeOnlyMapReduceSplittee(mapReducers.get(0), mr);
            
            log.info("Merged the only map-reduce splittee.");
            
            return;
        } 
        
        int numSplittees = successors.size();
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
              
        // case 4: multiple splittees and at least one of them has reducer  
        if (isMapOnly(mr) && mapReducers.size() > 0) {
                         
            PhysicalOperator leaf = splitterPl.getLeaves().get(0);
                                                            
            splitOp = (leaf instanceof POStore) ? getSplit() : (POSplit)leaf;
                    
            int n = mergeMapReduceSplittees(mapReducers, mr, splitOp);  
            
            log.info("Merged " + n + " map-reduce splittees.");
            
            numMerges += n;      
        }
       
        // finally, add original store to the split operator 
        // if there is splittee that hasn't been merged
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
        
        log.info("Merged " + numMerges + " out of total " 
                + numSplittees + " splittees.");
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
       
    private List<MapReduceOper> getMergeList(List<MapReduceOper> mapReducers) {
        List<MapReduceOper> mergeNoCmbList = new ArrayList<MapReduceOper>();
        List<MapReduceOper> mergeCmbList = new ArrayList<MapReduceOper>();

        for (MapReduceOper mrOp : mapReducers) {
            if (isSplitteeMergeable(mrOp)) {
                if (mrOp.combinePlan.isEmpty()) {
                    mergeNoCmbList.add(mrOp);
                } else {
                    mergeCmbList.add(mrOp);
                } 
            }           
        }     
        return (mergeNoCmbList.size() > mergeCmbList.size()) ?
                mergeNoCmbList : mergeCmbList;
    }
    
    private int mergeMapReduceSplittees(List<MapReduceOper> mapReducers, 
            MapReduceOper splitter, POSplit splitOp) throws VisitorException {
                
        List<MapReduceOper> mergeList = getMergeList(mapReducers);
    
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
    
    private int setIndexOnLRInSplit(int initial, POSplit splitOp)
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
            } else if (leaf instanceof POSplit) {
                POSplit spl = (POSplit)leaf;
                index = setIndexOnLRInSplit(index, spl);
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
            POSplit spl = (POSplit)leaf;
            curIndex = setIndexOnLRInSplit(index, spl);
        }
                    
        splitOp.addPlan(pl);
               
        return curIndex;
    }
    
    private int setBaseIndexOnDemux(int initial, PODemux demuxOp) 
            throws VisitorException {
        int index = initial;
        demuxOp.setBaseIndex(index++);

        List<PhysicalPlan> pls = demuxOp.getPlans();
        for (PhysicalPlan pl : pls) {
            PhysicalOperator leaf = pl.getLeaves().get(0);
            if (leaf instanceof POLocalRearrange) {
                POLocalRearrange lr = (POLocalRearrange)leaf;
                try {                    
                    lr.setMultiQueryIndex(initial + lr.getIndex());                   
                } catch (ExecException e) {                   
                    int errCode = 2136;
                    String msg = "Internal Error. Unable to set multi-query index for optimization.";
                    throw new OptimizerException(msg, errCode, PigException.BUG, e);                         
                }   
            }
            PhysicalOperator root = pl.getRoots().get(0);
            if (root instanceof PODemux) {                
                index = setBaseIndexOnDemux(index, (PODemux)root);
            } else {
                index++;
            }
        }
        return index;
    }
    
    private int setBaseIndexOnPackage(int initial, POMultiQueryPackage pkgOp) {
        int index = initial;
        pkgOp.setBaseIndex(index++);
        
        List<POPackage> pkgs = pkgOp.getPackages();
        for (POPackage pkg : pkgs) {            
            if (pkg instanceof POMultiQueryPackage) {
                POMultiQueryPackage mpkg = (POMultiQueryPackage)pkg;
                index = setBaseIndexOnPackage(index, mpkg);
            } else {
                index++;
            }
        }
        return index;
    }
    
    private void mergeOneReducePlanWithIndex(PhysicalPlan from, 
            PhysicalPlan to, int initial, int current) throws VisitorException {                    
        POPackage pk = (POPackage)from.getRoots().get(0);
        from.remove(pk);
 
        // XXX the index of the original keyInfo map is always 0,
        // we need to shift the index so that the lookups works
        // with the new indexed key       
        Map<Integer, Pair<Boolean, Map<Integer, Integer>>> keyInfo = pk.getKeyInfo();
        if (keyInfo != null && keyInfo.size() > 0) {      
            byte b = (byte)(initial | 0x80);
            keyInfo.put(new Integer(b), keyInfo.get(0));
        }     
        
        if (pk instanceof POMultiQueryPackage) {
            POMultiQueryPackage mpkg = (POMultiQueryPackage)pk;
            setBaseIndexOnPackage(initial, mpkg);
        }
                                
        PhysicalOperator root = from.getRoots().get(0);
        if (root instanceof PODemux) {
            PODemux demux = (PODemux)root;
            setBaseIndexOnDemux(initial, demux);
        }
                    
        POMultiQueryPackage pkg = (POMultiQueryPackage)to.getRoots().get(0);        
        for (int i=initial; i<current; i++) {
            pkg.addPackage(pk);
        }
        
        PODemux demux = (PODemux)to.getLeaves().get(0);
        for (int i=initial; i<current; i++) {
            demux.addPlan(from);
        }
               
        if (demux.isSameMapKeyType()) {
            pkg.setKeyType(pk.getKeyType());
        } else {
            pkg.setKeyType(DataType.TUPLE);
        }                
    }
    
    private void mergeOneCombinePlanWithIndex(PhysicalPlan from,
            PhysicalPlan to, int initial, int current) throws VisitorException {
        POPackage cpk = (POPackage)from.getRoots().get(0);
        from.remove(cpk);
       
        if (cpk instanceof POMultiQueryPackage) {
            POMultiQueryPackage mpkg = (POMultiQueryPackage)cpk;
            setBaseIndexOnPackage(initial, mpkg);
        }
        
        PODemux demux = (PODemux)to.getLeaves().get(0);
        
        boolean isSameKeyType = demux.isSameMapKeyType();
        
        PhysicalOperator leaf = from.getLeaves().get(0);
        if (leaf instanceof POLocalRearrange) {
            POLocalRearrange clr = (POLocalRearrange)leaf;
            try {
                clr.setMultiQueryIndex(initial);            
            } catch (ExecException e) {                                        
                int errCode = 2136;
                String msg = "Internal Error. Unable to set multi-query index for optimization.";
                throw new OptimizerException(msg, errCode, PigException.BUG, e);
            }
            
            // change the map key type to tuple when 
            // multiple splittees have different map key types
            if (!isSameKeyType) {
                clr.setKeyType(DataType.TUPLE);
            }
        } else if (leaf instanceof PODemux) {
            PODemux locDemux = (PODemux)leaf;
            setBaseIndexOnDemux(initial, locDemux);
        } 
       
        POMultiQueryPackage pkg = (POMultiQueryPackage)to.getRoots().get(0);     
        for (int i=initial; i<current; i++) {
            pkg.addPackage(cpk);
        }
        
        // all packages should have the same key type
        if (!isSameKeyType) {
            cpk.setKeyType(DataType.TUPLE);          
        } 
        
        pkg.setKeyType(cpk.getKeyType());
                
        for (int i=initial; i<current; i++) {
            demux.addPlan(from);
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
        PODemux demux = getDemux(sameKeyType, isCombiner);
        POMultiQueryPackage pkg= getMultiQueryPackage();
        
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
        
        log.info("Splittees have the same key type: " + sameKeyType);
        
        // create a new reduce plan that will be the container
        // for the multiple reducer plans of the MROpers in the mergeList
        PhysicalPlan redPl = createDemuxPlan(sameKeyType, false);
        
        // create a new combine plan that will be the container
        // for the multiple combiner plans of the MROpers in the mergeList                
        PhysicalPlan comPl = needCombiner(mergeList) ? 
                createDemuxPlan(sameKeyType, true) : null;

        log.info("Splittees have combiner: " + (comPl != null));
                
        int index = 0;             
        
        for (MapReduceOper mrOp : mergeList) {

            // merge the map plan            
            int incIndex = mergeOneMapPlanWithIndex(
                    mrOp.mapPlan, splitOp, index, sameKeyType);
                       
            // merge the combiner plan
            if (comPl != null) {
                if (!mrOp.combinePlan.isEmpty()) {                    
                    mergeOneCombinePlanWithIndex(
                            mrOp.combinePlan, comPl, index, incIndex);
                } else {         
                    int errCode = 2141;
                    String msg = "Internal Error. Cannot merge non-combiner with combiners for optimization.";
                    throw new OptimizerException(msg, errCode, PigException.BUG);
                }
            }

            // merge the reducer plan
            mergeOneReducePlanWithIndex(
                    mrOp.reducePlan, redPl, index, incIndex);
           
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
        return (pl.getRoots().size() == 1);
    }
    
    private POSplit getSplit(){
        return new POSplit(new OperatorKey(scope, nig.getNextNodeId(scope)));
    } 
    
    private PODemux getDemux(boolean sameMapKeyType, boolean inCombiner){
        PODemux demux = new PODemux(new OperatorKey(scope, nig.getNextNodeId(scope)));
        demux.setSameMapKeyType(sameMapKeyType);
        demux.setInCombiner(inCombiner);
        return demux;
    } 
    
    private POMultiQueryPackage getMultiQueryPackage(){
        return new POMultiQueryPackage(new OperatorKey(scope, nig.getNextNodeId(scope)));
    }  
}
