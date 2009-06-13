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
import org.apache.pig.impl.io.PigNullableWritable;
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
            curIndex = setIndexOnLRInSplit(index, spl);
        }
                    
        splitOp.addPlan(pl);
        
        // return the updated index after setting index
        // on all POLocalRearranges including ones
        // in inner plans of any POSplit operators
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
                    // if the baseindex is set on the demux, then
                    // POLocalRearranges in its inner plan should really
                    // be sending an index out by adding the base index
                    // This is because we would be replicating the demux
                    // as many times as there are inner plans in the demux
                    // hence the index coming out of POLocalRearranges
                    // needs to be adjusted accordingly
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
            PhysicalPlan to, int initial, int current, byte mapKeyType) throws VisitorException {                    
        POPackage pk = (POPackage)from.getRoots().get(0);
        from.remove(pk);
 
        if(!(pk instanceof POMultiQueryPackage)){
            // XXX the index of the original keyInfo map is always 0,
            // we need to shift the index so that the lookups works
            // with the new indexed key
            addShiftedKeyInfoIndex(initial, pk); 
        }
        
        if (pk instanceof POMultiQueryPackage) {
            POMultiQueryPackage mpkg = (POMultiQueryPackage)pk;
            setBaseIndexOnPackage(initial, mpkg);
            // we should update the keyinfo map of the 
            // POPackage objects in the POMultiQueryPackage to
            // have the shifted index - The index now will be
            // starting from "initial" going up to "current"
            // ORed with the multi query bit mask
            int retIndex = addShiftedKeyInfoIndex(initial, current, mpkg);
            if(retIndex != current) {
                int errCode = 2146;
                String msg = "Internal Error. Inconsistency in key index found during optimization.";
                throw new OptimizerException(msg, errCode, PigException.BUG);
            }
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
            demux.addPlan(from, mapKeyType);
        }
               
        if (demux.isSameMapKeyType()) {
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
        keyInfo.put(new Integer(newIndex), keyInfo.get(existingIndex));
        
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
        // recursively iterate over the packages in the
        // POMultiQueryPackage adding a shifted keyInfoIndex entry
        // in the packages in order going from initialIndex upto
        // onePastEndIndex (exclusive) flattening out any nested
        // packages in nested POMultiqueryPackages as we traverse
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
            if(pkg instanceof POMultiQueryPackage) {
                curIndex = addShiftedKeyInfoIndex(curIndex, onePastEndIndex, (POMultiQueryPackage)pkg);
            } else {
                addShiftedKeyInfoIndex(curIndex, pkg);
                curIndex++;
            }
            i++;
        }
        return curIndex; // could be used in a caller who recursively called this function
        
    }

    private void mergeOneCombinePlanWithIndex(PhysicalPlan from,
            PhysicalPlan to, int initial, int current, byte mapKeyType) throws VisitorException {
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
        
        // if current > initial + 1, it means we had
        // a split in the map of the MROper we are trying to
        // merge. In that case we would have changed the indices
        // of the POLocalRearranges in the split to be in the
        // range initial to current. To handle key, value pairs
        // coming out of those POLocalRearranges, we replicate
        // the Package as many times (in this case, the package
        // would have to be a POMultiQueryPackage since we had
        // a POSplit in the map). That Package would have a baseindex
        // correctly set (in the beginning of this method) and would
        // be able to handle the outputs from the different
        // POLocalRearranges.
        for (int i=initial; i<current; i++) {
            pkg.addPackage(cpk);
        }
        
        // all packages should have the same key type
        if (!isSameKeyType) {
            cpk.setKeyType(DataType.TUPLE);          
        } 
        
        pkg.setKeyType(cpk.getKeyType());
        
        // See comment above for why we replicated the Package
        // in the from plan - for the same reason, we replicate
        // the Demux operators now.
        for (int i=initial; i<current; i++) {
            demux.addPlan(from, mapKeyType);
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
            
            // In the combine and reduce plans the Demux and POMultiQueryPackage
            // operators' baseIndex is set whenever the incIndex above is > index + 1
            // What does this 'baseIndex' mean - here is an attempt to explain it:
            // Consider a map - reduce plan layout as shown below (the comments
            // apply even if a combine plan was present) - An explanation of the "index"
            // and "baseIndex" fields follows:
            // The numbers in parenthesis () are "index" values - Note that in multiquery
            // optimizations these indices are actually ORed with a bitmask (0x80) on the
            // map side in the LocalRearrange. The POMultiQueryPackage and PODemux operators
            // then remove this bitmask to restore the original index values. In the commentary
            // below, indices will be referred to without this bitmask - the bitmask is only
            // to identify the multiquery optimization during comparsion - for details see the comments
            // in POLocalRearrange.setIndex().
            // The numbers in brackets [] are "baseIndex" values. These baseIndex values are
            // used by POMultiQueryPackage and PODemux to calculate the an arrayList index which
            // they use to pick the right package or inner plan respectively. All this is needed
            // since on the map side the indices are assigned after flattening all POLocalRearranges
            // including those nested in Splits into one flat space (as can be noticed by the
            // numbering of the indices under the split in the example below). The optimizer then
            // duplicates the POMultiQueryPackage and Demux inner plan corresponding to the split
            // in the reduce plan (the entities with * in the figure below). Now if a key with index '1'
            // is emitted, it goes to the first POMultiQueryPackage in the reduce plan below, which 
            // then picks the second package in its arraylist of packages which is a 
            // POMultiQueryPackage (the first with a * below). This POMultiQueryPackage then picks 
            // the first package in its list (it arrives at this arraylist index of 0 by subtracting
            // the baseIndex (1) from the index coming in (1)). So the record emitted by LocalRearrange(1)
            // reaches the right package. Likewise, if LocalRearrange(2) emits a key,value they would have
            // an index 2. The first POMultiQueryPackage (with baseIndex 0 below) would pick the 3rd package
            // (arraylist index 2 arrived at by doing index - baseIndex which is 2 - 0). This is the
            // duplicated POMultiQueryPackage with baseIndex 1. This would inturn pick the second package
            // in its arraylist (arraylist index 1 arrived at by doing index - baseIndex which is 2 - 1)
            // The idea is that through duplication we make it easier to determine which POPackage to pick.
            // The exact same logic is used by PODemux to pick the right inner plan from its arraylist of
            // inner plans.
            
            // The arrows in the figure below show the correspondence between the different operators
            // and plans .i.e the end of an arrow points to the operator or plan which consumes
            // data coming from the root of the arrow
            
            // If you look at the layout below column-wise, each "column" is a MROper
            // which is being merged into the parent MROper - the Split represents a
            // MROper which inside of it has 2 MROpers merged in.
            // A star (*) next to an operator means it is the same object as the
            // other operator with a star(*) - Essentially the same object reference
            // is copied twice.
            // LocalRearrange(0)           Split                           LocalRearrange(3)
            //     |                       /     \                               |
            //     |         LocalRearrange(1)  LocalRearrange(2)                |
            //     |             |     MAP PLAN              |                   |
            // ----|-------------|---------------------------|-------------------|--------------------
            //     |             |     REDUCE PLAN           |                   |
            //     |             |   POMultiQueryPackage[0]  |                   |
            //     V             V         | contains        V                   V
            // [ POPackage, POMultiQueryPackage[1]*,POMultiQueryPackage[1]*,   POPackage]
            //      |           /    \               /      \                    |
            //      |       POPackage POPackage  POPackage  POPackage            |
            //      |              \                          |                  |
            //      |               \   Demux[0]              |                  |
            //      V                V    | contains          V                  V
            //  [ planOfRedOperators,planWithDemux*, planWithDemux*,      planOfRedOperators]             
            //                       /    |              |        \
            //                      /   Demux[1]        Demux[1]   \
            //                     V      |               |         V
            //            [planOfRedOps,planOfRedOps][planOfRedOps,planOfRedOps]
            // 
            
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
