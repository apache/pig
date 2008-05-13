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
package org.apache.pig.impl.mapReduceLayer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.builtin.BinStorage;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.logicalLayer.OperatorKey;
import org.apache.pig.impl.logicalLayer.parser.NodeIdGenerator;
import org.apache.pig.impl.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.impl.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.impl.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.physicalLayer.topLevelOperators.POFilter;
import org.apache.pig.impl.physicalLayer.topLevelOperators.POForEach;
import org.apache.pig.impl.physicalLayer.topLevelOperators.POGlobalRearrange;
import org.apache.pig.impl.physicalLayer.topLevelOperators.POLoad;
import org.apache.pig.impl.physicalLayer.topLevelOperators.POLocalRearrange;
import org.apache.pig.impl.physicalLayer.topLevelOperators.POPackage;
import org.apache.pig.impl.physicalLayer.topLevelOperators.POSplit;
import org.apache.pig.impl.physicalLayer.topLevelOperators.POStore;
import org.apache.pig.impl.physicalLayer.topLevelOperators.POUnion;
import org.apache.pig.impl.physicalLayer.topLevelOperators.PhysicalOperator;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.Operator;
import org.apache.pig.impl.plan.OperatorPlan;

/**
 * The compiler that compiles a given physical plan
 * into a DAG of MapReduce operators which can then 
 * be converted into the JobControl structure.
 * 
 * Is implemented as a visitor of the PhysicalPlan it
 * is compiling.
 * 
 * Currently supports all operators except the MR Sort
 * operator 
 * 
 * Uses a predecessor based depth first traversal. 
 * To compile an operator, first compiles
 * the predecessors into MapReduce Operators and tries to
 * merge the current operator into one of them. The goal
 * being to keep the number of MROpers to a minimum.
 * 
 * It also merges multiple Map jobs, created by compiling
 * the inputs individually, into a single job.
 * 
 * Only in case of blocking operators and splits, a new 
 * MapReduce operator is started using a store-load combination
 * to connect the two operators. Whenever this happens
 * care is taken to add the MROper into the MRPlan and connect it
 * appropriately.
 *
 */
public class MRCompiler extends PhyPlanVisitor<PhysicalOperator, PhysicalPlan<PhysicalOperator>> {
    
    private Log log = LogFactory.getLog(getClass());
    
    PigContext pigContext;
    
    //The plan that is being compiled
    PhysicalPlan<PhysicalOperator> plan;

    //The plan of MapReduce Operators
    MROperPlan MRPlan;
    
    //The current MapReduce Operator
    //that is being compiled
    MapReduceOper curMROp;
    
    //The output of compiling the inputs
    MapReduceOper[] compiledInputs = null;
    
    //The split operators seen till now. If not
    //maintained they will haunt you.
    //During the traversal a split is the only
    //operator that can be revisited from a different
    //path. So this map stores the split job. So 
    //whenever we hit the split, we create a new MROper
    //and connect the split job using load-store and also
    //in the MRPlan
    Map<OperatorKey, MapReduceOper> splitsSeen;
    
    NodeIdGenerator nig;

    private String scope;
    
    private Random r;
    
    public MRCompiler(PhysicalPlan<PhysicalOperator> plan) {
        this(plan,null);
    }
    
    public MRCompiler(PhysicalPlan<PhysicalOperator> plan,
            PigContext pigContext) {
        super(plan, new DepthFirstWalker<PhysicalOperator, PhysicalPlan<PhysicalOperator>>(plan));
        this.plan = plan;
        this.pigContext = pigContext;
        splitsSeen = new HashMap<OperatorKey, MapReduceOper>();
        MRPlan = new MROperPlan();
        nig = NodeIdGenerator.getGenerator();
        scope = "MRCompiler";
        r = new Random(1331);
        FileLocalizer.setR(r);
    }
    
    /**
     * Used to get the compiled plan
     * @return
     */
    public MROperPlan getMRPlan() {
        return MRPlan;
    }
    
    /**
     * Used to get the plan that was compiled
     * @return
     */
    public PhysicalPlan<PhysicalOperator> getPlan() {
        return plan;
    }
    
    /**
     * The front-end method that the user calls to compile
     * the plan. Assumes that all submitted plans have a Store
     * operators as the leaf.
     * @return
     * @throws IOException
     * @throws PlanException
     * @throws VisitorException
     */
    public MROperPlan compile() throws IOException, PlanException, VisitorException {
        List<PhysicalOperator> leaves = plan.getLeaves();
        /*for (PhysicalOperator operator : leaves) {
            compile(operator);
            if (!curMROp.isMapDone()) {
                curMROp.setMapDone(true);
            } else if (!curMROp.isReduceDone()) {
                curMROp.setReduceDone(true);
            }
        }*/
        POStore store = (POStore)leaves.get(0);
        compile(store);
        
        return MRPlan;
    }
    
    /**
     * Compiles the plan below op into a MapReduce Operator
     * and stores it in curMROp.
     * @param op
     * @throws IOException
     * @throws PlanException
     * @throws VisitorException
     */
    private void compile(PhysicalOperator op) throws IOException,
    PlanException, VisitorException {
        //An artifact of the Visitor. Need to save
        //this so that it is not overwritten.
        MapReduceOper[] prevCompInp = compiledInputs;
        
        //Compile each predecessor into the MROper and 
        //store them away so that we can use them for compiling
        //op.
        List<PhysicalOperator> predecessors = plan.getPredecessors(op);
        if (predecessors != null && predecessors.size() > 0) {
            Collections.sort(predecessors);
            compiledInputs = new MapReduceOper[predecessors.size()];
            int i = -1;
            for (PhysicalOperator pred : predecessors) {
                if(pred instanceof POSplit && splitsSeen.containsKey(pred.getOperatorKey())){
                    compiledInputs[++i] = startNew(((POSplit)pred).getSplitStore(), splitsSeen.get(pred.getOperatorKey()));
                    continue;
                }
                compile(pred);
                compiledInputs[++i] = curMROp;
            }
        } else {
            //No predecessors. Mostly a load. But this is where
            //we start. We create a new MROp and add its first
            //operator op. Also this should be added to the MRPlan.
            curMROp = getMROp();
            curMROp.mapPlan.add(op);
            MRPlan.add(curMROp);
            return;
        }
        
        //Now we have the inputs compiled. Do something
        //with the input oper op.
        op.visit(this);
        compiledInputs = prevCompInp;
    }
    
    private MapReduceOper getMROp(){
        return new MapReduceOper(new OperatorKey(scope,nig.getNextNodeId(scope)));
    }
    
    private POLoad getLoad(){
        POLoad ld = new POLoad(new OperatorKey(scope,nig.getNextNodeId(scope)));
        ld.setPc(pigContext);
        return ld;
    }
    
    private POStore getStore(){
        POStore st = new POStore(new OperatorKey(scope,nig.getNextNodeId(scope)));
        st.setPc(pigContext);
        return st;
    }
    
    /**
     * A map MROper is an MROper whose map plan is still open
     * for taking more non-blocking operators.
     * A reduce MROper is an MROper whose map plan is done but
     * the reduce plan is open for taking more non-blocking opers.
     * 
     * Used for compiling non-blocking operators. The logic here
     * is simple. If there is a single input, just push the operator
     * into whichever phase is open. Otherwise, we merge the compiled
     * inputs into a list of MROpers where the first oper is the merged
     * oper consisting of all map MROpers and the rest are reduce MROpers
     * as reduce plans can't be merged.
     * Then we add the input oper op into the merged map MROper's map plan
     * as a leaf and connect the reduce MROpers using store-load combinations
     * to the input operator which is the leaf. Also care is taken to 
     * connect the MROpers according to the dependencies.
     * @param op
     * @throws PlanException
     * @throws IOException 
     * @throws IOException
     */
    private void nonBlocking(PhysicalOperator op) throws PlanException, IOException{
        if (compiledInputs.length == 1) {
            //For speed
            MapReduceOper mro = compiledInputs[0];
            if (!mro.isMapDone()) {
                mro.mapPlan.addAsLeaf(op);
            } else if (mro.isMapDone() && !mro.isReduceDone()) {
                mro.reducePlan.addAsLeaf(op);
            } else {
                log.warn("Both map and reduce phases have been done. This is unexpected while compiling!");
            }
            curMROp = mro;
        } else {
            List<MapReduceOper> mergedPlans = merge(compiledInputs);
            
            //The first MROper is always the merged map MROper
            MapReduceOper mro = mergedPlans.remove(0);
            //Push the input operator into the merged map MROper
            mro.mapPlan.addAsLeaf(op);
            
            //Connect all the reduce MROpers
            if(mergedPlans.size()>0)
                connRedOper(mergedPlans, mro);
            
            //return the compiled MROper
            curMROp = mro;
        }
    }
    
    /**
     * Used for compiling blocking operators. If there is a single input
     * and its map phase is still open, then close it so that further
     * operators can be compiled into the reduce phase. If its reduce phase
     * is open, add a store and close it. Start a new map MROper into which
     * further operators can be compiled into. 
     * 
     * If there are multiple inputs, the logic 
     * is to merge all map MROpers into one map MROper and retain
     * the reduce MROpers. Since the operator is blocking, it has
     * to be a Global Rerrange at least now. This operator need not
     * be inserted into our plan as it is implemented by hadoop.
     * But this creates the map-reduce boundary. So the merged map MROper
     * is closed and its reduce phase is started. Depending on the number
     * of reduce MROpers and the number of pipelines in the map MRoper
     * a Union operator is inserted whenever necessary. This also leads to the 
     * possibility of empty map plans. So have to be careful while handling
     * it in the PigMapReduce class. If there are no map
     * plans, then a new one is created as a side effect of the merge
     * process. If there are no reduce MROpers, and only a single pipeline
     * in the map, then no union oper is added. Otherwise a Union oper is 
     * added to the merged map MROper to which all the reduce MROpers 
     * are connected by store-load combinations. Care is taken
     * to connect the MROpers in the MRPlan.  
     * @param op
     * @throws IOException
     * @throws PlanException
     */
    private void blocking(PhysicalOperator op) throws IOException, PlanException{
        if(compiledInputs.length==1){
            MapReduceOper mro = compiledInputs[0];
            if (!mro.isMapDone()) {
                mro.setMapDoneSingle(true);
                curMROp = mro;
            }
            else if(mro.isMapDone() && !mro.isReduceDone()){
                FileSpec fSpec = getTempFileSpec();
                
                POStore st = getStore();
                st.setSFile(fSpec);
                mro.reducePlan.addAsLeaf(st);
                mro.setReduceDone(true);
                curMROp = startNew(fSpec, mro);
                curMROp.setMapDone(true);
            }
        }
        else{
            List<MapReduceOper> mergedPlans = merge(compiledInputs);
            MapReduceOper mro = mergedPlans.remove(0);
            
            if(mergedPlans.size()>0)
                mro.setMapDoneMultiple(true);
            else
                mro.setMapDoneSingle(true);

            // Connect all the reduce MROpers
            if(mergedPlans.size()>0)
                connRedOper(mergedPlans, mro);
            curMROp = mro;
        }
    }
    
    /**
     * Connect the reduce MROpers to the leaf node in the map MROper mro
     * by adding appropriate loads
     * @param mergedPlans - The list of reduce MROpers
     * @param mro - The map MROper
     * @throws IOException 
     * @throws IOException
     */
    private void connRedOper(List<MapReduceOper> mergedPlans, MapReduceOper mro) throws PlanException, IOException{
        PhysicalOperator leaf = null;
        List<PhysicalOperator> leaves = mro.mapPlan.getLeaves();
        if(leaves!=null && leaves.size()>0)
            leaf = leaves.get(0);

        for (MapReduceOper mmro : mergedPlans) {
            mmro.setReduceDone(true);
            FileSpec fileSpec = getTempFileSpec();
            POLoad ld = getLoad();
            ld.setLFile(fileSpec);
            POStore str = getStore();
            str.setSFile(fileSpec);
            mmro.reducePlan.addAsLeaf(str);
            mro.mapPlan.add(ld);
            if(leaf!=null)
                mro.mapPlan.connect(ld, leaf);
            MRPlan.connect(mmro, mro);
        }
    }
    
    /**
     * Used to compile a split operator. The logic is to
     * close the split job by replacing the split oper by
     * a store and creating a new Map MRoper and return
     * that as the current MROper to which other operators
     * would be compiled into. The new MROper would be connected
     * to the split job by load-store. Also add the split oper 
     * to the splitsSeen map.
     * @param op
     * @throws IOException
     * @throws PlanException 
     */
    private void split(POSplit op) throws PlanException{
        MapReduceOper mro = compiledInputs[0];
        FileSpec fSpec = op.getSplitStore();
        POStore str = getStore();
        str.setSFile(fSpec);
        if (!mro.isMapDone()) {
            mro.mapPlan.addAsLeaf(str);
            mro.setMapDoneSingle(true);
        } else if (mro.isMapDone() && !mro.isReduceDone()) {
            mro.reducePlan.addAsLeaf(str);
            mro.setReduceDone(true);
        } else {
            log.warn("Both map and reduce phases have been done. This is unexpected while compiling!");
        }
        splitsSeen.put(op.getOperatorKey(), mro);
        curMROp = startNew(fSpec, mro);
    }
    
    /**
     * Starts a new MRoper and connects it to the old
     * one by load-store. The assumption is that the 
     * store is already inserted into the old MROper.
     * @param fSpec
     * @param old
     * @return
     * @throws IOException
     * @throws PlanException 
     */
    private MapReduceOper startNew(FileSpec fSpec, MapReduceOper old) throws PlanException{
        POLoad ld = getLoad();
        ld.setLFile(fSpec);
        MapReduceOper ret = getMROp();
        ret.mapPlan.add(ld);
        MRPlan.add(ret);
        MRPlan.connect(old, ret);
        return ret;
    }
    
    /**
     * Returns a temporary DFS Path
     * @return
     * @throws IOException
     */
    private FileSpec getTempFileSpec() throws IOException {
        return new FileSpec(FileLocalizer.getTemporaryPath(null, pigContext).toString(),
                BinStorage.class.getName());
    }
    
    /**
     * Merges the map MROpers in the compiledInputs into a single
     * merged map MRoper and returns a List with the merged map MROper
     * as the first oper and the rest being reduce MROpers.
     * 
     * Care is taken to remove the map MROpers that are merged from the
     * MRPlan and their connections moved over to the merged map MROper.
     * 
     * Merge is implemented as a sequence of binary merges.
     * merge(PhyPlan finPlan, List<PhyPlan> lst) := finPlan,merge(p) foreach p in lst 
     *   
     * @param compiledInputs
     * @return
     * @throws PlanException
     * @throws IOException
     */
    private List<MapReduceOper> merge(MapReduceOper[] compiledInputs)
            throws PlanException {
        List<MapReduceOper> ret = new ArrayList<MapReduceOper>();
        
        MapReduceOper mergedMap = getMROp();
        ret.add(mergedMap);
        MRPlan.add(mergedMap);
        
        Set<MapReduceOper> toBeConnected = new HashSet<MapReduceOper>();
        List<MapReduceOper> remLst = new ArrayList<MapReduceOper>();

        List<PhysicalPlan<PhysicalOperator>> mpLst = new ArrayList<PhysicalPlan<PhysicalOperator>>();

        for (MapReduceOper mro : compiledInputs) {
            if (!mro.isMapDone()) {
                remLst.add(mro);
                mpLst.add(mro.mapPlan);
                List<MapReduceOper> pmros = MRPlan.getPredecessors(mro);
                if(pmros!=null){
                    for(MapReduceOper pmro : pmros)
                        toBeConnected.add(pmro);
                }
            } else if (mro.isMapDone() && !mro.isReduceDone()) {
                ret.add(mro);
            } else {
                log.warn(
                        "Both map and reduce phases have been done. This is unexpected for a merge!");
                throw new PlanException(
                        "Both map and reduce phases have been done. This is unexpected for a merge!");
            }
        }
        merge(ret.get(0).mapPlan, mpLst);
        Iterator<MapReduceOper> it = toBeConnected.iterator();
        while(it.hasNext())
            MRPlan.connect(it.next(), mergedMap);
        for(MapReduceOper rmro : remLst)
            MRPlan.remove(rmro);
        return ret;
    }
    
    /**
     * The merge of a list of map plans
     * @param <O>
     * @param <E>
     * @param finPlan - Final Plan into which the list of plans is merged
     * @param plans - list of map plans to be merged
     * @throws PlanException
     */
    private <O extends Operator, E extends OperatorPlan<O>> void merge(
            E finPlan, List<E> plans) throws PlanException {
        for (E e : plans) {
            finPlan.merge(e);
        }
    }
    
    /**
     * The visitOp methods that decide what to do with the current operator
     */
    
    public void visitSplit(POSplit op) throws VisitorException{
        try{
            split(op);
        }catch(Exception e){
            VisitorException pe = new VisitorException(e.getMessage());
            pe.initCause(e);
            throw pe;
        }
    }
    
    public void visitLoad(POLoad op) throws VisitorException{
        try{
            nonBlocking(op);
        }catch(Exception e){
            VisitorException pe = new VisitorException(e.getMessage());
            pe.initCause(e);
            throw pe;
        }
    }
    
    public void visitStore(POStore op) throws VisitorException{
        try{
            nonBlocking(op);
        }catch(Exception e){
            VisitorException pe = new VisitorException(e.getMessage());
            pe.initCause(e);
            throw pe;
        }
    }
    
    public void visitFilter(POFilter op) throws VisitorException{
        try{
            nonBlocking(op);
        }catch(Exception e){
            VisitorException pe = new VisitorException(e.getMessage());
            pe.initCause(e);
            throw pe;
        }
    }
    
    public void visitLocalRearrange(POLocalRearrange op) throws VisitorException {
        try{
            nonBlocking(op);
        }catch(Exception e){
            VisitorException pe = new VisitorException(e.getMessage());
            pe.initCause(e);
            throw pe;
        }
    }
    
    public void visitForEach(POForEach op) throws VisitorException{
        try{
            nonBlocking(op);
        }catch(Exception e){
            VisitorException pe = new VisitorException(e.getMessage());
            pe.initCause(e);
            throw pe;
        }
    }
    
    public void visitGlobalRearrange(POGlobalRearrange op) throws VisitorException{
        try{
            blocking(op);
        }catch(Exception e){
            VisitorException pe = new VisitorException(e.getMessage());
            pe.initCause(e);
            throw pe;
        }
    }
    
    public void visitPackage(POPackage op) throws VisitorException{
        try{
            nonBlocking(op);
        }catch(Exception e){
            VisitorException pe = new VisitorException(e.getMessage());
            pe.initCause(e);
            throw pe;
        }
    }
    
    public void visitUnion(POUnion op) throws VisitorException{
        try{
            nonBlocking(op);
        }catch(Exception e){
            VisitorException pe = new VisitorException(e.getMessage());
            pe.initCause(e);
            throw pe;
        }
    }
}
