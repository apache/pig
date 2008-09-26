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
import org.apache.pig.FuncSpec;
import org.apache.pig.builtin.BinStorage;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.builtin.FindQuantiles;
import org.apache.pig.impl.builtin.RandomSampleLoader;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROpPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.UDFFinder;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.ConstantExpression;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POProject;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POUserFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.PODistinct;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFilter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POGlobalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLimit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSort;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSplit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStream;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POUnion;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.Operator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.OperatorPlan;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;

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
 * the inputs individually, into a single job. Here a new
 * map job is created and then the contents of the previous
 * map plans are added. However, any other state that was in
 * the previous map plans, should be manually moved over. So,
 * if you are adding something new take care about this.
 * Ex of this is in requestedParallelism
 * 
 * Only in case of blocking operators and splits, a new 
 * MapReduce operator is started using a store-load combination
 * to connect the two operators. Whenever this happens
 * care is taken to add the MROper into the MRPlan and connect it
 * appropriately.
 * 
 *
 */
public class MRCompiler extends PhyPlanVisitor {
    
    private Log log = LogFactory.getLog(getClass());
    
    PigContext pigContext;
    
    //The plan that is being compiled
    PhysicalPlan plan;

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
    
    private UDFFinder udfFinder;
    
    public MRCompiler(PhysicalPlan plan) {
        this(plan,null);
    }
    
    public MRCompiler(PhysicalPlan plan,
            PigContext pigContext) {
        super(plan, new DepthFirstWalker<PhysicalOperator, PhysicalPlan>(plan));
        this.plan = plan;
        this.pigContext = pigContext;
        splitsSeen = new HashMap<OperatorKey, MapReduceOper>();
        MRPlan = new MROperPlan();
        nig = NodeIdGenerator.getGenerator();
        scope = plan.getRoots().get(0).getOperatorKey().getScope();
        r = new Random(1331);
        FileLocalizer.setR(r);
        udfFinder = new UDFFinder();
    }
    
    public void randomizeFileLocalizer(){
        FileLocalizer.setR(new Random());
    }
    
    /**
     * Used to get the compiled plan
     * @return map reduce plan built by the compiler
     */
    public MROperPlan getMRPlan() {
        return MRPlan;
    }
    
    /**
     * Used to get the plan that was compiled
     * @return physical plan
     */
    public PhysicalPlan getPlan() {
        return plan;
    }
    
    /**
     * The front-end method that the user calls to compile
     * the plan. Assumes that all submitted plans have a Store
     * operators as the leaf.
     * @return A map reduce plan
     * @throws IOException
     * @throws PlanException
     * @throws VisitorException
     */
    public MROperPlan compile() throws IOException, PlanException, VisitorException {
        List<PhysicalOperator> leaves = plan.getLeaves();
        POStore store = (POStore)leaves.get(0);
        FileLocalizer.registerDeleteOnFail(store.getSFile().getFileName(), pigContext);
        compile(store);

        // I'm quite certain this is not the best way to do this.  The issue
        // is that for jobs that take multiple map reduce passes, for
        // non-sort jobs, the POLocalRearrange is being put into the reduce
        // of MR job n, with the map for MR job n+1 empty and the POPackage
        // in reduce of MR job n+1.  This causes problems in the collect of
        // the map MR job n+1.  To resolve this, the following visitor
        // walks the resulting compiled jobs, looks for the pattern described
        // above, and then moves the POLocalRearrange to the map of MR job
        // n+1.  It seems to me there are two possible better solutions:
        // 1) Change the logic in this compiler to put POLocalRearrange in
        // the correct place to begin with instead of patching it up later.
        // I'd do this but I don't fully understand the logic here and it's
        // complex.
        // 2) Change our map reduce execution to have a reduce only mode.  In
        // this case the map would not even try to parse the input, it would
        // just be 100% pass through.  I suspect this might be better though
        // I don't fully understand the consequences of this.
        // Given these issues, the following works for now, and we can fine
        // tune it when Shravan returns.
        RearrangeAdjuster ra = new RearrangeAdjuster(MRPlan);
        ra.visit();
        
        LimitAdjuster la = new LimitAdjuster(MRPlan);
        la.visit();
        la.adjust();
        
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
        if(op.getRequestedParallelism() > curMROp.requestedParallelism)
            curMROp.requestedParallelism = op.getRequestedParallelism();
        compiledInputs = prevCompInp;
    }
    
    private MapReduceOper getMROp(){
        return new MapReduceOper(new OperatorKey(scope,nig.getNextNodeId(scope)));
    }
    
    private POLoad getLoad(){
        POLoad ld = new POLoad(new OperatorKey(scope,nig.getNextNodeId(scope)), true);
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
                log.error("Both map and reduce phases have been done. This is unexpected while compiling!");
                throw new PlanException("Both map and reduce phases have been done. This is unexpected while compiling!");
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
    
    
    private MapReduceOper endSingleInputPlanWithStr(FileSpec fSpec) throws PlanException{
        if(compiledInputs.length>1) {
            log.error("Received a multi input plan when expecting only a single input one.");
            throw new PlanException("Received a multi input plan when expecting only a single input one.");
        }
        MapReduceOper mro = compiledInputs[0];
        POStore str = getStore();
        str.setSFile(fSpec);
        if (!mro.isMapDone()) {
            mro.mapPlan.addAsLeaf(str);
            mro.setMapDoneSingle(true);
        } else if (mro.isMapDone() && !mro.isReduceDone()) {
            mro.reducePlan.addAsLeaf(str);
            mro.setReduceDone(true);
        } else {
            log.error("Both map and reduce phases have been done. This is unexpected while compiling!");
            throw new PlanException("Both map and reduce phases have been done. This is unexpected while compiling!");
        }
        return mro;
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
                new FuncSpec(BinStorage.class.getName()));
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

        List<PhysicalPlan> mpLst = new ArrayList<PhysicalPlan>();

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
                log.error(
                        "Both map and reduce phases have been done. This is unexpected for a merge!");
                throw new PlanException(
                        "Both map and reduce phases have been done. This is unexpected for a merge!");
            }
        }
        merge(ret.get(0).mapPlan, mpLst);
        
        Iterator<MapReduceOper> it = toBeConnected.iterator();
        while(it.hasNext())
            MRPlan.connect(it.next(), mergedMap);
        for(MapReduceOper rmro : remLst){
            if(rmro.requestedParallelism > mergedMap.requestedParallelism)
                mergedMap.requestedParallelism = rmro.requestedParallelism;
            MRPlan.remove(rmro);
        }
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

    /*private void addUDFs(PhysicalPlan plan) throws VisitorException{
        if(plan!=null){
            udfFinderForExpr.setPlan(plan);
            udfFinderForExpr.visit();
            curMROp.UDFs.addAll(udfFinderForExpr.getUDFs());
        }
    }*/
    
    private void addUDFs(PhysicalPlan plan) throws VisitorException{
        if(plan!=null){
            udfFinder.setPlan(plan);
            udfFinder.visit();
            curMROp.UDFs.addAll(udfFinder.getUDFs());
        }
    }
    
    
    /* The visitOp methods that decide what to do with the current operator */
    
    /**
     * Compiles a split operator. The logic is to
     * close the split job by replacing the split oper by
     * a store and creating a new Map MRoper and return
     * that as the current MROper to which other operators
     * would be compiled into. The new MROper would be connected
     * to the split job by load-store. Also add the split oper 
     * to the splitsSeen map.
     * @param op - The split operator
     * @throws VisitorException
     */
    public void visitSplit(POSplit op) throws VisitorException{
        try{
            FileSpec fSpec = op.getSplitStore();
            MapReduceOper mro = endSingleInputPlanWithStr(fSpec);
            splitsSeen.put(op.getOperatorKey(), mro);
            curMROp = startNew(fSpec, mro);
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
            addUDFs(op.getPlan());
        }catch(Exception e){
            VisitorException pe = new VisitorException(e.getMessage());
            pe.initCause(e);
            throw pe;
        }
    }
    
    public void visitStream(POStream op) throws VisitorException{
        try{
            nonBlocking(op);
        }catch(Exception e){
            VisitorException pe = new VisitorException(e.getMessage());
            pe.initCause(e);
            throw pe;
        }
    }
    
    public void simpleConnectMapToReduce(MapReduceOper mro) throws PlanException
    {
    	PhysicalPlan ep = new PhysicalPlan();
        POProject prjStar = new POProject(new OperatorKey(scope,nig.getNextNodeId(scope)));
        prjStar.setResultType(DataType.TUPLE);
        prjStar.setStar(true);
        ep.add(prjStar);
        
        List<PhysicalPlan> eps = new ArrayList<PhysicalPlan>();
        eps.add(ep);
        
        POLocalRearrange lr = new POLocalRearrange(new OperatorKey(scope,nig.getNextNodeId(scope)));
        lr.setIndex(0);
        lr.setKeyType(DataType.TUPLE);
        lr.setPlans(eps);
        lr.setResultType(DataType.TUPLE);
        
        mro.mapPlan.addAsLeaf(lr);
        
        POPackage pkg = new POPackage(new OperatorKey(scope,nig.getNextNodeId(scope)));
        pkg.setKeyType(DataType.TUPLE);
        pkg.setNumInps(1);
        boolean[] inner = {false};
        pkg.setInner(inner);
        mro.reducePlan.add(pkg);
        
        List<PhysicalPlan> eps1 = new ArrayList<PhysicalPlan>();
        List<Boolean> flat1 = new ArrayList<Boolean>();
        PhysicalPlan ep1 = new PhysicalPlan();
        POProject prj1 = new POProject(new OperatorKey(scope,nig.getNextNodeId(scope)));
        prj1.setResultType(DataType.TUPLE);
        prj1.setStar(false);
        prj1.setColumn(1);
        prj1.setOverloaded(true);
        ep1.add(prj1);
        eps1.add(ep1);
        flat1.add(true);
        POForEach nfe1 = new POForEach(new OperatorKey(scope, nig
                .getNextNodeId(scope)), -1, eps1, flat1);
        nfe1.setResultType(DataType.BAG);
        
        mro.reducePlan.addAsLeaf(nfe1);
    }
    
    public void visitLimit(POLimit op) throws VisitorException{
        try{
        	
            MapReduceOper mro = compiledInputs[0];
            mro.limit = op.getLimit();
            if (!mro.isMapDone()) {
            	// if map plan is open, add a limit for optimization, eventually we
            	// will add another limit to reduce plan
                mro.mapPlan.addAsLeaf(op);
                mro.setMapDone(true);
                
                if (mro.reducePlan.isEmpty())
                {
                    simpleConnectMapToReduce(mro);
                    mro.requestedParallelism = 1;
                    POLimit pLimit2 = new POLimit(new OperatorKey(scope,nig.getNextNodeId(scope)));
                    pLimit2.setLimit(op.getLimit());
                    mro.reducePlan.addAsLeaf(pLimit2);
                }
                else
                {
                    log.warn("Something in the reduce plan while map plan is not done. Something wrong!");
                }
            } else if (mro.isMapDone() && !mro.isReduceDone()) {
            	// limit should add into reduce plan
                mro.reducePlan.addAsLeaf(op);
            } else {
                log.warn("Both map and reduce phases have been done. This is unexpected while compiling!");
            }
        }catch(Exception e){
            VisitorException pe = new VisitorException(e.getMessage());
            pe.initCause(e);
            throw pe;
        }
    }

    public void visitLocalRearrange(POLocalRearrange op) throws VisitorException {
        try{
            nonBlocking(op);
            List<PhysicalPlan> plans = op.getPlans();
            if(plans!=null)
                for(PhysicalPlan ep : plans)
                    addUDFs(ep);
        }catch(Exception e){
            VisitorException pe = new VisitorException(e.getMessage());
            pe.initCause(e);
            throw pe;
        }
    }
    
    public void visitPOForEach(POForEach op) throws VisitorException{
        try{
            nonBlocking(op);
            List<PhysicalPlan> plans = op.getInputPlans();
            if(plans!=null)
                for (PhysicalPlan plan : plans) {
                    addUDFs(plan);
                }
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
    
    

    @Override
    public void visitDistinct(PODistinct op) throws VisitorException {
        try{
            MapReduceOper mro = compiledInputs[0];
            PhysicalPlan ep = new PhysicalPlan();
            POProject prjStar = new POProject(new OperatorKey(scope,nig.getNextNodeId(scope)));
            prjStar.setResultType(DataType.TUPLE);
            prjStar.setStar(true);
            ep.add(prjStar);
            
            List<PhysicalPlan> eps = new ArrayList<PhysicalPlan>();
            eps.add(ep);
            
            POLocalRearrange lr = new POLocalRearrange(new OperatorKey(scope,nig.getNextNodeId(scope)));
            lr.setIndex(0);
            lr.setKeyType(DataType.TUPLE);
            lr.setPlans(eps);
            lr.setResultType(DataType.TUPLE);
            lr.setDistinct(true);
            if(!mro.isMapDone()){
                mro.mapPlan.addAsLeaf(lr);
            }
            else if(mro.isMapDone() && ! mro.isReduceDone()){
                mro.reducePlan.addAsLeaf(lr);
            }
            
            blocking(op);
            
            POPackage pkg = new POPackage(new OperatorKey(scope,nig.getNextNodeId(scope)));
            pkg.setKeyType(DataType.TUPLE);
            pkg.setNumInps(0);
            boolean[] inner = {false}; 
            pkg.setInner(inner);
            curMROp.reducePlan.add(pkg);
            
            List<PhysicalPlan> eps1 = new ArrayList<PhysicalPlan>();
            List<Boolean> flat1 = new ArrayList<Boolean>();
            PhysicalPlan ep1 = new PhysicalPlan();
            POProject prj1 = new POProject(new OperatorKey(scope,nig.getNextNodeId(scope)));
            prj1.setResultType(DataType.TUPLE);
            prj1.setStar(false);
            prj1.setColumn(0);
            prj1.setOverloaded(false);
            ep1.add(prj1);
            eps1.add(ep1);
            flat1.add(true);
            POForEach nfe1 = new POForEach(new OperatorKey(scope, nig
                    .getNextNodeId(scope)), op.getRequestedParallelism(), eps1,
                    flat1);
            nfe1.setResultType(DataType.BAG);
            curMROp.reducePlan.addAsLeaf(nfe1);
            curMROp.setNeedsDistinctCombiner(true);
        }catch(Exception e){
            VisitorException pe = new VisitorException(e.getMessage());
            pe.initCause(e);
            throw pe;
        }
    }

    @Override
    public void visitSort(POSort op) throws VisitorException {
        try{
            FileSpec fSpec = getTempFileSpec();
            MapReduceOper mro = endSingleInputPlanWithStr(fSpec);
            FileSpec quantFile = getTempFileSpec();
            int rp = op.getRequestedParallelism();
            int[] fields = getSortCols(op);
            MapReduceOper quant = getQuantileJob(op, mro, fSpec, quantFile, rp, fields);
            curMROp = getSortJob(op, quant, fSpec, quantFile, rp, fields);
            
            if(op.isUDFComparatorUsed){
                curMROp.UDFs.add(op.getMSortFunc().getFuncSpec().toString());
            }
        }catch(Exception e){
            VisitorException pe = new VisitorException(e.getMessage(), e);
            throw pe;
        }
    }
    
    private int[] getSortCols(POSort sort) throws PlanException {
        List<PhysicalPlan> plans = sort.getSortPlans();
        if(plans!=null){
            int[] ret = new int[plans.size()]; 
            int i=-1;
            for (PhysicalPlan plan : plans) {
                if (((POProject)plan.getLeaves().get(0)).isStar()) return null;
                ret[++i] = ((POProject)plan.getLeaves().get(0)).getColumn();
            }
            return ret;
        }
        log.error("No expression plan found in POSort");
        throw new PlanException("No Expression Plan found in POSort");
    }
    
    public MapReduceOper getSortJob(
            POSort sort,
            MapReduceOper quantJob,
            FileSpec lFile,
            FileSpec quantFile,
            int rp,
            int[] fields) throws PlanException{
        MapReduceOper mro = startNew(lFile, quantJob);
        mro.setQuantFile(quantFile.getFileName());
        mro.setGlobalSort(true);
        mro.requestedParallelism = rp;

        long limit = sort.getLimit();
        mro.limit = limit;
        
        List<PhysicalPlan> eps1 = new ArrayList<PhysicalPlan>();

        byte keyType = DataType.UNKNOWN;
        
        boolean[] sortOrder;

        List<Boolean> sortOrderList = sort.getMAscCols();
        if(sortOrderList != null) {
            sortOrder = new boolean[sortOrderList.size()];
            for(int i = 0; i < sortOrderList.size(); ++i) {
                sortOrder[i] = sortOrderList.get(i);
            }
            mro.setSortOrder(sortOrder);
        }

        if (fields == null) {
            // This is project *
            PhysicalPlan ep = new PhysicalPlan();
            POProject prj = new POProject(new OperatorKey(scope,nig.getNextNodeId(scope)));
            prj.setStar(true);
            prj.setOverloaded(false);
            prj.setResultType(DataType.TUPLE);
            ep.add(prj);
            eps1.add(ep);
        } else {
            /*
            for (int i : fields) {
                PhysicalPlan ep = new PhysicalPlan();
                POProject prj = new POProject(new OperatorKey(scope,
                    nig.getNextNodeId(scope)));
                prj.setColumn(i);
                prj.setOverloaded(false);
                prj.setResultType(DataType.BYTEARRAY);
                ep.add(prj);
                eps1.add(ep);
            }
            */
            // Attach the sort plans to the local rearrange to get the
            // projection.
            eps1.addAll(sort.getSortPlans());

            // Visit the first sort plan to figure out our key type.  We only
            // have to visit the first because if we have more than one plan,
            // then the key type will be tuple.
            try {
                FindKeyTypeVisitor fktv =
                    new FindKeyTypeVisitor(sort.getSortPlans().get(0));
                fktv.visit();
                keyType = fktv.keyType;
            } catch (VisitorException ve) {
                throw new PlanException(ve);
            }
        }
        
        POLocalRearrange lr = new POLocalRearrange(new OperatorKey(scope,nig.getNextNodeId(scope)));
        lr.setIndex(0);
        lr.setKeyType((fields == null || fields.length>1) ? DataType.TUPLE :
            keyType);
        lr.setPlans(eps1);
        lr.setResultType(DataType.TUPLE);
        mro.mapPlan.addAsLeaf(lr);
        
        mro.setMapDone(true);
        
        if (limit!=-1) {
        	POPackage pkg_c = new POPackage(new OperatorKey(scope,nig.getNextNodeId(scope)));
        	pkg_c.setKeyType((fields.length>1) ? DataType.TUPLE : keyType);
            pkg_c.setNumInps(1);
            //pkg.setResultType(DataType.TUPLE);
            boolean[] inner = {false};
            pkg_c.setInner(inner);
            mro.combinePlan.add(pkg_c);
        	
            List<PhysicalPlan> eps_c1 = new ArrayList<PhysicalPlan>();
            List<Boolean> flat_c1 = new ArrayList<Boolean>();
            PhysicalPlan ep_c1 = new PhysicalPlan();
            POProject prj_c1 = new POProject(new OperatorKey(scope,nig.getNextNodeId(scope)));
            prj_c1.setColumn(1);
            prj_c1.setOverloaded(false);
            prj_c1.setResultType(DataType.BAG);
            ep_c1.add(prj_c1);
            eps_c1.add(ep_c1);
            flat_c1.add(true);
            POForEach fe_c1 = new POForEach(new OperatorKey(scope,nig.getNextNodeId(scope)), 
            		-1, eps_c1, flat_c1);
            fe_c1.setResultType(DataType.TUPLE);
            mro.combinePlan.addAsLeaf(fe_c1);
            
            POLimit pLimit = new POLimit(new OperatorKey(scope,nig.getNextNodeId(scope)));
        	pLimit.setLimit(limit);
        	mro.combinePlan.addAsLeaf(pLimit);
            
            List<PhysicalPlan> eps_c2 = new ArrayList<PhysicalPlan>();
            eps_c2.addAll(sort.getSortPlans());
        
	        POLocalRearrange lr_c2 = new POLocalRearrange(new OperatorKey(scope,nig.getNextNodeId(scope)));
	        lr_c2.setIndex(0);
	        lr_c2.setKeyType((fields.length>1) ? DataType.TUPLE : keyType);
	        lr_c2.setPlans(eps_c2);
	        lr_c2.setResultType(DataType.TUPLE);
	        mro.combinePlan.addAsLeaf(lr_c2);
        }
        
        POPackage pkg = new POPackage(new OperatorKey(scope,nig.getNextNodeId(scope)));
        pkg.setKeyType((fields == null || fields.length>1) ? DataType.TUPLE :
            keyType);
        pkg.setNumInps(1);
        boolean[] inner = {false}; 
        pkg.setInner(inner);
        mro.reducePlan.add(pkg);
        
        PhysicalPlan ep = new PhysicalPlan();
        POProject prj = new POProject(new OperatorKey(scope,nig.getNextNodeId(scope)));
        prj.setColumn(1);
        prj.setOverloaded(false);
        prj.setResultType(DataType.BAG);
        ep.add(prj);
        List<PhysicalPlan> eps2 = new ArrayList<PhysicalPlan>();
        eps2.add(ep);
        List<Boolean> flattened = new ArrayList<Boolean>();
        flattened.add(true);
        POForEach nfe1 = new POForEach(new OperatorKey(scope,nig.getNextNodeId(scope)),-1,eps2,flattened);
        mro.reducePlan.add(nfe1);
        mro.reducePlan.connect(pkg, nfe1);
        
        if (limit!=-1)
        {
	        POLimit pLimit2 = new POLimit(new OperatorKey(scope,nig.getNextNodeId(scope)));
	    	pLimit2.setLimit(limit);
	    	mro.reducePlan.addAsLeaf(pLimit2);
        }

//        ep1.add(innGen);
        return mro;
    }

    public MapReduceOper getQuantileJob(POSort inpSort, MapReduceOper prevJob, FileSpec lFile, FileSpec quantFile, int rp, int[] fields) throws PlanException, VisitorException {
        FileSpec quantLdFilName = new FileSpec(lFile.getFileName(), new FuncSpec(RandomSampleLoader.class.getName()));
        MapReduceOper mro = startNew(quantLdFilName, prevJob);
        mro.UDFs.add(FindQuantiles.class.getName());
        POSort sort = new POSort(inpSort.getOperatorKey(), inpSort
                .getRequestedParallelism(), null, inpSort.getSortPlans(),
                inpSort.getMAscCols(), inpSort.getMSortFunc());
        if(sort.isUDFComparatorUsed) {
            mro.UDFs.add(sort.getMSortFunc().getFuncSpec().toString());
        }
        
        List<PhysicalPlan> eps1 = new ArrayList<PhysicalPlan>();
        List<Boolean> flat1 = new ArrayList<Boolean>();
        
        if (fields == null) {
            PhysicalPlan ep = new PhysicalPlan();
            POProject prj = new POProject(new OperatorKey(scope,
                nig.getNextNodeId(scope)));
            prj.setStar(true);
            prj.setOverloaded(false);
            prj.setResultType(DataType.TUPLE);
            ep.add(prj);
            eps1.add(ep);
            flat1.add(true);
        } else {
            for (int i : fields) {
                PhysicalPlan ep = new PhysicalPlan();
                POProject prj = new POProject(new OperatorKey(scope,nig.getNextNodeId(scope)));
                prj.setColumn(i);
                prj.setOverloaded(false);
                prj.setResultType(DataType.BYTEARRAY);
                ep.add(prj);
                eps1.add(ep);
                flat1.add(true);
            }
        }
        POForEach nfe1 = new POForEach(new OperatorKey(scope,nig.getNextNodeId(scope)),-1,eps1,flat1);
        mro.mapPlan.addAsLeaf(nfe1);
        
        PhysicalPlan ep1 = new PhysicalPlan();
        ConstantExpression ce = new ConstantExpression(new OperatorKey(scope,nig.getNextNodeId(scope)));
        ce.setValue("all");
        ce.setResultType(DataType.CHARARRAY);
        ep1.add(ce);
        
        List<PhysicalPlan> eps = new ArrayList<PhysicalPlan>();
        eps.add(ep1);
        
        POLocalRearrange lr = new POLocalRearrange(new OperatorKey(scope,nig.getNextNodeId(scope)));
        lr.setIndex(0);
        lr.setKeyType(DataType.CHARARRAY);
        lr.setPlans(eps);
        lr.setResultType(DataType.TUPLE);
        mro.mapPlan.add(lr);
        mro.mapPlan.connect(nfe1, lr);
        
        mro.setMapDone(true);
        
        POPackage pkg = new POPackage(new OperatorKey(scope,nig.getNextNodeId(scope)));
        pkg.setKeyType(DataType.CHARARRAY);
        pkg.setNumInps(1);
        boolean[] inner = {false}; 
        pkg.setInner(inner);
        mro.reducePlan.add(pkg);
        
        PhysicalPlan fe2Plan = new PhysicalPlan();
        
        POProject topPrj = new POProject(new OperatorKey(scope,nig.getNextNodeId(scope)));
        topPrj.setColumn(1);
        topPrj.setResultType(DataType.TUPLE);
        topPrj.setOverloaded(true);
        fe2Plan.add(topPrj);
        
        PhysicalPlan nesSortPlan = new PhysicalPlan();
        POProject prjStar2 = new POProject(new OperatorKey(scope,nig.getNextNodeId(scope)));
        prjStar2.setResultType(DataType.TUPLE);
        prjStar2.setStar(true);
        nesSortPlan.add(prjStar2);
        
        List<PhysicalPlan> nesSortPlanLst = new ArrayList<PhysicalPlan>();
        nesSortPlanLst.add(nesSortPlan);
        
        sort.setSortPlans(nesSortPlanLst);
        sort.setResultType(DataType.BAG);
        fe2Plan.add(sort);
        fe2Plan.connect(topPrj, sort);
        
        /*PhysicalPlan ep3 = new PhysicalPlan();
        POProject prjStar3 = new POProject(new OperatorKey(scope,nig.getNextNodeId(scope)));
        prjStar3.setResultType(DataType.BAG);
        prjStar3.setColumn(1);
        prjStar3.setStar(false);
        ep3.add(prjStar3);*/
        
        PhysicalPlan rpep = new PhysicalPlan();
        ConstantExpression rpce = new ConstantExpression(new OperatorKey(scope,nig.getNextNodeId(scope)));
        rpce.setRequestedParallelism(rp);
        rpce.setValue(rp<=0?1:rp);
        rpce.setResultType(DataType.INTEGER);
        rpep.add(rpce);
        
        List<PhysicalPlan> genEps = new ArrayList<PhysicalPlan>();
        genEps.add(rpep);
        genEps.add(fe2Plan);
        
        List<Boolean> flattened2 = new ArrayList<Boolean>();
        flattened2.add(false);
        flattened2.add(false);
        
        POForEach nfe2 = new POForEach(new OperatorKey(scope,nig.getNextNodeId(scope)),-1, genEps, flattened2);
        mro.reducePlan.add(nfe2);
        mro.reducePlan.connect(pkg, nfe2);
        
        PhysicalPlan ep4 = new PhysicalPlan();
        POProject prjStar4 = new POProject(new OperatorKey(scope,nig.getNextNodeId(scope)));
        prjStar4.setResultType(DataType.TUPLE);
        prjStar4.setStar(true);
        ep4.add(prjStar4);
        
        List ufInps = new ArrayList();
        ufInps.add(prjStar4);
        // Turn the asc/desc array into an array of strings so that we can pass it
        // to the FindQuantiles function.
        List<Boolean> ascCols = inpSort.getMAscCols();
        String[] ascs = new String[ascCols.size()];
        for (int i = 0; i < ascCols.size(); i++) ascs[i] = ascCols.get(i).toString();
        POUserFunc uf = new POUserFunc(new OperatorKey(scope,nig.getNextNodeId(scope)), -1, ufInps, 
            new FuncSpec(FindQuantiles.class.getName(), ascs));
        ep4.add(uf);
        ep4.connect(prjStar4, uf);
        
        List<PhysicalPlan> ep4s = new ArrayList<PhysicalPlan>();
        ep4s.add(ep4);
        List<Boolean> flattened3 = new ArrayList<Boolean>();
        flattened3.add(false);
        POForEach nfe3 = new POForEach(new OperatorKey(scope,nig.getNextNodeId(scope)), -1, ep4s, flattened3);
        
        mro.reducePlan.add(nfe3);
        mro.reducePlan.connect(nfe2, nfe3);
        
        POStore str = getStore();
        str.setSFile(quantFile);
        mro.reducePlan.add(str);
        mro.reducePlan.connect(nfe3, str);
        
        mro.setReduceDone(true);
//        mro.requestedParallelism = rp;
        return mro;
    }

    private class RearrangeAdjuster extends MROpPlanVisitor {

        RearrangeAdjuster(MROperPlan plan) {
            super(plan, new DepthFirstWalker<MapReduceOper, MROperPlan>(plan));
        }

        @Override
        public void visitMROp(MapReduceOper mr) throws VisitorException {
            // Look for map reduce operators whose reduce starts in a local
            // rearrange.  If it has a successor and that predecessor's map
            // plan is just a load, push the porearrange to the successor.
            // Else, throw an error.
            if (mr.reducePlan.isEmpty()) return;
            List<PhysicalOperator> mpLeaves = mr.reducePlan.getLeaves();
            if (mpLeaves.size() != 1) {
                String msg = new String("Expected reduce to have single leaf");
                log.error(msg);
                throw new VisitorException(msg);
            }
            PhysicalOperator mpLeaf = mpLeaves.get(0);
            if (!(mpLeaf instanceof POStore)) {
                String msg = new String("Expected leaf of reduce plan to " +
                    "always be POStore!");
                log.error(msg);
                throw new VisitorException(msg);
            }
            List<PhysicalOperator> preds =
                mr.reducePlan.getPredecessors(mpLeaf);
            if (preds == null) return;
            if (preds.size() > 1) {
                String msg = new String("Expected mr to have single predecessor");
                log.error(msg);
                throw new VisitorException(msg);
            }
            PhysicalOperator pred = preds.get(0);
            if (!(pred instanceof POLocalRearrange)) return;

            // Next question, does the next MROper have an empty map?
            List<MapReduceOper> succs = mPlan.getSuccessors(mr);
            if (succs == null) {
                String msg = new String("Found mro with POLocalRearrange as"
                    + " last oper but with no succesor!");
                log.error(msg);
                throw new VisitorException(msg);
            }
            if (succs.size() > 1) {
                String msg = new String("Expected mr to have single successor");
                log.error(msg);
                throw new VisitorException(msg);
            }
            MapReduceOper succ = succs.get(0);
            List<PhysicalOperator> succMpLeaves = succ.mapPlan.getLeaves();
            List<PhysicalOperator> succMpRoots = succ.mapPlan.getRoots();
            if (succMpLeaves == null || succMpLeaves.size() > 1 ||
                    succMpRoots == null || succMpRoots.size() > 1 ||
                    succMpLeaves.get(0) != succMpRoots.get(0)) {
                log.warn("Expected to find subsequent map " +
                    "with just a load, but didn't");
                return;
            }
            PhysicalOperator load = succMpRoots.get(0);

            try {
                mr.reducePlan.removeAndReconnect(pred);
                succ.mapPlan.add(pred);
                succ.mapPlan.connect(load, pred);
            } catch (PlanException pe) {
                throw new VisitorException(pe);
            }
        }
    }

    private class LimitAdjuster extends MROpPlanVisitor {
        ArrayList<MapReduceOper> opsToAdjust = new ArrayList<MapReduceOper>();  

        LimitAdjuster(MROperPlan plan) {
            super(plan, new DepthFirstWalker<MapReduceOper, MROperPlan>(plan));
        }

        @Override
        public void visitMROp(MapReduceOper mr) throws VisitorException {
            // Look for map reduce operators which contains limit operator.
            // If so and the requestedParallelism > 1, add one additional map-reduce
            // operator with 1 reducer into the original plan
            if (mr.limit!=-1 && mr.requestedParallelism>1)
            {
                opsToAdjust.add(mr);
            }
        }
        
        public void adjust() throws IOException, PlanException
        {
            for (MapReduceOper mr:opsToAdjust)
            {
                if (mr.reducePlan.isEmpty()) return;
                List<PhysicalOperator> mpLeaves = mr.reducePlan.getLeaves();
                if (mpLeaves.size() != 1) {
                    String msg = new String("Expected reduce to have single leaf");
                    log.error(msg);
                    throw new IOException(msg);
                }
                PhysicalOperator mpLeaf = mpLeaves.get(0);
                if (!(mpLeaf instanceof POStore)) {
                    String msg = new String("Expected leaf of reduce plan to " +
                        "always be POStore!");
                    log.error(msg);
                    throw new IOException(msg);
                }
                FileSpec oldSpec = ((POStore)mpLeaf).getSFile();
                
                FileSpec fSpec = getTempFileSpec();
                ((POStore)mpLeaf).setSFile(fSpec);
                mr.setReduceDone(true);
                MapReduceOper limitAdjustMROp = getMROp();
                POLoad ld = getLoad();
                ld.setLFile(fSpec);
                limitAdjustMROp.mapPlan.add(ld);
                POLimit pLimit = new POLimit(new OperatorKey(scope,nig.getNextNodeId(scope)));
                pLimit.setLimit(mr.limit);
                limitAdjustMROp.mapPlan.addAsLeaf(pLimit);
                simpleConnectMapToReduce(limitAdjustMROp);
                POLimit pLimit2 = new POLimit(new OperatorKey(scope,nig.getNextNodeId(scope)));
                pLimit2.setLimit(mr.limit);
                limitAdjustMROp.reducePlan.addAsLeaf(pLimit2);
                POStore st = getStore();
                st.setSFile(oldSpec);
                limitAdjustMROp.reducePlan.addAsLeaf(st);
                limitAdjustMROp.requestedParallelism = 1;
                // If the operator we're following has global sort set, we
                // need to indicate that this is a limit after a sort.
                // This will assure that we get the right sort comparator
                // set.  Otherwise our order gets wacked (PIG-461).
                if (mr.isGlobalSort()) limitAdjustMROp.setLimitAfterSort(true);
                
                List<MapReduceOper> successorList = MRPlan.getSuccessors(mr);
                MapReduceOper successors[] = null;
                
                // Save a snapshot for successors, since we will modify MRPlan, 
                // use the list directly will be problematic
                if (successorList!=null && successorList.size()>0)
                {
                    successors = new MapReduceOper[successorList.size()];
                    int i=0;
                    for (MapReduceOper op:successorList)
                        successors[i++] = op;
                }
                
                MRPlan.add(limitAdjustMROp);
                MRPlan.connect(mr, limitAdjustMROp);
                
                if (successors!=null)
                {
                    for (int i=0;i<successors.length;i++)
                    {
                        MapReduceOper nextMr = successors[i];
                        if (nextMr!=null)
                            MRPlan.disconnect(mr, nextMr);
                        
                        if (nextMr!=null)
                            MRPlan.connect(limitAdjustMROp, nextMr);                        
                    }
                }
            }
        }
    }

    private class FindKeyTypeVisitor extends PhyPlanVisitor {

        byte keyType = DataType.UNKNOWN;

        FindKeyTypeVisitor(PhysicalPlan plan) {
            super(plan,
                new DepthFirstWalker<PhysicalOperator, PhysicalPlan>(plan));
        }

        @Override
        public void visitProject(POProject p) throws VisitorException {
            keyType = p.getResultType();
        }
    }
    
}
