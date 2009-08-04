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
import org.apache.pig.PigException;
import org.apache.pig.PigWarning;
import org.apache.pig.builtin.BinStorage;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.builtin.FindQuantiles;
import org.apache.pig.impl.builtin.RandomSampleLoader;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecutionEngine;
import org.apache.pig.backend.hadoop.executionengine.HExecutionEngine;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROpPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.UDFFinder;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.ConstantExpression;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POProject;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POUserFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFRJoin;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.PODistinct;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFilter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POGlobalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POJoinPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLimit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackageLite;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSort;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSplit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStream;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POUnion;
import org.apache.pig.impl.plan.CompilationMessageCollector;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.Operator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.OperatorPlan;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.plan.CompilationMessageCollector.MessageType;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;

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

    //Mapping of which MapReduceOper a store belongs to.
    Map<POStore, MapReduceOper> storeToMapReduceMap;
    
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
    
    private CompilationMessageCollector messageCollector = null;
    
    public static String USER_COMPARATOR_MARKER = "user.comparator.func:";
    
    public MRCompiler(PhysicalPlan plan) throws MRCompilerException {
        this(plan,null);
    }
    
    public MRCompiler(PhysicalPlan plan,
            PigContext pigContext) throws MRCompilerException {
        super(plan, new DepthFirstWalker<PhysicalOperator, PhysicalPlan>(plan));
        this.plan = plan;
        this.pigContext = pigContext;
        splitsSeen = new HashMap<OperatorKey, MapReduceOper>();
        MRPlan = new MROperPlan();
        nig = NodeIdGenerator.getGenerator();
        r = new Random(1331);
        FileLocalizer.setR(r);
        udfFinder = new UDFFinder();
        List<PhysicalOperator> roots = plan.getRoots();
        if((roots == null) || (roots.size() <= 0)) {
        	int errCode = 2053;
        	String msg = "Internal error. Did not find roots in the physical plan.";
        	throw new MRCompilerException(msg, errCode, PigException.BUG);
        }
        scope = roots.get(0).getOperatorKey().getScope();
        messageCollector = new CompilationMessageCollector() ;
        storeToMapReduceMap = new HashMap<POStore, MapReduceOper>();
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
    
    public CompilationMessageCollector getMessageCollector() {
    	return messageCollector;
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

        for (PhysicalOperator op : leaves) {
            if (!(op instanceof POStore)) {
                int errCode = 2025;
                String msg = "Expected leaf of reduce plan to " +
                    "always be POStore. Found " + op.getClass().getSimpleName();
                throw new MRCompilerException(msg, errCode, PigException.BUG);
            }
        }

        List<POStore> stores = PlanHelper.getStores(plan);
        for (POStore store: stores) {
            compile(store);
        }

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
            // When processing an entire script (multiquery), we can
            // get into a situation where a load has
            // predecessors. This means that it depends on some store
            // earlier in the plan. We need to take that dependency
            // and connect the respective MR operators, while at the
            // same time removing the connection between the Physical
            // operators. That way the jobs will run in the right
            // order.
            if (op instanceof POLoad) {

                if (predecessors.size() != 1) {
                    int errCode = 2125;
                    String msg = "Expected at most one predecessor of load. Got "+predecessors.size();
                    throw new PlanException(msg, errCode, PigException.BUG);
                }

                PhysicalOperator p = predecessors.get(0);
                if (!(p instanceof POStore)) {
                    int errCode = 2126;
                    String msg = "Predecessor of load should be a store. Got "+p.getClass();
                    throw new PlanException(msg, errCode, PigException.BUG);
                }

                // Need new operator
                curMROp = getMROp();
                curMROp.mapPlan.add(op);
                MRPlan.add(curMROp);
                
                MapReduceOper oper = storeToMapReduceMap.get((POStore)p);

                plan.disconnect(op, p);
                MRPlan.connect(oper, curMROp);
                return;
            }
            
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
        // mark store as tmp store. These could be removed by the
        // optimizer, because it wasn't the user requesting it.
        st.setIsTmpStore(true);
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
                int errCode = 2022;
                String msg = "Both map and reduce phases have been done. This is unexpected while compiling.";                
                throw new PlanException(msg, errCode, PigException.BUG);
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
     * @throws PlanException 
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
            int errCode = 2023;
            String msg = "Received a multi input plan when expecting only a single input one.";
            throw new PlanException(msg, errCode, PigException.BUG);
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
            int errCode = 2022;
            String msg = "Both map and reduce phases have been done. This is unexpected while compiling.";
            throw new PlanException(msg, errCode, PigException.BUG);
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
                int errCode = 2027;
                String msg = "Both map and reduce phases have been done. This is unexpected for a merge."; 
                throw new PlanException(msg, errCode, PigException.BUG);
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
            mro.setSplitter(true);
            splitsSeen.put(op.getOperatorKey(), mro);
            curMROp = startNew(fSpec, mro);
        }catch(Exception e){
            int errCode = 2034;
            String msg = "Error compiling operator " + op.getClass().getSimpleName();
            throw new MRCompilerException(msg, errCode, PigException.BUG, e);
        }
    }
    
    public void visitLoad(POLoad op) throws VisitorException{
        try{
            nonBlocking(op);
        }catch(Exception e){
            int errCode = 2034;
            String msg = "Error compiling operator " + op.getClass().getSimpleName();
            throw new MRCompilerException(msg, errCode, PigException.BUG, e);
        }
    }
    
    public void visitStore(POStore op) throws VisitorException{
        try{
            storeToMapReduceMap.put(op, curMROp);
            nonBlocking(op);
        }catch(Exception e){
            int errCode = 2034;
            String msg = "Error compiling operator " + op.getClass().getSimpleName();
            throw new MRCompilerException(msg, errCode, PigException.BUG, e);
        }
    }
    
    public void visitFilter(POFilter op) throws VisitorException{
        try{
            nonBlocking(op);
            addUDFs(op.getPlan());
        }catch(Exception e){
            int errCode = 2034;
            String msg = "Error compiling operator " + op.getClass().getSimpleName();
            throw new MRCompilerException(msg, errCode, PigException.BUG, e);
        }
    }
    
    public void visitStream(POStream op) throws VisitorException{
        try{
            nonBlocking(op);
        }catch(Exception e){
            int errCode = 2034;
            String msg = "Error compiling operator " + op.getClass().getSimpleName();
            throw new MRCompilerException(msg, errCode, PigException.BUG, e);
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
        try {
            lr.setIndex(0);
        } catch (ExecException e) {
        	int errCode = 2058;
        	String msg = "Unable to set index on the newly created POLocalRearrange.";
            throw new PlanException(msg, errCode, PigException.BUG, e);
        }
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
                    messageCollector.collect("Something in the reduce plan while map plan is not done. Something wrong!", 
                    		MessageType.Warning, PigWarning.REDUCE_PLAN_NOT_EMPTY_WHILE_MAP_PLAN_UNDER_PROCESS);
                }
            } else if (mro.isMapDone() && !mro.isReduceDone()) {
            	// limit should add into reduce plan
                mro.reducePlan.addAsLeaf(op);
            } else {
            	messageCollector.collect("Both map and reduce phases have been done. This is unexpected while compiling!",
            			MessageType.Warning, PigWarning.UNREACHABLE_CODE_BOTH_MAP_AND_REDUCE_PLANS_PROCESSED);
            }
        }catch(Exception e){
            int errCode = 2034;
            String msg = "Error compiling operator " + op.getClass().getSimpleName();
            throw new MRCompilerException(msg, errCode, PigException.BUG, e);
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
            int errCode = 2034;
            String msg = "Error compiling operator " + op.getClass().getSimpleName();
            throw new MRCompilerException(msg, errCode, PigException.BUG, e);
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
            int errCode = 2034;
            String msg = "Error compiling operator " + op.getClass().getSimpleName();
            throw new MRCompilerException(msg, errCode, PigException.BUG, e);
        }
    }
    
    public void visitGlobalRearrange(POGlobalRearrange op) throws VisitorException{
        try{
            blocking(op);
        }catch(Exception e){
            int errCode = 2034;
            String msg = "Error compiling operator " + op.getClass().getSimpleName();
            throw new MRCompilerException(msg, errCode, PigException.BUG, e);
        }
    }
    
    public void visitPackage(POPackage op) throws VisitorException{
        try{
            nonBlocking(op);
        }catch(Exception e){
            int errCode = 2034;
            String msg = "Error compiling operator " + op.getClass().getSimpleName();
            throw new MRCompilerException(msg, errCode, PigException.BUG, e);
        }
    }
    
    public void visitUnion(POUnion op) throws VisitorException{
        try{
            nonBlocking(op);
        }catch(Exception e){
            int errCode = 2034;
            String msg = "Error compiling operator " + op.getClass().getSimpleName();
            throw new MRCompilerException(msg, errCode, PigException.BUG, e);
        }
    }
    
    /**
     * This is an operator which will have multiple inputs(= to number of join inputs)
     * But it prunes off all inputs but the fragment input and creates separate MR jobs
     * for each of the replicated inputs and uses these as the replicated files that
     * are configured in the POFRJoin operator. It also sets that this is FRJoin job
     * and some parametes associated with it.
     */
    @Override
    public void visitFRJoin(POFRJoin op) throws VisitorException {
        try{
            FileSpec[] replFiles = new FileSpec[op.getInputs().size()];
            for (int i=0; i<replFiles.length; i++) {
                if(i==op.getFragment()) continue;
                replFiles[i] = getTempFileSpec();
            }
            op.setReplFiles(replFiles);
            
            List<OperatorKey> opKeys = new ArrayList<OperatorKey>(op.getInputs().size());
            for (PhysicalOperator pop : op.getInputs()) {
                opKeys.add(pop.getOperatorKey());
            }
            int fragPlan = 0;
            for(int i=0;i<compiledInputs.length;i++){
                MapReduceOper mro = compiledInputs[i];
                OperatorKey opKey = (!mro.isMapDone()) ?  mro.mapPlan.getLeaves().get(0).getOperatorKey()
                                                       :  mro.reducePlan.getLeaves().get(0).getOperatorKey();
                if(opKeys.indexOf(opKey)==op.getFragment()){
                    curMROp = mro;
                    fragPlan = i;
                    continue;
                }
                POStore str = getStore();
                str.setSFile(replFiles[opKeys.indexOf(opKey)]);
                if (!mro.isMapDone()) {
                    mro.mapPlan.addAsLeaf(str);
                    mro.setMapDoneSingle(true);
                } else if (mro.isMapDone() && !mro.isReduceDone()) {
                    mro.reducePlan.addAsLeaf(str);
                    mro.setReduceDone(true);
                } else {
                	int errCode = 2022;
                    String msg = "Both map and reduce phases have been done. This is unexpected while compiling.";
                    throw new PlanException(msg, errCode, PigException.BUG);
                }
            }
            for(int i=0;i<compiledInputs.length;i++){
                if(i==fragPlan) continue;
                MRPlan.connect(compiledInputs[i], curMROp);
            }
            
            if (!curMROp.isMapDone()) {
                curMROp.mapPlan.addAsLeaf(op);
            } else if (curMROp.isMapDone() && !curMROp.isReduceDone()) {
                curMROp.reducePlan.addAsLeaf(op);
            } else {
            	int errCode = 2022;
                String msg = "Both map and reduce phases have been done. This is unexpected while compiling.";
                throw new PlanException(msg, errCode, PigException.BUG);
            }
            List<List<PhysicalPlan>> joinPlans = op.getJoinPlans();
            if(joinPlans!=null)
                for (List<PhysicalPlan> joinPlan : joinPlans) {
                    if(joinPlan!=null)
                        for (PhysicalPlan plan : joinPlan) {
                            addUDFs(plan);
                        }
                }
            curMROp.setFrjoin(true);
            curMROp.setFragment(op.getFragment());
            curMROp.setReplFiles(op.getReplFiles());
        }catch(Exception e){
            int errCode = 2034;
            String msg = "Error compiling operator " + op.getClass().getSimpleName();
            throw new MRCompilerException(msg, errCode, PigException.BUG, e);
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
            pkg.setDistinct(true);
            pkg.setNumInps(1);
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
            int errCode = 2034;
            String msg = "Error compiling operator " + op.getClass().getSimpleName();
            throw new MRCompilerException(msg, errCode, PigException.BUG, e);
        }
    }

    @Override
    public void visitSort(POSort op) throws VisitorException {
        try{
            FileSpec fSpec = getTempFileSpec();
            MapReduceOper mro = endSingleInputPlanWithStr(fSpec);
            FileSpec quantFile = getTempFileSpec();
            int rp = op.getRequestedParallelism();
            Pair<Integer,Byte>[] fields = getSortCols(op);
            Pair<MapReduceOper, Integer> quantJobParallelismPair = 
                getQuantileJob(op, mro, fSpec, quantFile, rp, fields);
            curMROp = getSortJob(op, quantJobParallelismPair.first, fSpec, quantFile, 
                    quantJobParallelismPair.second, fields);
            
            if(op.isUDFComparatorUsed){
                curMROp.UDFs.add(op.getMSortFunc().getFuncSpec().toString());
            }
        }catch(Exception e){
            int errCode = 2034;
            String msg = "Error compiling operator " + op.getClass().getSimpleName();
            throw new MRCompilerException(msg, errCode, PigException.BUG, e);
        }
    }
    
    private Pair<Integer, Byte>[] getSortCols(POSort sort) throws PlanException, ExecException {
        List<PhysicalPlan> plans = sort.getSortPlans();
        if(plans!=null){
            Pair[] ret = new Pair[plans.size()]; 
            int i=-1;
            for (PhysicalPlan plan : plans) {
                if (((POProject)plan.getLeaves().get(0)).isStar()) return null;
                int first = ((POProject)plan.getLeaves().get(0)).getColumn();
                byte second = ((POProject)plan.getLeaves().get(0)).getResultType();
                ret[++i] = new Pair<Integer,Byte>(first,second);
            }
            return ret;
        }
        int errCode = 2026;
        String msg = "No expression plan found in POSort.";
        throw new PlanException(msg, errCode, PigException.BUG);
    }
    
    public MapReduceOper getSortJob(
            POSort sort,
            MapReduceOper quantJob,
            FileSpec lFile,
            FileSpec quantFile,
            int rp,
            Pair<Integer,Byte>[] fields) throws PlanException{
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
                int errCode = 2035;
                String msg = "Internal error. Could not compute key type of sort operator.";
                throw new PlanException(msg, errCode, PigException.BUG, ve);
            }
        }
        
        POLocalRearrange lr = new POLocalRearrange(new OperatorKey(scope,nig.getNextNodeId(scope)));
        try {
            lr.setIndex(0);
        } catch (ExecException e) {
        	int errCode = 2058;
        	String msg = "Unable to set index on newly created POLocalRearrange.";
            throw new PlanException(msg, errCode, PigException.BUG, e);
        }
        lr.setKeyType((fields == null || fields.length>1) ? DataType.TUPLE :
            keyType);
        lr.setPlans(eps1);
        lr.setResultType(DataType.TUPLE);
        mro.mapPlan.addAsLeaf(lr);
        
        mro.setMapDone(true);
        
        if (limit!=-1) {
       	    POPackageLite pkg_c = new POPackageLite(new OperatorKey(scope,nig.getNextNodeId(scope)));
       	    pkg_c.setKeyType((fields.length>1) ? DataType.TUPLE : keyType);
            pkg_c.setNumInps(1);
            //pkg.setResultType(DataType.TUPLE);
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
	        try {
                lr_c2.setIndex(0);
            } catch (ExecException e) {
            	int errCode = 2058;
            	String msg = "Unable to set index on newly created POLocalRearrange.";            	
                throw new PlanException(msg, errCode, PigException.BUG, e);
            }
	        lr_c2.setKeyType((fields.length>1) ? DataType.TUPLE : keyType);
	        lr_c2.setPlans(eps_c2);
	        lr_c2.setResultType(DataType.TUPLE);
	        mro.combinePlan.addAsLeaf(lr_c2);
        }
        
        POPackageLite pkg = new POPackageLite(new OperatorKey(scope,nig.getNextNodeId(scope)));
        pkg.setKeyType((fields == null || fields.length>1) ? DataType.TUPLE :
            keyType);
        pkg.setNumInps(1);
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

    public Pair<MapReduceOper,Integer> getQuantileJob(POSort inpSort, MapReduceOper prevJob, FileSpec lFile, FileSpec quantFile, int rp, Pair<Integer,Byte>[] fields) throws PlanException, VisitorException {
        FileSpec quantLdFilName = new FileSpec(lFile.getFileName(), new FuncSpec(RandomSampleLoader.class.getName()));
        MapReduceOper mro = startNew(quantLdFilName, prevJob);
        POSort sort = new POSort(inpSort.getOperatorKey(), inpSort
                .getRequestedParallelism(), null, inpSort.getSortPlans(),
                inpSort.getMAscCols(), inpSort.getMSortFunc());
        if(sort.isUDFComparatorUsed) {
            mro.UDFs.add(sort.getMSortFunc().getFuncSpec().toString());
        }
        
        List<PhysicalPlan> eps1 = new ArrayList<PhysicalPlan>();
        List<Boolean> flat1 = new ArrayList<Boolean>();
        // Set up the projections of the key columns 
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
            for (Pair<Integer,Byte> i : fields) {
                PhysicalPlan ep = new PhysicalPlan();
                POProject prj = new POProject(new OperatorKey(scope,nig.getNextNodeId(scope)));
                prj.setColumn(i.first);
                prj.setOverloaded(false);
                prj.setResultType(i.second);
                ep.add(prj);
                eps1.add(ep);
                flat1.add(true);
            }
        }
        // This foreach will pick the sort key columns from the RandomSampleLoader output 
        POForEach nfe1 = new POForEach(new OperatorKey(scope,nig.getNextNodeId(scope)),-1,eps1,flat1);
        mro.mapPlan.addAsLeaf(nfe1);
        
        // Now set up a POLocalRearrange which has "all" as the key and the output of the
        // foreach will be the "value" out of POLocalRearrange
        PhysicalPlan ep1 = new PhysicalPlan();
        ConstantExpression ce = new ConstantExpression(new OperatorKey(scope,nig.getNextNodeId(scope)));
        ce.setValue("all");
        ce.setResultType(DataType.CHARARRAY);
        ep1.add(ce);
        
        List<PhysicalPlan> eps = new ArrayList<PhysicalPlan>();
        eps.add(ep1);
        
        POLocalRearrange lr = new POLocalRearrange(new OperatorKey(scope,nig.getNextNodeId(scope)));
        try {
            lr.setIndex(0);
        } catch (ExecException e) {
        	int errCode = 2058;
        	String msg = "Unable to set index on newly created POLocalRearrange.";
            throw new PlanException(msg, errCode, PigException.BUG, e);
        }
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
        
        // Lets start building the plan which will have the sort
        // for the foreach
        PhysicalPlan fe2Plan = new PhysicalPlan();
        // Top level project which just projects the tuple which is coming 
        // from the foreach after the package
        POProject topPrj = new POProject(new OperatorKey(scope,nig.getNextNodeId(scope)));
        topPrj.setColumn(1);
        topPrj.setResultType(DataType.TUPLE);
        topPrj.setOverloaded(true);
        fe2Plan.add(topPrj);
        
        // the projections which will form sort plans
        List<PhysicalPlan> nesSortPlanLst = new ArrayList<PhysicalPlan>();
        if (fields == null) {
            PhysicalPlan ep = new PhysicalPlan();
            POProject prj = new POProject(new OperatorKey(scope,
                nig.getNextNodeId(scope)));
            prj.setStar(true);
            prj.setOverloaded(false);
            prj.setResultType(DataType.TUPLE);
            ep.add(prj);
            nesSortPlanLst.add(ep);
        } else {
            for (int i=0; i<fields.length;i++) {
                PhysicalPlan ep = new PhysicalPlan();
                POProject prj = new POProject(new OperatorKey(scope,nig.getNextNodeId(scope)));
                prj.setColumn(i);
                prj.setOverloaded(false);
                prj.setResultType(fields[i].second);
                ep.add(prj);
                nesSortPlanLst.add(ep);
            }
        }
        
        sort.setSortPlans(nesSortPlanLst);
        sort.setResultType(DataType.BAG);
        fe2Plan.add(sort);
        fe2Plan.connect(topPrj, sort);
        
        // The plan which will have a constant representing the
        // degree of parallelism for the final order by map-reduce job
        // this will either come from a "order by parallel x" in the script
        // or will be the default number of reducers for the cluster if
        // "parallel x" is not used in the script
        PhysicalPlan rpep = new PhysicalPlan();
        ConstantExpression rpce = new ConstantExpression(new OperatorKey(scope,nig.getNextNodeId(scope)));
        rpce.setRequestedParallelism(rp);
        int val = rp;
        if(val<=0){
            ExecutionEngine eng = pigContext.getExecutionEngine();
            if(eng instanceof HExecutionEngine){
                try {
                    val = Math.round(0.9f * ((HExecutionEngine)eng).getJobClient().getDefaultReduces());
                    if(val<=0)
                        val = 1;
                } catch (IOException e) {
                    int errCode = 6015;
                    String msg = "Problem getting the default number of reduces from the Job Client.";
                    throw new MRCompilerException(msg, errCode, PigException.REMOTE_ENVIRONMENT, e);
                }
            } else {
            	val = 1; // local mode, set it to 1
            }
        }
        int parallelismForSort = (rp <= 0 ? val : rp);
        rpce.setValue(parallelismForSort);
        
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
        
        // Let's connect the output from the foreach containing
        // number of quantiles and the sorted bag of samples to
        // another foreach with the FindQuantiles udf. The input
        // to the FindQuantiles udf is a project(*) which takes the 
        // foreach input and gives it to the udf
        PhysicalPlan ep4 = new PhysicalPlan();
        POProject prjStar4 = new POProject(new OperatorKey(scope,nig.getNextNodeId(scope)));
        prjStar4.setResultType(DataType.TUPLE);
        prjStar4.setStar(true);
        ep4.add(prjStar4);
        
        List<PhysicalOperator> ufInps = new ArrayList<PhysicalOperator>();
        ufInps.add(prjStar4);
        // Turn the asc/desc array into an array of strings so that we can pass it
        // to the FindQuantiles function.
        List<Boolean> ascCols = inpSort.getMAscCols();
        String[] ascs = new String[ascCols.size()];
        for (int i = 0; i < ascCols.size(); i++) ascs[i] = ascCols.get(i).toString();
        // check if user defined comparator is used in the sort, if so
        // prepend the name of the comparator as the first fields in the
        // constructor args array to the FindQuantiles udf
        String[] ctorArgs = ascs;
        if(sort.isUDFComparatorUsed) {
            String userComparatorFuncSpec = sort.getMSortFunc().getFuncSpec().toString();
            ctorArgs = new String[ascs.length + 1];
            ctorArgs[0] = USER_COMPARATOR_MARKER + userComparatorFuncSpec;
            for(int j = 0; j < ascs.length; j++) {
                ctorArgs[j+1] = ascs[j];
            }
        }
        
        POUserFunc uf = new POUserFunc(new OperatorKey(scope,nig.getNextNodeId(scope)), -1, ufInps, 
            new FuncSpec(FindQuantiles.class.getName(), ctorArgs));
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
        mro.requestedParallelism = 1;
        return new Pair<MapReduceOper, Integer>(mro, parallelismForSort);
    }

    static class LastInputStreamingOptimizer extends MROpPlanVisitor {
        
        Log log = LogFactory.getLog(this.getClass());
        String chunkSize;
        LastInputStreamingOptimizer(MROperPlan plan, String chunkSize) {
            super(plan, new DepthFirstWalker<MapReduceOper, MROperPlan>(plan));
            this.chunkSize = chunkSize;
        }
        
        /**indTupIter
         * Look for pattern POPackage->POForEach(if both are flatten), change it to POJoinPackage
         * We can avoid materialize the input and construct the result of join on the fly
         * 
         * @param mr - map-reduce plan to optimize
         */ 
        @Override
        public void visitMROp(MapReduceOper mr) throws VisitorException {
            // Only optimize:
            // 1. POPackage->POForEach is the root of reduce plan
            // 2. POUnion is the leaf of map plan (so that we exclude distinct, sort...)
            // 3. No combiner plan
            // 4. POForEach nested plan only contains POProject in any depth
            // 5. Inside POForEach, all occurrences of the last input are flattened
            
            if (mr.mapPlan.isEmpty()) return;
            if (mr.reducePlan.isEmpty()) return;

            // Check combiner plan
            if (!mr.combinePlan.isEmpty()) {
                return;
            }
            
            // Check map plan
            List<PhysicalOperator> mpLeaves = mr.mapPlan.getLeaves();
            if (mpLeaves.size()!=1) {
                return;
            }
            PhysicalOperator op = mpLeaves.get(0);
            
            if (!(op instanceof POUnion)) {
                return;
            }
            
            // Check reduce plan
            List<PhysicalOperator> mrRoots = mr.reducePlan.getRoots();
            if (mrRoots.size()!=1) {
                return;
            }
            
            op = mrRoots.get(0);
            if (!(op instanceof POPackage)) {
                return;
            }
            POPackage pack = (POPackage)op;
            
            List<PhysicalOperator> sucs = mr.reducePlan.getSuccessors(pack);
            if (sucs.size()!=1) {
                return;
            }
            
            op = sucs.get(0);
            boolean lastInputFlattened = true;
            boolean allSimple = true;
            if (op instanceof POForEach)
            {
                POForEach forEach = (POForEach)op;
                List<PhysicalPlan> planList = forEach.getInputPlans();
                List<Boolean> flatten = forEach.getToBeFlattened();
                POProject projOfLastInput = null;
                int i = 0;
                // check all nested foreach plans
                // 1. If it is simple projection
                // 2. If last input is all flattened
                for (PhysicalPlan p:planList)
                {
                    PhysicalOperator opProj = p.getRoots().get(0);
                    if (!(opProj instanceof POProject))
                    {
                        allSimple = false;
                        break;
                    }
                    POProject proj = (POProject)opProj;
                    // the project should just be for one column
                    // from the input
                    if(proj.getColumns().size() != 1) {
                        allSimple = false;
                        break;
                    }
                    
                    try {
                        // if input to project is the last input
                        if (proj.getColumn() == pack.getNumInps())
                        {
                            // if we had already seen another project
                            // which was also for the last input, then
                            // we might be trying to flatten twice on the
                            // last input in which case we can't optimize by
                            // just streaming the tuple to those projects
                            // IMPORTANT NOTE: THIS WILL NEED TO CHANGE WHEN WE
                            // OPTIMIZE BUILTINS LIKE SUM() AND COUNT() TO
                            // TAKE IN STREAMING INPUT
                            if(projOfLastInput != null) {
                                allSimple = false;
                                break;
                            }
                            projOfLastInput = proj;
                            // make sure the project is on a bag which needs to be
                            // flattened
                            if (!flatten.get(i) || proj.getResultType() != DataType.BAG)
                            {
                                lastInputFlattened = false;
                                break;
                            }
                        }
                    } catch (ExecException e) {
                        int errCode = 2069;
                        String msg = "Error during map reduce compilation. Problem in accessing column from project operator.";
                        throw new MRCompilerException(msg, errCode, PigException.BUG, e);
                    }
                    
                    // if all deeper operators are all project
                    PhysicalOperator succ = p.getSuccessors(proj)!=null?p.getSuccessors(proj).get(0):null;
                    while (succ!=null)
                    {
                        if (!(succ instanceof POProject))
                        {
                            allSimple = false;
                            break;
                        }
                        // make sure successors of the last project also project bags
                        // we will be changing it to project tuples
                        if(proj == projOfLastInput && ((POProject)succ).getResultType() != DataType.BAG) {
                            allSimple = false;
                            break;
                        }
                        succ = p.getSuccessors(succ)!=null?p.getSuccessors(succ).get(0):null;
                    }
                    i++;
                    if (allSimple==false)
                        break;
                }
                
                if (lastInputFlattened && allSimple && projOfLastInput != null)
                {
                    // Now we can optimize the map-reduce plan
                    // Replace POPackage->POForeach to POJoinPackage
                    replaceWithPOJoinPackage(mr.reducePlan, pack, forEach, chunkSize);
                }
            }
        }

        public static void replaceWithPOJoinPackage(PhysicalPlan plan,
                POPackage pack, POForEach forEach, String chunkSize) throws VisitorException {
            String scope = pack.getOperatorKey().scope;
            NodeIdGenerator nig = NodeIdGenerator.getGenerator();
            POJoinPackage joinPackage;
            joinPackage = new POJoinPackage(
                        new OperatorKey(scope, nig.getNextNodeId(scope)), 
                        -1, pack, forEach);
            joinPackage.setChunkSize(Long.parseLong(chunkSize));
            List<PhysicalOperator> succs = plan.getSuccessors(forEach);
            if (succs!=null)
            {
                if (succs.size()!=1)
                {
                    int errCode = 2028;
                    String msg = "ForEach can only have one successor. Found " + succs.size() + " successors.";
                    throw new MRCompilerException(msg, errCode, PigException.BUG);
                }
            }
            plan.remove(pack);
            
            try {
                plan.replace(forEach, joinPackage);
            } catch (PlanException e) {
                int errCode = 2029;
                String msg = "Error rewriting POJoinPackage.";
                throw new MRCompilerException(msg, errCode, PigException.BUG, e);
            }
            
            LogFactory.
            getLog(LastInputStreamingOptimizer.class).info("Rewrite: POPackage->POForEach to POJoinPackage");
        }

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
                int errCode = 2024; 
                String msg = "Expected reduce to have single leaf. Found " + mpLeaves.size() + " leaves.";
                throw new MRCompilerException(msg, errCode, PigException.BUG);
            }
            PhysicalOperator mpLeaf = mpLeaves.get(0);
            if (!(mpLeaf instanceof POStore)) {
                int errCode = 2025;
                String msg = "Expected leaf of reduce plan to " +
                    "always be POStore. Found " + mpLeaf.getClass().getSimpleName();
                throw new MRCompilerException(msg, errCode, PigException.BUG);
            }
            List<PhysicalOperator> preds =
                mr.reducePlan.getPredecessors(mpLeaf);
            if (preds == null) return;
            if (preds.size() > 1) {
                int errCode = 2030;
                String msg ="Expected reduce plan leaf to have a single predecessor. Found " + preds.size() + " predecessors.";
                throw new MRCompilerException(msg, errCode, PigException.BUG);
            }
            PhysicalOperator pred = preds.get(0);
            if (!(pred instanceof POLocalRearrange)) return;

            // Next question, does the next MROper have an empty map?
            List<MapReduceOper> succs = mPlan.getSuccessors(mr);
            if (succs == null) {
                int errCode = 2031;
                String msg = "Found map reduce operator with POLocalRearrange as"
                    + " last oper but with no succesor.";
                throw new MRCompilerException(msg, errCode, PigException.BUG);
            }
            if (succs.size() > 1) {
                int errCode = 2032;
                String msg = "Expected map reduce operator to have a single successor. Found " + succs.size() + " successors.";
                throw new MRCompilerException(msg, errCode, PigException.BUG);
            }
            MapReduceOper succ = succs.get(0);
            List<PhysicalOperator> succMpLeaves = succ.mapPlan.getLeaves();
            List<PhysicalOperator> succMpRoots = succ.mapPlan.getRoots();
            if (succMpLeaves == null || succMpLeaves.size() > 1 ||
                    succMpRoots == null || succMpRoots.size() > 1 ||
                    succMpLeaves.get(0) != succMpRoots.get(0)) {
            		messageCollector.collect("Expected to find subsequent map " +
                    "with just a load, but didn't",
                    MessageType.Warning, PigWarning.DID_NOT_FIND_LOAD_ONLY_MAP_PLAN);
                return;
            }
            PhysicalOperator load = succMpRoots.get(0);

            try {
                mr.reducePlan.removeAndReconnect(pred);
                succ.mapPlan.add(pred);
                succ.mapPlan.connect(load, pred);
            } catch (PlanException pe) {
                int errCode = 2033;
                String msg = "Problems in rearranging map reduce operators in plan.";
                throw new MRCompilerException(msg, errCode, PigException.BUG, pe);
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
            if (mr.limit!=-1 && mr.requestedParallelism!=1)
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
                    int errCode = 2024; 
                    String msg = "Expected reduce to have single leaf. Found " + mpLeaves.size() + " leaves.";
                    throw new MRCompilerException(msg, errCode, PigException.BUG);
                }
                PhysicalOperator mpLeaf = mpLeaves.get(0);
                if (!(mpLeaf instanceof POStore)) {
                    int errCode = 2025;
                    String msg = "Expected leaf of reduce plan to " +
                        "always be POStore. Found " + mpLeaf.getClass().getSimpleName();
                    throw new MRCompilerException(msg, errCode, PigException.BUG);
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
