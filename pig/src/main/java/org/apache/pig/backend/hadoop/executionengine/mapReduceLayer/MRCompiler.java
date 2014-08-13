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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.CollectableLoadFunc;
import org.apache.pig.ExecType;
import org.apache.pig.FuncSpec;
import org.apache.pig.IndexableLoadFunc;
import org.apache.pig.LoadFunc;
import org.apache.pig.OrderedLoadFunc;
import org.apache.pig.PigConfiguration;
import org.apache.pig.PigException;
import org.apache.pig.PigWarning;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROpPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.ScalarPhyFinder;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.UDFFinder;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.ConstantExpression;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POProject;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POUserFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.JoinPackager;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.LitePackager;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POCollectedGroup;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POCounter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POCross;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.PODistinct;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFRJoin;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFilter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POGlobalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLimit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POMergeCogroup;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POMergeJoin;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.PONative;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPartitionRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.PORank;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSkewedJoin;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSort;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSplit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStream;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POUnion;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.Packager;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.Packager.PackageType;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.backend.hadoop.executionengine.shims.HadoopShims;
import org.apache.pig.backend.hadoop.executionengine.util.MapRedUtil;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.builtin.DefaultIndexableLoader;
import org.apache.pig.impl.builtin.FindQuantiles;
import org.apache.pig.impl.builtin.GetMemNumRows;
import org.apache.pig.impl.builtin.PartitionSkewedKeys;
import org.apache.pig.impl.builtin.PoissonSampleLoader;
import org.apache.pig.impl.builtin.RandomSampleLoader;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.plan.CompilationMessageCollector;
import org.apache.pig.impl.plan.CompilationMessageCollector.MessageType;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.Operator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.OperatorPlan;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.CompilerUtils;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.impl.util.UriUtil;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.newplan.logical.relational.LOJoin;

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

    private UDFFinder udfFinder;

    private CompilationMessageCollector messageCollector = null;

    private Map<PhysicalOperator,MapReduceOper> phyToMROpMap;

    public static final String USER_COMPARATOR_MARKER = "user.comparator.func:";

    private static final Log LOG = LogFactory.getLog(MRCompiler.class);

    public static final String FILE_CONCATENATION_THRESHOLD = "pig.files.concatenation.threshold";
    public static final String OPTIMISTIC_FILE_CONCATENATION = "pig.optimistic.files.concatenation";

    private int fileConcatenationThreshold = 100;
    private boolean optimisticFileConcatenation = false;

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
        udfFinder = new UDFFinder();
        List<PhysicalOperator> roots = plan.getRoots();
        if((roots == null) || (roots.size() <= 0)) {
            int errCode = 2053;
            String msg = "Internal error. Did not find roots in the physical plan.";
            throw new MRCompilerException(msg, errCode, PigException.BUG);
        }
        scope = roots.get(0).getOperatorKey().getScope();
        messageCollector = new CompilationMessageCollector() ;
        phyToMROpMap = new HashMap<PhysicalOperator, MapReduceOper>();

        fileConcatenationThreshold = Integer.parseInt(pigContext.getProperties()
                .getProperty(FILE_CONCATENATION_THRESHOLD, "100"));
        optimisticFileConcatenation = pigContext.getProperties().getProperty(
                OPTIMISTIC_FILE_CONCATENATION, "false").equals("true");
        LOG.info("File concatenation threshold: " + fileConcatenationThreshold
                + " optimistic? " + optimisticFileConcatenation);
    }

    public void aggregateScalarsFiles() throws PlanException, IOException {
        List<MapReduceOper> mrOpList = new ArrayList<MapReduceOper>();
        for(MapReduceOper mrOp: MRPlan) {
            mrOpList.add(mrOp);
        }

        Configuration conf =
            ConfigurationUtil.toConfiguration(pigContext.getProperties());
        boolean combinable = !conf.getBoolean("pig.noSplitCombination", false);

        Set<FileSpec> seen = new HashSet<FileSpec>();

        for(MapReduceOper mro_scalar_consumer: mrOpList) {
            for(PhysicalOperator scalar: mro_scalar_consumer.scalars) {
                MapReduceOper mro_scalar_producer = phyToMROpMap.get(scalar);
                if (scalar instanceof POStore) {
                    FileSpec oldSpec = ((POStore)scalar).getSFile();
                    if( seen.contains(oldSpec) ) {
                      continue;
                    }
                    seen.add(oldSpec);
                    if ( combinable
                         && (mro_scalar_producer.reducePlan.isEmpty() ?
                              hasTooManyInputFiles(mro_scalar_producer, conf)
                              : (mro_scalar_producer.requestedParallelism >= fileConcatenationThreshold))) {
                        PhysicalPlan pl = mro_scalar_producer.reducePlan.isEmpty() ?
                                            mro_scalar_producer.mapPlan : mro_scalar_producer.reducePlan;
                        FileSpec newSpec = getTempFileSpec();

                        // replace oldSpec in mro with newSpec
                        new FindStoreNameVisitor(pl, newSpec, oldSpec).visit();
                        seen.add(newSpec);

                        POStore newSto = getStore();
                        newSto.setSFile(oldSpec);
                        MapReduceOper catMROp = getConcatenateJob(newSpec, mro_scalar_producer, newSto);
                        MRPlan.connect(mro_scalar_producer, catMROp);

                        // Need to add it to the PhysicalPlan and phyToMROpMap
                        // so that softlink can be created
                        phyToMROpMap.put(newSto, catMROp);
                        plan.add(newSto);

                        for (PhysicalOperator succ :
                                plan.getSoftLinkSuccessors(scalar).toArray(new PhysicalOperator[0])) {
                            plan.createSoftLink(newSto, succ);
                            plan.removeSoftLink(scalar, succ);
                        }
                    }
                }
            }
        }
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
    @Override
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

        if (!pigContext.inIllustrator)
        for (PhysicalOperator op : leaves) {
            if (!(op instanceof POStore)) {
                int errCode = 2025;
                String msg = "Expected leaf of reduce plan to " +
                    "always be POStore. Found " + op.getClass().getSimpleName();
                throw new MRCompilerException(msg, errCode, PigException.BUG);
            }
        }

        // get all stores and nativeMR operators, sort them in order(operator id)
        // and compile their plans
        List<POStore> stores = PlanHelper.getPhysicalOperators(plan, POStore.class);
        List<PONative> nativeMRs= PlanHelper.getPhysicalOperators(plan, PONative.class);
        List<PhysicalOperator> ops;
        if (!pigContext.inIllustrator) {
            ops = new ArrayList<PhysicalOperator>(stores.size() + nativeMRs.size());
            ops.addAll(stores);
        } else {
            ops = new ArrayList<PhysicalOperator>(leaves.size() + nativeMRs.size());
            ops.addAll(leaves);
        }
        ops.addAll(nativeMRs);
        Collections.sort(ops);

        for (PhysicalOperator op : ops) {
            compile(op);
        }

        return MRPlan;
    }

    public void connectSoftLink() throws PlanException, IOException {
        for (PhysicalOperator op : plan) {
            if (plan.getSoftLinkPredecessors(op)!=null) {
                for (PhysicalOperator pred : plan.getSoftLinkPredecessors(op)) {
                    MapReduceOper from = phyToMROpMap.get(pred);
                    MapReduceOper to = phyToMROpMap.get(op);
                    if (from==to)
                        continue;
                    if (MRPlan.getPredecessors(to)==null || !MRPlan.getPredecessors(to).contains(from)) {
                        MRPlan.connect(from, to);
                    }
                }
            }
        }
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
        if(op instanceof PONative){
            // the predecessor (store) has already been processed
            // don't process it again
        }
        else if (predecessors != null && predecessors.size() > 0) {
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
                MapReduceOper oper = null;
                if(p instanceof POStore || p instanceof PONative){
                    oper = phyToMROpMap.get(p);
                }else{
                    int errCode = 2126;
                    String msg = "Predecessor of load should be a store or mapreduce operator. Got "+p.getClass();
                    throw new PlanException(msg, errCode, PigException.BUG);
                }

                // Need new operator
                curMROp = getMROp();
                curMROp.mapPlan.add(op);
                MRPlan.add(curMROp);

                plan.disconnect(op, p);
                MRPlan.connect(oper, curMROp);
                phyToMROpMap.put(op, curMROp);
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
            if (op !=null && op instanceof POLoad)
            {
                if (((POLoad)op).getLFile()!=null && ((POLoad)op).getLFile().getFuncSpec()!=null)
                    curMROp.UDFs.add(((POLoad)op).getLFile().getFuncSpec().toString());
            }
            MRPlan.add(curMROp);
            phyToMROpMap.put(op, curMROp);
            return;
        }

        //Now we have the inputs compiled. Do something
        //with the input oper op.
        op.visit(this);
        if(op.getRequestedParallelism() > curMROp.requestedParallelism ) {
            // we don't want to change prallelism for skewed join due to sampling
            // and pre-allocated reducers for skewed keys
            if (!curMROp.isSkewedJoin()) {
                curMROp.requestedParallelism = op.getRequestedParallelism();
            }
        }
        compiledInputs = prevCompInp;
    }

    private MapReduceOper getMROp(){
        return new MapReduceOper(new OperatorKey(scope,nig.getNextNodeId(scope)));
    }

    private NativeMapReduceOper getNativeMROp(String mrJar, String[] parameters) {
        return new NativeMapReduceOper(new OperatorKey(scope,nig.getNextNodeId(scope)), mrJar, parameters);
    }

    private POLoad getLoad(){
        POLoad ld = new POLoad(new OperatorKey(scope,nig.getNextNodeId(scope)));
        ld.setPc(pigContext);
        ld.setIsTmpLoad(true);
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

    private void addToMap(PhysicalOperator op) throws PlanException, IOException{

        if (compiledInputs.length == 1) {
            //For speed
            MapReduceOper mro = compiledInputs[0];
            if (!mro.isMapDone()) {
                mro.mapPlan.addAsLeaf(op);
            } else if (mro.isMapDone() && !mro.isReduceDone()) {
                FileSpec fSpec = getTempFileSpec();

                POStore st = getStore();
                st.setSFile(fSpec);
                mro.reducePlan.addAsLeaf(st);
                mro.setReduceDone(true);
                mro = startNew(fSpec, mro);
                mro.mapPlan.addAsLeaf(op);
                compiledInputs[0] = mro;
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


    /**
     * Force an end to the current map reduce job with a store into a temporary
     * file.
     * @param fSpec Temp file to force a store into.
     * @return MR operator that now is finished with a store.
     * @throws PlanException
     */
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
        return new FileSpec(FileLocalizer.getTemporaryPath(pigContext).toString(),
                new FuncSpec(Utils.getTmpFileCompressorName(pigContext)));
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
            for (String udf:rmro.UDFs)
            {
                if (!mergedMap.UDFs.contains(udf))
                    mergedMap.UDFs.add(udf);
            }
            // We also need to change scalar marking
            for(PhysicalOperator physOp: rmro.scalars) {
                if(!mergedMap.scalars.contains(physOp)) {
                    mergedMap.scalars.add(physOp);
                }
            }
            Set<PhysicalOperator> opsToChange = new HashSet<PhysicalOperator>();
            for (Map.Entry<PhysicalOperator, MapReduceOper> entry : phyToMROpMap.entrySet()) {
                if (entry.getValue()==rmro) {
                    opsToChange.add(entry.getKey());
                }
            }
            for (PhysicalOperator op : opsToChange) {
                phyToMROpMap.put(op, mergedMap);
            }

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

    private void processUDFs(PhysicalPlan plan) throws VisitorException{
        if(plan!=null){
            //Process Scalars (UDF with referencedOperators)
            ScalarPhyFinder scalarPhyFinder = new ScalarPhyFinder(plan);
            scalarPhyFinder.visit();
            curMROp.scalars.addAll(scalarPhyFinder.getScalars());

            //Process UDFs
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
    @Override
    public void visitSplit(POSplit op) throws VisitorException{
        try{
            FileSpec fSpec = op.getSplitStore();
            MapReduceOper mro = endSingleInputPlanWithStr(fSpec);
            mro.setSplitter(true);
            splitsSeen.put(op.getOperatorKey(), mro);
            curMROp = startNew(fSpec, mro);
            phyToMROpMap.put(op, curMROp);
        }catch(Exception e){
            int errCode = 2034;
            String msg = "Error compiling operator " + op.getClass().getSimpleName();
            throw new MRCompilerException(msg, errCode, PigException.BUG, e);
        }
    }

    @Override
    public void visitLoad(POLoad op) throws VisitorException{
        try{
            nonBlocking(op);
            phyToMROpMap.put(op, curMROp);
        }catch(Exception e){
            int errCode = 2034;
            String msg = "Error compiling operator " + op.getClass().getSimpleName();
            throw new MRCompilerException(msg, errCode, PigException.BUG, e);
        }
    }

    @Override
    public void visitNative(PONative op) throws VisitorException{
        // We will explode the native operator here to add a new MROper for native Mapreduce job
        try{
            // add a map reduce boundary
            MapReduceOper nativeMROper = getNativeMROp(op.getNativeMRjar(), op.getParams());
            MRPlan.add(nativeMROper);
            MRPlan.connect(curMROp, nativeMROper);
            phyToMROpMap.put(op, nativeMROper);
            curMROp = nativeMROper;
        }catch(Exception e){
            int errCode = 2034;
            String msg = "Error compiling operator " + op.getClass().getSimpleName();
            throw new MRCompilerException(msg, errCode, PigException.BUG, e);
        }
    }

    @Override
    public void visitStore(POStore op) throws VisitorException{
        try{
            nonBlocking(op);
            phyToMROpMap.put(op, curMROp);
            if (op.getSFile()!=null && op.getSFile().getFuncSpec()!=null)
                curMROp.UDFs.add(op.getSFile().getFuncSpec().toString());
        }catch(Exception e){
            int errCode = 2034;
            String msg = "Error compiling operator " + op.getClass().getSimpleName();
            throw new MRCompilerException(msg, errCode, PigException.BUG, e);
        }
    }

    @Override
    public void visitFilter(POFilter op) throws VisitorException{
        try{
            nonBlocking(op);
            processUDFs(op.getPlan());
            phyToMROpMap.put(op, curMROp);
        }catch(Exception e){
            int errCode = 2034;
            String msg = "Error compiling operator " + op.getClass().getSimpleName();
            throw new MRCompilerException(msg, errCode, PigException.BUG, e);
        }
    }

    @Override
    public void visitCross(POCross op) throws VisitorException {
        try{
            nonBlocking(op);
            phyToMROpMap.put(op, curMROp);
        }catch(Exception e){
            int errCode = 2034;
            String msg = "Error compiling operator " + op.getClass().getSimpleName();
            throw new MRCompilerException(msg, errCode, PigException.BUG, e);
        }
    }

    @Override
    public void visitStream(POStream op) throws VisitorException{
        try{
            nonBlocking(op);
            phyToMROpMap.put(op, curMROp);
        }catch(Exception e){
            int errCode = 2034;
            String msg = "Error compiling operator " + op.getClass().getSimpleName();
            throw new MRCompilerException(msg, errCode, PigException.BUG, e);
        }
    }

    @Override
    public void visitLimit(POLimit op) throws VisitorException{
        try{
            MapReduceOper mro = compiledInputs[0];
            mro.limit = op.getLimit();
            if (op.getLimitPlan() != null) {
                processUDFs(op.getLimitPlan());
                mro.limitPlan = op.getLimitPlan();
            }
            if (!mro.isMapDone()) {
                // if map plan is open, add a limit for optimization, eventually we
                // will add another limit to reduce plan
                if (!pigContext.inIllustrator)
                {
                    mro.mapPlan.addAsLeaf(op);
                    mro.setMapDone(true);
                }

                if (mro.reducePlan.isEmpty())
                {
                    MRUtil.simpleConnectMapToReduce(mro, scope, nig);
                    mro.requestedParallelism = 1;
                    if (!pigContext.inIllustrator) {
                        POLimit pLimit2 = new POLimit(new OperatorKey(scope,nig.getNextNodeId(scope)));
                        pLimit2.setLimit(op.getLimit());
                        pLimit2.setLimitPlan(op.getLimitPlan());
                        mro.reducePlan.addAsLeaf(pLimit2);
                    } else {
                        mro.reducePlan.addAsLeaf(op);
                    }
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
            phyToMROpMap.put(op, mro);
        }catch(Exception e){
            int errCode = 2034;
            String msg = "Error compiling operator " + op.getClass().getSimpleName();
            throw new MRCompilerException(msg, errCode, PigException.BUG, e);
        }
    }

    @Override
    public void visitLocalRearrange(POLocalRearrange op) throws VisitorException {
        try{
            addToMap(op);
            List<PhysicalPlan> plans = op.getPlans();
            if(plans!=null)
                for(PhysicalPlan ep : plans)
                    processUDFs(ep);
            phyToMROpMap.put(op, curMROp);
        }catch(Exception e){
            int errCode = 2034;
            String msg = "Error compiling operator " + op.getClass().getSimpleName();
            throw new MRCompilerException(msg, errCode, PigException.BUG, e);
        }
    }

    @Override
    public void visitCollectedGroup(POCollectedGroup op) throws VisitorException {

        if(!curMROp.mapDone){

            List<PhysicalOperator> roots = curMROp.mapPlan.getRoots();
            if(roots.size() != 1){
                int errCode = 2171;
                String errMsg = "Expected one but found more then one root physical operator in physical plan.";
                throw new MRCompilerException(errMsg,errCode,PigException.BUG);
            }

            PhysicalOperator phyOp = roots.get(0);
            if(! (phyOp instanceof POLoad)){
                int errCode = 2172;
                String errMsg = "Expected physical operator at root to be POLoad. Found : "+phyOp.getClass().getCanonicalName();
                throw new MRCompilerException(errMsg,errCode,PigException.BUG);
            }


            LoadFunc loadFunc = ((POLoad)phyOp).getLoadFunc();
            try {
                if(!(CollectableLoadFunc.class.isAssignableFrom(loadFunc.getClass()))){
                    int errCode = 2249;
                    throw new MRCompilerException("While using 'collected' on group; data must be loaded via loader implementing CollectableLoadFunc.", errCode);
                }
                ((CollectableLoadFunc)loadFunc).ensureAllKeyInstancesInSameSplit();
            } catch (MRCompilerException e){
                throw (e);
            } catch (IOException e) {
                int errCode = 2034;
                String msg = "Error compiling operator " + op.getClass().getSimpleName();
                throw new MRCompilerException(msg, errCode, PigException.BUG, e);
            }

            try{
                nonBlocking(op);
                List<PhysicalPlan> plans = op.getPlans();
                if(plans!=null)
                    for(PhysicalPlan ep : plans)
                        processUDFs(ep);
                phyToMROpMap.put(op, curMROp);
            }catch(Exception e){
                int errCode = 2034;
                String msg = "Error compiling operator " + op.getClass().getSimpleName();
                throw new MRCompilerException(msg, errCode, PigException.BUG, e);
            }
        }
        else if(!curMROp.reduceDone){
            int errCode=2250;
            String msg = "Blocking operators are not allowed before Collected Group. Consider dropping using 'collected'.";
            throw new MRCompilerException(msg, errCode, PigException.BUG);
        }
        else{
            int errCode = 2022;
            String msg = "Both map and reduce phases have been done. This is unexpected while compiling.";
            throw new MRCompilerException(msg, errCode, PigException.BUG);
        }

    }

    @Override
    public void visitPOForEach(POForEach op) throws VisitorException{
        try{
            nonBlocking(op);
            List<PhysicalPlan> plans = op.getInputPlans();
            if(plans!=null)
                for (PhysicalPlan plan : plans) {
                    processUDFs(plan);
                }
            phyToMROpMap.put(op, curMROp);
        }catch(Exception e){
            int errCode = 2034;
            String msg = "Error compiling operator " + op.getClass().getSimpleName();
            throw new MRCompilerException(msg, errCode, PigException.BUG, e);
        }
    }

    @Override
    public void visitGlobalRearrange(POGlobalRearrange op) throws VisitorException{
        try{
            blocking(op);
            curMROp.customPartitioner = op.getCustomPartitioner();
            phyToMROpMap.put(op, curMROp);
        }catch(Exception e){
            int errCode = 2034;
            String msg = "Error compiling operator " + op.getClass().getSimpleName();
            throw new MRCompilerException(msg, errCode, PigException.BUG, e);
        }
    }

    @Override
    public void visitPackage(POPackage op) throws VisitorException{
        try{
            nonBlocking(op);
            phyToMROpMap.put(op, curMROp);
            if (op.getPkgr().getPackageType() == PackageType.JOIN) {
                curMROp.markRegularJoin();
            } else if (op.getPkgr().getPackageType() == PackageType.GROUP) {
                if (op.getNumInps() == 1) {
                    curMROp.markGroupBy();
                } else if (op.getNumInps() > 1) {
                    curMROp.markCogroup();
                }
            }
        }catch(Exception e){
            int errCode = 2034;
            String msg = "Error compiling operator " + op.getClass().getSimpleName();
            throw new MRCompilerException(msg, errCode, PigException.BUG, e);
        }
    }

    @Override
    public void visitUnion(POUnion op) throws VisitorException{
        try{
            nonBlocking(op);
            phyToMROpMap.put(op, curMROp);
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


            curMROp = phyToMROpMap.get(op.getInputs().get(op.getFragment()));
            for(int i=0;i<compiledInputs.length;i++){
                MapReduceOper mro = compiledInputs[i];
                if(curMROp.equals(mro))
                    continue;
                POStore str = getStore();
                str.setSFile(replFiles[i]);

                Configuration conf =
                    ConfigurationUtil.toConfiguration(pigContext.getProperties());
                boolean combinable = !conf.getBoolean("pig.noSplitCombination", false);

                if (!mro.isMapDone()) {
                    if (combinable && hasTooManyInputFiles(mro, conf)) {
                        POStore tmpSto = getStore();
                        FileSpec fSpec = getTempFileSpec();
                        tmpSto.setSFile(fSpec);
                        mro.mapPlan.addAsLeaf(tmpSto);
                        mro.setMapDoneSingle(true);
                        MapReduceOper catMROp = getConcatenateJob(fSpec, mro, str);
                        MRPlan.connect(catMROp, curMROp);
                    } else {
                        mro.mapPlan.addAsLeaf(str);
                        mro.setMapDoneSingle(true);
                        MRPlan.connect(mro, curMROp);
                    }
                } else if (mro.isMapDone() && !mro.isReduceDone()) {
                    if (combinable && (mro.requestedParallelism >= fileConcatenationThreshold)) {
                        POStore tmpSto = getStore();
                        FileSpec fSpec = getTempFileSpec();
                        tmpSto.setSFile(fSpec);
                        mro.reducePlan.addAsLeaf(tmpSto);
                        mro.setReduceDone(true);
                        MapReduceOper catMROp = getConcatenateJob(fSpec, mro, str);
                        MRPlan.connect(catMROp, curMROp);
                    } else {
                        mro.reducePlan.addAsLeaf(str);
                        mro.setReduceDone(true);
                        MRPlan.connect(mro, curMROp);
                    }
                } else {
                    int errCode = 2022;
                    String msg = "Both map and reduce phases have been done. This is unexpected while compiling.";
                    throw new PlanException(msg, errCode, PigException.BUG);
                }
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
                            processUDFs(plan);
                        }
                }
            phyToMROpMap.put(op, curMROp);
        }catch(Exception e){
            int errCode = 2034;
            String msg = "Error compiling operator " + op.getClass().getSimpleName();
            throw new MRCompilerException(msg, errCode, PigException.BUG, e);
        }
    }

    @SuppressWarnings("unchecked")
    private boolean hasTooManyInputFiles(MapReduceOper mro, Configuration conf) {
        if (pigContext == null || pigContext.getExecType() == ExecType.LOCAL) {
            return false;
        }

        if (mro instanceof NativeMapReduceOper) {
            return optimisticFileConcatenation ? false : true;
        }

        PhysicalPlan mapPlan = mro.mapPlan;

        List<PhysicalOperator> roots = mapPlan.getRoots();
        if (roots == null || roots.size() == 0) return false;

        int numFiles = 0;
        boolean ret = false;
        try {
            for (PhysicalOperator root : roots) {
                POLoad ld = (POLoad) root;
                String fileName = ld.getLFile().getFileName();

                if(UriUtil.isHDFSFile(fileName)){
                    // Only if the input is an hdfs file, this optimization is
                    // useful (to reduce load on namenode)

                    //separate out locations separated by comma
                    String [] locations = LoadFunc.getPathStrings(fileName);
                    for(String location : locations){
                        if(!UriUtil.isHDFSFile(location))
                            continue;
                        Path path = new Path(location);
                        FileSystem fs = path.getFileSystem(conf);
                        if (fs.exists(path)) {
                            LoadFunc loader = (LoadFunc) PigContext
                            .instantiateFuncFromSpec(ld.getLFile()
                                    .getFuncSpec());
                            Job job = new Job(conf);
                            loader.setUDFContextSignature(ld.getSignature());
                            loader.setLocation(location, job);
                            InputFormat inf = loader.getInputFormat();
                            List<InputSplit> splits = inf.getSplits(HadoopShims.cloneJobContext(job));
                            List<List<InputSplit>> results = MapRedUtil
                            .getCombinePigSplits(splits,
                                    HadoopShims.getDefaultBlockSize(fs, path),
                                    conf);
                            numFiles += results.size();
                        } else {
                            List<MapReduceOper> preds = MRPlan.getPredecessors(mro);
                            if (preds != null && preds.size() == 1) {
                                MapReduceOper pred = preds.get(0);
                                if (!pred.reducePlan.isEmpty()) {
                                    numFiles += pred.requestedParallelism;
                                } else { // map-only job
                                    ret = hasTooManyInputFiles(pred, conf);
                                    break;
                                }
                            } else if (!optimisticFileConcatenation) {
                                // can't determine the number of input files.
                                // Treat it as having too manyfiles
                                numFiles = fileConcatenationThreshold;
                                break;
                            }
                        }
                    }
                }
            }
        } catch (IOException e) {
            LOG.warn("failed to get number of input files", e);
        } catch (InterruptedException e) {
            LOG.warn("failed to get number of input files", e);
        }

        LOG.info("number of input files: " + numFiles);
        return ret ? true : (numFiles >= fileConcatenationThreshold);
    }

    /*
     * Use Mult File Combiner to concatenate small input files
     */
    private MapReduceOper getConcatenateJob(FileSpec fSpec, MapReduceOper old, POStore str)
            throws PlanException, ExecException {

        MapReduceOper mro = startNew(fSpec, old);
        mro.mapPlan.addAsLeaf(str);
        mro.setMapDone(true);

        LOG.info("Insert a file-concatenation job");

        return mro;
    }

    /** Leftmost relation is referred as base relation (this is the one fed into mappers.)
     *  First, close all MROpers except for first one (referred as baseMROPer)
     *  Then, create a MROper which will do indexing job (idxMROper)
     *  Connect idxMROper before the mappedMROper in the MRPlan.
     */

    @Override
    public void visitMergeCoGroup(POMergeCogroup poCoGrp) throws VisitorException {

        if(compiledInputs.length < 2){
            int errCode=2251;
            String errMsg = "Merge Cogroup work on two or more relations." +
                    "To use map-side group-by on single relation, use 'collected' qualifier.";
            throw new MRCompilerException(errMsg, errCode);
        }

        List<FuncSpec> funcSpecs = new ArrayList<FuncSpec>(compiledInputs.length-1);
        List<String> fileSpecs = new ArrayList<String>(compiledInputs.length-1);
        List<String> loaderSigns = new ArrayList<String>(compiledInputs.length-1);

        try{
            // Iterate through all the MROpers, disconnect side MROPers from
            // MROPerPlan and collect all the information needed in different lists.

            for(int i=0 ; i < compiledInputs.length; i++){

                MapReduceOper mrOper = compiledInputs[i];
                PhysicalPlan mapPlan = mrOper.mapPlan;
                if(mapPlan.getRoots().size() != 1){
                    int errCode = 2171;
                    String errMsg = "Expected one but found more then one root physical operator in physical plan.";
                    throw new MRCompilerException(errMsg,errCode,PigException.BUG);
                }

                PhysicalOperator rootPOOp = mapPlan.getRoots().get(0);
                if(! (rootPOOp instanceof POLoad)){
                    int errCode = 2172;
                    String errMsg = "Expected physical operator at root to be POLoad. Found : "+rootPOOp.getClass().getCanonicalName();
                    throw new MRCompilerException(errMsg,errCode);
                }

                POLoad sideLoader = (POLoad)rootPOOp;
                FileSpec loadFileSpec = sideLoader.getLFile();
                FuncSpec funcSpec = loadFileSpec.getFuncSpec();
                LoadFunc loadfunc = sideLoader.getLoadFunc();
                if(i == 0){

                    if(!(CollectableLoadFunc.class.isAssignableFrom(loadfunc.getClass()))){
                        int errCode = 2252;
                        throw new MRCompilerException("Base loader in Cogroup must implement CollectableLoadFunc.", errCode);
                    }

                    ((CollectableLoadFunc)loadfunc).ensureAllKeyInstancesInSameSplit();
                    continue;
                }
                if(!(IndexableLoadFunc.class.isAssignableFrom(loadfunc.getClass()))){
                    int errCode = 2253;
                    throw new MRCompilerException("Side loaders in cogroup must implement IndexableLoadFunc.", errCode);
                }

                funcSpecs.add(funcSpec);
                fileSpecs.add(loadFileSpec.getFileName());
                loaderSigns.add(sideLoader.getSignature());
                MRPlan.remove(mrOper);
            }

            poCoGrp.setSideLoadFuncs(funcSpecs);
            poCoGrp.setSideFileSpecs(fileSpecs);
            poCoGrp.setLoaderSignatures(loaderSigns);

            // Use map-reduce operator of base relation for the cogroup operation.
            MapReduceOper baseMROp = phyToMROpMap.get(poCoGrp.getInputs().get(0));
            if(baseMROp.mapDone || !baseMROp.reducePlan.isEmpty()){
                int errCode = 2254;
                throw new MRCompilerException("Currently merged cogroup is not supported after blocking operators.", errCode);
            }

            // Create new map-reduce operator for indexing job and then configure it.
            MapReduceOper indexerMROp = getMROp();
            FileSpec idxFileSpec = getIndexingJob(indexerMROp, baseMROp, poCoGrp.getLRInnerPlansOf(0));
            poCoGrp.setIdxFuncSpec(idxFileSpec.getFuncSpec());
            poCoGrp.setIndexFileName(idxFileSpec.getFileName());

            baseMROp.mapPlan.addAsLeaf(poCoGrp);
            for (FuncSpec funcSpec : funcSpecs)
                baseMROp.UDFs.add(funcSpec.toString());
            MRPlan.add(indexerMROp);
            MRPlan.connect(indexerMROp, baseMROp);

            phyToMROpMap.put(poCoGrp,baseMROp);
            // Going forward, new operators should be added in baseMRop. To make
            // sure, reset curMROp.
            curMROp = baseMROp;
        }
        catch (ExecException e){
           throw new MRCompilerException(e.getDetailedMessage(),e.getErrorCode(),e.getErrorSource(),e);
        }
        catch (MRCompilerException mrce){
            throw(mrce);
        }
        catch (CloneNotSupportedException e) {
            throw new MRCompilerException(e);
        }
        catch(PlanException e){
            int errCode = 2034;
            String msg = "Error compiling operator " + poCoGrp.getClass().getCanonicalName();
            throw new MRCompilerException(msg, errCode, PigException.BUG, e);
        }
        catch (IOException e){
            int errCode = 3000;
            String errMsg = "IOException caught while compiling POMergeCoGroup";
            throw new MRCompilerException(errMsg, errCode,e);
        }
    }

    // Sets up the indexing job for map-side cogroups.
    private FileSpec getIndexingJob(MapReduceOper indexerMROp,
            final MapReduceOper baseMROp, final List<PhysicalPlan> mapperLRInnerPlans)
        throws MRCompilerException, PlanException, ExecException, IOException, CloneNotSupportedException {

        // First replace loader with  MergeJoinIndexer.
        PhysicalPlan baseMapPlan = baseMROp.mapPlan;
        POLoad baseLoader = (POLoad)baseMapPlan.getRoots().get(0);
        FileSpec origLoaderFileSpec = baseLoader.getLFile();
        FuncSpec funcSpec = origLoaderFileSpec.getFuncSpec();
        LoadFunc loadFunc = baseLoader.getLoadFunc();

        if (! (OrderedLoadFunc.class.isAssignableFrom(loadFunc.getClass()))){
            int errCode = 1104;
            String errMsg = "Base relation of merge-coGroup must implement " +
            "OrderedLoadFunc interface. The specified loader "
            + funcSpec + " doesn't implement it";
            throw new MRCompilerException(errMsg,errCode);
        }

        String[] indexerArgs = new String[6];
        indexerArgs[0] = funcSpec.toString();
        indexerArgs[1] = ObjectSerializer.serialize((Serializable)mapperLRInnerPlans);
        indexerArgs[3] = baseLoader.getSignature();
        indexerArgs[4] = baseLoader.getOperatorKey().scope;
        indexerArgs[5] = Boolean.toString(false); // we care for nulls.

        PhysicalPlan phyPlan;
        if (baseMapPlan.getSuccessors(baseLoader) == null
                || baseMapPlan.getSuccessors(baseLoader).isEmpty()){
         // Load-Load-Cogroup case.
            phyPlan = null;
        }

        else{ // We got something. Yank it and set it as inner plan.
            phyPlan = baseMapPlan.clone();
            PhysicalOperator root = phyPlan.getRoots().get(0);
            phyPlan.disconnect(root, phyPlan.getSuccessors(root).get(0));
            phyPlan.remove(root);

        }
        indexerArgs[2] = ObjectSerializer.serialize(phyPlan);

        POLoad idxJobLoader = getLoad();
        idxJobLoader.setLFile(new FileSpec(origLoaderFileSpec.getFileName(),
                new FuncSpec(MergeJoinIndexer.class.getName(), indexerArgs)));
        indexerMROp.mapPlan.add(idxJobLoader);
        indexerMROp.UDFs.add(baseLoader.getLFile().getFuncSpec().toString());

        // Loader of mro will return a tuple of form -
        // (key1, key2, .. , WritableComparable, splitIndex). See MergeJoinIndexer for details.

        // After getting an index entry in each mapper, send all of them to one
        // reducer where they will be sorted on the way by Hadoop.
        MRUtil.simpleConnectMapToReduce(indexerMROp, scope, nig);

        indexerMROp.requestedParallelism = 1; // we need exactly one reducer for indexing job.

        // We want to use typed tuple comparator for this job, instead of default
        // raw binary comparator used by Pig, to make sure index entries are
        // sorted correctly by Hadoop.
        indexerMROp.useTypedComparator(true);

        POStore st = getStore();
        FileSpec strFile = getTempFileSpec();
        st.setSFile(strFile);
        indexerMROp.reducePlan.addAsLeaf(st);
        indexerMROp.setReduceDone(true);

        return strFile;
    }

    /** Since merge-join works on two inputs there are exactly two MROper predecessors identified  as left and right.
     *  Instead of merging two operators, both are used to generate a MR job each. First MR oper is run to generate on-the-fly index on right side.
     *  Second is used to actually do the join. First MR oper is identified as rightMROper and second as curMROper.

     *  1) RightMROper: If it is in map phase. It can be preceded only by POLoad. If there is anything else
     *                  in physical plan, that is yanked and set as inner plans of joinOp.
     *                  If it is reduce phase. Close this operator and start new MROper.
     *  2) LeftMROper:  If it is in map phase, add the Join operator in it.
     *                  If it is in reduce phase. Close it and start new MROper.
     */

    @Override
    public void visitMergeJoin(POMergeJoin joinOp) throws VisitorException {

        try{
            if(compiledInputs.length != 2 || joinOp.getInputs().size() != 2){
                int errCode=1101;
                throw new MRCompilerException("Merge Join must have exactly two inputs. Found : "+compiledInputs.length, errCode);
            }

            curMROp = phyToMROpMap.get(joinOp.getInputs().get(0));

            MapReduceOper rightMROpr = null;
            if(curMROp.equals(compiledInputs[0]))
                rightMROpr = compiledInputs[1];
            else
                rightMROpr = compiledInputs[0];

            // We will first operate on right side which is indexer job.
            // First yank plan of the compiled right input and set that as an inner plan of right operator.
            PhysicalPlan rightPipelinePlan;
            if(!rightMROpr.mapDone){
                PhysicalPlan rightMapPlan = rightMROpr.mapPlan;
                if(rightMapPlan.getRoots().size() != 1){
                    int errCode = 2171;
                    String errMsg = "Expected one but found more then one root physical operator in physical plan.";
                    throw new MRCompilerException(errMsg,errCode,PigException.BUG);
                }

                PhysicalOperator rightLoader = rightMapPlan.getRoots().get(0);
                if(! (rightLoader instanceof POLoad)){
                    int errCode = 2172;
                    String errMsg = "Expected physical operator at root to be POLoad. Found : "+rightLoader.getClass().getCanonicalName();
                    throw new MRCompilerException(errMsg,errCode);
                }

                if (rightMapPlan.getSuccessors(rightLoader) == null || rightMapPlan.getSuccessors(rightLoader).isEmpty())
                    // Load - Join case.
                    rightPipelinePlan = null;

                else{ // We got something on right side. Yank it and set it as inner plan of right input.
                    rightPipelinePlan = rightMapPlan.clone();
                    PhysicalOperator root = rightPipelinePlan.getRoots().get(0);
                    rightPipelinePlan.disconnect(root, rightPipelinePlan.getSuccessors(root).get(0));
                    rightPipelinePlan.remove(root);
                    rightMapPlan.trimBelow(rightLoader);
                }
            }

            else if(!rightMROpr.reduceDone){
                // Indexer must run in map. If we are in reduce, close it and start new MROper.
                // No need of yanking in this case. Since we are starting brand new MR Operator and it will contain nothing.
                POStore rightStore = getStore();
                FileSpec rightStrFile = getTempFileSpec();
                rightStore.setSFile(rightStrFile);
                rightMROpr.reducePlan.addAsLeaf(rightStore);
                rightMROpr.setReduceDone(true);
                rightMROpr = startNew(rightStrFile, rightMROpr);
                rightPipelinePlan = null;
            }

            else{
                int errCode = 2022;
                String msg = "Both map and reduce phases have been done. This is unexpected while compiling.";
                throw new PlanException(msg, errCode, PigException.BUG);
            }

            joinOp.setupRightPipeline(rightPipelinePlan);
            rightMROpr.requestedParallelism = 1; // we need exactly one reducer for indexing job.

            // At this point, we must be operating on map plan of right input and it would contain nothing else other then a POLoad.
            POLoad rightLoader = (POLoad)rightMROpr.mapPlan.getRoots().get(0);
            joinOp.setSignature(rightLoader.getSignature());
            LoadFunc rightLoadFunc = rightLoader.getLoadFunc();
            List<String> udfs = new ArrayList<String>();
            if(IndexableLoadFunc.class.isAssignableFrom(rightLoadFunc.getClass())) {
                joinOp.setRightLoaderFuncSpec(rightLoader.getLFile().getFuncSpec());
                joinOp.setRightInputFileName(rightLoader.getLFile().getFileName());
                udfs.add(rightLoader.getLFile().getFuncSpec().toString());

                // we don't need the right MROper since
                // the right loader is an IndexableLoadFunc which can handle the index
                // itself
                MRPlan.remove(rightMROpr);
                if(rightMROpr == compiledInputs[0]) {
                    compiledInputs[0] = null;
                } else if(rightMROpr == compiledInputs[1]) {
                    compiledInputs[1] = null;
                }
                rightMROpr = null;

                // validate that the join keys in merge join are only
                // simple column projections or '*' and not expression - expressions
                // cannot be handled when the index is built by the storage layer on the sorted
                // data when the sorted data (and corresponding index) is written.
                // So merge join will be restricted not have expressions as
                // join keys
                int numInputs = mPlan.getPredecessors(joinOp).size(); // should be 2
                for(int i = 0; i < numInputs; i++) {
                    List<PhysicalPlan> keyPlans = joinOp.getInnerPlansOf(i);
                    for (PhysicalPlan keyPlan : keyPlans) {
                        for(PhysicalOperator op : keyPlan) {
                            if(!(op instanceof POProject)) {
                                int errCode = 1106;
                                String errMsg = "Merge join is possible only for simple column or '*' join keys when using " +
                                rightLoader.getLFile().getFuncSpec() + " as the loader";
                                throw new MRCompilerException(errMsg, errCode, PigException.INPUT);
                            }
                        }
                    }
                }
            } else {
                LoadFunc loadFunc = rightLoader.getLoadFunc();
                //Replacing POLoad with indexer is disabled for 'merge-sparse' joins.  While
                //this feature would be useful, the current implementation of DefaultIndexableLoader
                //is not designed to handle multiple calls to seekNear.  Specifically, it rereads the entire index
                //for each call.  Some refactoring of this class is required - and then the check below could be removed.
                if (joinOp.getJoinType() == LOJoin.JOINTYPE.MERGESPARSE) {
                    int errCode = 1104;
                    String errMsg = "Right input of merge-join must implement IndexableLoadFunc. " +
                    "The specified loader " + loadFunc + " doesn't implement it";
                    throw new MRCompilerException(errMsg,errCode);
                }

                // Replace POLoad with  indexer.

                if (! (OrderedLoadFunc.class.isAssignableFrom(loadFunc.getClass()))){
                    int errCode = 1104;
                    String errMsg = "Right input of merge-join must implement " +
                    "OrderedLoadFunc interface. The specified loader "
                    + loadFunc + " doesn't implement it";
                    throw new MRCompilerException(errMsg,errCode);
                }

                String[] indexerArgs = new String[6];
                List<PhysicalPlan> rightInpPlans = joinOp.getInnerPlansOf(1);
                FileSpec origRightLoaderFileSpec = rightLoader.getLFile();

                indexerArgs[0] = origRightLoaderFileSpec.getFuncSpec().toString();
                indexerArgs[1] = ObjectSerializer.serialize((Serializable)rightInpPlans);
                indexerArgs[2] = ObjectSerializer.serialize(rightPipelinePlan);
                indexerArgs[3] = rightLoader.getSignature();
                indexerArgs[4] = rightLoader.getOperatorKey().scope;
                indexerArgs[5] = Boolean.toString(true);

                FileSpec lFile = new FileSpec(rightLoader.getLFile().getFileName(),new FuncSpec(MergeJoinIndexer.class.getName(), indexerArgs));
                rightLoader.setLFile(lFile);

                // Loader of mro will return a tuple of form -
                // (keyFirst1, keyFirst2, .. , position, splitIndex) See MergeJoinIndexer

                MRUtil.simpleConnectMapToReduce(rightMROpr, scope, nig);
                rightMROpr.useTypedComparator(true);

                POStore st = getStore();
                FileSpec strFile = getTempFileSpec();
                st.setSFile(strFile);
                rightMROpr.reducePlan.addAsLeaf(st);
                rightMROpr.setReduceDone(true);

                // set up the DefaultIndexableLoader for the join operator
                String[] defaultIndexableLoaderArgs = new String[5];
                defaultIndexableLoaderArgs[0] = origRightLoaderFileSpec.getFuncSpec().toString();
                defaultIndexableLoaderArgs[1] = strFile.getFileName();
                defaultIndexableLoaderArgs[2] = strFile.getFuncSpec().toString();
                defaultIndexableLoaderArgs[3] = joinOp.getOperatorKey().scope;
                defaultIndexableLoaderArgs[4] = origRightLoaderFileSpec.getFileName();
                joinOp.setRightLoaderFuncSpec((new FuncSpec(DefaultIndexableLoader.class.getName(), defaultIndexableLoaderArgs)));
                joinOp.setRightInputFileName(origRightLoaderFileSpec.getFileName());

                joinOp.setIndexFile(strFile.getFileName());
                udfs.add(origRightLoaderFileSpec.getFuncSpec().toString());
            }

            // We are done with right side. Lets work on left now.
            // Join will be materialized in leftMROper.
            if(!curMROp.mapDone) // Life is easy
                curMROp.mapPlan.addAsLeaf(joinOp);

            else if(!curMROp.reduceDone){  // This is a map-side join. Close this MROper and start afresh.
                POStore leftStore = getStore();
                FileSpec leftStrFile = getTempFileSpec();
                leftStore.setSFile(leftStrFile);
                curMROp.reducePlan.addAsLeaf(leftStore);
                curMROp.setReduceDone(true);
                curMROp = startNew(leftStrFile, curMROp);
                curMROp.mapPlan.addAsLeaf(joinOp);
            }

            else{
                int errCode = 2022;
                String msg = "Both map and reduce phases have been done. This is unexpected while compiling.";
                throw new PlanException(msg, errCode, PigException.BUG);
            }
            if(rightMROpr != null) {
                rightMROpr.markIndexer();
                // We want to ensure indexing job runs prior to actual join job. So, connect them in order.
                MRPlan.connect(rightMROpr, curMROp);
            }
            phyToMROpMap.put(joinOp, curMROp);
            // no combination of small splits as there is currently no way to guarantee the sortness
            // of the combined splits.
            curMROp.noCombineSmallSplits();
            curMROp.UDFs.addAll(udfs);
        }
        catch(PlanException e){
            int errCode = 2034;
            String msg = "Error compiling operator " + joinOp.getClass().getCanonicalName();
            throw new MRCompilerException(msg, errCode, PigException.BUG, e);
        }
        catch (IOException e){
           int errCode = 3000;
           String errMsg = "IOException caught while compiling POMergeJoin";
            throw new MRCompilerException(errMsg, errCode,e);
        }
        catch(CloneNotSupportedException e){
           int errCode = 2127;
           String errMsg = "Cloning exception caught while compiling POMergeJoin";
           throw new MRCompilerException(errMsg, errCode, PigException.BUG, e);
       }
    }

    @Override
    public void visitDistinct(PODistinct op) throws VisitorException {
        try{
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

            addToMap(lr);

            blocking(op);
            curMROp.customPartitioner = op.getCustomPartitioner();

            POPackage pkg = new POPackage(new OperatorKey(scope,nig.getNextNodeId(scope)));
            Packager pkgr = pkg.getPkgr();
            pkgr.setKeyType(DataType.TUPLE);
            pkgr.setDistinct(true);
            pkg.setNumInps(1);
            boolean[] inner = {false};
            pkgr.setInner(inner);
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
            phyToMROpMap.put(op, curMROp);
            curMROp.phyToMRMap.put(op, nfe1);
        }catch(Exception e){
            int errCode = 2034;
            String msg = "Error compiling operator " + op.getClass().getSimpleName();
            throw new MRCompilerException(msg, errCode, PigException.BUG, e);
        }
    }

    @Override
    public void visitSkewedJoin(POSkewedJoin op) throws VisitorException {
        try {
            if (compiledInputs.length != 2) {
                int errCode = 2255;
                throw new VisitorException("POSkewedJoin operator has " + compiledInputs.length + " inputs. It should have 2.", errCode);
            }

            //change plan to store the first join input into a temp file
            FileSpec fSpec = getTempFileSpec();
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

            FileSpec partitionFile = getTempFileSpec();
            int rp = op.getRequestedParallelism();

            Pair<MapReduceOper, Integer> sampleJobPair = getSkewedJoinSampleJob(op, mro, fSpec, partitionFile, rp);
            rp = sampleJobPair.second;

            // set parallelism of SkewedJoin as the value calculated by sampling job
            // if "parallel" is specified in join statement, "rp" is equal to that number
            // if not specified, use the value that sampling process calculated
            // based on default.
            op.setRequestedParallelism(rp);

            // load the temp file for first table as input of join
            MapReduceOper[] joinInputs = new MapReduceOper[] {startNew(fSpec, sampleJobPair.first), compiledInputs[1]};
            MapReduceOper[] rearrangeOutputs = new MapReduceOper[2];

            compiledInputs = new MapReduceOper[] {joinInputs[0]};
            // run POLocalRearrange for first join table
            POLocalRearrange lr = new POLocalRearrange(new OperatorKey(scope,nig.getNextNodeId(scope)), rp);
            try {
                lr.setIndex(0);
            } catch (ExecException e) {
                int errCode = 2058;
                String msg = "Unable to set index on newly created POLocalRearrange.";
                throw new PlanException(msg, errCode, PigException.BUG, e);
            }

            List<PhysicalOperator> l = plan.getPredecessors(op);
            MultiMap<PhysicalOperator, PhysicalPlan> joinPlans = op.getJoinPlans();
            List<PhysicalPlan> groups = joinPlans.get(l.get(0));
            // check the type of group keys, if there are more than one field, the key is TUPLE.
            byte type = DataType.TUPLE;
            if (groups.size() == 1) {
                type = groups.get(0).getLeaves().get(0).getResultType();
            }

            lr.setKeyType(type);
            lr.setPlans(groups);
            lr.setResultType(DataType.TUPLE);

            lr.visit(this);
            if(lr.getRequestedParallelism() > curMROp.requestedParallelism)
                curMROp.requestedParallelism = lr.getRequestedParallelism();
            rearrangeOutputs[0] = curMROp;

            compiledInputs = new MapReduceOper[] {joinInputs[1]};
            // if the map for current input is already closed, then start a new job
            if (compiledInputs[0].isMapDone() && !compiledInputs[0].isReduceDone()) {
                FileSpec f = getTempFileSpec();
                POStore s = getStore();
                s.setSFile(f);
                compiledInputs[0].reducePlan.addAsLeaf(s);
                compiledInputs[0].setReduceDone(true);
                compiledInputs[0] = startNew(f, compiledInputs[0]);
            }

            // run POPartitionRearrange for second join table
            POPartitionRearrange pr =
                new POPartitionRearrange(new OperatorKey(scope,nig.getNextNodeId(scope)), rp);
            pr.setPigContext(pigContext);
            lr = pr;
            try {
                lr.setIndex(1);
            } catch (ExecException e) {
                int errCode = 2058;
                String msg = "Unable to set index on newly created POLocalRearrange.";
                throw new PlanException(msg, errCode, PigException.BUG, e);
            }

            groups = joinPlans.get(l.get(1));
            lr.setPlans(groups);
            lr.setKeyType(type);
            lr.setResultType(DataType.BAG);

            lr.visit(this);
            if(lr.getRequestedParallelism() > curMROp.requestedParallelism)
                curMROp.requestedParallelism = lr.getRequestedParallelism();
            rearrangeOutputs[1] = curMROp;
            compiledInputs = rearrangeOutputs;


            // create POGlobalRearrange
            POGlobalRearrange gr = new POGlobalRearrange(new OperatorKey(scope,nig.getNextNodeId(scope)), rp);
            // Skewed join has its own special partitioner
            gr.setResultType(DataType.TUPLE);
            gr.visit(this);
            if(gr.getRequestedParallelism() > curMROp.requestedParallelism)
                curMROp.requestedParallelism = gr.getRequestedParallelism();
            compiledInputs = new MapReduceOper[] {curMROp};

            // create POPakcage
            POPackage pkg = new POPackage(new OperatorKey(scope,nig.getNextNodeId(scope)), rp);
            Packager pkgr = pkg.getPkgr();
            pkgr.setKeyType(type);
            pkg.setResultType(DataType.TUPLE);
            pkg.setNumInps(2);
            boolean [] inner = op.getInnerFlags();
            pkgr.setInner(inner);
            pkg.visit(this);
            compiledInputs = new MapReduceOper[] {curMROp};

            // create POForEach
            List<PhysicalPlan> eps = new ArrayList<PhysicalPlan>();
            List<Boolean> flat = new ArrayList<Boolean>();

            PhysicalPlan ep;
            // Add corresponding POProjects
            for (int i=0; i < 2; i++ ) {
                ep = new PhysicalPlan();
                POProject prj = new POProject(new OperatorKey(scope,nig.getNextNodeId(scope)));
                prj.setColumn(i+1);
                prj.setOverloaded(false);
                prj.setResultType(DataType.BAG);
                ep.add(prj);
                eps.add(ep);
                if (!inner[i]) {
                    // Add an empty bag for outer join
                    CompilerUtils.addEmptyBagOuterJoin(ep, op.getSchema(i));
                }
                flat.add(true);
            }

            POForEach fe = new POForEach(new OperatorKey(scope,nig.getNextNodeId(scope)), -1, eps, flat);
            fe.setResultType(DataType.TUPLE);

            fe.visit(this);

            curMROp.setSkewedJoinPartitionFile(partitionFile.getFileName());
            phyToMROpMap.put(op, curMROp);
        }catch(PlanException e) {
            int errCode = 2034;
            String msg = "Error compiling operator " + op.getClass().getSimpleName();
            throw new MRCompilerException(msg, errCode, PigException.BUG, e);
        }catch(IOException e) {
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
            Pair<POProject, Byte>[] fields = getSortCols(op.getSortPlans());
            Pair<MapReduceOper, Integer> quantJobParallelismPair =
                getQuantileJob(op, mro, fSpec, quantFile, rp);
            curMROp = getSortJob(op, quantJobParallelismPair.first, fSpec, quantFile,
                    quantJobParallelismPair.second, fields);

            if(op.isUDFComparatorUsed){
                curMROp.UDFs.add(op.getMSortFunc().getFuncSpec().toString());
                curMROp.isUDFComparatorUsed = true;
            }
            phyToMROpMap.put(op, curMROp);
        }catch(Exception e){
            int errCode = 2034;
            String msg = "Error compiling operator " + op.getClass().getSimpleName();
            throw new MRCompilerException(msg, errCode, PigException.BUG, e);
        }
    }

    /**
     * For the counter job, it depends if it is row number or not.
     * In case of being a row number, any previous jobs are saved
     * and POCounter is added as a leaf on a map task.
     * If it is not, then POCounter is added as a leaf on a reduce
     * task (last sorting phase).
     **/
    @Override
    public void visitCounter(POCounter op) throws VisitorException {
        try{
            if(op.isRowNumber()) {
                List<PhysicalOperator> mpLeaves = curMROp.mapPlan.getLeaves();
                PhysicalOperator leaf = mpLeaves.get(0);
                if ( !curMROp.isMapDone() && !curMROp.isRankOperation() )
                {
                    curMROp.mapPlan.addAsLeaf(op);
                } else {
                    FileSpec fSpec = getTempFileSpec();
                    MapReduceOper prevMROper = endSingleInputPlanWithStr(fSpec);
                    MapReduceOper mrCounter = startNew(fSpec, prevMROper);
                    mrCounter.mapPlan.addAsLeaf(op);
                    curMROp = mrCounter;
                }
            } else {
                curMROp.reducePlan.addAsLeaf(op);
            }

            phyToMROpMap.put(op, curMROp);
        }catch(Exception e){
            int errCode = 2034;
            String msg = "Error compiling operator " + op.getClass().getSimpleName();
            throw new MRCompilerException(msg, errCode, PigException.BUG, e);
        }
    }

    /**
     * In case of PORank, it is closed any other previous job (containing
     * POCounter as a leaf) and PORank is added on map phase.
     **/
    @Override
    public void visitRank(PORank op) throws VisitorException {
        try{
            FileSpec fSpec = getTempFileSpec();
            MapReduceOper prevMROper = endSingleInputPlanWithStr(fSpec);

            curMROp = startNew(fSpec, prevMROper);
            curMROp.mapPlan.addAsLeaf(op);

            phyToMROpMap.put(op, curMROp);
        }catch(Exception e){
            int errCode = 2034;
            String msg = "Error compiling operator " + op.getClass().getSimpleName();
            throw new MRCompilerException(msg, errCode, PigException.BUG, e);
        }
    }

    private Pair<POProject,Byte> [] getSortCols(List<PhysicalPlan> plans) throws PlanException, ExecException {
        if(plans!=null){
            @SuppressWarnings("unchecked")
            Pair<POProject,Byte>[] ret = new Pair[plans.size()];
            int i=-1;
            for (PhysicalPlan plan : plans) {
                PhysicalOperator op = plan.getLeaves().get(0);
                POProject proj;
                if (op instanceof POProject) {
                    if (((POProject)op).isStar()) return null;
                    proj = (POProject)op;
                } else {
                    proj = null;
                }
                byte type = op.getResultType();
                ret[++i] = new Pair<POProject, Byte>(proj, type);
            }
            return ret;
        }
        int errCode = 2026;
        String msg = "No expression plan found in POSort.";
        throw new PlanException(msg, errCode, PigException.BUG);
    }

    private MapReduceOper getSortJob(
            POSort sort,
            MapReduceOper quantJob,
            FileSpec lFile,
            FileSpec quantFile,
            int rp,
            Pair<POProject, Byte>[] fields) throws PlanException{
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
        lr.addOriginalLocation(sort.getAlias(), sort.getOriginalLocations());
        mro.mapPlan.addAsLeaf(lr);

        mro.setMapDone(true);

        if (limit!=-1) {
            POPackage pkg_c = new POPackage(new OperatorKey(scope,
                    nig.getNextNodeId(scope)));
            LitePackager pkgr = new LitePackager();
            pkgr.setKeyType((fields.length > 1) ? DataType.TUPLE : keyType);
            pkg_c.setPkgr(pkgr);
            pkg_c.setNumInps(1);
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

        POPackage pkg = new POPackage(new OperatorKey(scope,
                nig.getNextNodeId(scope)));
        LitePackager pkgr = new LitePackager();
        pkgr.setKeyType((fields == null || fields.length > 1) ? DataType.TUPLE : keyType);
        pkg.setPkgr(pkgr);
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
        mro.phyToMRMap.put(sort, nfe1);
        if (limit!=-1)
        {
            POLimit pLimit2 = new POLimit(new OperatorKey(scope,nig.getNextNodeId(scope)));
            pLimit2.setLimit(limit);
            mro.reducePlan.addAsLeaf(pLimit2);
            mro.phyToMRMap.put(sort, pLimit2);
        }

        return mro;
    }

    private Pair<MapReduceOper,Integer> getQuantileJob(
            POSort inpSort,
            MapReduceOper prevJob,
            FileSpec lFile,
            FileSpec quantFile,
            int rp) throws PlanException, VisitorException {

        POSort sort = new POSort(inpSort.getOperatorKey(), inpSort
                .getRequestedParallelism(), null, inpSort.getSortPlans(),
                inpSort.getMAscCols(), inpSort.getMSortFunc());
        sort.addOriginalLocation(inpSort.getAlias(), inpSort.getOriginalLocations());

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

        return getSamplingJob(sort, prevJob, null, lFile, quantFile, rp, null, FindQuantiles.class.getName(), ctorArgs, RandomSampleLoader.class.getName());
    }

    /**
     * Create Sampling job for skewed join.
     */
    private Pair<MapReduceOper, Integer> getSkewedJoinSampleJob(POSkewedJoin op, MapReduceOper prevJob,
            FileSpec lFile, FileSpec sampleFile, int rp ) throws PlanException, VisitorException {

        MultiMap<PhysicalOperator, PhysicalPlan> joinPlans = op.getJoinPlans();

        List<PhysicalOperator> l = plan.getPredecessors(op);
        List<PhysicalPlan> groups = joinPlans.get(l.get(0));
        List<Boolean> ascCol = new ArrayList<Boolean>();
        for(int i=0; i<groups.size(); i++) {
            ascCol.add(false);
        }

        POSort sort = new POSort(op.getOperatorKey(), op.getRequestedParallelism(), null, groups, ascCol, null);

        // set up transform plan to get keys and memory size of input tuples
        // it first adds all the plans to get key columns,
        List<PhysicalPlan> transformPlans = new ArrayList<PhysicalPlan>();
        transformPlans.addAll(groups);

        // then it adds a column for memory size
        POProject prjStar = new POProject(new OperatorKey(scope,nig.getNextNodeId(scope)));
        prjStar.setResultType(DataType.TUPLE);
        prjStar.setStar(true);

        List<PhysicalOperator> ufInps = new ArrayList<PhysicalOperator>();
        ufInps.add(prjStar);

        PhysicalPlan ep = new PhysicalPlan();
        POUserFunc uf = new POUserFunc(new OperatorKey(scope,nig.getNextNodeId(scope)), -1, ufInps,
                    new FuncSpec(GetMemNumRows.class.getName(), (String[])null));
        uf.setResultType(DataType.TUPLE);
        ep.add(uf);
        ep.add(prjStar);
        ep.connect(prjStar, uf);

        transformPlans.add(ep);

        try{
            // pass configurations to the User Function
            String per = pigContext.getProperties().getProperty("pig.skewedjoin.reduce.memusage",
                                   String.valueOf(PartitionSkewedKeys.DEFAULT_PERCENT_MEMUSAGE));
            String mc = pigContext.getProperties().getProperty("pig.skewedjoin.reduce.maxtuple", "0");
            String inputFile = lFile.getFileName();

            return getSamplingJob(sort, prevJob, transformPlans, lFile, sampleFile, rp, null,
                                PartitionSkewedKeys.class.getName(), new String[]{per, mc, inputFile}, PoissonSampleLoader.class.getName());
        }catch(Exception e) {
            throw new PlanException(e);
        }
    }


    /**
     * Create a sampling job to collect statistics by sampling an input file. The sequence of operations is as
     * following:
     * <li>Transform input sample tuples into another tuple.</li>
     * <li>Add an extra field &quot;all&quot; into the tuple </li>
     * <li>Package all tuples into one bag </li>
     * <li>Add constant field for number of reducers. </li>
     * <li>Sorting the bag </li>
     * <li>Invoke UDF with the number of reducers and the sorted bag.</li>
     * <li>Data generated by UDF is stored into a file.</li>
     *
     * @param sort  the POSort operator used to sort the bag
     * @param prevJob  previous job of current sampling job
     * @param transformPlans  PhysicalPlans to transform input samples
     * @param lFile  path of input file
     * @param sampleFile  path of output file
     * @param rp  configured parallemism
     * @param sortKeyPlans  PhysicalPlans to be set into POSort operator to get sorting keys
     * @param udfClassName  the class name of UDF
     * @param udfArgs   the arguments of UDF
     * @param sampleLdrClassName class name for the sample loader
     * @return pair<mapreduceoper,integer>
     * @throws PlanException
     * @throws VisitorException
     */
    @SuppressWarnings("deprecation")
    private Pair<MapReduceOper,Integer> getSamplingJob(POSort sort, MapReduceOper prevJob, List<PhysicalPlan> transformPlans,
              FileSpec lFile, FileSpec sampleFile, int rp, List<PhysicalPlan> sortKeyPlans,
              String udfClassName, String[] udfArgs, String sampleLdrClassName ) throws PlanException, VisitorException {

        String[] rslargs = new String[2];
        // SampleLoader expects string version of FuncSpec
        // as its first constructor argument.

        rslargs[0] = (new FuncSpec(Utils.getTmpFileCompressorName(pigContext))).toString();
        // This value is only used by order by. For skewed join, it's calculated
        // based on the file size.
        rslargs[1] = pigContext.getProperties().getProperty(PigConfiguration.PIG_RANDOM_SAMPLER_SAMPLE_SIZE, "100");
        FileSpec quantLdFilName = new FileSpec(lFile.getFileName(),
                new FuncSpec(sampleLdrClassName, rslargs));

        MapReduceOper mro = startNew(quantLdFilName, prevJob);

        if(sort.isUDFComparatorUsed) {
            mro.UDFs.add(sort.getMSortFunc().getFuncSpec().toString());
            curMROp.isUDFComparatorUsed = true;
        }

        List<Boolean> flat1 = new ArrayList<Boolean>();
        List<PhysicalPlan> eps1 = new ArrayList<PhysicalPlan>();

        // if transform plans are not specified, project the columns of sorting keys
        if (transformPlans == null) {
            Pair<POProject, Byte>[] sortProjs = null;
            try{
                sortProjs = getSortCols(sort.getSortPlans());
            }catch(Exception e) {
                throw new RuntimeException(e);
            }
            // Set up the projections of the key columns
            if (sortProjs == null) {
                PhysicalPlan ep = new PhysicalPlan();
                POProject prj = new POProject(new OperatorKey(scope,
                    nig.getNextNodeId(scope)));
                prj.setStar(true);
                prj.setOverloaded(false);
                prj.setResultType(DataType.TUPLE);
                ep.add(prj);
                eps1.add(ep);
                flat1.add(false);
            } else {
                for (Pair<POProject, Byte> sortProj : sortProjs) {
                    // Check for proj being null, null is used by getSortCols for a non POProject
                    // operator. Since Order by does not allow expression operators,
                    //it should never be set to null
                    if(sortProj == null){
                        int errCode = 2174;
                        String msg = "Internal exception. Could not create a sampler job";
                        throw new MRCompilerException(msg, errCode, PigException.BUG);
                    }
                    PhysicalPlan ep = new PhysicalPlan();
                    POProject prj;
                    try {
                        prj = sortProj.first.clone();
                    } catch (CloneNotSupportedException e) {
                        //should not get here
                        throw new AssertionError(
                                "Error cloning project caught exception" + e
                        );
                    }
                    ep.add(prj);
                    eps1.add(ep);
                    flat1.add(false);
                }
            }
        }else{
            for(int i=0; i<transformPlans.size(); i++) {
                eps1.add(transformPlans.get(i));
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
        lr.addOriginalLocation(sort.getAlias(), sort.getOriginalLocations());
        mro.mapPlan.add(lr);
        mro.mapPlan.connect(nfe1, lr);

        mro.setMapDone(true);

        POPackage pkg = new POPackage(new OperatorKey(scope,nig.getNextNodeId(scope)));
        Packager pkgr = new Packager();
        pkg.setPkgr(pkgr);
        pkgr.setKeyType(DataType.CHARARRAY);
        pkg.setNumInps(1);
        boolean[] inner = {false};
        pkgr.setInner(inner);
        mro.reducePlan.add(pkg);

        // Lets start building the plan which will have the sort
        // for the foreach
        PhysicalPlan fe2Plan = new PhysicalPlan();
        // Top level project which just projects the tuple which is coming
        // from the foreach after the package
        POProject topPrj = new POProject(new OperatorKey(scope,nig.getNextNodeId(scope)));
        topPrj.setColumn(1);
        topPrj.setResultType(DataType.BAG);
        topPrj.setOverloaded(true);
        fe2Plan.add(topPrj);

        // the projections which will form sort plans
        List<PhysicalPlan> nesSortPlanLst = new ArrayList<PhysicalPlan>();
        if (sortKeyPlans != null) {
            for(int i=0; i<sortKeyPlans.size(); i++) {
                nesSortPlanLst.add(sortKeyPlans.get(i));
            }
        }else{
            Pair<POProject, Byte>[] sortProjs = null;
            try{
                sortProjs = getSortCols(sort.getSortPlans());
            }catch(Exception e) {
                throw new RuntimeException(e);
            }
            // Set up the projections of the key columns
            if (sortProjs == null) {
                PhysicalPlan ep = new PhysicalPlan();
                POProject prj = new POProject(new OperatorKey(scope,
                    nig.getNextNodeId(scope)));
                prj.setStar(true);
                prj.setOverloaded(false);
                prj.setResultType(DataType.TUPLE);
                ep.add(prj);
                nesSortPlanLst.add(ep);
            } else {
                for (int i=0; i<sortProjs.length; i++) {
                    POProject prj =
                        new POProject(new OperatorKey(scope,nig.getNextNodeId(scope)));

                    prj.setResultType(sortProjs[i].second);
                    if(sortProjs[i].first != null && sortProjs[i].first.isProjectToEnd()){
                        if(i != sortProjs.length -1){
                            //project to end has to be the last sort column
                            throw new AssertionError("Project-range to end (x..)" +
                            " is supported in order-by only as last sort column");
                        }
                        prj.setProjectToEnd(i);
                        break;
                    }
                    else{
                        prj.setColumn(i);
                    }
                    prj.setOverloaded(false);

                    PhysicalPlan ep = new PhysicalPlan();
                    ep.add(prj);
                    nesSortPlanLst.add(ep);
                }
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

        // We temporarily set it to rp and will adjust it at runtime, because the final degree of parallelism
        // is unknown until we are ready to submit it. See PIG-2779.
        rpce.setValue(rp);

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

        POUserFunc uf = new POUserFunc(new OperatorKey(scope,nig.getNextNodeId(scope)), -1, ufInps,
            new FuncSpec(udfClassName, udfArgs));
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
        str.setSFile(sampleFile);

        mro.reducePlan.add(str);
        mro.reducePlan.connect(nfe3, str);

        mro.setReduceDone(true);
        mro.requestedParallelism = 1;
        mro.markSampler();
        return new Pair<MapReduceOper, Integer>(mro, rp);
    }

    static class LastInputStreamingOptimizer extends MROpPlanVisitor {
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
            if (sucs == null || sucs.size()!=1) {
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
                    if(proj.isProjectToEnd() || proj.getColumns().size() != 1) {
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
                    replaceWithPOJoinPackage(mr.reducePlan, mr, pack, forEach, chunkSize);
                }
            }
        }

        public static void replaceWithPOJoinPackage(PhysicalPlan plan, MapReduceOper mr,
                POPackage pack, POForEach forEach, String chunkSize) throws VisitorException {
            JoinPackager pkgr = new JoinPackager(pack.getPkgr(), forEach);
            pkgr.setChunkSize(Long.parseLong(chunkSize));
            pack.setPkgr(pkgr);
            List<PhysicalOperator> succs = plan.getSuccessors(forEach);
            if (succs != null) {
                if (succs.size() != 1) {
                    int errCode = 2028;
                    String msg = "ForEach can only have one successor. Found "
                            + succs.size() + " successors.";
                    throw new MRCompilerException(msg, errCode,
                            PigException.BUG);
                }
            }
            plan.remove(pack);
            try {
                plan.replace(forEach, pack);
            } catch (PlanException e) {
                int errCode = 2029;
                String msg = "Error rewriting join package.";
                throw new MRCompilerException(msg, errCode, PigException.BUG, e);
            }
            mr.phyToMRMap.put(forEach, pack);
            LogFactory.getLog(LastInputStreamingOptimizer.class).info(
                    "Rewrite: POPackage->POForEach to POPackage(JoinPackager)");
        }

    }

    private static class FindKeyTypeVisitor extends PhyPlanVisitor {

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

    private static class FindStoreNameVisitor extends PhyPlanVisitor {

        FileSpec newSpec;
        FileSpec oldSpec;

        FindStoreNameVisitor (PhysicalPlan plan, FileSpec newSpec, FileSpec oldSpec) {
            super(plan,
                new DepthFirstWalker<PhysicalOperator, PhysicalPlan>(plan));
            this.newSpec = newSpec;
            this.oldSpec = oldSpec;
        }

        @Override
        public void visitStore(POStore sto) throws VisitorException {
            FileSpec spec = sto.getSFile();
            if (oldSpec.equals(spec)) {
                sto.setSFile(newSpec);
            }
        }
    }
}
