/**
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
package org.apache.pig.backend.hadoop.executionengine.tez;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.PigException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.ScalarPhyFinder;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.UDFFinder;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
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
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage.PackageType;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.PORank;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSkewedJoin;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSort;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSplit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStream;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POUnion;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.Operator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.OperatorPlan;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * The compiler that compiles a given physical plan into a DAG of Tez
 * operators which can then be converted into the JobControl structure.
 */
public class TezCompiler extends PhyPlanVisitor {
    private static final Log LOG = LogFactory.getLog(TezCompiler.class);

    private PigContext pigContext;

    //The plan that is being compiled
    private PhysicalPlan plan;

    //The plan of Tez Operators
    private TezOperPlan tezPlan;

    //The current Tez Operator that is being compiled
    private TezOperator curTezOp;

    //The output of compiling the inputs
    private TezOperator[] compiledInputs = null;

    //The split operators seen till now. If not maintained they will haunt you.
    //During the traversal a split is the only operator that can be revisited
    //from a different path. So this map stores the split job. So  whenever we
    //hit the split, we create a new TezOper and connect the split job using
    //load-store and also in the TezPlan
    private Map<OperatorKey, TezOperator> splitsSeen;

    private NodeIdGenerator nig;

    private String scope;

    private Random r;

    private UDFFinder udfFinder;

    private Map<PhysicalOperator, TezOperator> phyToTezOpMap;

    public static final String USER_COMPARATOR_MARKER = "user.comparator.func:";
    public static final String FILE_CONCATENATION_THRESHOLD = "pig.files.concatenation.threshold";
    public static final String OPTIMISTIC_FILE_CONCATENATION = "pig.optimistic.files.concatenation";

    private int fileConcatenationThreshold = 100;
    private boolean optimisticFileConcatenation = false;

    public TezCompiler(PhysicalPlan plan) throws TezCompilerException {
        this(plan, null);
    }

    public TezCompiler(PhysicalPlan plan, PigContext pigContext) throws TezCompilerException {
        super(plan, new DepthFirstWalker<PhysicalOperator, PhysicalPlan>(plan));
        this.plan = plan;
        this.pigContext = pigContext;
        splitsSeen = Maps.newHashMap();
        tezPlan = new TezOperPlan();
        nig = NodeIdGenerator.getGenerator();
        r = new Random(1331);
        FileLocalizer.setR(r);
        udfFinder = new UDFFinder();
        List<PhysicalOperator> roots = plan.getRoots();
        if((roots == null) || (roots.size() <= 0)) {
            int errCode = 2053;
            String msg = "Internal error. Did not find roots in the physical plan.";
            throw new TezCompilerException(msg, errCode, PigException.BUG);
        }
        scope = roots.get(0).getOperatorKey().getScope();
        phyToTezOpMap = Maps.newHashMap();

        fileConcatenationThreshold = Integer.parseInt(pigContext.getProperties()
                .getProperty(FILE_CONCATENATION_THRESHOLD, "100"));
        optimisticFileConcatenation = pigContext.getProperties().getProperty(
                OPTIMISTIC_FILE_CONCATENATION, "false").equals("true");
        LOG.info("File concatenation threshold: " + fileConcatenationThreshold
                + " optimistic? " + optimisticFileConcatenation);
    }

    public TezOperPlan getTezPlan() {
        return tezPlan;
    }

    /**
     * The front-end method that the user calls to compile the plan. Assumes
     * that all submitted plans have a Store operators as the leaf.
     * @return A Tez plan
     * @throws IOException
     * @throws PlanException
     * @throws VisitorException
     */
    public TezOperPlan compile() throws IOException, PlanException, VisitorException {
        List<PhysicalOperator> leaves = plan.getLeaves();

        if (!pigContext.inIllustrator) {
            for (PhysicalOperator op : leaves) {
                if (!(op instanceof POStore)) {
                    int errCode = 2025;
                    String msg = "Expected leaf of reduce plan to " +
                        "always be POStore. Found " + op.getClass().getSimpleName();
                    throw new TezCompilerException(msg, errCode, PigException.BUG);
                }
            }
        }

        // get all stores, sort them in order(operator id) and compile their
        // plans
        List<POStore> stores = PlanHelper.getPhysicalOperators(plan, POStore.class);
        List<PhysicalOperator> ops = Lists.newArrayList();
        ops.addAll(stores);
        Collections.sort(ops);

        for (PhysicalOperator op : ops) {
            compile(op);
        }

        connectSoftLink();

        return tezPlan;
    }

    private void connectSoftLink() throws PlanException, IOException {
        for (PhysicalOperator op : plan) {
            if (plan.getSoftLinkPredecessors(op)!=null) {
                for (PhysicalOperator pred : plan.getSoftLinkPredecessors(op)) {
                    TezOperator from = phyToTezOpMap.get(pred);
                    TezOperator to = phyToTezOpMap.get(op);
                    if (from==to) {
                        continue;
                    }
                    if (tezPlan.getPredecessors(to)==null || !tezPlan.getPredecessors(to).contains(from)) {
                        tezPlan.connect(from, to);
                    }
                }
            }
        }
    }

    /**
     * Compiles the plan below op into a Tez Operator
     * and stores it in curTezOp.
     * @param op
     * @throws IOException
     * @throws PlanException
     * @throws VisitorException
     */
    private void compile(PhysicalOperator op) throws IOException, PlanException, VisitorException {
        // An artifact of the Visitor. Need to save this so that it is not
        // overwritten.
        TezOperator[] prevCompInp = compiledInputs;

        // Compile each predecessor into the TezOperator and  store them away so
        // that we can use them for compiling op.
        List<PhysicalOperator> predecessors = plan.getPredecessors(op);
        if (predecessors != null && predecessors.size() > 0) {
            // When processing an entire script (multiquery), we can get into a
            // situation where a load has predecessors. This means that it
            // depends on some store earlier in the plan. We need to take that
            // dependency and connect the respective Tez operators, while at the
            // same time removing the connection between the Physical operators.
            // That way the jobs will run in the right order.
            if (op instanceof POLoad) {
                if (predecessors.size() != 1) {
                    int errCode = 2125;
                    String msg = "Expected at most one predecessor of load. Got "+predecessors.size();
                    throw new PlanException(msg, errCode, PigException.BUG);
                }

                PhysicalOperator p = predecessors.get(0);
                TezOperator oper = null;
                if (p instanceof POStore) {
                    oper = phyToTezOpMap.get(p);
                } else {
                    int errCode = 2126;
                    String msg = "Predecessor of load should be a store. Got "+p.getClass();
                    throw new PlanException(msg, errCode, PigException.BUG);
                }

                // Need new operator
                curTezOp = getTezOp();
                curTezOp.plan.add(op);
                tezPlan.add(curTezOp);

                plan.disconnect(op, p);
                tezPlan.connect(oper, curTezOp);
                phyToTezOpMap.put(op, curTezOp);
                return;
            }

            Collections.sort(predecessors);
            compiledInputs = new TezOperator[predecessors.size()];
            int i = -1;
            for (PhysicalOperator pred : predecessors) {
                if (pred instanceof POSplit && splitsSeen.containsKey(pred.getOperatorKey())) {
                    compiledInputs[++i] = startNew(((POSplit)pred).getSplitStore(), splitsSeen.get(pred.getOperatorKey()));
                    continue;
                }
                compile(pred);
                compiledInputs[++i] = curTezOp;
            }
        } else {
            // No predecessors. Mostly a load. But this is where we start. We
            // create a new TezOp and add its first operator op. Also this
            // should be added to the tezPlan.
            curTezOp = getTezOp();
            curTezOp.plan.add(op);
            if (op !=null && op instanceof POLoad) {
                if (((POLoad)op).getLFile()!=null && ((POLoad)op).getLFile().getFuncSpec()!=null)
                    curTezOp.UDFs.add(((POLoad)op).getLFile().getFuncSpec().toString());
            }
            tezPlan.add(curTezOp);
            phyToTezOpMap.put(op, curTezOp);
            return;
        }

        // Now we have the inputs compiled. Do something with the input oper op.
        op.visit(this);
        if (op.getRequestedParallelism() > curTezOp.requestedParallelism) {
            curTezOp.requestedParallelism = op.getRequestedParallelism();
        }
        compiledInputs = prevCompInp;
    }

    private TezOperator getTezOp() {
        return new TezOperator(new OperatorKey(scope, nig.getNextNodeId(scope)));
    }

    private POLoad getLoad() {
        POLoad ld = new POLoad(new OperatorKey(scope, nig.getNextNodeId(scope)));
        ld.setPc(pigContext);
        ld.setIsTmpLoad(true);
        return ld;
    }

    /**
     * Starts a new TezOperator and connects it to the old one by load-store.
     * The assumption is that the store is already inserted into the old
     * TezOperator.
     * @param fSpec
     * @param old
     * @return
     * @throws IOException
     * @throws PlanException
     */
    private TezOperator startNew(FileSpec fSpec, TezOperator old) throws PlanException {
        TezOperator ret = getTezOp();
        POLoad ld = getLoad();
        ld.setLFile(fSpec);
        ret.plan.add(ld);
        tezPlan.add(ret);
        tezPlan.connect(old, ret);
        return ret;
    }

    private void nonBlocking(PhysicalOperator op) throws PlanException, IOException {
        TezOperator tezOp;
        if (compiledInputs.length == 1) {
            tezOp = compiledInputs[0];
            if (tezOp.isClosed()) {
                int errCode = 2027;
                String msg = "Tez operator has been closed. This is unexpected for a merge.";
                throw new PlanException(msg, errCode, PigException.BUG);
            }
        } else {
            tezOp = merge(compiledInputs);
        }
        tezOp.plan.addAsLeaf(op);
        curTezOp = tezOp;
    }

    private void blocking(PhysicalOperator op) throws IOException, PlanException {
        TezOperator newTezOp = getTezOp();
        tezPlan.add(newTezOp);
        for (TezOperator tezOp : compiledInputs) {
            tezOp.setClosed(true);
            tezPlan.connect(tezOp, newTezOp);
        }
        curTezOp = newTezOp;
    }

    /**
     * Merges the TezOperators in the compiledInputs into a single merged
     * TezOperator.
     *
     * Care is taken to remove the TezOperators that are merged from the TezPlan
     * and their connections moved over to the merged map TezOperator.
     *
     * Merge is implemented as a sequence of binary merges.
     * merge(PhyPlan finPlan, List<PhyPlan> lst) := finPlan, merge(p) foreach p in lst
     *
     * @param compiledInputs
     * @return merged TezOperator
     * @throws PlanException
     * @throws IOException
     */
    private TezOperator merge(TezOperator[] compiledInputs) throws PlanException {
        TezOperator ret = getTezOp();
        tezPlan.add(ret);

        Set<TezOperator> toBeConnected = Sets.newHashSet();
        List<TezOperator> toBeRemoved = Lists.newArrayList();
        List<PhysicalPlan> toBeMerged = Lists.newArrayList();

        for (TezOperator tezOp : compiledInputs) {
            if (!tezOp.isClosed()) {
                toBeRemoved.add(tezOp);
                toBeMerged.add(tezOp.plan);
                List<TezOperator> predecessors = tezPlan.getPredecessors(tezOp);
                if (predecessors != null) {
                    for (TezOperator predecessorTezOp : predecessors) {
                        toBeConnected.add(predecessorTezOp);
                    }
                }
            } else {
                int errCode = 2027;
                String msg = "Tez operator has been closed. This is unexpected for a merge.";
                throw new PlanException(msg, errCode, PigException.BUG);
            }
        }

        merge(ret.plan, toBeMerged);

        Iterator<TezOperator> it = toBeConnected.iterator();
        while (it.hasNext()) {
            tezPlan.connect(it.next(), ret);
        }
        for (TezOperator tezOp : toBeRemoved) {
            if (tezOp.requestedParallelism > ret.requestedParallelism) {
                ret.requestedParallelism = tezOp.requestedParallelism;
            }
            for (String udf : tezOp.UDFs) {
                if (!ret.UDFs.contains(udf)) {
                    ret.UDFs.add(udf);
                }
            }
            // We also need to change scalar marking
            for (PhysicalOperator physOp : tezOp.scalars) {
                if (!ret.scalars.contains(physOp)) {
                    ret.scalars.add(physOp);
                }
            }
            Set<PhysicalOperator> opsToChange = Sets.newHashSet();
            for (Map.Entry<PhysicalOperator, TezOperator> entry : phyToTezOpMap.entrySet()) {
                if (entry.getValue()==tezOp) {
                    opsToChange.add(entry.getKey());
                }
            }
            for (PhysicalOperator op : opsToChange) {
                phyToTezOpMap.put(op, ret);
            }

            tezPlan.remove(tezOp);
        }
        return ret;
    }

    /**
     * The merge of a list of plans into a single plan
     * @param <O>
     * @param <E>
     * @param finPlan - Final Plan into which the list of plans is merged
     * @param plans - list of plans to be merged
     * @throws PlanException
     */
    private <O extends Operator<?>, E extends OperatorPlan<O>> void merge(
            E finPlan, List<E> plans) throws PlanException {
        for (E e : plans) {
            finPlan.merge(e);
        }
    }

    private void processUDFs(PhysicalPlan plan) throws VisitorException {
        if (plan != null) {
            //Process Scalars (UDF with referencedOperators)
            ScalarPhyFinder scalarPhyFinder = new ScalarPhyFinder(plan);
            scalarPhyFinder.visit();
            curTezOp.scalars.addAll(scalarPhyFinder.getScalars());

            //Process UDFs
            udfFinder.setPlan(plan);
            udfFinder.visit();
            curTezOp.UDFs.addAll(udfFinder.getUDFs());
        }
    }

    // visit methods in alphabetical order

    @Override
    public void visitCollectedGroup(POCollectedGroup op) throws VisitorException {
        int errCode = 2034;
        String msg = "Cannot compile " + op.getClass().getSimpleName();
        throw new TezCompilerException(msg, errCode, PigException.BUG);
    }

    @Override
    public void visitCounter(POCounter op) throws VisitorException {
        int errCode = 2034;
        String msg = "Cannot compile " + op.getClass().getSimpleName();
        throw new TezCompilerException(msg, errCode, PigException.BUG);
    }

    @Override
    public void visitCross(POCross op) throws VisitorException {
        int errCode = 2034;
        String msg = "Cannot compile " + op.getClass().getSimpleName();
        throw new TezCompilerException(msg, errCode, PigException.BUG);
    }

    @Override
    public void visitDistinct(PODistinct op) throws VisitorException {
        int errCode = 2034;
        String msg = "Cannot compile " + op.getClass().getSimpleName();
        throw new TezCompilerException(msg, errCode, PigException.BUG);
    }

    @Override
    public void visitFilter(POFilter op) throws VisitorException {
        try {
            nonBlocking(op);
            processUDFs(op.getPlan());
            phyToTezOpMap.put(op, curTezOp);
        } catch (Exception e) {
            int errCode = 2034;
            String msg = "Error compiling operator " + op.getClass().getSimpleName();
            throw new TezCompilerException(msg, errCode, PigException.BUG, e);
        }
    }

    @Override
    public void visitFRJoin(POFRJoin op) throws VisitorException {
        int errCode = 2034;
        String msg = "Cannot compile " + op.getClass().getSimpleName();
        throw new TezCompilerException(msg, errCode, PigException.BUG);
    }

    @Override
    public void visitLimit(POLimit op) throws VisitorException {
        int errCode = 2034;
        String msg = "Cannot compile " + op.getClass().getSimpleName();
        throw new TezCompilerException(msg, errCode, PigException.BUG);
    }

    @Override
    public void visitLoad(POLoad op) throws VisitorException {
        try {
            nonBlocking(op);
            phyToTezOpMap.put(op, curTezOp);
        } catch (Exception e) {
            int errCode = 2034;
            String msg = "Error compiling operator " + op.getClass().getSimpleName();
            throw new TezCompilerException(msg, errCode, PigException.BUG, e);
        }
    }

    @Override
    public void visitLocalRearrange(POLocalRearrange op) throws VisitorException {
        try{
            nonBlocking(op);
            List<PhysicalPlan> plans = op.getPlans();
            if (plans != null) {
                for (PhysicalPlan ep : plans) {
                    processUDFs(ep);
                }
            }
            phyToTezOpMap.put(op, curTezOp);
        } catch (Exception e) {
            int errCode = 2034;
            String msg = "Error compiling operator " + op.getClass().getSimpleName();
            throw new TezCompilerException(msg, errCode, PigException.BUG, e);
        }
    }

    @Override
    public void visitGlobalRearrange(POGlobalRearrange op) throws VisitorException {
        try {
            blocking(op);
            curTezOp.customPartitioner = op.getCustomPartitioner();
            phyToTezOpMap.put(op, curTezOp);
        } catch (Exception e) {
            int errCode = 2034;
            String msg = "Error compiling operator " + op.getClass().getSimpleName();
            throw new TezCompilerException(msg, errCode, PigException.BUG, e);
        }
    }

    @Override
    public void visitMergeCoGroup(POMergeCogroup op) throws VisitorException {
        int errCode = 2034;
        String msg = "Cannot compile " + op.getClass().getSimpleName();
        throw new TezCompilerException(msg, errCode, PigException.BUG);
    }

    @Override
    public void visitMergeJoin(POMergeJoin op) throws VisitorException {
        int errCode = 2034;
        String msg = "Cannot compile " + op.getClass().getSimpleName();
        throw new TezCompilerException(msg, errCode, PigException.BUG);
    }

    @Override
    public void visitNative(PONative op) throws VisitorException {
        int errCode = 2034;
        String msg = "Cannot compile " + op.getClass().getSimpleName();
        throw new TezCompilerException(msg, errCode, PigException.BUG);
    }

    @Override
    public void visitPackage(POPackage op) throws VisitorException{
        try{
            nonBlocking(op);
            phyToTezOpMap.put(op, curTezOp);
            if (op.getPackageType() == PackageType.JOIN) {
                curTezOp.markRegularJoin();
            } else if (op.getPackageType() == PackageType.GROUP) {
                if (op.getNumInps() == 1) {
                    curTezOp.markGroupBy();
                } else if (op.getNumInps() > 1) {
                    curTezOp.markCogroup();
                }
            }
        } catch (Exception e) {
            int errCode = 2034;
            String msg = "Error compiling operator " + op.getClass().getSimpleName();
            throw new TezCompilerException(msg, errCode, PigException.BUG, e);
        }
    }

    @Override
    public void visitPOForEach(POForEach op) throws VisitorException{
        try{
            nonBlocking(op);
            List<PhysicalPlan> plans = op.getInputPlans();
            if (plans != null) {
                for (PhysicalPlan ep : plans) {
                    processUDFs(ep);
                }
            }
            phyToTezOpMap.put(op, curTezOp);
        } catch (Exception e) {
            int errCode = 2034;
            String msg = "Error compiling operator " + op.getClass().getSimpleName();
            throw new TezCompilerException(msg, errCode, PigException.BUG, e);
        }
    }

    @Override
    public void visitRank(PORank op) throws VisitorException {
        int errCode = 2034;
        String msg = "Cannot compile " + op.getClass().getSimpleName();
        throw new TezCompilerException(msg, errCode, PigException.BUG);
    }

    @Override
    public void visitSkewedJoin(POSkewedJoin op) throws VisitorException {
        int errCode = 2034;
        String msg = "Cannot compile " + op.getClass().getSimpleName();
        throw new TezCompilerException(msg, errCode, PigException.BUG);
    }

    @Override
    public void visitSort(POSort op) throws VisitorException {
        int errCode = 2034;
        String msg = "Cannot compile " + op.getClass().getSimpleName();
        throw new TezCompilerException(msg, errCode, PigException.BUG);
    }

    @Override
    public void visitSplit(POSplit op) throws VisitorException {
        int errCode = 2034;
        String msg = "Cannot compile " + op.getClass().getSimpleName();
        throw new TezCompilerException(msg, errCode, PigException.BUG);
    }

    @Override
    public void visitStore(POStore op) throws VisitorException {
        try {
            nonBlocking(op);
            phyToTezOpMap.put(op, curTezOp);
            if (op.getSFile()!=null && op.getSFile().getFuncSpec()!=null)
                curTezOp.UDFs.add(op.getSFile().getFuncSpec().toString());
        } catch (Exception e) {
            int errCode = 2034;
            String msg = "Error compiling operator " + op.getClass().getSimpleName();
            throw new TezCompilerException(msg, errCode, PigException.BUG, e);
        }
    }

    @Override
    public void visitStream(POStream op) throws VisitorException {
        int errCode = 2034;
        String msg = "Cannot compile " + op.getClass().getSimpleName();
        throw new TezCompilerException(msg, errCode, PigException.BUG);
    }

    @Override
    public void visitUnion(POUnion op) throws VisitorException {
        try {
            nonBlocking(op);
            phyToTezOpMap.put(op, curTezOp);
        } catch (Exception e) {
            int errCode = 2034;
            String msg = "Error compiling operator " + op.getClass().getSimpleName();
            throw new TezCompilerException(msg, errCode, PigException.BUG, e);
        }
    }
}

