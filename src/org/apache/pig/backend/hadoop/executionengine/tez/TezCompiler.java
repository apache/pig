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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.ScalarPhyFinder;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.UDFFinder;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.ConstantExpression;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POProject;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POUserFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
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
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.PORank;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSkewedJoin;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSort;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSplit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStream;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POUnion;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.Packager.PackageType;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.builtin.FindQuantiles;
import org.apache.pig.impl.builtin.RandomSampleLoader;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.Operator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.OperatorPlan;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.impl.util.Utils;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.runtime.library.input.ShuffledUnorderedKVInput;
import org.apache.tez.runtime.library.output.OnFileUnorderedKVOutput;

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

    //The split operators seen till now.
    //During the traversal a split is the only operator that can be revisited
    //from a different path.
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

    public TezCompiler(PhysicalPlan plan, PigContext pigContext)
            throws TezCompilerException {
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

    // Segment a single DAG into a DAG graph
    public TezPlanContainer getPlanContainer() throws PlanException {
        TezPlanContainer tezPlanContainer = new TezPlanContainer(pigContext);
        TezPlanContainerNode node = new TezPlanContainerNode(new OperatorKey(scope, nig.getNextNodeId(scope)), tezPlan);
        tezPlanContainer.add(node);
        tezPlanContainer.split(node);
        return tezPlanContainer;
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
            if (curTezOp.isSplitSubPlan()) {
                // Set inputs to null as POSplit will attach input to roots
                for (PhysicalOperator root : curTezOp.plan.getRoots()) {
                    root.setInputs(null);
                }
                TezOperator splitOp = splitsSeen.get(curTezOp.getSplitOperatorKey());
                POSplit split = findPOSplit(splitOp, curTezOp.getSplitOperatorKey());
                split.addPlan(curTezOp.plan);
                addSubPlanPropertiesToParent(splitOp, curTezOp);
                curTezOp = splitOp;
            }
        }

        for (TezOperator tezOper : splitsSeen.values()) {
            int idx = 0;
            List<POLocalRearrange> rearranges = PlanHelper
                    .getPhysicalOperators(tezOper.plan, POLocalRearrange.class);
            for (POLocalRearrange op : rearranges) {
                op.setIndex(idx++);
            }
            idx = 0;
            List<POStore> strs = PlanHelper.getPhysicalOperators(tezOper.plan,
                    POStore.class);
            for (POStore op : strs) {
                op.setIndex(idx++);
            }
            tezOper.setClosed(true);
        }

        connectSoftLink();

        return tezPlan;
    }

    private void addSubPlanPropertiesToParent(TezOperator parentOper, TezOperator subPlanOper) {
        if (subPlanOper.requestedParallelism > parentOper.requestedParallelism) {
            parentOper.requestedParallelism = subPlanOper.requestedParallelism;
        }
        if (subPlanOper.UDFs != null) {
            parentOper.UDFs.addAll(subPlanOper.UDFs);
        }
        if (subPlanOper.outEdges != null) {
            for (Entry<OperatorKey, TezEdgeDescriptor> entry: subPlanOper.outEdges.entrySet()) {
                parentOper.outEdges.put(entry.getKey(), entry.getValue());
            }
        }
        // TODO: handle custom partitioner, secondary key on edges
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
        handleSplitAndConnect(old, ret);
        return ret;
    }

    /**
     * Start a new TezOperator whose plan will be the sub-plan of POSplit
     *
     * @param splitOperatorKey
     *            OperatorKey of the POSplit for which the new plan is a sub-plan
     * @return the new TezOperator
     * @throws PlanException
     */
    private TezOperator startNew(OperatorKey splitOperatorKey) throws PlanException {
        TezOperator ret = getTezOp();
        ret.setSplitOperatorKey(splitOperatorKey);
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

    private void connect(TezOperPlan plan, TezOperator from, TezOperator to) throws PlanException {
        plan.connect(from, to);
        // Add edge descriptors to old and new operators
        to.inEdges.put(from.getOperatorKey(), new TezEdgeDescriptor());
        from.outEdges.put(to.getOperatorKey(), new TezEdgeDescriptor());
    }

    private void blocking() throws IOException, PlanException {
        TezOperator newTezOp = getTezOp();
        tezPlan.add(newTezOp);
        for (TezOperator tezOp : compiledInputs) {
            tezOp.setClosed(true);
            handleSplitAndConnect(tezOp, newTezOp);
        }
        curTezOp = newTezOp;
    }

    private void handleSplitAndConnect(TezOperator from, TezOperator to)
            throws PlanException {
        // Add edge descriptors from POLocalRearrange in POSplit
        // sub-plan to new operators
        PhysicalOperator leaf = from.plan.getLeaves().get(0);
        // It could be POStoreTez incase of sampling job in order by
        if (leaf instanceof POLocalRearrangeTez) {
            POLocalRearrangeTez lr = (POLocalRearrangeTez) leaf;
            lr.setOutputKey(to.getOperatorKey().toString());
        }
        if (from.isSplitSubPlan()) {
            // Set inputs to null as POSplit will attach input to roots
            for (PhysicalOperator root : from.plan.getRoots()) {
                root.setInputs(null);
            }
            TezOperator splitOp = splitsSeen.get(from.getSplitOperatorKey());
            POSplit split = findPOSplit(splitOp, from.getSplitOperatorKey());
            split.addPlan(from.plan);
            addSubPlanPropertiesToParent(splitOp, curTezOp);
            connect(tezPlan, splitOp, to);
        } else {
            connect(tezPlan, from, to);
        }
    }

    private POSplit findPOSplit(TezOperator tezOp, OperatorKey splitKey)
            throws PlanException {
        POSplit split = (POSplit) tezOp.plan.getOperator(splitKey);
        if (split != null) {
            // The split is the leaf operator.
            return split;
        } else {
            // The split is a nested split
            Stack<POSplit> stack = new Stack<POSplit>();
            split = (POSplit) tezOp.plan.getLeaves().get(0);
            stack.push(split);
            while (!stack.isEmpty()) {
                split = stack.pop();
                for (PhysicalPlan plan : split.getPlans()) {
                    PhysicalOperator op = plan.getLeaves().get(0);
                    if (op instanceof POSplit) {
                        split = (POSplit) op;
                        if (split.getOperatorKey().equals(splitKey)) {
                            return split;
                        } else {
                            stack.push(split);
                        }
                    }
                }
            }
        }
        // TODO: what should be the new error code??
        throw new PlanException(
                "Could not find the split operator " + splitKey, 2059,
                PigException.BUG);
    }

    /**
     * Remove the operator and the whole tree connected to that operator from
     * the plan. Only remove corresponding connected sub-plan if you encounter
     * another Split operator in the predecessor.
     *
     * @param op Operator to remove
     * @throws VisitorException
     */
    public void removeDupOpTreeOfSplit(TezOperPlan plan, TezOperator op)
            throws VisitorException {
        Stack<TezOperator> stack = new Stack<TezOperator>();
        stack.push(op);
        while (!stack.isEmpty()) {
            op = stack.pop();
            List<TezOperator> predecessors = plan.getPredecessors(op);
            if (predecessors != null) {
                for (TezOperator pred : predecessors) {
                    List<POSplit> splits = PlanHelper.getPhysicalOperators(
                            pred.plan, POSplit.class);
                    if (splits.isEmpty()) {
                        stack.push(pred);
                    } else {
                        for (POSplit split : splits) {
                            PhysicalPlan planToRemove = null;
                            for (PhysicalPlan splitPlan : split.getPlans()) {
                                PhysicalOperator phyOp = splitPlan.getLeaves().get(0);
                                if (phyOp instanceof POLocalRearrangeTez) {
                                    POLocalRearrangeTez lr = (POLocalRearrangeTez) phyOp;
                                    if (lr.getOutputKey().equals(
                                            op.getOperatorKey().toString())) {
                                        planToRemove = splitPlan;
                                        break;
                                    }
                                }
                            }
                            if (planToRemove != null) {
                                split.getPlans().remove(planToRemove);
                                break;
                            }
                        }
                    }
                }
            }
            plan.remove(op);
        }
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
        try {
            POLocalRearrange lr = getLocalRearrange();
            lr.setDistinct(true);
            curTezOp.plan.addAsLeaf(lr);
            curTezOp.customPartitioner = op.getCustomPartitioner();

            // Mark the start of a new TezOperator, connecting the inputs.
            // TODO add distinct combiner as an optimization when supported by Tez
            blocking();

            POPackage pkg = getPackage();
            pkg.getPkgr().setDistinct(true);
            curTezOp.plan.add(pkg);

            POProject project = new POProject(new OperatorKey(scope, nig.getNextNodeId(scope)));
            project.setResultType(DataType.TUPLE);
            project.setStar(false);
            project.setColumn(0);
            project.setOverloaded(false);

            // Note that the PODistinct is not actually added to any Tez vertex, but rather is
            // implemented by the action of the local rearrange, shuffle and project operations.
            POForEach forEach = getForEach(project, op.getRequestedParallelism());
            curTezOp.plan.addAsLeaf(forEach);
            phyToTezOpMap.put(op, curTezOp);
        } catch (Exception e) {
            int errCode = 2034;
            String msg = "Cannot compile " + op.getClass().getSimpleName();
            throw new TezCompilerException(msg, errCode, PigException.BUG);
        }
    }

    @Override
    public void visitFilter(POFilter op) throws VisitorException {
        try {
            if (curTezOp.isSplitSubPlan()) {
                // Do not add the filter. Refer NoopFilterRemover.java of MR
                PhysicalPlan filterPlan = op.getPlan();
                if (filterPlan.size() == 1) {
                    PhysicalOperator fp = filterPlan.getRoots().get(0);
                    if (fp instanceof ConstantExpression) {
                        ConstantExpression exp = (ConstantExpression)fp;
                        Object value = exp.getValue();
                        if (value instanceof Boolean) {
                            Boolean filterValue = (Boolean)value;
                            if (filterValue) {
                                return;
                            }
                        }
                    }
                }
            }
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
        try {
            FileSpec[] replFiles = new FileSpec[op.getInputs().size()];
            for (int i = 0; i < replFiles.length; i++) {
                if (i == op.getFragment()) {
                    continue;
                }
                replFiles[i] = getTempFileSpec();
            }
            op.setReplFiles(replFiles);

            List<String> inputKeys = Lists.newArrayList();
            curTezOp = phyToTezOpMap.get(op.getInputs().get(op.getFragment()));

            for (int i = 0; i < compiledInputs.length; i++) {
                TezOperator tezOp = compiledInputs[i];
                if (curTezOp.equals(tezOp)) {
                    continue;
                }

                if (!tezOp.isClosed()) {
                    POLocalRearrangeTez lr = new POLocalRearrangeTez(op.getLRs()[i]);
                    lr.setOutputKey(curTezOp.getOperatorKey().toString());

                    tezOp.plan.addAsLeaf(lr);
                    connect(tezPlan, tezOp, curTezOp);
                    inputKeys.add(tezOp.getOperatorKey().toString());

                    // Configure broadcast edges for replicated tables
                    TezEdgeDescriptor edge = curTezOp.inEdges.get(tezOp.getOperatorKey());
                    edge.dataMovementType = DataMovementType.BROADCAST;
                    edge.outputClassName = OnFileUnorderedKVOutput.class.getName();
                    edge.inputClassName = ShuffledUnorderedKVInput.class.getName();
                } else {
                    int errCode = 2022;
                    String msg = "The current operator is closed. This is unexpected while compiling.";
                    throw new TezCompilerException(msg, errCode, PigException.BUG);
                }
            }

            if (!curTezOp.isClosed()) {
                curTezOp.plan.addAsLeaf(new POFRJoinTez(op, inputKeys));
            } else {
                int errCode = 2022;
                String msg = "The current operator is closed. This is unexpected while compiling.";
                throw new TezCompilerException(msg, errCode, PigException.BUG);
            }

            List<List<PhysicalPlan>> joinPlans = op.getJoinPlans();
            if (joinPlans != null) {
                for (List<PhysicalPlan> joinPlan : joinPlans) {
                    if (joinPlan != null) {
                        for (PhysicalPlan plan : joinPlan) {
                            processUDFs(plan);
                        }
                    }
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
    public void visitLimit(POLimit op) throws VisitorException {
        try {
            if (op.getLimitPlan() != null) {
                processUDFs(op.getLimitPlan());
            }

            // As an optimization, we'll add a limit to the end of the last tezOp. Note that in
            // some cases, such as when we have ORDER BY followed by LIMIT, the LimitOptimizer has
            // already removed the POLimit from the physical plan.
            if (!pigContext.inIllustrator) {
                nonBlocking(op);
                phyToTezOpMap.put(op, curTezOp);
            }

            // Need to add POLocalRearrange to the end of the last tezOp before we shuffle.
            POLocalRearrange lr = getLocalRearrange();
            curTezOp.plan.addAsLeaf(lr);

            // Mark the start of a new TezOperator, connecting the inputs. Note the parallelism is
            // currently fixed to 1 for all TezOperators.
            // TODO Explicitly set the parallelism once this is supported by TezOperator.
            blocking();

            // Then add a POPackage and a POForEach to the start of the new tezOp.
            POPackage pkg = getPackage();
            POForEach forEach = getForEachPlain();
            curTezOp.plan.add(pkg);
            curTezOp.plan.addAsLeaf(forEach);

            if (!pigContext.inIllustrator) {
                POLimit limitCopy = new POLimit(new OperatorKey(scope, nig.getNextNodeId(scope)));
                limitCopy.setLimit(op.getLimit());
                limitCopy.setLimitPlan(op.getLimitPlan());
                curTezOp.plan.addAsLeaf(limitCopy);
            } else {
                curTezOp.plan.addAsLeaf(op);
            }
        } catch (Exception e) {
            int errCode = 2034;
            String msg = "Error compiling operator " + op.getClass().getSimpleName();
            throw new TezCompilerException(msg, errCode, PigException.BUG, e);
        }
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
            POLocalRearrange opTez = new POLocalRearrangeTez(op);
            nonBlocking(opTez);
            List<PhysicalPlan> plans = opTez.getPlans();
            if (plans != null) {
                for (PhysicalPlan ep : plans) {
                    processUDFs(ep);
                }
            }
            phyToTezOpMap.put(opTez, curTezOp);
        } catch (Exception e) {
            int errCode = 2034;
            String msg = "Error compiling operator " + op.getClass().getSimpleName();
            throw new TezCompilerException(msg, errCode, PigException.BUG, e);
        }
    }

    @Override
    public void visitGlobalRearrange(POGlobalRearrange op) throws VisitorException {
        try {
            blocking();
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
            if (op.getPkgr().getPackageType() == PackageType.UNION) {
                curTezOp.markUnion();
            } else if (op.getPkgr().getPackageType() == PackageType.JOIN) {
                curTezOp.markRegularJoin();
            } else if (op.getPkgr().getPackageType() == PackageType.GROUP) {
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

    /**
     * Returns a temporary DFS Path
     * @return
     * @throws IOException
     */
    private FileSpec getTempFileSpec() throws IOException {
        return new FileSpec(FileLocalizer.getTemporaryPath(pigContext).toString(),
                new FuncSpec(Utils.getTmpFileCompressorName(pigContext)));
    }

    private POStore getStore(){
        POStore st = new POStoreTez(new OperatorKey(scope, nig.getNextNodeId(scope)));
        // mark store as tmp store. These could be removed by the
        // optimizer, because it wasn't the user requesting it.
        st.setIsTmpStore(true);
        return st;
    }

    /**
     * Force an end to the current vertex with a store into a temporary
     * file.
     * @param fSpec Temp file to force a store into.
     * @return Tez operator that now is finished with a store.
     * @throws PlanException
     */
    private TezOperator endSingleInputPlanWithStr(FileSpec fSpec) throws PlanException{
        if(compiledInputs.length>1) {
            int errCode = 2023;
            String msg = "Received a multi input plan when expecting only a single input one.";
            throw new PlanException(msg, errCode, PigException.BUG);
        }
        TezOperator oper = compiledInputs[0];
        if (!oper.isClosed()) {
            POStore str = getStore();
            str.setSFile(fSpec);
            oper.plan.addAsLeaf(str);
        } else {
            int errCode = 2022;
            String msg = "Both map and reduce phases have been done. This is unexpected while compiling.";
            throw new PlanException(msg, errCode, PigException.BUG);
        }
        return oper;
    }

    private Pair<TezOperator[],Integer> getQuantileJobs(
            POSort inpSort,
            TezOperator prevJob,
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

        return getSamplingJobs(sort, prevJob, null, lFile, quantFile, rp, null, FindQuantiles.class.getName(), ctorArgs, RandomSampleLoader.class.getName());
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
     * @return pair<tezoperator[],integer>
     * @throws PlanException
     * @throws VisitorException
     */
    private Pair<TezOperator[],Integer> getSamplingJobs(POSort sort, TezOperator prevJob, List<PhysicalPlan> transformPlans,
            FileSpec lFile, FileSpec sampleFile, int rp, List<PhysicalPlan> sortKeyPlans,
            String udfClassName, String[] udfArgs, String sampleLdrClassName ) throws PlanException, VisitorException {

        String[] rslargs = new String[2];
        // SampleLoader expects string version of FuncSpec
        // as its first constructor argument.

        rslargs[0] = (new FuncSpec(Utils.getTmpFileCompressorName(pigContext))).toString();

        rslargs[1] = "100"; // The value is calculated based on the file size for skewed join
        FileSpec quantLdFilName = new FileSpec(lFile.getFileName(),
                new FuncSpec(sampleLdrClassName, rslargs));

        TezOperator[] opers = new TezOperator[2];

        TezOperator oper1 = startNew(quantLdFilName, prevJob);
        opers[0] = oper1;

        // TODO: Review sort udf
//        if(sort.isUDFComparatorUsed) {
//            mro.UDFs.add(sort.getMSortFunc().getFuncSpec().toString());
//            curMROp.isUDFComparatorUsed = true;
//        }

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
                        throw new TezCompilerException(msg, errCode, PigException.BUG);
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
        oper1.plan.addAsLeaf(nfe1);

        // Now set up a POLocalRearrange which has "all" as the key and the output of the
        // foreach will be the "value" out of POLocalRearrange
        PhysicalPlan ep1 = new PhysicalPlan();
        ConstantExpression ce = new ConstantExpression(new OperatorKey(scope,nig.getNextNodeId(scope)));
        ce.setValue("all");
        ce.setResultType(DataType.CHARARRAY);
        ep1.add(ce);

        List<PhysicalPlan> eps = new ArrayList<PhysicalPlan>();
        eps.add(ep1);

        POLocalRearrangeTez lr = new POLocalRearrangeTez(new OperatorKey(scope,nig.getNextNodeId(scope)));
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
        oper1.plan.add(lr);
        oper1.plan.connect(nfe1, lr);

        oper1.setClosed(true);

        TezOperator oper2 = getTezOp();
        opers[1] = oper2;
        tezPlan.add(oper2);
        connect(tezPlan, oper1, oper2);
        lr.setOutputKey(oper2.getOperatorKey().toString());

        POPackage pkg = new POPackage(new OperatorKey(scope,nig.getNextNodeId(scope)));
        pkg.getPkgr().setKeyType(DataType.CHARARRAY);
        pkg.setNumInps(1);
        boolean[] inner = {false};
        pkg.getPkgr().setInner(inner);
        oper2.plan.add(pkg);

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
        oper2.plan.add(nfe2);
        oper2.plan.connect(pkg, nfe2);

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

        oper2.plan.add(nfe3);
        oper2.plan.connect(nfe2, nfe3);

        POStore str = getStore();
        str.setSFile(sampleFile);

        oper2.plan.add(str);
        oper2.plan.connect(nfe3, str);

        oper2.setClosed(true);
        oper2.requestedParallelism = 1;
        // oper2.markSampler();
        return new Pair<TezOperator[], Integer>(opers, rp);
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

    private TezOperator[] getSortJobs(
            POSort sort,
            TezOperator quantJob,
            FileSpec lFile,
            FileSpec quantFile,
            int rp,
            Pair<POProject, Byte>[] fields) throws PlanException{
        TezOperator[] opers = new TezOperator[2];
        TezOperator oper1 = startNew(lFile, quantJob);
        tezPlan.add(oper1);
        opers[0] = oper1;
        oper1.setQuantFile(quantFile.getFileName());
        oper1.setGlobalSort(true);
        oper1.requestedParallelism = rp;

        long limit = sort.getLimit();
        oper1.limit = limit;

        List<PhysicalPlan> eps1 = new ArrayList<PhysicalPlan>();

        byte keyType = DataType.UNKNOWN;

        boolean[] sortOrder;

        List<Boolean> sortOrderList = sort.getMAscCols();
        if(sortOrderList != null) {
            sortOrder = new boolean[sortOrderList.size()];
            for(int i = 0; i < sortOrderList.size(); ++i) {
                sortOrder[i] = sortOrderList.get(i);
            }
            oper1.setSortOrder(sortOrder);
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

        POLocalRearrangeTez lr = new POLocalRearrangeTez(new OperatorKey(scope,nig.getNextNodeId(scope)));
        try {
            lr.setIndex(0);
        } catch (ExecException e) {
            int errCode = 2058;
            String msg = "Unable to set index on newly created POLocalRearrange.";
            throw new PlanException(msg, errCode, PigException.BUG, e);
        }
        lr.setKeyType((fields == null || fields.length>1) ? DataType.TUPLE : keyType);
        lr.setPlans(eps1);
        lr.setResultType(DataType.TUPLE);
        lr.addOriginalLocation(sort.getAlias(), sort.getOriginalLocations());
        oper1.plan.addAsLeaf(lr);

        oper1.setClosed(true);

        TezOperator oper2 = getTezOp();
        opers[1] = oper2;
        tezPlan.add(oper2);
        connect(tezPlan, oper1, oper2);
        lr.setOutputKey(oper2.getOperatorKey().toString());

        if (limit!=-1) {
            POPackage pkg_c = new POPackage(new OperatorKey(scope, nig.getNextNodeId(scope)));
            pkg_c.setPkgr(new LitePackager());
            pkg_c.getPkgr().setKeyType((fields.length > 1) ? DataType.TUPLE : keyType);
            pkg_c.setNumInps(1);
            oper2.inEdges.put(oper1.getOperatorKey(), new TezEdgeDescriptor());
            PhysicalPlan combinePlan = oper2.inEdges.get(oper1.getOperatorKey()).combinePlan;

            combinePlan.add(pkg_c);

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

            combinePlan.addAsLeaf(fe_c1);

            POLimit pLimit = new POLimit(new OperatorKey(scope,nig.getNextNodeId(scope)));
            pLimit.setLimit(limit);
            combinePlan.addAsLeaf(pLimit);

            List<PhysicalPlan> eps_c2 = new ArrayList<PhysicalPlan>();
            eps_c2.addAll(sort.getSortPlans());

            POLocalRearrangeTez lr_c2 = new POLocalRearrangeTez(new OperatorKey(scope,nig.getNextNodeId(scope)));
            lr_c2.setOutputKey(oper2.getOperatorKey().toString());
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
            combinePlan.addAsLeaf(lr_c2);
        }

        POPackage pkg = new POPackage(new OperatorKey(scope, nig.getNextNodeId(scope)));
        pkg.setPkgr(new LitePackager());
        pkg.getPkgr().setKeyType((fields == null || fields.length > 1) ? DataType.TUPLE : keyType);
        pkg.setNumInps(1);
        oper2.plan.add(pkg);

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
        oper2.plan.add(nfe1);
        oper2.plan.connect(pkg, nfe1);
        if (limit!=-1) {
            POLimit pLimit2 = new POLimit(new OperatorKey(scope,nig.getNextNodeId(scope)));
            pLimit2.setLimit(limit);
            oper2.plan.addAsLeaf(pLimit2);
        }

        return opers;
    }

    @Override
    public void visitSort(POSort op) throws VisitorException {
        try{
            FileSpec fSpec = getTempFileSpec();
            TezOperator oper = endSingleInputPlanWithStr(fSpec);
            oper.segmentBelow = true;
            FileSpec quantFile = getTempFileSpec();
            int rp = op.getRequestedParallelism();
            Pair<POProject, Byte>[] fields = getSortCols(op.getSortPlans());
            Pair<TezOperator[], Integer> quantJobParallelismPair =
                getQuantileJobs(op, oper, fSpec, quantFile, rp);
            TezOperator[] opers = getSortJobs(op, quantJobParallelismPair.first[1], fSpec, quantFile,
                    quantJobParallelismPair.second, fields);

            quantJobParallelismPair.first[1].segmentBelow = true;

            curTezOp = opers[1];

            // TODO: Review sort udf
//            if(op.isUDFComparatorUsed){
//                curTezOp.UDFs.add(op.getMSortFunc().getFuncSpec().toString());
//                curTezOp.isUDFComparatorUsed = true;
//            }
            phyToTezOpMap.put(op, curTezOp);
        }catch(Exception e){
            int errCode = 2034;
            String msg = "Error compiling operator " + op.getClass().getSimpleName();
            throw new TezCompilerException(msg, errCode, PigException.BUG, e);
        }
    }

    @Override
    public void visitSplit(POSplit op) throws VisitorException {
        try {
            if (splitsSeen.containsKey(op.getOperatorKey())) {
                // Since the plan for this split already exists in the tez plan,
                // discard the hierarchy or tez operators we constructed so far
                // till we encountered the split in this tree
                removeDupOpTreeOfSplit(tezPlan, curTezOp);
                curTezOp = startNew(op.getOperatorKey());
            } else {
                nonBlocking(op);
                if(curTezOp.isSplitSubPlan()) {
                    // Split followed by another split
                    // Set inputs to null as POSplit will attach input to roots
                    for (PhysicalOperator root : curTezOp.plan.getRoots()) {
                        root.setInputs(null);
                    }
                    TezOperator splitOp = splitsSeen.get(curTezOp.getSplitOperatorKey());
                    POSplit split = findPOSplit(splitOp, curTezOp.getSplitOperatorKey());
                    split.addPlan(curTezOp.plan);
                    addSubPlanPropertiesToParent(splitOp, curTezOp);
                    splitsSeen.put(op.getOperatorKey(), splitOp);
                    phyToTezOpMap.put(op, splitOp);
                } else {
                    splitsSeen.put(op.getOperatorKey(), curTezOp);
                    phyToTezOpMap.put(op, curTezOp);
                }
                curTezOp = startNew(op.getOperatorKey());
            }
        } catch (Exception e) {
            int errCode = 2034;
            String msg = "Error compiling operator "
                    + op.getClass().getSimpleName();
            throw new TezCompilerException(msg, errCode, PigException.BUG, e);
        }

    }

    @Override
    public void visitStore(POStore op) throws VisitorException {
        try {
            POStoreTez store = new POStoreTez(op);
            nonBlocking(store);
            phyToTezOpMap.put(store, curTezOp);
            if (store.getSFile()!=null && store.getSFile().getFuncSpec()!=null)
                curTezOp.UDFs.add(store.getSFile().getFuncSpec().toString());
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
            // Need to add POLocalRearrange to the end of each previous tezOp
            // before we broadcast.
            for (int i = 0; i < compiledInputs.length; i++) {
                POLocalRearrangeTez lr = getLocalRearrange(i);
                lr.setUnion(true);
                compiledInputs[i].plan.addAsLeaf(lr);
            }

            // Mark the start of a new TezOperator, connecting the inputs. Note
            // the parallelism is currently fixed to 1 for all TezOperators.
            blocking();

            // Then add a POPackage to the start of the new tezOp.
            POPackage pkg = getPackage(compiledInputs.length);
            curTezOp.markUnion();
            curTezOp.plan.add(pkg);
            // TODO: Union should use OnFileUnorderedKVOutput instead of
            // OnFileSortedOutput. Currently, it's not supported by Tez.
            // (TEZ-661)
        } catch (Exception e) {
            int errCode = 2034;
            String msg = "Error compiling operator " + op.getClass().getSimpleName();
            throw new TezCompilerException(msg, errCode, PigException.BUG, e);
        }
    }

    private POForEach getForEach(POProject project, int rp) {
        PhysicalPlan forEachPlan = new PhysicalPlan();
        forEachPlan.add(project);

        List<PhysicalPlan> forEachPlans = Lists.newArrayList();
        forEachPlans.add(forEachPlan);

        List<Boolean> flatten = Lists.newArrayList();
        flatten.add(true);

        POForEach forEach = new POForEach(new OperatorKey(scope, nig.getNextNodeId(scope)), rp, forEachPlans, flatten);
        forEach.setResultType(DataType.BAG);
        return forEach;
    }

    // Get a plain POForEach: ForEach X generate flatten($1)
    private POForEach getForEachPlain() {
        POProject project = new POProject(new OperatorKey(scope, nig.getNextNodeId(scope)));
        project.setResultType(DataType.TUPLE);
        project.setStar(false);
        project.setColumn(1);
        project.setOverloaded(true);
        return getForEach(project, -1);
    }

    private POLoad getLoad() {
        POLoad ld = new POLoad(new OperatorKey(scope, nig.getNextNodeId(scope)));
        ld.setPc(pigContext);
        ld.setIsTmpLoad(true);
        return ld;
    }

    private POLocalRearrangeTez getLocalRearrange() throws PlanException {
        return getLocalRearrange(0);
    }

    private POLocalRearrangeTez getLocalRearrange(int index) throws PlanException {
        POProject projectStar = new POProject(new OperatorKey(scope, nig.getNextNodeId(scope)));
        projectStar.setResultType(DataType.TUPLE);
        projectStar.setStar(true);

        PhysicalPlan addPlan = new PhysicalPlan();
        addPlan.add(projectStar);

        List<PhysicalPlan> addPlans = Lists.newArrayList();
        addPlans.add(addPlan);

        POLocalRearrangeTez lr = new POLocalRearrangeTez(new OperatorKey(scope, nig.getNextNodeId(scope)));
        try {
            lr.setIndex(index);
        } catch (ExecException e) {
            int errCode = 2058;
            String msg = "Unable to set index on the newly created POLocalRearrange.";
            throw new PlanException(msg, errCode, PigException.BUG, e);
        }
        lr.setKeyType(DataType.TUPLE);
        lr.setPlans(addPlans);
        lr.setResultType(DataType.TUPLE);
        return lr;
    }

    private POPackage getPackage() {
        return getPackage(1);
    }

    private POPackage getPackage(int numOfInputs) {
        // The default value of boolean is false
        boolean[] inner = new boolean[numOfInputs];
        POPackage pkg = new POPackage(new OperatorKey(scope, nig.getNextNodeId(scope)));
        pkg.getPkgr().setInner(inner);
        pkg.getPkgr().setKeyType(DataType.TUPLE);
        pkg.setNumInps(numOfInputs);
        return pkg;
    }

    private TezOperator getTezOp() {
        return new TezOperator(new OperatorKey(scope, nig.getNextNodeId(scope)));
    }
}

