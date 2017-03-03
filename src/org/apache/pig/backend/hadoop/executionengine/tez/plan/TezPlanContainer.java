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
package org.apache.pig.backend.hadoop.executionengine.tez.plan;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.tez.TezResourceManager;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.operator.POStoreTez;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.OperatorPlan;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.JarManager;

public class TezPlanContainer extends OperatorPlan<TezPlanContainerNode> {
    private static final long serialVersionUID = 1L;
    private PigContext pigContext;
    private String jobName;
    private long dagId = 0;
    //Incrementing static counter if multiple pig scripts have same name
    private static long scopeId = 0;

    public TezPlanContainer(PigContext pigContext) {
        this.pigContext = pigContext;
        this.jobName = pigContext.getProperties().getProperty(PigContext.JOB_NAME, "pig");
    }

    // Add the Pig jar and the UDF jars as AM resources (all DAG's in the planContainer
    // will use them for simplicity). This differs from MR Pig, where they are added to
    // the job jar.
    public Map<String, LocalResource> getLocalResources() throws Exception {
        Set<URI> jarLists = new HashSet<URI>();

        for (String jarFile : JarManager.getDefaultJars()) {
            jarLists.add(new File(jarFile).toURI());
        }

        // In MR Pig the extra jars and script jars get put in Distributed Cache, but
        // in Tez we'll add them as local resources.
        for (URL jarUrl : pigContext.extraJars) {
            jarLists.add(jarUrl.toURI());
        }

        for (String jarFile : pigContext.scriptJars) {
            jarLists.add(new File(jarFile).toURI());
        }

        // Script files for non-Java UDF's are added to the Job.jar by the JarManager class,
        // except for Groovy files, which need to be explicitly added as local resources due
        // to the GroovyScriptEngine (see JarManager.java for comments).
        for (Map.Entry<String, File> scriptFile : pigContext.getScriptFiles().entrySet()) {
            if (scriptFile.getKey().endsWith(".groovy")) {
                jarLists.add(scriptFile.getValue().toURI());
            }
        }

        TezPlanContainerUDFCollector tezPlanContainerUDFCollector = new TezPlanContainerUDFCollector(this);
        tezPlanContainerUDFCollector.visit();
        Set<String> udfs = tezPlanContainerUDFCollector.getUdfs();

        for (String func : udfs) {
            Class<?> clazz = pigContext.getClassForAlias(func);
            if (clazz != null) {
                String jarName = JarManager.findContainingJar(clazz);
                if (jarName == null) {
                    // It's possible that clazz is not registered by an external
                    // jar. For eg, in unit tests, UDFs that are not in default
                    // packages may be used. In that case, we should skip it to
                    // avoid NPE.
                    continue;
                }
                URI jarUri = new File(jarName).toURI();
                jarLists.add(jarUri);
            }
        }

        File scriptUDFJarFile = JarManager.createPigScriptUDFJar(pigContext);
        if (scriptUDFJarFile != null) {
            jarLists.add(scriptUDFJarFile.toURI());
        }

        return TezResourceManager.getInstance().addTezResources(jarLists);
    }

    public TezPlanContainerNode getNextPlan(List<TezOperPlan> processedPlans) {
        synchronized(this) {
            while (getRoots()!=null && !getRoots().isEmpty()) {
                TezPlanContainerNode currentPlan = null;
                for (TezPlanContainerNode plan : getRoots()) {
                    if (!processedPlans.contains(plan.getTezOperPlan())) {
                        currentPlan = plan;
                        break;
                    }
                }
                if (currentPlan!=null) {
                    return currentPlan;
                } else {
                    try {
                        wait();
                    } catch (InterruptedException e) {
                    }
                }
            }
        }
        return null;
    }

    public void addPlan(TezOperPlan plan) throws PlanException {
        TezPlanContainerNode node = new TezPlanContainerNode(generateNodeOperatorKey(), plan);
        this.add(node);
        this.split(node);
    }

    public void updatePlan(TezOperPlan plan, boolean succ) {
        TezPlanContainerNode tezPlanContainerNode = new TezPlanContainerNode(
                generateNodeOperatorKey(), plan);
        synchronized(this) {
            if (succ) {
                remove(tezPlanContainerNode);
            } else {
                // job fail
                trimBelow(tezPlanContainerNode);
                remove(tezPlanContainerNode);
            }
            notify();
        }
    }

    public void split(TezPlanContainerNode planNode) throws PlanException {

        TezOperPlan tezOperPlan = planNode.getTezOperPlan();
        if (tezOperPlan.size() == 1) {
            // Nothing to split. Only one operator in the plan
            return;
        }

        List<TezOperator> opersToSegment = null;
        try {
            // Split top down from root to leaves
            // Get list of operators closer to the root that can be segmented together
            FirstLevelSegmentOperatorsFinder finder = new FirstLevelSegmentOperatorsFinder(tezOperPlan);
            finder.visit();
            opersToSegment = finder.getOperatorsToSegment();
        } catch (VisitorException e) {
            throw new PlanException(e);
        }
        if (!opersToSegment.isEmpty()) {
            Set<TezOperator> commonSplitterPredecessors = new HashSet<>();
            for (TezOperator operToSegment : opersToSegment) {
                for (TezOperator succ : tezOperPlan.getSuccessors(operToSegment)) {
                    commonSplitterPredecessors
                            .addAll(getCommonSplitterPredecessors(tezOperPlan,
                                    operToSegment, succ));
                }
            }

            if (commonSplitterPredecessors.isEmpty()) {
                List<TezOperator> allSuccs = new ArrayList<TezOperator>();
                // Disconnect all the successors and move them to a new plan
                for (TezOperator operToSegment : opersToSegment) {
                    List<TezOperator> succs = new ArrayList<TezOperator>();
                    succs.addAll(tezOperPlan.getSuccessors(operToSegment));
                    allSuccs.addAll(succs);
                    for (TezOperator succ : succs) {
                        tezOperPlan.disconnect(operToSegment, succ);
                    }
                }
                TezOperPlan newOperPlan = new TezOperPlan();
                for (TezOperator succ : allSuccs) {
                    tezOperPlan.moveTree(succ, newOperPlan);
                }
                TezPlanContainerNode newPlanNode = new TezPlanContainerNode(
                        generateNodeOperatorKey(), newOperPlan);
                add(newPlanNode);
                connect(planNode, newPlanNode);
                split(newPlanNode);
            } else {
                // If there is a common splitter predecessor between operToSegment and the successor,
                // we have to separate out that split to be able to segment.
                // So we store the output of split to a temp store and then change the
                // splittees to load from it.
                String scope = opersToSegment.get(0).getOperatorKey().getScope();
                for (TezOperator splitter : commonSplitterPredecessors) {
                    try {
                        List<TezOperator> succs = new ArrayList<TezOperator>();
                        succs.addAll(tezOperPlan.getSuccessors(splitter));
                        FileSpec fileSpec = TezCompiler.getTempFileSpec(pigContext);
                        POStore tmpStore = getTmpStore(scope, fileSpec);
                        // Replace POValueOutputTez with POStore
                        splitter.plan.remove(splitter.plan.getLeaves().get(0));
                        splitter.plan.addAsLeaf(tmpStore);
                        splitter.segmentBelow = true;
                        splitter.setSplitter(false);
                        for (TezOperator succ : succs) {
                            // Replace POValueInputTez with POLoad
                            POLoad tmpLoad = getTmpLoad(scope, fileSpec);
                            succ.plan.replace(succ.plan.getRoots().get(0), tmpLoad);
                        }
                    } catch (Exception e) {
                        throw new PlanException(e);
                    }
                }
            }
            split(planNode);
        }
    }

    private static class FirstLevelSegmentOperatorsFinder extends TezOpPlanVisitor {

        private List<TezOperator> opersToSegment = new ArrayList<>();

        public FirstLevelSegmentOperatorsFinder(TezOperPlan plan) {
            super(plan, new DependencyOrderWalker<TezOperator, TezOperPlan>(plan));
        }

        public List<TezOperator> getOperatorsToSegment() {
            return opersToSegment;
        }

        @Override
        public void visitTezOp(TezOperator tezOp) throws VisitorException {
            if (tezOp.needSegmentBelow() && getPlan().getSuccessors(tezOp) != null) {
                if (opersToSegment.isEmpty()) {
                    opersToSegment.add(tezOp);
                } else {
                    // If the operator does not have dependency on previous
                    // operators chosen for segmenting then add it to the
                    // operators to be segmented together
                    if (!hasPredecessor(tezOp, opersToSegment)) {
                        opersToSegment.add(tezOp);
                    }
                }
            }
        }

        /**
         * Check if the tezOp has one of the opsToCheck as a predecessor.
         * It can be a immediate predecessor or multiple levels up.
         */
        private boolean hasPredecessor(TezOperator tezOp, List<TezOperator> opsToCheck) {
            List<TezOperator> predecessors = getPlan().getPredecessors(tezOp);
            if (predecessors != null) {
                for (TezOperator pred : predecessors) {
                    if (opersToSegment.contains(pred)) {
                        return true;
                    } else {
                        if (hasPredecessor(pred, opsToCheck)) {
                            return true;
                        }
                    }
                }
            }
            return false;
        }

    }

    private Set<TezOperator> getCommonSplitterPredecessors(TezOperPlan plan, TezOperator operToSegment, TezOperator successor) {
        Set<TezOperator> splitters1 = new HashSet<>();
        Set<TezOperator> splitters2 = new HashSet<>();
        Set<TezOperator> processedPredecessors = new HashSet<>();
        // Find predecessors which are splitters
        fetchSplitterPredecessors(plan, operToSegment, processedPredecessors, splitters1, false);
        if (!splitters1.isEmpty()) {
            // For the successor, traverse rest of the plan below it and
            // search the predecessors of its successors to find any predecessor that might be a splitter.
            Set<TezOperator> allSuccs = new HashSet<>();
            getAllSuccessors(plan, successor, allSuccs);
            processedPredecessors.clear();
            processedPredecessors.add(successor);
            for (TezOperator succ : allSuccs) {
                fetchSplitterPredecessors(plan, succ, processedPredecessors, splitters2, true);
            }
            // Find the common ones
            splitters1.retainAll(splitters2);
        }
        return splitters1;
    }

    private void fetchSplitterPredecessors(TezOperPlan plan, TezOperator tezOp,
            Set<TezOperator> processedPredecessors, Set<TezOperator> splitters, boolean stopAtSplit) {
        List<TezOperator> predecessors = plan.getPredecessors(tezOp);
        if (predecessors != null) {
            for (TezOperator pred : predecessors) {
                // Skip processing already processed predecessor to avoid loops
                if (processedPredecessors.contains(pred)) {
                    continue;
                }
                if (pred.isSplitter()) {
                    splitters.add(pred);
                    if (!stopAtSplit) {
                        processedPredecessors.add(pred);
                        fetchSplitterPredecessors(plan, pred, processedPredecessors, splitters, stopAtSplit);
                    }
                } else if (!pred.needSegmentBelow()) {
                    processedPredecessors.add(pred);
                    fetchSplitterPredecessors(plan, pred, processedPredecessors, splitters, stopAtSplit);
                }
            }
        }
    }

    private void getAllSuccessors(TezOperPlan plan, TezOperator tezOp, Set<TezOperator> allSuccs) {
        List<TezOperator> successors = plan.getSuccessors(tezOp);
        if (successors != null) {
            for (TezOperator succ : successors) {
                if (!allSuccs.contains(succ)) {
                    allSuccs.add(succ);
                    getAllSuccessors(plan, succ, allSuccs);
                }
            }
        }
    }

    private synchronized OperatorKey generateNodeOperatorKey() {
        OperatorKey opKey = new OperatorKey(jobName + "-" + dagId + "_scope", scopeId);
        scopeId++;
        dagId++;
        return opKey;
    }

    public static void resetScope() {
        scopeId = 0;
    }

    private POLoad getTmpLoad(String scope, FileSpec fileSpec){
        POLoad ld = new POLoad(new OperatorKey(scope, NodeIdGenerator.getGenerator().getNextNodeId(scope)));
        ld.setPc(pigContext);
        ld.setIsTmpLoad(true);
        ld.setLFile(fileSpec);
        return ld;
    }

    private POStore getTmpStore(String scope, FileSpec fileSpec){
        POStore st = new POStore(new OperatorKey(scope, NodeIdGenerator.getGenerator().getNextNodeId(scope)));
        st.setIsTmpStore(true);
        st.setSFile(fileSpec);
        return new POStoreTez(st);
    }

    @Override
    public String toString() {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        TezPlanContainerPrinter printer = new TezPlanContainerPrinter(ps, this);
        printer.setVerbose(true);
        try {
            printer.visit();
        } catch (VisitorException e) {
            throw new RuntimeException(e);
        }
        return baos.toString();
    }
}

