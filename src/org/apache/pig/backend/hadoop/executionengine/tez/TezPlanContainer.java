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
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.OperatorPlan;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.JarManager;

public class TezPlanContainer extends OperatorPlan<TezPlanContainerNode> {
    private static final long serialVersionUID = 1L;
    private PigContext pigContext;

    public TezPlanContainer(PigContext pigContext) {
        this.pigContext = pigContext;
    }

    // Add the Pig jar and the UDF jars as AM resources (all DAG's in the planContainer
    // will use them for simplicity). This differs from MR Pig, where they are added to
    // the job jar.
    public Map<String, LocalResource> getLocalResources() throws Exception {
        Set<URI> jarLists = new HashSet<URI>();

        jarLists.add(TezResourceManager.getInstance().getBootStrapJar().toURI());

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

        // Streaming UDF's are not working under Hadoop 2 (PIG-3478), so don't bother adding
        // resources for them yet.
        // if ("StreamingUDF".equals(clazz.getSimpleName())) {
        //     for (String fileName : StreamingUDF.getResourcesForJar()) {
        //         jarLists.add(new File(fileName).toURI().toURL());
        //     }
        // }

        return TezResourceManager.getInstance().addTezResources(jarLists);
    }

    public TezOperPlan getNextPlan(List<TezOperPlan> processedPlans) {
        synchronized(this) {
            while (getRoots()!=null && !getRoots().isEmpty()) {
                TezPlanContainerNode currentPlan = null;
                for (TezPlanContainerNode plan : getRoots()) {
                    if (!processedPlans.contains(plan.getNode())) {
                        currentPlan = plan;
                        break;
                    }
                }
                if (currentPlan!=null) {
                    return currentPlan.getNode();
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

    public void updatePlan(TezOperPlan plan, boolean succ) {
        String scope = getRoots().get(0).getOperatorKey().getScope();
        TezPlanContainerNode tezPlanContainerNode = new TezPlanContainerNode(new OperatorKey(scope,
                NodeIdGenerator.getGenerator().getNextNodeId(scope)), plan);
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
        TezOperPlan tezOperPlan = planNode.getNode();
        TezOperator operToSegment = null;
        List<TezOperator> succs = new ArrayList<TezOperator>();
        for (TezOperator tezOper : tezOperPlan) {
            if (tezOper.needSegmentBelow() && tezOperPlan.getSuccessors(tezOper)!=null) {
                operToSegment = tezOper;
                succs.addAll(tezOperPlan.getSuccessors(tezOper));
                break;
            }
        }
        if (operToSegment != null) {
            for (TezOperator succ : succs) {
                tezOperPlan.disconnect(operToSegment, succ);
                TezOperPlan newOperPlan = new TezOperPlan();
                List<TezPlanContainerNode> containerSuccs = new ArrayList<TezPlanContainerNode>();
                if (getSuccessors(planNode)!=null) {
                    containerSuccs.addAll(getSuccessors(planNode));
                }
                tezOperPlan.moveTree(succ, newOperPlan);
                String scope = operToSegment.getOperatorKey().getScope();
                TezPlanContainerNode newPlanNode = new TezPlanContainerNode(new OperatorKey(scope, NodeIdGenerator.getGenerator().getNextNodeId(scope)), newOperPlan);
                add(newPlanNode);
                for (TezPlanContainerNode containerNodeSucc : containerSuccs) {
                    disconnect(planNode, containerNodeSucc);
                    connect(newPlanNode, containerNodeSucc);
                }
                connect(planNode, newPlanNode);
                split(newPlanNode);
            }
            split(planNode);
        }
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

