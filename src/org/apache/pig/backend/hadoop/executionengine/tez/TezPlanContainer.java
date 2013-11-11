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

import java.io.File;
import java.io.IOException;
import java.net.URL;
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
import org.apache.pig.impl.util.JarManager;

public class TezPlanContainer extends OperatorPlan<TezPlanContainerNode> {
    private static final long serialVersionUID = 1L;
    private PigContext pigContext;

    public TezPlanContainer(PigContext pigContext) {
        this.pigContext = pigContext;
    }

    // Use pig.jar and udf jars for the AM resources (all DAG in the planContainer will
    // use it for simplicity)
    public Map<String, LocalResource> getLocalResources() throws IOException {
        Set<URL> jarLists = new HashSet<URL>();

        jarLists.add(TezResourceManager.getBootStrapJar());

        jarLists.addAll(pigContext.extraJars);

        TezPlanContainerUDFCollector tezPlanContainerUDFCollector = new TezPlanContainerUDFCollector(this);
        tezPlanContainerUDFCollector.visit();
        Set<String> udfs = tezPlanContainerUDFCollector.getUdfs();

        for (String func: udfs) {
            Class clazz = pigContext.getClassForAlias(func);
            if (clazz != null) {
                String jarName = JarManager.findContainingJar(clazz);
                URL jarUrl = new File(jarName).toURI().toURL();
                jarLists.add(jarUrl);
            }
        }

        return TezResourceManager.getTezResources(jarLists);
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
        for (TezOperator tezOper : tezOperPlan) {
            if (tezOper.needSegmentBelow() && tezOperPlan.getSuccessors(tezOper)!=null) {
                for (TezOperator succ : tezOperPlan.getSuccessors(tezOper)) {
                    tezOperPlan.disconnect(tezOper, succ);
                    TezOperPlan newOperPlan = new TezOperPlan();
                    tezOperPlan.moveTree(succ, newOperPlan);
                    TezPlanContainerNode newPlanNode = new TezPlanContainerNode(tezOper.getOperatorKey(), newOperPlan);
                    add(newPlanNode);
                    connect(planNode, newPlanNode);
                    split(planNode);
                    split(newPlanNode);
                }
            }
        }
    }
}

