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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.EdgeProperty.DataSourceType;
import org.apache.tez.dag.api.EdgeProperty.SchedulingType;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.Vertex;

/**
 * A visitor to construct DAG out of Tez plan.
 */
public class TezDagBuilder extends TezOpPlanVisitor {
    private DAG dag;

    public TezDagBuilder(DAG dag, TezOperPlan plan) {
        super(plan, new DependencyOrderWalker<TezOperator, TezOperPlan>(plan));
        this.dag = dag;
    }

    @Override
    public void visitTezOp(TezOperator tezOp) throws VisitorException {
        // Construct vertex for the current Tez operator
        Vertex to = null;
        try {
            to = newVertex(tezOp);
            dag.addVertex(to);
        } catch (IOException e) {
            throw new VisitorException("Cannot create vertex for " + tezOp.name(), e);
        }

        // Connect the new vertex with dependent vertices
        TezOperPlan tezPlan =  getPlan();
        List<TezOperator> predecessors = tezPlan.getPredecessors(tezOp);
        if (predecessors != null) {
            for (TezOperator predecessor : predecessors) {
                // TODO: We should encapsulate edge properties in TezOperator.
                // For now, we always create a shuffle edge.
                EdgeProperty prop = new EdgeProperty(
                        DataMovementType.SCATTER_GATHER,
                        DataSourceType.PERSISTED,
                        SchedulingType.SEQUENTIAL,
                        new OutputDescriptor(tezOp.getProcessorName()),
                        new InputDescriptor(predecessor.getProcessorName()));
                // Since this is a dependency order walker, dependent vertices
                // must have already been created.
                Vertex from = dag.getVertex(predecessor.name());
                Edge edge = new Edge(from, to, prop);
                dag.addEdge(edge);
            }
        }
    }

    private Vertex newVertex(TezOperator tezOp) throws IOException {
        ProcessorDescriptor procDesc = new ProcessorDescriptor(tezOp.getProcessorName());
        // Pass physical plans to vertex as user payload.
        Configuration conf = new Configuration();
        conf.set(PigProcessor.PLAN, ObjectSerializer.serialize(tezOp.plan));
        conf.set(PigProcessor.COMBINE_PLAN, ObjectSerializer.serialize(tezOp.combinePlan));
        byte[] userPayload = TezUtils.createUserPayloadFromConf(conf);
        procDesc.setUserPayload(userPayload);
        return new Vertex(tezOp.name(), procDesc, tezOp.requestedParallelism,
                Resource.newInstance(tezOp.requestedMemory, tezOp.requestedCpu));
    }
}

