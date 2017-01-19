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
package org.apache.pig.backend.hadoop.executionengine.tez.runtime;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.VertexManagerPlugin;
import org.apache.tez.dag.api.VertexManagerPluginContext;
import org.apache.tez.dag.api.VertexManagerPluginContext.TaskWithLocationHint;
import org.apache.tez.dag.api.event.VertexState;
import org.apache.tez.dag.api.event.VertexStateUpdate;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.events.VertexManagerEvent;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * VertexManagerPlugin used by sorting job of order by and skewed join.
 * What is does is to set parallelism of the sorting vertex
 * according to numParallelism specified by the predecessor vertex.
 * The complex part is the PigOrderByEdgeManager, which specify how
 * partition in the previous setting routes to the new vertex setting
 */
public class PartitionerDefinedVertexManager extends VertexManagerPlugin {
    private static final Log LOG = LogFactory.getLog(PartitionerDefinedVertexManager.class);

    private volatile boolean parallelismSet;
    private int dynamicParallelism = -1;
    private int numConfiguredSources;
    private int numSources = -1;
    private volatile boolean configured;
    private volatile boolean started;
    private volatile boolean scheduled;

    public PartitionerDefinedVertexManager(VertexManagerPluginContext context) {
        super(context);
    }

    @Override
    public void initialize() {
        // this will prevent vertex from starting until we notify we are done
        getContext().vertexReconfigurationPlanned();
        parallelismSet = false;
        numConfiguredSources = 0;
        configured = false;
        started = false;
        numSources = getContext().getInputVertexEdgeProperties().size();
        // wait for sources and self to start
        Map<String, EdgeProperty> edges = getContext().getInputVertexEdgeProperties();
        for (String entry : edges.keySet()) {
            getContext().registerForVertexStateUpdates(entry, EnumSet.of(VertexState.CONFIGURED));
        }
    }

    @Override
    public synchronized void onVertexStateUpdated(VertexStateUpdate stateUpdate)
            throws Exception {
        numConfiguredSources++;
        LOG.info("For vertex: " + getContext().getVertexName() + " Received configured signal from: "
            + stateUpdate.getVertexName() + " numConfiguredSources: " + numConfiguredSources
            + " needed: " + numSources);
        Preconditions.checkState(numConfiguredSources <= numSources, "Vertex: " + getContext().getVertexName());
        if (numConfiguredSources == numSources) {
            configure();
        }
    }

    @Override
    public void onRootVertexInitialized(String inputName, InputDescriptor inputDescriptor,
            List<Event> events) {
        // Nothing to do
    }

    @Override
    public void onSourceTaskCompleted(String srcVertexName, Integer srcTaskId) {
        // Nothing to do
    }

    @Override
    public void onVertexManagerEventReceived(VertexManagerEvent vmEvent) throws Exception {
        // There could be multiple partition vertex sending VertexManagerEvent
        // Only need to setVertexParallelism once
        if (parallelismSet) {
            return;
        }
        // Need to distinguish from VertexManagerEventPayloadProto emitted by OrderedPartitionedKVOutput
        if (vmEvent.getUserPayload().limit()==4) {
            dynamicParallelism = vmEvent.getUserPayload().getInt();
        } else {
            return;
        }
        int currentParallelism = getContext().getVertexNumTasks(getContext().getVertexName());
        if (dynamicParallelism != -1) {
            if (dynamicParallelism!=currentParallelism) {
                LOG.info("Pig Partitioner Defined Vertex Manager: reset parallelism to " + dynamicParallelism
                        + " from " + currentParallelism);
                Map<String, EdgeProperty> edgeManagers = new HashMap<String, EdgeProperty>();
                for(Map.Entry<String,EdgeProperty> entry : getContext().getInputVertexEdgeProperties().entrySet()) {
                    EdgeProperty edge = entry.getValue();
                    edge = EdgeProperty.create(DataMovementType.SCATTER_GATHER, edge.getDataSourceType(), edge.getSchedulingType(),
                            edge.getEdgeSource(), edge.getEdgeDestination());
                    edgeManagers.put(entry.getKey(), edge);
                }
                getContext().reconfigureVertex(dynamicParallelism, null, edgeManagers);
                parallelismSet = true;
                configure();
            }
        }
    }

    private void configure() {
        if(parallelismSet && (numSources == numConfiguredSources)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Done reconfiguring vertex " + getContext().getVertexName());
            }
            getContext().doneReconfiguringVertex();
            configured = true;
            trySchedulingTasks();
        }
    }

    private synchronized void trySchedulingTasks() {
        if (configured && started && !scheduled) {
            LOG.info("Scheduling " + dynamicParallelism + " tasks for vertex " + getContext().getVertexName());
            List<TaskWithLocationHint> tasksToStart = Lists.newArrayListWithCapacity(dynamicParallelism);
            for (int i = 0; i < dynamicParallelism; ++i) {
                tasksToStart.add(new TaskWithLocationHint(new Integer(i), null));
            }
            getContext().scheduleVertexTasks(tasksToStart);
            scheduled = true;
        }
    }

    @Override
    public void onVertexStarted(Map<String, List<Integer>> completions) {
        // This vertex manager will be getting the following calls
        //   1) onVertexManagerEventReceived - Parallelism vertex manager event sent by sample aggregator vertex
        //   2) onVertexStateUpdated - Vertex CONFIGURED status updates from
        //       - Order by Partitioner vertex (1-1) in case of Order by
        //       - Skewed Join Left Partitioner (1-1) and Right Input Vertices in case of SkewedJoin
        //   3) onVertexStarted
        // Calls 2) and 3) can happen in any order. So we should schedule tasks
        // only after start is called and configuration is also complete
        started = true;
        if (LOG.isDebugEnabled()) {
            LOG.debug("Vertex start received for " + getContext().getVertexName());
        }
        trySchedulingTasks();
    }

}
