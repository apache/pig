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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tez.dag.api.EdgeManagerPluginDescriptor;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.VertexManagerPlugin;
import org.apache.tez.dag.api.VertexManagerPluginContext;
import org.apache.tez.dag.api.VertexManagerPluginContext.TaskWithLocationHint;
import org.apache.tez.dag.app.dag.impl.ScatterGatherEdgeManager;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.events.VertexManagerEvent;

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

    private boolean isParallelismSet = false;
    private int dynamicParallelism = -1;

    public PartitionerDefinedVertexManager(VertexManagerPluginContext context) {
        super(context);
    }

    @Override
    public void initialize() {
        // Nothing to do
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
        if (isParallelismSet) {
            return;
        }
        isParallelismSet = true;
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
            }
        }
    }

    @Override
    public void onVertexStarted(Map<String, List<Integer>> completions) {
        if (dynamicParallelism != -1) {
            List<TaskWithLocationHint> tasksToStart = Lists.newArrayListWithCapacity(dynamicParallelism);
            for (int i=0; i<dynamicParallelism; ++i) {
                tasksToStart.add(new TaskWithLocationHint(new Integer(i), null));
            }
            getContext().scheduleVertexTasks(tasksToStart);
        }
    }
}
