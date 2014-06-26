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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tez.dag.api.EdgeManagerDescriptor;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.VertexManagerPlugin;
import org.apache.tez.dag.api.VertexManagerPluginContext;
import org.apache.tez.dag.api.VertexManagerPluginContext.TaskWithLocationHint;
import org.apache.tez.dag.app.dag.impl.ScatterGatherEdgeManager;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.events.VertexManagerEvent;

import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;

/**
 * VertexManagerPlugin used by sorting job of order by and skewed join. 
 * What is does is to set parallelism of the sorting vertex
 * according to numParallelism specified by the predecessor vertex.
 * The complex part is the PigOrderByEdgeManager, which specify how
 * partition in the previous setting routes to the new vertex setting
 */
public class PartitionerDefinedVertexManager extends VertexManagerPlugin {

    private VertexManagerPluginContext context;
    private boolean isParallelismSet = false;
    
    private static final Log LOG = LogFactory.getLog(PartitionerDefinedVertexManager.class);

    @Override
    public void initialize(VertexManagerPluginContext context) {
        this.context = context;
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
    public void onVertexManagerEventReceived(VertexManagerEvent vmEvent) {
        // There could be multiple partition vertex sending VertexManagerEvent
        // Only need to setVertexParallelism once
        if (isParallelismSet) {
            return;
        }
        isParallelismSet = true;
        int dynamicParallelism = -1;
        // Need to distinguish from VertexManagerEventPayloadProto emitted by OnFileSortedOutput
        if (vmEvent.getUserPayload().length==4) {
            dynamicParallelism = Ints.fromByteArray(vmEvent.getUserPayload());
        } else {
            return;
        }
        int currentParallelism = context.getVertexNumTasks(context.getVertexName());
        if (dynamicParallelism != -1) {
            if (dynamicParallelism!=currentParallelism) {
                LOG.info("Pig Partitioner Defined Vertex Manager: reset parallelism to " + dynamicParallelism
                        + " from " + currentParallelism);
                Map<String, EdgeManagerDescriptor> edgeManagers =
                    new HashMap<String, EdgeManagerDescriptor>();
                for(String vertex : context.getInputVertexEdgeProperties().keySet()) {
                    EdgeManagerDescriptor edgeManagerDescriptor =
                            new EdgeManagerDescriptor(ScatterGatherEdgeManager.class.getName());
                    edgeManagers.put(vertex, edgeManagerDescriptor);
                }
                context.setVertexParallelism(dynamicParallelism, null, edgeManagers, null);
            }
            List<TaskWithLocationHint> tasksToStart = Lists.newArrayListWithCapacity(dynamicParallelism);
            for (int i=0; i<dynamicParallelism; ++i) {
                tasksToStart.add(new TaskWithLocationHint(new Integer(i), null));
            }
            context.scheduleVertexTasks(tasksToStart);
        }
    }

    @Override
    public void onVertexStarted(Map<String, List<Integer>> completions) {
        // Nothing to do
    }
}
