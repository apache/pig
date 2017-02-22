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

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.InputSizeReducerEstimator;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezOperPlan;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezOperator;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.optimizer.ParallelismSetter;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.optimizer.TezEstimatedParallelismClearer;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.PigImplConstants;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.VertexManagerPluginContext;
import org.apache.tez.dag.api.event.VertexState;
import org.apache.tez.dag.api.event.VertexStateUpdate;
import org.apache.tez.dag.library.vertexmanager.ShuffleVertexManager;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

public class PigGraceShuffleVertexManager extends ShuffleVertexManager {

    private TezOperPlan tezPlan;
    private List<String> grandParents = new ArrayList<String>();
    private List<String> finishedGrandParents = new ArrayList<String>();
    private long bytesPerTask;
    private Configuration conf;
    private PigContext pc;
    private int thisParallelism = -1;
    private boolean parallelismSet = false;

    private static final Log LOG = LogFactory.getLog(PigGraceShuffleVertexManager.class);

    public PigGraceShuffleVertexManager(VertexManagerPluginContext context) {
        super(context);
    }

    @Override
    public synchronized void initialize() {
        try {
            conf = TezUtils.createConfFromUserPayload(getContext().getUserPayload());
            bytesPerTask = conf.getLong(InputSizeReducerEstimator.BYTES_PER_REDUCER_PARAM,
                    InputSizeReducerEstimator.DEFAULT_BYTES_PER_REDUCER);
            pc = (PigContext)ObjectSerializer.deserialize(conf.get(PigImplConstants.PIG_CONTEXT));
            tezPlan = (TezOperPlan)ObjectSerializer.deserialize(conf.get("pig.tez.plan"));
            TezEstimatedParallelismClearer clearer = new TezEstimatedParallelismClearer(tezPlan);
            try {
                clearer.visit();
            } catch (VisitorException e) {
                throw new TezUncheckedException(e);
            }
            TezOperator op = tezPlan.getOperator(OperatorKey.fromString(getContext().getVertexName()));

            // Collect grandparents of the vertex
            Function<TezOperator, String> tezOpToString = new Function<TezOperator, String>() {
                @Override
                public String apply(TezOperator op) { return op.getOperatorKey().toString(); }
            };
            grandParents = Lists.transform(TezOperPlan.getGrandParentsForGraceParallelism(tezPlan, op), tezOpToString);
        } catch (IOException e) {
            throw new TezUncheckedException(e);
        }

        // Register notification for grandparents
        for (String grandParent : grandParents) {
            getContext().registerForVertexStateUpdates(grandParent, EnumSet.of(VertexState.SUCCEEDED));
        }
        super.initialize();
    }

    @Override
    public synchronized void onVertexStateUpdated(VertexStateUpdate stateUpdate) {
        super.onVertexStateUpdated(stateUpdate);
        if (parallelismSet) {
            return;
        }
        String vertexName = stateUpdate.getVertexName();
        if (grandParents.contains(vertexName)) {
            if (!finishedGrandParents.contains(vertexName)) {
                finishedGrandParents.add(vertexName);
            }
        }

        TezOperator op = tezPlan.getOperator(OperatorKey.fromString(getContext().getVertexName()));

        List<TezOperator> preds = tezPlan.getPredecessors(op);
        boolean anyPredAboutToStart = false;
        for (TezOperator pred : preds) {
            List<TezOperator> predPreds = tezPlan.getPredecessors(pred);
            boolean predAboutToStart = true;
            for (TezOperator predPred : predPreds) {
                if (!finishedGrandParents.contains(predPred.getOperatorKey().toString())) {
                    predAboutToStart = false;
                    break;
                }
            }
            if (predAboutToStart) {
                LOG.info("All predecessors for " + pred.getOperatorKey().toString() + " are finished, time to " +
                        "set parallelism for " + getContext().getVertexName());
                anyPredAboutToStart = true;
                break;
            }
        }

        // Now one of the predecessor is about to start, we need to make a decision now
        if (anyPredAboutToStart) {
            // All grandparents finished, start parents with right parallelism

            for (TezOperator pred : preds) {
                if (pred.getRequestedParallelism()==-1) {
                    List<TezOperator> predPreds = tezPlan.getPredecessors(pred);
                    if (predPreds!=null) {
                        for (TezOperator predPred : predPreds) {
                            String predPredVertexName = predPred.getOperatorKey().toString();
                            if (finishedGrandParents.contains(predPredVertexName)) {
                                // We shall get precise output size since all those nodes are finished
                                long outputSize = getContext().getVertexStatistics(predPredVertexName).getOutputStatistics(pred.getOperatorKey().toString()).getDataSize();
                                int desiredNumReducers = (int)Math.ceil((double)outputSize/bytesPerTask);
                                predPred.setEstimatedParallelism(desiredNumReducers);
                                LOG.info(getContext().getVertexName() +  ": Grandparent " + predPred.getOperatorKey().toString() +
                                        " finished with actual output " + outputSize + " (desired parallelism " + desiredNumReducers + ")");
                            }
                        }
                    }
                }
            }
            try {
                ParallelismSetter parallelismSetter = new ParallelismSetter(tezPlan, pc);
                parallelismSetter.visit();
                thisParallelism = op.getEstimatedParallelism();
            } catch (IOException e) {
                throw new TezUncheckedException(e);
            }
            Map<String, EdgeProperty> edgeManagers = new HashMap<String, EdgeProperty>();
            for(Map.Entry<String,EdgeProperty> entry : getContext().getInputVertexEdgeProperties().entrySet()) {
                EdgeProperty edge = entry.getValue();
                edge = EdgeProperty.create(DataMovementType.SCATTER_GATHER, edge.getDataSourceType(), edge.getSchedulingType(),
                        edge.getEdgeSource(), edge.getEdgeDestination());
                edgeManagers.put(entry.getKey(), edge);
            }
            getContext().reconfigureVertex(thisParallelism, null, edgeManagers);
            parallelismSet = true;
            LOG.info("Initialize parallelism for " + getContext().getVertexName() + " to " + thisParallelism);
        }
    }
}
