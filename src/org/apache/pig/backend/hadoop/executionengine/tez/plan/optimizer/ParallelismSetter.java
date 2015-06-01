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
package org.apache.pig.backend.hadoop.executionengine.tez.plan.optimizer;

import java.util.LinkedList;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.PigConfiguration;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezEdgeDescriptor;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezOpPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezOperPlan;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezOperator;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.operator.NativeTezOper;
import org.apache.pig.backend.hadoop.executionengine.tez.util.TezCompilerUtil;
import org.apache.pig.backend.hadoop.executionengine.util.ParallelConstantVisitor;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.PigImplConstants;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;

public class ParallelismSetter extends TezOpPlanVisitor {

    private static final Log LOG = LogFactory.getLog(ParallelismSetter.class);
    private Configuration conf;
    private PigContext pc;
    private TezParallelismEstimator estimator;
    private boolean autoParallelismEnabled;
    private int estimatedTotalParallelism = 0;

    public ParallelismSetter(TezOperPlan plan, PigContext pigContext) {
        super(plan, new DependencyOrderWalker<TezOperator, TezOperPlan>(plan));
        this.pc = pigContext;
        this.conf = ConfigurationUtil.toConfiguration(pc.getProperties());
        this.autoParallelismEnabled = conf.getBoolean(PigConfiguration.PIG_TEZ_AUTO_PARALLELISM, true);
        try {
            this.estimator = conf.get(PigConfiguration.PIG_EXEC_REDUCER_ESTIMATOR) == null ? new TezOperDependencyParallelismEstimator()
            : PigContext.instantiateObjectFromParams(conf,
                    PigConfiguration.PIG_EXEC_REDUCER_ESTIMATOR, PigConfiguration.PIG_EXEC_REDUCER_ESTIMATOR_CONSTRUCTOR_ARG_KEY,
                    TezParallelismEstimator.class);
            this.estimator.setPigContext(pc);

        } catch (ExecException e) {
            throw new RuntimeException("Error instantiating TezParallelismEstimator", e);
        }
    }

    public int getEstimatedTotalParallelism() {
        return estimatedTotalParallelism;
    }

    @Override
    public void visitTezOp(TezOperator tezOp) throws VisitorException {
        if (tezOp instanceof NativeTezOper) {
            return;
        }
        try {
            // Can only set parallelism here if the parallelism isn't derived from
            // splits
            int parallelism = -1;
            boolean intermediateReducer = false;
            LinkedList<POStore> stores = tezOp.getStores();
            if (stores.size() <= 0) {
                intermediateReducer = true;
            }
            if (tezOp.getLoaderInfo().getLoads() != null && tezOp.getLoaderInfo().getLoads().size() > 0) {
                // requestedParallelism of Loader vertex is handled in LoaderProcessor
                // propogate to vertexParallelism
                tezOp.setVertexParallelism(tezOp.getRequestedParallelism());
                return;
            } else {
                int prevParallelism = -1;
                boolean isOneToOneParallelism = false;
                intermediateReducer = TezCompilerUtil.isIntermediateReducer(tezOp);

                for (Map.Entry<OperatorKey,TezEdgeDescriptor> entry : tezOp.inEdges.entrySet()) {
                    if (entry.getValue().dataMovementType == DataMovementType.ONE_TO_ONE) {
                        TezOperator pred = mPlan.getOperator(entry.getKey());
                        parallelism = pred.getEffectiveParallelism(pc.defaultParallel);
                        if (prevParallelism == -1) {
                            prevParallelism = parallelism;
                        } else if (prevParallelism != parallelism) {
                            throw new VisitorException("one to one sources parallelism for vertex "
                                    + tezOp.getOperatorKey().toString() + " are not equal");
                        }
                        tezOp.setRequestedParallelism(pred.getRequestedParallelism());
                        // If tezOp.estimatedParallelism already set, don't override
                        // The only case is in PigGraceShuffleVertexManager, which
                        // set the estimated parallelism according to the output data size of the node
                        if (tezOp.getEstimatedParallelism()==-1) {
                            tezOp.setEstimatedParallelism(pred.getEstimatedParallelism());
                        }
                        isOneToOneParallelism = true;
                        incrementTotalParallelism(tezOp, parallelism);
                        parallelism = -1;
                    }
                }
                if (!isOneToOneParallelism) {
                    if (tezOp.getRequestedParallelism() != -1) {
                        parallelism = tezOp.getRequestedParallelism();
                    } else if (pc.defaultParallel != -1) {
                        parallelism = pc.defaultParallel;
                    }
                    boolean overrideRequestedParallelism = false;
                    if (parallelism != -1
                            && autoParallelismEnabled
                            && intermediateReducer
                            && !tezOp.isDontEstimateParallelism()
                            && tezOp.isOverrideIntermediateParallelism()) {
                        overrideRequestedParallelism = true;
                    }
                    if (parallelism == -1 || overrideRequestedParallelism) {
                        if (tezOp.getEstimatedParallelism() == -1) {
                            // Override user specified parallelism with the estimated value
                            // if it is intermediate reducer
                            parallelism = estimator.estimateParallelism(mPlan, tezOp, conf);
                            if (overrideRequestedParallelism) {
                                if (tezOp.getRequestedParallelism() != parallelism) {
                                    LOG.info("Increased requested parallelism of " + tezOp.getOperatorKey() + " to " + parallelism);
                                }
                                tezOp.setRequestedParallelism(parallelism);
                            } else {
                                tezOp.setEstimatedParallelism(parallelism);
                            }
                        } else {
                            parallelism = tezOp.getEstimatedParallelism();
                        }
                        if (tezOp.isGlobalSort() || tezOp.isSkewedJoin()) {
                            boolean additionalEdge = false;
                            if (tezOp.isGlobalSort() && getPlan().getPredecessors(tezOp).size() != 1 ||
                                    tezOp.isSkewedJoin() && getPlan().getPredecessors(tezOp).size() != 2) {
                                additionalEdge = true;
                            }
                            if (!overrideRequestedParallelism && !additionalEdge) {
                                incrementTotalParallelism(tezOp, parallelism);
                                // PartitionerDefinedVertexManager will determine parallelism.
                                // So call setVertexParallelism with -1
                                // setEstimatedParallelism still needs to have some positive value
                                // so that TezDAGBuilder sets the PartitionerDefinedVertexManager
                                parallelism = -1;
                            } else {
                                // We are overriding the parallelism. We need to update the
                                // Constant value in sampleAggregator to same parallelism
                                // Currently will happen when you have orderby or
                                // skewed join followed by group by with combiner
                                for (TezOperator pred : mPlan.getPredecessors(tezOp)) {
                                    if (pred.isSampleBasedPartitioner()) {
                                        for (TezOperator partitionerPred : mPlan.getPredecessors(pred)) {
                                            if (partitionerPred.isSampleAggregation() && partitionerPred.plan!=null) {
                                                LOG.debug("Updating parallelism constant value to " + parallelism + " in " + partitionerPred.plan);
                                                ParallelConstantVisitor visitor =
                                                        new ParallelConstantVisitor(partitionerPred.plan, parallelism);
                                                visitor.visit();
                                                partitionerPred.setNeedEstimatedQuantile(false);
                                                break;
                                            }
                                        }
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
            }

            incrementTotalParallelism(tezOp, parallelism);
            tezOp.setVertexParallelism(parallelism);

            // TODO: Fix case where vertex parallelism is -1 for auto parallelism with PartitionerDefinedVertexManager.
            // i.e order by or skewed join followed by cross
            if (tezOp.getCrossKeys() != null) {
                for (String key : tezOp.getCrossKeys()) {
                    pc.getProperties().put(PigImplConstants.PIG_CROSS_PARALLELISM + "." + key,
                            Integer.toString(tezOp.getVertexParallelism()));
                }
            }
        } catch (Exception e) {
            throw new VisitorException(e);
        }
    }

    private void incrementTotalParallelism(TezOperator tezOp, int tezOpParallelism) {
        if (tezOp.isVertexGroup()) {
            return;
        }
        if (tezOpParallelism != -1) {
            estimatedTotalParallelism += tezOpParallelism;
        }
    }

}
