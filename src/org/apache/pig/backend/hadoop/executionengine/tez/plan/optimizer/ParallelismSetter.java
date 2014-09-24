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

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.PigConfiguration;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezEdgeDescriptor;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezOpPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezOperPlan;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezOperator;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.operator.NativeTezOper;
import org.apache.pig.backend.hadoop.executionengine.tez.util.TezCompilerUtil;
import org.apache.pig.backend.hadoop.executionengine.util.ParallelConstantVisitor;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;

public class ParallelismSetter extends TezOpPlanVisitor {
    private Configuration conf;
    private PigContext pc;
    private TezParallelismEstimator estimator;
    private boolean autoParallelismEnabled;

    public ParallelismSetter(TezOperPlan plan, PigContext pigContext) {
        super(plan, new DependencyOrderWalker<TezOperator, TezOperPlan>(plan));
        this.pc = pigContext;
        this.conf = ConfigurationUtil.toConfiguration(pc.getProperties());
        this.autoParallelismEnabled = conf.getBoolean(PigConfiguration.TEZ_AUTO_PARALLELISM, true);
        try {
            this.estimator = conf.get(PigConfiguration.REDUCER_ESTIMATOR_KEY) == null ? new TezOperDependencyParallelismEstimator()
            : PigContext.instantiateObjectFromParams(conf,
                    PigConfiguration.REDUCER_ESTIMATOR_KEY, PigConfiguration.REDUCER_ESTIMATOR_ARG_KEY,
                    TezParallelismEstimator.class);
            this.estimator.setPigContext(pc);

        } catch (ExecException e) {
            throw new RuntimeException("Error instantiating TezParallelismEstimator", e);
        }
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
            LinkedList<POStore> stores = PlanHelper.getPhysicalOperators(tezOp.plan, POStore.class);
            if (stores.size() <= 0) {
                intermediateReducer = true;
            }
            if (tezOp.getLoaderInfo().getLoads() != null && tezOp.getLoaderInfo().getLoads().size() > 0) {
                // TODO: Can be set to -1 if TEZ-601 gets fixed and getting input
                // splits can be moved to if(loads) block below
                parallelism = tezOp.getLoaderInfo().getInputSplitInfo().getNumTasks();
                tezOp.setRequestedParallelism(parallelism);
            } else {
                int prevParallelism = -1;
                boolean isOneToOneParallelism = false;
                intermediateReducer = TezCompilerUtil.isIntermediateReducer(tezOp);

                for (Map.Entry<OperatorKey,TezEdgeDescriptor> entry : tezOp.inEdges.entrySet()) {
                    if (entry.getValue().dataMovementType == DataMovementType.ONE_TO_ONE) {
                        TezOperator pred = mPlan.getOperator(entry.getKey());
                        parallelism = pred.getEffectiveParallelism();
                        if (prevParallelism == -1) {
                            prevParallelism = parallelism;
                        } else if (prevParallelism != parallelism) {
                            throw new VisitorException("one to one sources parallelism for vertex "
                                    + tezOp.getOperatorKey().toString() + " are not equal");
                        }
                        tezOp.setRequestedParallelism(pred.getRequestedParallelism());
                        tezOp.setEstimatedParallelism(pred.getEstimatedParallelism());
                        isOneToOneParallelism = true;
                        parallelism = -1;
                    }
                }
                if (!isOneToOneParallelism) {
                    if (tezOp.getRequestedParallelism() != -1) {
                        parallelism = tezOp.getRequestedParallelism();
                    } else if (pc.defaultParallel != -1) {
                        parallelism = pc.defaultParallel;
                    }
                    if (autoParallelismEnabled &&
                            ((parallelism == -1 || intermediateReducer) && !tezOp.isDontEstimateParallelism())) {
                        if (tezOp.getEstimatedParallelism() == -1) {
                            // Override user specified parallelism with the estimated value
                            // if it is intermediate reducer
                            parallelism = estimator.estimateParallelism(mPlan, tezOp, conf);
                            tezOp.setEstimatedParallelism(parallelism);
                        } else {
                            parallelism = tezOp.getEstimatedParallelism();
                        }
                        if (tezOp.isGlobalSort() || tezOp.isSkewedJoin()) {
                            // Vertex manager will set parallelism
                            parallelism = -1;
                        }
                    }
                }
            }

            // Once we decide the parallelism of the sampler, propagate to
            // downstream operators if necessary
            if (tezOp.isSampler() && autoParallelismEnabled) {
                // There could be multiple sampler and share the same sample aggregation job
                // and partitioner job
                TezOperator sampleAggregationOper = null;
                TezOperator sampleBasedPartionerOper = null;
                TezOperator sortOper = null;
                for (TezOperator succ : mPlan.getSuccessors(tezOp)) {
                    if (succ.isVertexGroup()) {
                        succ = mPlan.getSuccessors(succ).get(0);
                    }
                    if (succ.isSampleAggregation()) {
                        sampleAggregationOper = succ;
                    } else if (succ.isSampleBasedPartitioner()) {
                        sampleBasedPartionerOper = succ;
                    }
                }
                sortOper = mPlan.getSuccessors(sampleBasedPartionerOper).get(0);

                if ((sortOper.getRequestedParallelism() == -1 && pc.defaultParallel == -1) || TezCompilerUtil.isIntermediateReducer(sortOper)) {
                    // set estimate parallelism for order by/skewed join to sampler parallelism
                    // that include:
                    // 1. sort operator
                    // 2. constant for sample aggregation oper
                    sortOper.setEstimatedParallelism(parallelism);
                    ParallelConstantVisitor visitor =
                            new ParallelConstantVisitor(sampleAggregationOper.plan, parallelism);
                    visitor.visit();
                    sampleAggregationOper.setNeedEstimatedQuantile(true);
                }
            }

            tezOp.setVertexParallelism(parallelism);

            if (tezOp.getCrossKey()!=null) {
                pc.getProperties().put(PigConfiguration.PIG_CROSS_PARALLELISM_HINT + "." + tezOp.getCrossKey(),
                        Integer.toString(tezOp.getVertexParallelism()));
            }
        } catch (Exception e) {
            throw new VisitorException(e);
        }
    }

}
