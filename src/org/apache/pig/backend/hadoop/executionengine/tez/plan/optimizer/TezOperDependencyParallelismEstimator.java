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

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigReducerEstimator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.ConstantExpression;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.JoinPackager;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFRJoin;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFilter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLimit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POMergeJoin;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSplit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezEdgeDescriptor;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezOperPlan;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezOperator;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.operator.POLocalRearrangeTez;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.operator.POValueOutputTez;
import org.apache.pig.backend.hadoop.executionengine.tez.util.TezCompilerUtil;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;

/**
 * Estimate the parallelism of the vertex using:
 * 1. parallelism of the predecessors
 * 2. bloating factor of the physical plan of the predecessor
 *
 * Since currently it is only possible to reduce the parallelism
 * estimation is exaggerated and will rely on Tez runtime to
 * descrease the parallelism
 */
public class TezOperDependencyParallelismEstimator implements TezParallelismEstimator {

    static private int maxTaskCount;
    static final double DEFAULT_FLATTEN_FACTOR = 10;
    static final double DEFAULT_FILTER_FACTOR = 0.7;
    static final double DEFAULT_LIMIT_FACTOR = 0.1;

    private PigContext pc;

    @Override
    public void setPigContext(PigContext pc) {
        this.pc = pc;
    }

    @Override
    public int estimateParallelism(TezOperPlan plan, TezOperator tezOper, Configuration conf) throws IOException {

        if (tezOper.isVertexGroup()) {
            return -1;
        }

        boolean intermediateReducer = TezCompilerUtil.isIntermediateReducer(tezOper);

        // TODO: If map opts and reduce opts are same estimate higher parallelism
        // for tasks based on the count of number of map tasks else be conservative as now
        maxTaskCount = conf.getInt(PigReducerEstimator.MAX_REDUCER_COUNT_PARAM,
                PigReducerEstimator.DEFAULT_MAX_REDUCER_COUNT_PARAM);

        // If parallelism is set explicitly, respect it
        if (!intermediateReducer && tezOper.getRequestedParallelism()!=-1) {
            return tezOper.getRequestedParallelism();
        }

        // If we have already estimated parallelism, use that one
        if (tezOper.getEstimatedParallelism()!=-1) {
            return tezOper.getEstimatedParallelism();
        }

        List<TezOperator> preds = plan.getPredecessors(tezOper);
        if (preds==null) {
            throw new IOException("Cannot estimate parallelism for source vertex");
        }

        double estimatedParallelism = 0;

        for (Entry<OperatorKey, TezEdgeDescriptor> entry : tezOper.inEdges.entrySet()) {
            TezOperator pred = getPredecessorWithKey(plan, tezOper, entry.getKey().toString());

            // Don't include broadcast edge, broadcast edge is used for
            // replicated join (covered in TezParallelismFactorVisitor.visitFRJoin)
            // and sample/scalar (does not impact parallelism)
            if (entry.getValue().dataMovementType==DataMovementType.SCATTER_GATHER ||
                    entry.getValue().dataMovementType==DataMovementType.ONE_TO_ONE) {
                double predParallelism = pred.getEffectiveParallelism(pc.defaultParallel);
                if (predParallelism==-1) {
                    throw new IOException("Cannot estimate parallelism for " + tezOper.getOperatorKey().toString()
                            + ", effective parallelism for predecessor " + tezOper.getOperatorKey().toString()
                            + " is -1");
                }

                //For cases like Union we can just limit to sum of pred vertices parallelism
                boolean applyFactor = !tezOper.isUnion();
                if (!pred.isVertexGroup() && applyFactor) {
                    predParallelism = predParallelism * pred.getParallelismFactor();
                }
                estimatedParallelism += predParallelism;
            }
        }

        int roundedEstimatedParallelism = (int)Math.ceil(estimatedParallelism);

        if (intermediateReducer && tezOper.isOverrideIntermediateParallelism()) {
            // Estimated reducers should not be more than the configured limit
            roundedEstimatedParallelism = Math.min(roundedEstimatedParallelism, maxTaskCount);
            int userSpecifiedParallelism = pc.defaultParallel;
            if (tezOper.getRequestedParallelism() != -1) {
                userSpecifiedParallelism = tezOper.getRequestedParallelism();
            }
            int intermediateParallelism = Math.max(userSpecifiedParallelism, roundedEstimatedParallelism);
            if (userSpecifiedParallelism != -1 &&
                    (intermediateParallelism > 200 && intermediateParallelism > (2 * userSpecifiedParallelism))) {
                // Estimated reducers shall not be more than 2x of requested parallelism
                // if greater than 200 and we are overriding user specified values
                intermediateParallelism = 2 * userSpecifiedParallelism;
            }
            roundedEstimatedParallelism = intermediateParallelism;
        } else {
            roundedEstimatedParallelism = Math.min(roundedEstimatedParallelism, maxTaskCount);
        }

        return roundedEstimatedParallelism;
    }

    private static TezOperator getPredecessorWithKey(TezOperPlan plan, TezOperator tezOper, String inputKey) {
        List<TezOperator> preds = plan.getPredecessors(tezOper);
        for (TezOperator pred : preds) {
            if (pred.isVertexGroup()) {
                for (OperatorKey unionPred : pred.getVertexGroupMembers()) {
                    if (unionPred.toString().equals(inputKey)) {
                        return plan.getOperator(unionPred);
                    }
                }

            }
            else if (pred.getOperatorKey().toString().equals(inputKey)) {
                return pred;
            }
        }
        return null;
    }

    public static class TezParallelismFactorVisitor extends PhyPlanVisitor {
        private double factor = 1;
        private String outputKey;
        public TezParallelismFactorVisitor(PhysicalPlan plan, String outputKey) {
            super(plan, new DepthFirstWalker<PhysicalOperator, PhysicalPlan>(plan));
            this.outputKey = outputKey;
        }

        @Override
        public void visitFilter(POFilter fl) throws VisitorException {
            if (fl.getPlan().size()==1 && fl.getPlan().getRoots().get(0) instanceof ConstantExpression) {
                ConstantExpression cons = (ConstantExpression)fl.getPlan().getRoots().get(0);
                if (cons.getValue().equals(Boolean.TRUE)) {
                    // skip all true condition
                    return;
                }
            }
            factor *= DEFAULT_FILTER_FACTOR;
        }

        @Override
        public void visitPOForEach(POForEach nfe) throws VisitorException {
            List<Boolean> flattens = nfe.getToBeFlattened();
            boolean containFlatten = false;
            for (boolean flatten : flattens) {
                if (flatten) {
                    containFlatten = true;
                    break;
                }
            }
            if (containFlatten) {
                factor *= DEFAULT_FLATTEN_FACTOR;
            }
        }

        @Override
        public void visitLimit(POLimit lim) throws VisitorException {
            factor = DEFAULT_LIMIT_FACTOR;
        }

        @Override
        public void visitFRJoin(POFRJoin join) throws VisitorException {
            factor *= DEFAULT_FLATTEN_FACTOR;
        }

        @Override
        public void visitMergeJoin(POMergeJoin join) throws VisitorException {
            factor *= DEFAULT_FLATTEN_FACTOR;
        }

        @Override
        public void visitPackage(POPackage pkg) throws VisitorException{
            // JoinPackager is equivalent to a foreach flatten after shuffle
            if (pkg.getPkgr() instanceof JoinPackager) {
                factor *= DEFAULT_FLATTEN_FACTOR;
            }
        }

        @Override
        public void visitSplit(POSplit sp) throws VisitorException {
            // Find the split branch connecting to current operator
            // accumulating the bloating factor in this branch
            PhysicalPlan plan = getSplitBranch(sp, outputKey);
            pushWalker(mCurrentWalker.spawnChildWalker(plan));
            visit();
            popWalker();
        }

        private static PhysicalPlan getSplitBranch(POSplit split, String outputKey) throws VisitorException {
            List<PhysicalPlan> plans = split.getPlans();
            for (PhysicalPlan plan : plans) {
                LinkedList<POLocalRearrangeTez> lrs = PlanHelper.getPhysicalOperators(plan, POLocalRearrangeTez.class);
                if (!lrs.isEmpty()) {
                    return plan;
                }
                LinkedList<POValueOutputTez> vos = PlanHelper.getPhysicalOperators(plan, POValueOutputTez.class);
                if (!vos.isEmpty()) {
                    return plan;
                }
            }
            return null;
        }

        public double getFactor() {
            return factor;
        }

    }
}
