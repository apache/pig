/*
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
package org.apache.pig.backend.hadoop.executionengine.spark.optimizer;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POProject;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POUserFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.CombinerPackager;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.PODistinct;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POGlobalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLimit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPreCombinerLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.Packager;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.backend.hadoop.executionengine.spark.operator.POGlobalRearrangeSpark;
import org.apache.pig.backend.hadoop.executionengine.spark.operator.POReduceBySpark;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkOpPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkOperPlan;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkOperator;
import org.apache.pig.backend.hadoop.executionengine.util.CombinerOptimizerUtil;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.plan.optimizer.OptimizerException;
import org.apache.pig.impl.util.Pair;

import com.google.common.collect.Maps;

/**
 * This class goes through the physical plan are replaces GlobalRearrange with ReduceBy
 * where there are algebraic operations.
 */
public class CombinerOptimizer extends SparkOpPlanVisitor {

    private static Log LOG = LogFactory.getLog(CombinerOptimizer.class);

    public CombinerOptimizer(SparkOperPlan plan) {
        super(plan, new DepthFirstWalker<>(plan));
    }

    @Override
    public void visitSparkOp(SparkOperator sparkOp) throws VisitorException {
        try {
            addCombiner(sparkOp.physicalPlan);
        } catch (Exception e) {
            throw new VisitorException(e);
        }
    }

    // Checks for algebraic operations and if they exist.
    // Replaces global rearrange (cogroup) with reduceBy as follows:
    // Input:
    // foreach (using algebraicOp)
    //   -> packager
    //      -> globalRearrange
    //          -> localRearrange
    // Output:
    // foreach (using algebraicOp.Final)
    //   -> reduceBy (uses algebraicOp.Intermediate)
    //         -> foreach (using algebraicOp.Initial)
    //             -> CombinerRearrange
    private void addCombiner(PhysicalPlan phyPlan) throws VisitorException, PlanException, CloneNotSupportedException {

        List<PhysicalOperator> leaves = phyPlan.getLeaves();
        if (leaves == null || leaves.size() != 1) {
            return;
        }

        // Ensure there is grouping.
        List<POGlobalRearrange> glrs = PlanHelper.getPhysicalOperators(phyPlan, POGlobalRearrange.class);
        if (glrs == null || glrs.size() == 0) {
            return;
        }
        for (POGlobalRearrange glr : glrs) {
            List<PhysicalOperator> glrSuccessors = phyPlan.getSuccessors(glr);
            if (glrSuccessors == null || glrSuccessors.isEmpty()) {
                continue;
            }

            if (!(glrSuccessors.get(0) instanceof POPackage)) {
                continue;
            }
            POPackage poPackage = (POPackage) glrSuccessors.get(0);

            List<PhysicalOperator> poPackageSuccessors = phyPlan.getSuccessors(poPackage);
            if (poPackageSuccessors == null || poPackageSuccessors.size() != 1) {
                continue;
            }
            PhysicalOperator successor = poPackageSuccessors.get(0);

            // Retaining the original successor to be used later in modifying the plan.
            PhysicalOperator packageSuccessor = successor;

            if (successor instanceof POLimit) {
                // POLimit is acceptable, as long as it has a single foreach as
                // successor
                List<PhysicalOperator> limitSucs = phyPlan.getSuccessors(successor);
                if (limitSucs != null && limitSucs.size() == 1 &&
                        limitSucs.get(0) instanceof POForEach) {
                    // the code below will now further examine the foreach
                    successor = limitSucs.get(0);
                }
            }
            if (successor instanceof POForEach) {
                POForEach foreach = (POForEach) successor;
                List<PhysicalOperator> foreachSuccessors = phyPlan.getSuccessors(foreach);
                // multi-query
                if (foreachSuccessors == null || foreachSuccessors.size() != 1) {
                    continue;
                }
                // Clone foreach so it can be modified to a post-reduce foreach.
                POForEach postReduceFE = foreach.clone();
                List<PhysicalPlan> feInners = postReduceFE.getInputPlans();

                // find algebraic operators and also check if the foreach statement
                // is suitable for combiner use
                List<Pair<PhysicalOperator, PhysicalPlan>> algebraicOps = CombinerOptimizerUtil.findAlgebraicOps
                        (feInners);
                if (algebraicOps == null || algebraicOps.size() == 0) {
                    // the plan is not combinable or there is nothing to combine
                    // we're done
                    continue;
                }
                try {
                    List<PhysicalOperator> glrPredecessors = phyPlan.getPredecessors(glr);
                    // Exclude co-group from optimization
                    if (glrPredecessors == null || glrPredecessors.size() != 1) {
                        continue;
                    }

                    if (!(glrPredecessors.get(0) instanceof POLocalRearrange)) {
                        continue;
                    }

                    POLocalRearrange rearrange = (POLocalRearrange) glrPredecessors.get(0);

                    LOG.info("Algebraic operations found. Optimizing plan to use combiner.");

                    // Trim the global rearrange and the preceeding package.
                    convertToMapSideForEach(phyPlan, poPackage);

                    // replace PODistinct->Project[*] with distinct udf (which is Algebraic)
                    for (Pair<PhysicalOperator, PhysicalPlan> op2plan : algebraicOps) {
                        if (!(op2plan.first instanceof PODistinct)) {
                            continue;
                        }
                        CombinerOptimizerUtil.DistinctPatcher distinctPatcher
                                = new CombinerOptimizerUtil.DistinctPatcher(op2plan.second);
                        distinctPatcher.visit();
                        if (distinctPatcher.getDistinct() == null) {
                            int errCode = 2073;
                            String msg = "Problem with replacing distinct operator with distinct built-in function.";
                            throw new PlanException(msg, errCode, PigException.BUG);
                        }
                        op2plan.first = distinctPatcher.getDistinct();
                    }

                    // create new map foreach -
                    POForEach mfe = CombinerOptimizerUtil.createForEachWithGrpProj(postReduceFE, poPackage.getPkgr()
                            .getKeyType());
                    Map<PhysicalOperator, Integer> op2newpos = Maps.newHashMap();
                    Integer pos = 1;
                    // create plan for each algebraic udf and add as inner plan in map-foreach
                    for (Pair<PhysicalOperator, PhysicalPlan> op2plan : algebraicOps) {
                        PhysicalPlan udfPlan = CombinerOptimizerUtil.createPlanWithPredecessors(op2plan.first,
                                op2plan.second);
                        mfe.addInputPlan(udfPlan, false);
                        op2newpos.put(op2plan.first, pos++);
                    }
                    CombinerOptimizerUtil.changeFunc(mfe, POUserFunc.INITIAL);

                    // since we will only be creating SingleTupleBag as input to
                    // the map foreach, we should flag the POProjects in the map
                    // foreach inner plans to also use SingleTupleBag
                    for (PhysicalPlan mpl : mfe.getInputPlans()) {
                        try {
                            new CombinerOptimizerUtil.fixMapProjects(mpl).visit();
                        } catch (VisitorException e) {
                            int errCode = 2089;
                            String msg = "Unable to flag project operator to use single tuple bag.";
                            throw new PlanException(msg, errCode, PigException.BUG, e);
                        }
                    }

                    // create new combine foreach
                    POForEach cfe = CombinerOptimizerUtil.createForEachWithGrpProj(postReduceFE, poPackage.getPkgr()
                            .getKeyType());
                    // add algebraic functions with appropriate projection
                    CombinerOptimizerUtil.addAlgebraicFuncToCombineFE(cfe, op2newpos);

                    // we have modified the foreach inner plans - so set them again
                    // for the foreach so that foreach can do any re-initialization
                    // around them.
                    mfe.setInputPlans(mfe.getInputPlans());
                    cfe.setInputPlans(cfe.getInputPlans());

                    // tell POCombinerPackage which fields need projected and which
                    // placed in bags. First field is simple project rest need to go
                    // into bags
                    int numFields = algebraicOps.size() + 1; // algebraic funcs + group key
                    boolean[] bags = new boolean[numFields];
                    bags[0] = false;
                    for (int i = 1; i < numFields; i++) {
                        bags[i] = true;
                    }

                    // Use the POCombiner package in the combine plan
                    // as it needs to act differently than the regular
                    // package operator.
                    CombinerPackager pkgr = new CombinerPackager(poPackage.getPkgr(), bags);
                    POPackage combinePack = poPackage.clone();
                    combinePack.setPkgr(pkgr);

                    // A specialized local rearrange operator will replace
                    // the normal local rearrange in the map plan.
                    POLocalRearrange newRearrange = CombinerOptimizerUtil.getNewRearrange(rearrange);
                    POPreCombinerLocalRearrange combinerLocalRearrange = CombinerOptimizerUtil.getPreCombinerLR
                            (rearrange);
                    phyPlan.replace(rearrange, combinerLocalRearrange);

                    // Create a reduceBy operator.
                    POReduceBySpark reduceOperator = new POReduceBySpark(cfe.getOperatorKey(), cfe
                            .getRequestedParallelism(),
                            cfe.getInputPlans(), cfe.getToBeFlattened(), combinePack, newRearrange);
                    reduceOperator.setCustomPartitioner(glr.getCustomPartitioner());
                    fixReduceSideFE(postReduceFE, algebraicOps);
                    CombinerOptimizerUtil.changeFunc(reduceOperator, POUserFunc.INTERMEDIATE);
                    updatePackager(reduceOperator, newRearrange);

                    // Add the new operators
                    phyPlan.add(reduceOperator);
                    phyPlan.add(mfe);
                    // Connect the new operators as follows:
                    // reduceBy (using algebraicOp.Intermediate)
                    //      -> foreach (using algebraicOp.Initial)
                     phyPlan.connect(mfe, reduceOperator);

                    // Insert the reduce stage between combiner rearrange and its successor.
                    phyPlan.disconnect(combinerLocalRearrange, packageSuccessor);
                    phyPlan.connect(reduceOperator, packageSuccessor);
                    phyPlan.connect(combinerLocalRearrange, mfe);

                    // Replace foreach with post reduce foreach
                    phyPlan.add(postReduceFE);
                    phyPlan.replace(foreach, postReduceFE);
                } catch (Exception e) {
                    int errCode = 2018;
                    String msg = "Internal error. Unable to introduce the combiner for optimization.";
                    throw new OptimizerException(msg, errCode, PigException.BUG, e);
                }
            }
        }
    }

    // Modifies the input plans of the post reduce foreach to match the output of reduce stage.
    private void fixReduceSideFE(POForEach postReduceFE, List<Pair<PhysicalOperator, PhysicalPlan>> algebraicOps)
            throws ExecException, PlanException {
        int i=1;
        for (Pair<PhysicalOperator, PhysicalPlan> algebraicOp : algebraicOps) {
            POUserFunc combineUdf = (POUserFunc) algebraicOp.first;
            PhysicalPlan pplan = algebraicOp.second;
            combineUdf.setAlgebraicFunction(POUserFunc.FINAL);

            POProject newProj = new POProject(
                    CombinerOptimizerUtil.createOperatorKey(postReduceFE.getOperatorKey().getScope()),
                    1, i
            );
            newProj.setResultType(DataType.BAG);

            PhysicalOperator udfInput = pplan.getPredecessors(combineUdf).get(0);
            pplan.disconnect(udfInput, combineUdf);
            pplan.add(newProj);
            pplan.connect(newProj, combineUdf);
            i++;
        }
        postReduceFE.setResultType(DataType.TUPLE);
    }

    // Modifies the map side of foreach (before reduce).
    private void convertToMapSideForEach(PhysicalPlan physicalPlan, POPackage poPackage)
            throws PlanException {
        LinkedList<PhysicalOperator> operatorsToRemove = new LinkedList<>();
        for (PhysicalOperator physicalOperator : physicalPlan.getPredecessors(poPackage)) {
            if (physicalOperator instanceof POGlobalRearrangeSpark) {
                operatorsToRemove.add(physicalOperator);
                break;
            }
        }
        // Remove global rearranges preceeding POPackage
        for (PhysicalOperator po : operatorsToRemove) {
            physicalPlan.removeAndReconnect(po);
        }
        // Remove POPackage itself.
        physicalPlan.removeAndReconnect(poPackage);
    }

    // Update the ReduceBy Operator with the packaging used by Local rearrange.
    private void updatePackager(POReduceBySpark reduceOperator, POLocalRearrange lrearrange) throws OptimizerException {
        Packager pkgr = reduceOperator.getPkg().getPkgr();
        // annotate the package with information from the LORearrange
        // update the keyInfo information if already present in the POPackage
        Map<Integer, Pair<Boolean, Map<Integer, Integer>>> keyInfo = pkgr.getKeyInfo();
        if (keyInfo == null)
            keyInfo = new HashMap<>();

        if (keyInfo.get(Integer.valueOf(lrearrange.getIndex())) != null) {
            // something is wrong - we should not be getting key info
            // for the same index from two different Local Rearranges
            int errCode = 2087;
            String msg = "Unexpected problem during optimization." +
                    " Found index:" + lrearrange.getIndex() +
                    " in multiple LocalRearrange operators.";
            throw new OptimizerException(msg, errCode, PigException.BUG);

        }
        keyInfo.put(Integer.valueOf(lrearrange.getIndex()),
                new Pair<Boolean, Map<Integer, Integer>>(
                        lrearrange.isProjectStar(), lrearrange.getProjectedColsMap()));
        pkgr.setKeyInfo(keyInfo);
        pkgr.setKeyTuple(lrearrange.isKeyTuple());
        pkgr.setKeyCompound(lrearrange.isKeyCompound());
    }

    /**
     * Look for a algebraic POUserFunc that is the leaf of an input plan.
     *
     * @param pplan physical plan
     * @return null if any operator other POProject or non-algebraic POUserFunc is
     * found while going down the plan, otherwise algebraic POUserFunc is returned
     */
    private static POUserFunc getAlgebraicSuccessor(PhysicalPlan pplan) {
        // check if it ends in an UDF
        List<PhysicalOperator> leaves = pplan.getLeaves();
        if (leaves == null || leaves.size() != 1) {
            return null;
        }

        PhysicalOperator succ = leaves.get(0);
        if (succ instanceof POUserFunc && ((POUserFunc) succ).combinable()) {
            return (POUserFunc) succ;
        }

        // some other operator ? can't combine
        return null;
    }
}
