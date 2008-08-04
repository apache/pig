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
package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.pig.data.DataType;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROpPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POUserFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POProject;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.PODistinct;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFilter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPostCombinerPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSort;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.Pair;

/**
 * Optimize map reduce plans to use the combiner where possible.
 * Currently Foreach is copied to the combiner phase if it does not contain a
 * nested plan and all UDFs in the generate statement are algebraic.
 * The version of the foreach in the combiner
 * stage will use the initial function, and the version in the reduce stage
 * will be changed to use the final function.
 *
 * Major areas for enhancement:
 * 1) Currently, scripts such as:
 *     B = group A by $0;
 *     C = foreach B {
 *         C1 = distinct A;
 *         generate group, COUNT(C1);
 *     }
 * do not use the combiner.  The issue is being able to properly decompose
 * the expression in the UDF's plan.  The current code just takes whatever is
 * the argument to the algebraic UDF and replaces it with a project.  This
 * works for things like generate group, SUM(A.$1 + 1).  But it fails for
 * things like the above.  Certain types of inner plans will never be
 * movable (like filters).  But distinct or order by in the inner plan
 * should be moble.  And, things like:
 *      C = cogroup A by $0, B by $0;
 *      D = foreach C {
 *          D1 = distinct A;
 *          D2 = distinct B;
 *          generate UDF(D1 + D2);
 *      }
 * make it even harder.  The first step is probably just to handle queries
 * like the first above, as they will probably be the most common.
 *
 * 2) Scripts such as:
 *     B = group A by $0;
 *     C = foreach B generate algebraic(A), nonalgebraic(A);
 * currently aren't moved into the combiner, even though they could be.
 * Again, the trick here is properly decomposing the plan since A may be more
 * than a simply projection.
 *
 * #2 should probably be the next area of focus.
 *
 */
public class CombinerOptimizer extends MROpPlanVisitor {

    private Log log = LogFactory.getLog(getClass());

    private enum ExprType { SIMPLE_PROJECT, ALGEBRAIC, NOT_ALGEBRAIC,
        DISTINCT };

    private int mKeyField = -1;

    private byte mKeyType = 0;

    public CombinerOptimizer(MROperPlan plan) {
        super(plan, new DepthFirstWalker<MapReduceOper, MROperPlan>(plan));
    }

    @Override
    public void visitMROp(MapReduceOper mr) throws VisitorException {
        log.trace("Entering CombinerOptimizer.visitMROp");
        if (mr.reducePlan.isEmpty()) return;

        // Find the POLocalRearrange in the map.  I'll need it later.
        List<PhysicalOperator> mapLeaves = mr.mapPlan.getLeaves();
        if (mapLeaves == null || mapLeaves.size() != 1) {
            log.warn("Expected map to have single leaf!");
            return;
        }
        PhysicalOperator mapLeaf = mapLeaves.get(0);
        if (!(mapLeaf instanceof POLocalRearrange)) {
            return;
        }
        POLocalRearrange rearrange = (POLocalRearrange)mapLeaf;

        List<PhysicalOperator> reduceRoots = mr.reducePlan.getRoots();
        if (reduceRoots.size() != 1) {
            log.warn("Expected reduce to have single leaf");
            return;
        }

        // I expect that the first root should always be a POPackage.  If
        // not, I don't know what's going on, so I'm out of here.
        PhysicalOperator root = reduceRoots.get(0);
        if (!(root instanceof POPackage)) {
            log.warn("Expected reduce root to be a POPackage");
            return;
        }
        POPackage pack = (POPackage)root;

        List<PhysicalOperator> packSuccessors =
            mr.reducePlan.getSuccessors(root);
        if (packSuccessors == null || packSuccessors.size() != 1) return;
        PhysicalOperator successor = packSuccessors.get(0);

        // Need to check if this is a distinct.
        if (successor instanceof POFilter) {
            /*
               Later
            POFilter filter = (POFilter)successor;
            PhysicalPlan filterInner = filter.getPlan();
            if (onKeysOnly(filterInner)) {
                // TODO move filter to combiner
                // TODO Patch up projects of filter successor
                // Call ourselves again, as we may be able to move the next
                // operator too.
                visitMROp(mr);
            } else if (algebraic(filterInner)) {
                // TODO Duplicate filter to combiner
            }
            */
        } else if (successor instanceof POForEach) {
            POForEach foreach = (POForEach)successor;
            List<PhysicalPlan> feInners = foreach.getInputPlans();
            List<ExprType> ap = algebraic(feInners, foreach.getToBeFlattened());
            if (ap != null) {
                log.info("Choosing to move algebraic foreach to combiner");

                // Need to insert a foreach in the combine plan.  It will
                // have one inner plan for each inner plan in the foreach
                // we're duplicating.  For projections, the plan will be
                // the same.  For algebraic udfs, the plan will have the
                // initial version of the function.  The reduce plan will
                // be changed to have the final version.
                if (mr.combinePlan.getRoots().size() != 0) {
                    log.warn("Wasn't expecting to find anything already "
                        + "in the combiner!");
                    return;
                }
                mr.combinePlan = new PhysicalPlan();
                try {
                    // If we haven't already found the key (and thus the
                    // key type) we need to figure out the key type now.
                    if (mKeyType == 0) {
                        mKeyType = rearrange.getKeyType();
                    }
                    POPackage cp = pack.clone();
                    mr.combinePlan.add(cp);
                    POForEach cfe = foreach.clone();
                    fixUpForeachs(cfe, foreach, ap);
                    mr.combinePlan.add(cfe);
                    mr.combinePlan.connect(cp, cfe);
                    // No need to connect projections in cfe to cp, because
                    // PigCombiner directly attaches output from package to
                    // root of remaining plan.
                    POLocalRearrange clr = rearrange.clone();
                    fixUpRearrange(clr);
                    mr.combinePlan.add(clr);
                    mr.combinePlan.connect(cfe, clr);

                    // Use the ExprType list returned from algebraic to tell
                    // POPostCombinerPackage which fields need projected and
                    // which placed in bags.
                    int numFields = (mKeyField >= ap.size()) ? mKeyField + 1 :
                        ap.size();
                    boolean[] bags = new boolean[numFields];
                    for (int i = 0; i < ap.size(); i++) {
                        if (ap.get(i) == ExprType.SIMPLE_PROJECT) bags[i] = false;
                        else bags[i] = true;
                    }
                    bags[mKeyField] = false;
                    // Change the package operator in the reduce plan to
                    // be the post combiner package, as it needs to act
                    // differently than the regular package operator.
                    POPostCombinerPackage newPack =
                        new POPostCombinerPackage(pack, bags);
                    mr.reducePlan.replace(pack, newPack);
                } catch (Exception e) {
                    throw new VisitorException(e);
                }
            }
        }
    }

    /*
    private boolean onKeysOnly(PhysicalPlan pp) {
        // TODO
        return false;
    }
    */

    // At some point in the future we can expand to deconstructing
    // non-algebraic expressions to find an algebraic or projected root.  For
    // example, given a query: 
    // foreach b generate group, algebraic(a), nonalgebraic(a)
    // this could be transformed to:
    // combiner: foreach group, initial(a), a
    // reducer: foreach group, final(a), nonalgebraic(a)
    // This code doesn't do this now, because deconstructing expressions is
    // tricky.
    private List<ExprType> algebraic(
            List<PhysicalPlan> plans,
            List<Boolean> flattens) throws VisitorException {
        List<ExprType> types = new ArrayList<ExprType>(plans.size());
        boolean atLeastOneAlgebraic = false;
        boolean noNonAlgebraics = true;
        for (int i = 0; i < plans.size(); i++) {
            ExprType t = algebraic(plans.get(i), flattens.get(i), i);
            types.add(t);
            atLeastOneAlgebraic |= (t == ExprType.ALGEBRAIC);
            noNonAlgebraics &= (t != ExprType.NOT_ALGEBRAIC);
        }
        if (!atLeastOneAlgebraic || !noNonAlgebraics) return null;
        else return types;
    }

    private ExprType algebraic(
            PhysicalPlan pp,
            Boolean toBeFlattened,
            int field) throws VisitorException {
        // A plan will be considered algebraic if
        // each element is a single field OR an algebraic UDF  
        List<PhysicalOperator> leaves = pp.getLeaves();
        if (leaves == null || leaves.size() != 1) {
            // Don't know what this is, but it isn't algebraic
            return ExprType.NOT_ALGEBRAIC;
        }

        // Check that it doesn't have anything in the nested plan that I
        // can't make algebraic.  At this point this is just filters and
        // foreach.  Filters are left out because they are not necessarily
        // algebraic.  Foreach is left out because it's difficult to patch
        // up the plan properly around them.  This is an area for future
        // enhancement.
        AlgebraicPlanChecker apc = new AlgebraicPlanChecker(pp);
        apc.visit();
        if (apc.sawNonAlgebraic) return ExprType.NOT_ALGEBRAIC;

        PhysicalOperator leaf = leaves.get(0);
        if (leaf instanceof POProject) {
            POProject proj = (POProject)leaf;
            // Check that it's a simple project.  We can't currently handle
            // things like group.$0, because that requires resetting types on
            // the reduce side.
            if (pp.getPredecessors(proj) != null) return ExprType.NOT_ALGEBRAIC;

            // Check to see if this is a projection of the grouping column.
            // If so, it will be a projection of col 0 and will have no
            // predecessors (to avoid things like group.$0, which isn't what we
            // want).
            List<Integer> cols = proj.getColumns();
            if (cols != null && cols.size() == 1 && cols.get(0) == 0 &&
                    pp.getPredecessors(proj) == null) {
                mKeyField = field;
                mKeyType = proj.getResultType();
            } else {
                // It can't be a flatten except on the grouping column
                if (toBeFlattened) return ExprType.NOT_ALGEBRAIC;
            }
            return ExprType.SIMPLE_PROJECT;
        } else if (leaf instanceof POUserFunc) {
            return ((POUserFunc)leaf).combinable() ? ExprType.ALGEBRAIC :
                ExprType.NOT_ALGEBRAIC;
        } else {
            return ExprType.NOT_ALGEBRAIC;
        }
    }

    // Returns number of fields that this will project, including the added
    // key field if that is necessary
    private void fixUpForeachs(
            POForEach cfe, // combiner foreach
            POForEach rfe, // reducer foreach
            List<ExprType> exprs) throws PlanException {
        List<PhysicalPlan> cPlans = cfe.getInputPlans();
        List<PhysicalPlan> rPlans = rfe.getInputPlans();
        for (int i = 0; i < exprs.size(); i++) {
            if (exprs.get(i) == ExprType.ALGEBRAIC) {
                changeFunc(cfe, cPlans.get(i), POUserFunc.INITIAL);
                changeFunc(rfe, rPlans.get(i), POUserFunc.FINAL);
            }
        }

        // Set flattens for combiner ForEach to false
        List<Boolean> cfeFlattens = new ArrayList<Boolean>(cPlans.size());
        for (int i = 0; i < cPlans.size(); i++) {
            cfeFlattens.add(false);
        }
        cfe.setToBeFlattened(cfeFlattens);

        // If the key field isn't in the project of the combiner foreach, add
        // it to the end.
        if (mKeyField == -1) {
            PhysicalPlan newPlan = new PhysicalPlan();
            String scope = cfe.getOperatorKey().scope;
            POProject proj = new POProject(new OperatorKey(scope, 
                NodeIdGenerator.getGenerator().getNextNodeId(scope)), -1, 0);
            proj.setResultType(mKeyType);
            newPlan.add(proj);
            cfe.addInputPlan(newPlan, false);
            mKeyField = cPlans.size() - 1;
        }

        // Change the plans on the reduce foreach to project from the column
        // they are in.  UDFs will be left the same but their
        // inputs altered.  Any straight projections will also be altered.
        for (int i = 0; i < rPlans.size(); i++) {
            List<PhysicalOperator> leaves = rPlans.get(i).getLeaves();
            if (leaves == null || leaves.size() != 1) {
                throw new RuntimeException("Expected to find plan with single leaf!");
            }
            PhysicalOperator leaf = leaves.get(0);

            // Leaf should be either a projection or a UDF
            if (leaf instanceof POProject) {
                ((POProject)leaf).setColumn(i);
            } else if (leaf instanceof POUserFunc) {
                String scope = leaf.getOperatorKey().scope;
                POProject proj = new POProject(new OperatorKey(scope, 
                    NodeIdGenerator.getGenerator().getNextNodeId(scope)),
                    leaf.getRequestedParallelism(), i);
                proj.setResultType(DataType.BAG);
                // Remove old connections and elements from the plan
                rPlans.get(i).trimAbove(leaf);
                rPlans.get(i).add(proj);
                rPlans.get(i).connect(proj, leaf);
                List<PhysicalOperator> inputs =
                    new ArrayList<PhysicalOperator>(1);
                inputs.add(proj);
                leaf.setInputs(inputs);
            }
        }
    }

    private void changeFunc(POForEach fe, PhysicalPlan plan, byte type) {
        List<PhysicalOperator> leaves = plan.getLeaves();
        if (leaves == null || leaves.size() != 1) {
            throw new RuntimeException("Expected to find plan with single leaf!");
        }

        PhysicalOperator leaf = leaves.get(0);
        if (!(leaf instanceof POUserFunc)) {
            throw new RuntimeException("Expected to find plan with UDF leaf!");
        }
        POUserFunc func = (POUserFunc)leaf;
        func.setAlgebraicFunction(type);
    }

    private void fixUpRearrange(POLocalRearrange rearrange) {
        // Set the projection to be the key
        PhysicalPlan newPlan = new PhysicalPlan();
        String scope = rearrange.getOperatorKey().scope;
        POProject proj = new POProject(new OperatorKey(scope, 
            NodeIdGenerator.getGenerator().getNextNodeId(scope)), -1,
            mKeyField);
        proj.setResultType(mKeyType);
        newPlan.add(proj);
        List<PhysicalPlan> plans = new ArrayList<PhysicalPlan>(1);
        plans.add(newPlan);
        rearrange.setPlans(plans);
        rearrange.setIndex(mKeyField);
    }

    private class AlgebraicPlanChecker extends PhyPlanVisitor {
        boolean sawNonAlgebraic = false;

        AlgebraicPlanChecker(PhysicalPlan plan) {
            super(plan, new DepthFirstWalker<PhysicalOperator, PhysicalPlan>(plan));
        }

        @Override
        public void visitDistinct(PODistinct distinct) throws VisitorException {
            sawNonAlgebraic = true;
        }

        @Override
        public void visitFilter(POFilter filter) throws VisitorException {
            sawNonAlgebraic = true;
        }

        @Override
        public void visitPOForEach(POForEach fe) throws VisitorException {
            sawNonAlgebraic = true;
        }

        @Override
        public void visitSort(POSort sort) throws VisitorException {
            sawNonAlgebraic = true;
        }

    }

}
