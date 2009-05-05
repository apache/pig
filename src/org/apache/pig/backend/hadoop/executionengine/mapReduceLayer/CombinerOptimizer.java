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

import org.apache.pig.PigException;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigWarning;
import org.apache.pig.data.DataType;
import org.apache.pig.backend.executionengine.ExecException;
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
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLimit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POCombinerPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPreCombinerLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSort;
import org.apache.pig.impl.plan.CompilationMessageCollector;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.PlanWalker;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.plan.CompilationMessageCollector.MessageType;
import org.apache.pig.impl.plan.optimizer.OptimizerException;

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

    private static final String DISTINCT_UDF_CLASSNAME = org.apache.pig.builtin.Distinct.class.getName();

    private Log log = LogFactory.getLog(getClass());

    private enum ExprType { SIMPLE_PROJECT, ALGEBRAIC, NOT_ALGEBRAIC,
        DISTINCT };

    private int mKeyField = -1;

    private byte mKeyType = 0;
    
    private String chunkSize;
    
    private CompilationMessageCollector messageCollector = null;

    public CombinerOptimizer(MROperPlan plan, String chunkSize) {
        super(plan, new DepthFirstWalker<MapReduceOper, MROperPlan>(plan));
        this.chunkSize = chunkSize;
        messageCollector = new CompilationMessageCollector() ; 
    }

    public CombinerOptimizer(MROperPlan plan, String chunkSize, 
    		CompilationMessageCollector messageCollector) {
        super(plan, new DepthFirstWalker<MapReduceOper, MROperPlan>(plan));
        this.chunkSize = chunkSize;
        this.messageCollector = messageCollector ; 
    }
    
    public CompilationMessageCollector getMessageCollector() {
    	return messageCollector;
    }

    @Override
    public void visitMROp(MapReduceOper mr) throws VisitorException {
        resetState();
        log.trace("Entering CombinerOptimizer.visitMROp");
        if (mr.reducePlan.isEmpty()) return;

        // Find the POLocalRearrange in the map.  I'll need it later.
        List<PhysicalOperator> mapLeaves = mr.mapPlan.getLeaves();
        if (mapLeaves == null || mapLeaves.size() != 1) {
            messageCollector.collect("Expected map to have single leaf!", MessageType.Warning, PigWarning.MULTI_LEAF_MAP);
            return;
        }
        PhysicalOperator mapLeaf = mapLeaves.get(0);
        if (!(mapLeaf instanceof POLocalRearrange)) {
            return;
        }
        POLocalRearrange rearrange = (POLocalRearrange)mapLeaf;

        List<PhysicalOperator> reduceRoots = mr.reducePlan.getRoots();
        if (reduceRoots.size() != 1) {
        	messageCollector.collect("Expected reduce to have single leaf", MessageType.Warning, PigWarning.MULTI_LEAF_REDUCE);
            return;
        }

        // I expect that the first root should always be a POPackage.  If
        // not, I don't know what's going on, so I'm out of here.
        PhysicalOperator root = reduceRoots.get(0);
        if (!(root instanceof POPackage)) {
        	messageCollector.collect("Expected reduce root to be a POPackage", MessageType.Warning, PigWarning.NON_PACKAGE_REDUCE_PLAN_ROOT);
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

                // Need to insert two new foreachs - one  in the combine
				// and one in the map plan which will be based on the reduce foreach.
				// The map foreach will have one inner plan for each 
				// inner plan in the foreach we're duplicating.  For 
				// projections, the plan will be the same.  For algebraic 
				// udfs, the plan will have the initial version of the function.
				
				// The combine foreach will have one inner plan for each 
				// inner plan in the foreach we're duplicating.  For 
				// projections, the project operators will be changed to
				// project the same column as its position in the
				// foreach. For algebraic udfs, the plan will have the 
				// intermediate version of the function. The input to the
				// udf will be a POProject which will project the column
				// corresponding to the position of the udf in the foreach
				
			    // In the inner plans of the reduce foreach for 	
				// projections, the project operators will be changed to
				// project the same column as its position in the
				// foreach. For algebraic udfs, the plan will have the 
				// final version of the function. The input to the
				// udf will be a POProject which will project the column
				// corresponding to the position of the udf in the foreach
                if (mr.combinePlan.getRoots().size() != 0) {
                	messageCollector.collect("Wasn't expecting to find anything already "
                        + "in the combiner!", MessageType.Warning, PigWarning.NON_EMPTY_COMBINE_PLAN);
                    return;
                }
                mr.combinePlan = new PhysicalPlan();
                try {
                    // If we haven't already found the key (and thus the
                    // key type) we need to figure out the key type now.
                    if (mKeyType == 0) {
                        mKeyType = rearrange.getKeyType();
                    }

                    POForEach mfe = foreach.clone();
                    POForEach cfe = foreach.clone();
                    fixUpForeachs(mfe, cfe, foreach, ap);
                    
                    
                    // Use the ExprType list returned from algebraic to tell
                    // POCombinerPackage which fields need projected and
                    // which placed in bags.
                    int numFields = (mKeyField >= ap.size()) ? mKeyField + 1 :
                        ap.size();
                    boolean[] bags = new boolean[numFields];
                    for (int i = 0; i < ap.size(); i++) {
                        if (ap.get(i) == ExprType.SIMPLE_PROJECT) bags[i] = false;
                        else bags[i] = true;
                    }
                    bags[mKeyField] = false;
					// Use the POCombiner package in the combine plan
					// as it needs to act differently than the regular
					// package operator.
                    POCombinerPackage combinePack =
                        new POCombinerPackage(pack, bags);
                    mr.combinePlan.add(combinePack);
                    mr.combinePlan.add(cfe);
                    mr.combinePlan.connect(combinePack, cfe);
                    // No need to connect projections in cfe to cp, because
                    // PigCombiner directly attaches output from package to
                    // root of remaining plan.
                    
                    POLocalRearrange mlr = rearrange.clone();
                    fixUpRearrange(mlr);

                    // A specialized local rearrange operator will replace
                    // the normal local rearrange in the map plan. This behaves
                    // like the regular local rearrange in the getNext() 
                    // as far as getting its input and constructing the 
                    // "key" out of the input. It then returns a tuple with
                    // two fields - the key in the first position and the
                    // "value" inside a bag in the second position. This output
                    // format resembles the format out of a Package. This output
                    // will feed to the map foreach which expects this format.
                    // If the key field isn't in the project of the combiner or map foreach,
                    // it is added to the end (This is required so that we can 
                    // set up the inner plan of the new Local Rearrange leaf in the map
                    // and combine plan to contain just the project of the key).
                    patchUpMap(mr.mapPlan, getPreCombinerLR(rearrange), mfe, mlr);
                    POLocalRearrange clr = rearrange.clone();
                    fixUpRearrange(clr);

                    mr.combinePlan.add(clr);
                    mr.combinePlan.connect(cfe, clr);
                    
                    // Change the package operator in the reduce plan to
                    // be the POCombiner package, as it needs to act
                    // differently than the regular package operator.
                    POCombinerPackage newReducePack =
                        new POCombinerPackage(pack, bags);
                    mr.reducePlan.replace(pack, newReducePack);
                    
                    // the replace() above only changes
                    // the plan and does not change "inputs" to 
                    // operators
                    // set up "inputs" for the operator after
                    // package correctly
                    List<PhysicalOperator> packList = new ArrayList<PhysicalOperator>();
                    packList.add(newReducePack);
                    List<PhysicalOperator> sucs = mr.reducePlan.getSuccessors(newReducePack);
                    // there should be only one successor to package
                    sucs.get(0).setInputs(packList);
                } catch (Exception e) {
                    int errCode = 2018;
                    String msg = "Internal error. Unable to introduce the combiner for optimization.";
                    throw new OptimizerException(msg, errCode, PigException.BUG, e);
                }
            }
        }
    }

    /**
     * @param mapPlan
     * @param preCombinerLR
     * @param mfe
     * @param mlr
     * @throws PlanException 
     */
    private void patchUpMap(PhysicalPlan mapPlan, POPreCombinerLocalRearrange preCombinerLR,
            POForEach mfe, POLocalRearrange mlr) throws PlanException {
        
        POLocalRearrange oldLR = (POLocalRearrange)mapPlan.getLeaves().get(0);
        mapPlan.replace(oldLR, preCombinerLR);
        
        mapPlan.add(mfe);
        mapPlan.connect(preCombinerLR, mfe);
        
        mapPlan.add(mlr);
        mapPlan.connect(mfe, mlr);
    }

    /**
     * @param rearrange
     * @return
     */
    private POPreCombinerLocalRearrange getPreCombinerLR(POLocalRearrange rearrange) {
        
        String scope = rearrange.getOperatorKey().scope;
        POPreCombinerLocalRearrange pclr = new POPreCombinerLocalRearrange(
                new OperatorKey(scope, NodeIdGenerator.getGenerator().getNextNodeId(scope)),
                rearrange.getRequestedParallelism(), rearrange.getInputs());
        pclr.setPlans(rearrange.getPlans());
        return pclr;
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
            atLeastOneAlgebraic |= 
                (t == ExprType.ALGEBRAIC || t == ExprType.DISTINCT);
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
        if(apc.sawDistinctAgg) return ExprType.DISTINCT;
        
        // we did not see a Non algebraic or a distinct so far
        // proceed to check leaves
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
            POForEach mfe, // map foreach
            POForEach cfe, // combiner foreach
            POForEach rfe, // reducer foreach
            List<ExprType> exprs) throws PlanException {
        List<PhysicalPlan> mPlans = mfe.getInputPlans();
        List<PhysicalPlan> cPlans = cfe.getInputPlans();
        List<PhysicalPlan> rPlans = rfe.getInputPlans();
        for (int i = 0; i < exprs.size(); i++) {
            if (exprs.get(i) == ExprType.ALGEBRAIC) {
                changeFunc(mfe, mPlans.get(i), POUserFunc.INITIAL);
                changeFunc(cfe, cPlans.get(i), POUserFunc.INTERMEDIATE);
                changeFunc(rfe, rPlans.get(i), POUserFunc.FINAL);
            } else if (exprs.get(i) == ExprType.DISTINCT) {
                // A PODistinct in the plan will always have
                // a Project[bag](*) as its successor.
                // We will replace it with a POUserFunc with "Distinct" as 
                // the underlying UDF. 
                // In the map and combine, we will make this POUserFunc
                // the leaf of the plan by removing other operators which
                // are descendants up to the leaf.
                // In the reduce we will keep descendants intact. Further
                // down in fixProjectAndInputs we will change the inputs to
                // this POUserFunc in the combine and reduce plans to be
                // just projections of the column "i"
                PhysicalPlan[] plans = new PhysicalPlan[] { 
                        mPlans.get(i), cPlans.get(i), rPlans.get(i) };
                byte[] funcTypes = new byte[] { POUserFunc.INITIAL, 
                        POUserFunc.INTERMEDIATE, POUserFunc.FINAL };
                for (int j = 0; j < plans.length; j++) {
                    DistinctPatcher dp = new DistinctPatcher(plans[j]);
                    try {
                        dp.visit();
                    } catch (VisitorException e) {
                        int errCode = 2073;
                        String msg = "Problem with replacing distinct operator with distinct built-in function.";
                        throw new PlanException(msg, errCode, PigException.BUG, e);
                    }
                    
                    
                    PhysicalOperator leaf = plans[j].getLeaves().get(0);
                    // make the Distinct POUserFunc the leaf in the map and combine plans.
                    if( j != plans.length - 1) {
                        while(!((leaf instanceof POUserFunc) && 
                                ((POUserFunc)leaf).getFuncSpec().getClassName().startsWith(DISTINCT_UDF_CLASSNAME))) {
                            plans[j].remove(leaf);
                            // get the new leaf
                            leaf = plans[j].getLeaves().get(0);
                        }
                        
                    } 
                    // Also set the Distinct's function to type Initial in map
                    // to type Intermediate in combine plan and to type Final in
                    // the reduce
                    POUserFunc distinctFunc = (POUserFunc)getDistinctUserFunc(plans[j], leaf);
                    try {
                        distinctFunc.setAlgebraicFunction(funcTypes[j]);
                    } catch (ExecException e) {
                        int errCode = 2074;
                        String msg = "Could not configure distinct's algebraic functions in map reduce plan.";
                        throw new PlanException(msg, errCode, PigException.BUG, e);
                    }
                }
                
            }
        }
        
        // since we will only be creating SingleTupleBag as input to
        // the map foreach, we should flag the POProjects in the map
        // foreach inner plans to also use SingleTupleBag
        for (PhysicalPlan mpl : mPlans) {
            try {
                new fixMapProjects(mpl).visit();
            } catch (VisitorException e) {
                int errCode = 2089;
                String msg = "Unable to flag project operator to use single tuple bag.";
                throw new PlanException(msg, errCode, PigException.BUG, e);
            }
        }


        // Set flattens for map and combiner ForEach to false
        List<Boolean> feFlattens = new ArrayList<Boolean>(cPlans.size());
        for (int i = 0; i < cPlans.size(); i++) {
            feFlattens.add(false);
        }
        mfe.setToBeFlattened(feFlattens);
        cfe.setToBeFlattened(feFlattens);

        // If the key field isn't in the project of the combiner or map foreach, add
        // it to the end (This is required so that we can set up the inner plan
        // of the new Local Rearrange in the map and combine plan to contain just the
        // project of the key).
        if (mKeyField == -1) {
            addKeyProject(mfe);
            addKeyProject(cfe);
            mKeyField = cPlans.size() - 1;
        }

        // Change the plans on the reduce/combine foreach to project from the column
        // they are in ( we just want to take output from the combine and
        // use that as input in the reduce/combine plan).  UDFs will be left the same but their
        // inputs altered.  Any straight projections will also be altered.
        fixProjectAndInputs(cPlans, exprs);
        fixProjectAndInputs(rPlans, exprs);
        
        
        // we have modified the foreach inner plans - so set them
        // again for the foreach so that foreach can do any re-initialization
        // around them.
        // FIXME - this is a necessary evil right now because the leaves are explicitly
        // stored in the POForeach as a list rather than computed each time at 
        // run time from the plans for optimization. Do we want to have the Foreach
        // compute the leaves each time and have Java optimize it (will Java optimize?)?
        mfe.setInputPlans(mPlans);
        cfe.setInputPlans(cPlans);
        rfe.setInputPlans(rPlans);
    }

    /**
     * @param plans
     * @param exprs 
     * @throws PlanException 
     */
    private void fixProjectAndInputs(List<PhysicalPlan> plans, List<ExprType> exprs) throws PlanException {
        for (int i = 0; i < plans.size(); i++) {
                List<PhysicalOperator> leaves = plans.get(i).getLeaves();
                if (leaves == null || leaves.size() != 1) {
                    int errCode = 2019;
                    String msg = "Expected to find plan with single leaf. Found " + leaves.size() + " leaves.";
                    throw new PlanException(msg, errCode, PigException.BUG);
                }
                PhysicalOperator leaf = leaves.get(0);
                // the combine plan could have an extra foreach inner plan
                // to project the key - so make sure we check the index
                // before looking in exprs
                if(i < exprs.size()  && exprs.get(i) == ExprType.DISTINCT) {
                    // if there is a distinctagg, we have to
                    // look for the Distinct POUserFunc and 
                    // change its input to be a project of
                    // column "i"
                    PhysicalOperator op = getDistinctUserFunc(plans.get(i), leaf);
                    setProjectInput(op, plans.get(i), i);
                } else {
                    // Leaf should be either a projection or a UDF
                    if (leaf instanceof POProject) {
                        ((POProject)leaf).setColumn(i);
                    } else if (leaf instanceof POUserFunc) {
                        setProjectInput(leaf, plans.get(i), i);
                    }
              }
        }
    }

    /**
     * @param op
     * @param index 
     * @param plan 
     * @throws PlanException 
     */
    private void setProjectInput(PhysicalOperator op, PhysicalPlan plan, int index) throws PlanException {
        String scope = op.getOperatorKey().scope;
        POProject proj = new POProject(new OperatorKey(scope, 
            NodeIdGenerator.getGenerator().getNextNodeId(scope)),
            op.getRequestedParallelism(), index);
        proj.setResultType(DataType.BAG);
        // Remove old connections and elements from the plan
        plan.trimAbove(op);
        plan.add(proj);
        plan.connect(proj, op);
        List<PhysicalOperator> inputs =
            new ArrayList<PhysicalOperator>(1);
        inputs.add(proj);
        op.setInputs(inputs);
        
    }

    /**
     * @param plan
     * @param operator
     * @return
     */
    private PhysicalOperator getDistinctUserFunc(PhysicalPlan plan, PhysicalOperator operator) {
        if(operator instanceof POUserFunc ) { 
            if(((POUserFunc)operator).getFuncSpec().getClassName().startsWith(DISTINCT_UDF_CLASSNAME)) {
                return operator;
            }
        }
        return getDistinctUserFunc(plan, plan.getPredecessors(operator).get(0));
        
    }


    /**
     * @param fe
     */
    private void addKeyProject(POForEach fe) {
        PhysicalPlan newForEachInnerPlan = new PhysicalPlan();
        String scope = fe.getOperatorKey().scope;
        POProject proj = new POProject(new OperatorKey(scope, 
            NodeIdGenerator.getGenerator().getNextNodeId(scope)), -1, 0);
        proj.setResultType(mKeyType);
        newForEachInnerPlan.add(proj);
        fe.addInputPlan(newForEachInnerPlan, false);
    }

    private void changeFunc(POForEach fe, PhysicalPlan plan, byte type) throws PlanException {
        List<PhysicalOperator> leaves = plan.getLeaves();
        if (leaves == null || leaves.size() != 1) {
            int errCode = 2019;
            String msg = "Expected to find plan with single leaf. Found " + leaves.size() + " leaves.";
            throw new PlanException(msg, errCode, PigException.BUG);
        }

        PhysicalOperator leaf = leaves.get(0);
        if (!(leaf instanceof POUserFunc)) {
            int errCode = 2020;
            String msg = "Expected to find plan with UDF leaf. Found " + leaf.getClass().getSimpleName();
            throw new PlanException(msg, errCode, PigException.BUG);
        }
        POUserFunc func = (POUserFunc)leaf;
        try {
            func.setAlgebraicFunction(type);
        } catch (ExecException e) {
            int errCode = 2075;
            String msg = "Could not set algebraic function type.";
            throw new PlanException(msg, errCode, PigException.BUG, e);
        }
    }

    private void fixUpRearrange(POLocalRearrange rearrange) throws PlanException {
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
        rearrange.setPlansFromCombiner(plans);
    }

    private class AlgebraicPlanChecker extends PhyPlanVisitor {
        boolean sawNonAlgebraic = false;
        boolean sawDistinctAgg = false;
        private boolean sawForeach = false;

        AlgebraicPlanChecker(PhysicalPlan plan) {
            super(plan, new DependencyOrderWalker<PhysicalOperator, PhysicalPlan>(plan));
        }

        /* (non-Javadoc)
         * @see org.apache.pig.impl.plan.PlanVisitor#visit()
         */
        @Override
        public void visit() throws VisitorException {
            super.visit();
            // if we saw foreach and distinct agg its ok
            // else if we only saw foreach, mark it as non algebraic
            if(sawForeach && !sawDistinctAgg) {
                sawNonAlgebraic = true;
            }
        }
        
        @Override
        public void visitDistinct(PODistinct distinct) throws VisitorException {
            if(sawDistinctAgg) {
                // we want to combine only in the case where there is only
                // one PODistinct which is the only input to an agg
                // we apparently have seen a PODistinct before, so lets not
                // combine.
                sawNonAlgebraic = true;
            }
            // check that this distinct is the only input to an agg
            // We could have the following two cases
            // script 1:
            // ..
            // b = group a by ...
            // c = foreach b { x = distinct a; generate AGG(x), ...}
            // The above script leads to the following plan for AGG(x):
            // POUserFunc(org.apache.pig.builtin.COUNT)[long] 
            //   |
            //   |---Project[bag][*] 
            //       |
            //       |---PODistinct[bag] 
            //           |
            //           |---Project[tuple][1] 

            // script 2:
            // ..
            // b = group a by ...
            // c = foreach b { x = distinct a; generate AGG(x.$1), ...}
            // The above script leads to the following plan for AGG(x.$1):
            // POUserFunc(org.apache.pig.builtin.IntSum)[long]
            //   |
            //   |---Project[bag][1]
            //       |
            //       |---Project[bag][*]
            //           |
            //           |---PODistinct[bag]
            //               |
            //               |---Project[tuple][1]
            // So tracing from the PODistinct to its successors upto the leaf, we should
            // see a Project[bag][*] as the immediate successor and an optional Project[bag]
            // as the next successor till we see the leaf.
            PhysicalOperator leaf = mPlan.getLeaves().get(0);
            // the leaf has to be a POUserFunc (need not be algebraic)
            if(leaf instanceof POUserFunc) {
                List<PhysicalOperator> immediateSuccs = mPlan.getSuccessors(distinct);
                if(immediateSuccs.size() == 1 && immediateSuccs.get(0) instanceof POProject) {
                    if(checkSuccessorIsLeaf(leaf, immediateSuccs.get(0))) { // script 1 above
                        sawDistinctAgg = true;
                        return;
                    } else { // check for script 2 scenario above
                        List<PhysicalOperator> nextSuccs = mPlan.getSuccessors(immediateSuccs.get(0));
                        if(nextSuccs.size() == 1) {
                            PhysicalOperator op = nextSuccs.get(0);
                            if(op instanceof POProject) {
                                if(checkSuccessorIsLeaf(leaf, op)) {
                                    sawDistinctAgg = true;
                                    return;
                                }
                            }
                        }
                        
                    }
                }
            }
            // if we did not return above, that means we did not see
            // the pattern we expected
            sawNonAlgebraic = true;
        }
        
        public void visitLimit(POLimit limit) throws VisitorException {
            sawNonAlgebraic = true;
        }

        private boolean checkSuccessorIsLeaf(PhysicalOperator leaf, PhysicalOperator opToCheck) {
            List<PhysicalOperator> succs = mPlan.getSuccessors(opToCheck);
            if(succs.size() == 1) {
                PhysicalOperator op = succs.get(0);
                if(op == leaf) {
                    return true;
                }
            }
            return false;
        }
        
        @Override
        public void visitFilter(POFilter filter) throws VisitorException {
            sawNonAlgebraic = true;
        }

        @Override
        public void visitPOForEach(POForEach fe) throws VisitorException {
            // we need to allow foreach as input for distinct
            // but don't want it for other things (why?). So lets
            // flag the presence of Foreach and if this is present
            // with a distinct agg, it will be allowed.
            sawForeach = true;
        }

        @Override
        public void visitSort(POSort sort) throws VisitorException {
            sawNonAlgebraic = true;
        }

    }
    
    /**
     * A visitor to replace   
     * Project[bag][*] 
     *  |
     *  |---PODistinct[bag]
     * with 
     * POUserFunc(org.apache.pig.builtin.Distinct)[DataBag]    
     */
    private class DistinctPatcher extends PhyPlanVisitor {

        public boolean patched = false;
        /**
         * @param plan
         * @param walker
         */
        public DistinctPatcher(PhysicalPlan plan,
                PlanWalker<PhysicalOperator, PhysicalPlan> walker) {
            super(plan, walker);
        }

        /**
         * @param physicalPlan
         */
        public DistinctPatcher(PhysicalPlan physicalPlan) {
            this(physicalPlan, new DependencyOrderWalker<PhysicalOperator, PhysicalPlan>(physicalPlan));
        }
        
        /* (non-Javadoc)
         * @see org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor#visitProject(org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POProject)
         */
        @Override
        public void visitProject(POProject proj) throws VisitorException {
            // check if this project is preceded by PODistinct and
            // has the return type bag
            
            
            List<PhysicalOperator> preds = mPlan.getPredecessors(proj);
            if(preds == null) return; // this is a leaf project and so not interesting for patching
            PhysicalOperator pred = preds.get(0);
            if(preds.size() == 1 && pred instanceof PODistinct) {
                if(patched) {
                    // we should not already have been patched since the
                    // Project-Distinct pair should occur only once
                    int errCode = 2076;
                    String msg = "Unexpected Project-Distinct pair while trying to set up plans for use with combiner.";
                    throw new OptimizerException(msg, errCode, PigException.BUG);
                }
                // we have stick in the POUserfunc(org.apache.pig.builtin.Distinct)[DataBag]
                // in place of the Project-PODistinct pair
                PhysicalOperator distinctPredecessor = mPlan.getPredecessors(pred).get(0);
                
                try {
                    String scope = proj.getOperatorKey().scope;
                    List<PhysicalOperator> funcInput = new ArrayList<PhysicalOperator>();
                    FuncSpec fSpec = new FuncSpec(DISTINCT_UDF_CLASSNAME);
                    funcInput.add(distinctPredecessor);
                    // explicitly set distinctPredecessor's result type to
                    // be tuple - this is relevant when distinctPredecessor is
                    // originally a POForeach with return type BAG - we need to
                    // set it to tuple so we get a stream of tuples. 
                    distinctPredecessor.setResultType(DataType.TUPLE);
                    POUserFunc func = new POUserFunc(new OperatorKey(scope, 
                            NodeIdGenerator.getGenerator().getNextNodeId(scope)),-1, funcInput, fSpec);
                    func.setResultType(DataType.BAG);
                    mPlan.replace(proj, func);
                    mPlan.remove(pred);
                    // connect the the newly added "func" to
                    // the predecessor to the earlier PODistinct
                    mPlan.connect(distinctPredecessor, func);
                } catch (PlanException e) {
                    int errCode = 2077;
                    String msg = "Problem with reconfiguring plan to add distinct built-in function.";
                    throw new OptimizerException(msg, errCode, PigException.BUG, e);
                }
                patched = true;
            } 
        }

    }
    
    private class fixMapProjects extends PhyPlanVisitor {

        public fixMapProjects(PhysicalPlan plan) {
            this(plan, new DepthFirstWalker<PhysicalOperator, PhysicalPlan>(
                    plan));
        }

        /**
         * @param plan
         * @param walker
         */
        public fixMapProjects(PhysicalPlan plan,
                PlanWalker<PhysicalOperator, PhysicalPlan> walker) {
            super(plan, walker);
        }

        /*
         * (non-Javadoc)
         * 
         * @see org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor#visitProject(org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POProject)
         */
        @Override
        public void visitProject(POProject proj) throws VisitorException {
            if (proj.getResultType() == DataType.BAG) {

                // IMPORTANT ASSUMPTION:
                // we should be calling this visitor only for
                // fixing up the projects in the map's foreach
                // inner plan. In the map side, we are dealing
                // with single tuple bags - so set the flag in
                // the project to use single tuple bags. If in
                // future we don't have single tuple bags in the
                // input to map's foreach, we should NOT be doing
                // this!
                proj.setResultSingleTupleBag(true);

            }
        }

    }

    // Reset any member variables since we may have already visited one
    // combine.
    private void resetState() {
        mKeyField = -1;
        mKeyType = 0;
    }
}
