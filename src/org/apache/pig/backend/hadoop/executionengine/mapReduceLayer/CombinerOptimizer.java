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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import org.apache.pig.PigException;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigWarning;
import org.apache.pig.data.DataType;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROpPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.ConstantExpression;
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
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPartialAgg;
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
import org.apache.pig.impl.util.Pair;

/**
 * Optimize map reduce plans to use the combiner where possible.
 * Algebriac functions and distinct in nested plan of a foreach are partially 
 * computed in the map and combine phase.
 * A new foreach statement with initial and intermediate forms of algebraic
 * functions are added to map and combine plans respectively. 
 * 
 * If bag portion of group-by result is projected or a non algebraic 
 * expression/udf has bag as input, combiner will not be used. This is because 
 * the use of combiner in such case is likely to degrade performance 
 * as there will not be much reduction in data size in combine stage to 
 * offset the cost of the additional number of times (de)serialization is done.
 * 
 * 
 * Major areas for enhancement:
 * 1. use of combiner in cogroup
 * 2. queries with order-by, limit or sort in a nested foreach after group-by
 * 3. case where group-by is followed by filter that has algebraic expression
 *
 * 
 *
 *
 */
public class CombinerOptimizer extends MROpPlanVisitor {

    private static final String DISTINCT_UDF_CLASSNAME = org.apache.pig.builtin.Distinct.class.getName();

    private Log log = LogFactory.getLog(getClass());


    private CompilationMessageCollector messageCollector = null;

    private boolean doMapAgg;

    public CombinerOptimizer(MROperPlan plan, boolean doMapAgg) {
        this(plan, doMapAgg, new CompilationMessageCollector());
    }

    public CombinerOptimizer(MROperPlan plan, boolean doMapAgg, 
            CompilationMessageCollector messageCollector) {

        super(plan, new DepthFirstWalker<MapReduceOper, MROperPlan>(plan));
        this.messageCollector = messageCollector;
        this.doMapAgg = doMapAgg;
    }

    public CompilationMessageCollector getMessageCollector() {
        return messageCollector;
    }

    @Override
    public void visitMROp(MapReduceOper mr) throws VisitorException {
        log.trace("Entering CombinerOptimizer.visitMROp");
        if (mr.reducePlan.isEmpty()) return;

        // part one - check if this MR job represents a group-by + foreach
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

        if (successor instanceof POLimit) {
            //POLimit is acceptable, as long has it has a single foreach
            // as successor
            List<PhysicalOperator> limitSucs =
                mr.reducePlan.getSuccessors(successor);
            if(limitSucs != null && limitSucs.size() == 1 && 
                    limitSucs.get(0) instanceof POForEach) {
                // the code below will now further examine
                // the foreach
                successor = limitSucs.get(0);
            }

        } 
        if (successor instanceof POForEach) {
            POForEach foreach = (POForEach)successor;
            List<PhysicalPlan> feInners = foreach.getInputPlans();

            // find algebraic operators and also check if the foreach statement
            // is suitable for combiner use
            List<Pair<PhysicalOperator, PhysicalPlan>> algebraicOps = 
                findAlgebraicOps(feInners);
            if(algebraicOps == null || algebraicOps.size() == 0){
                // the plan is not  combinable or there is nothing to combine
                //we're done
                return;
            }
            if (mr.combinePlan.getRoots().size() != 0) {
                messageCollector.collect("Wasn't expecting to find anything already "
                        + "in the combiner!", MessageType.Warning, PigWarning.NON_EMPTY_COMBINE_PLAN);
                return;
            }

            log.info("Choosing to move algebraic foreach to combiner");

            try {


                // replace PODistinct->Project[*] with distinct udf (which is Algebriac)
                for(Pair<PhysicalOperator, PhysicalPlan> op2plan : algebraicOps ){
                    if(! (op2plan.first instanceof PODistinct))
                        continue;
                    DistinctPatcher distinctPatcher = new DistinctPatcher(op2plan.second);
                    distinctPatcher.visit();
                    if(distinctPatcher.getDistinct() == null){
                        int errCode = 2073;
                        String msg = "Problem with replacing distinct operator with distinct built-in function.";
                        throw new PlanException(msg, errCode, PigException.BUG);
                    }
                    op2plan.first = distinctPatcher.getDistinct();
                }

                //create new map foreach
                POForEach mfe = createForEachWithGrpProj(foreach, rearrange.getKeyType());                
                Map<PhysicalOperator, Integer> op2newpos = 
                    new HashMap<PhysicalOperator, Integer>();
                Integer pos = 1;
                //create plan for each algebraic udf and add as inner plan in map-foreach 
                for(Pair<PhysicalOperator, PhysicalPlan> op2plan : algebraicOps ){
                    PhysicalPlan udfPlan = createPlanWithPredecessors(op2plan.first, op2plan.second);
                    mfe.addInputPlan(udfPlan, false);
                    op2newpos.put(op2plan.first, pos++);
                }
                changeFunc(mfe, POUserFunc.INITIAL);

                // since we will only be creating SingleTupleBag as input to
                // the map foreach, we should flag the POProjects in the map
                // foreach inner plans to also use SingleTupleBag
                for (PhysicalPlan mpl : mfe.getInputPlans()) {
                    try {
                        new fixMapProjects(mpl).visit();
                    } catch (VisitorException e) {
                        int errCode = 2089;
                        String msg = "Unable to flag project operator to use single tuple bag.";
                        throw new PlanException(msg, errCode, PigException.BUG, e);
                    }
                }

                //create new combine foreach
                POForEach cfe = createForEachWithGrpProj(foreach, rearrange.getKeyType());
                //add algebraic functions with appropriate projection
                addAlgebraicFuncToCombineFE(cfe, op2newpos);
                changeFunc(cfe, POUserFunc.INTERMEDIATE);

                //fix projection and function time for algebraic functions in reduce foreach
                for(Pair<PhysicalOperator, PhysicalPlan> op2plan : algebraicOps ){
                    setProjectInput(op2plan.first, op2plan.second, op2newpos.get(op2plan.first));
                    ((POUserFunc)op2plan.first).setAlgebraicFunction(POUserFunc.FINAL);
                }


                // we have modified the foreach inner plans - so set them
                // again for the foreach so that foreach can do any re-initialization
                // around them.
                // FIXME - this is a necessary evil right now because the leaves are explicitly
                // stored in the POForeach as a list rather than computed each time at 
                // run time from the plans for optimization. Do we want to have the Foreach
                // compute the leaves each time and have Java optimize it (will Java optimize?)?
                mfe.setInputPlans(mfe.getInputPlans());
                cfe.setInputPlans(cfe.getInputPlans());
                foreach.setInputPlans(foreach.getInputPlans());

                //tell POCombinerPackage which fields need projected and
                // which placed in bags. First field is simple project
                // rest need to go into bags
                int numFields = algebraicOps.size() + 1; // algebraic funcs + group key
                boolean[] bags = new boolean[numFields];
                bags[0] = false;
                for (int i = 1; i < numFields; i++) {
                    bags[i] = true;
                }

                // Use the POCombiner package in the combine plan
                // as it needs to act differently than the regular
                // package operator.
                mr.combinePlan = new PhysicalPlan();
                POCombinerPackage combinePack =
                    new POCombinerPackage(pack, bags);
                mr.combinePlan.add(combinePack);
                mr.combinePlan.add(cfe);
                mr.combinePlan.connect(combinePack, cfe);

                // No need to connect projections in cfe to cp, because
                // PigCombiner directly attaches output from package to
                // root of remaining plan.

                POLocalRearrange mlr = getNewRearrange(rearrange);

                POPartialAgg mapAgg = null;
                if(doMapAgg){
                    mapAgg = createPartialAgg(cfe);
                }

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
                patchUpMap(mr.mapPlan, getPreCombinerLR(rearrange), mfe, mapAgg, mlr);
                POLocalRearrange clr = getNewRearrange(rearrange);

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


    /**
     * Translate POForEach in combiner into a POPartialAgg
     * @param combineFE
     * @return partial aggregate operator
     * @throws CloneNotSupportedException 
     */
    private POPartialAgg createPartialAgg(POForEach combineFE)
            throws CloneNotSupportedException {
        String scope = combineFE.getOperatorKey().scope;
        POPartialAgg poAgg = new POPartialAgg(new OperatorKey(scope, 
                NodeIdGenerator.getGenerator().getNextNodeId(scope)));
        poAgg.addOriginalLocation(combineFE.getAlias(), combineFE.getOriginalLocations());
        poAgg.setResultType(combineFE.getResultType());

        //first plan in combine foreach is the group key
        poAgg.setKeyPlan(combineFE.getInputPlans().get(0).clone());

        List<PhysicalPlan> valuePlans = new ArrayList<PhysicalPlan>();
        for(int i=1; i<combineFE.getInputPlans().size(); i++){
            valuePlans.add(combineFE.getInputPlans().get(i).clone());
        }
        poAgg.setValuePlans(valuePlans);
        return poAgg;
    }

    /**
     * find algebraic operators and also check if the foreach statement
     *  is suitable for combiner use
     * @param feInners inner plans of foreach
     * @return null if plan is not combinable, otherwise list of combinable operators
     * @throws VisitorException
     */
    private List<Pair<PhysicalOperator, PhysicalPlan>> 
    findAlgebraicOps(List<PhysicalPlan> feInners)
    throws VisitorException {
        ArrayList<Pair<PhysicalOperator, PhysicalPlan>> algebraicOps = new ArrayList<Pair<PhysicalOperator, PhysicalPlan>>();

        //check each foreach inner plan
        for(PhysicalPlan pplan : feInners){
            //check for presence of non combinable operators
            AlgebraicPlanChecker algChecker = new AlgebraicPlanChecker(pplan);
            algChecker.visit();
            if(algChecker.sawNonAlgebraic){
                return null;
            }

            //if we found a combinable distinct add that to list
            if(algChecker.sawDistinctAgg){
                algebraicOps.add(new Pair<PhysicalOperator, PhysicalPlan>(algChecker.getDistinct(), pplan));
                continue;
            }


            List<PhysicalOperator> roots = pplan.getRoots();
            //combinable operators have to be attached to POProject root(s)  
            // if root does not have a successor that is combinable, the project 
            // has to be projecting the group column . Otherwise this MR job
            //is considered not combinable as we don't want to use combiner for
            // cases where this foreach statement is projecting bags (likely to 
            // bad for performance because of additional (de)serialization costs)

            for(PhysicalOperator root : roots){
                if(root instanceof ConstantExpression){
                    continue;
                }
                if(! (root  instanceof POProject)){
                    // how can this happen? - expect root of inner plan to be 
                    // constant or project.  not combining it
                    //TODO: Warn
                    return null;
                }
                POProject proj = (POProject)root;
                POUserFunc combineUdf = getAlgebraicSuccessor(proj, pplan);
                if(combineUdf == null){
                    
                    if(proj.isProjectToEnd()){
                        //project-star or project to end
                        // not combinable
                        return null;
                    }
                    
                    // Check to see if this is a projection of the grouping column.
                    // If so, it will be a projection of col 0 
                    List<Integer> cols = proj.getColumns();
                    if (cols != null && cols.size() == 1 && cols.get(0) == 0) {
                        //it is project of grouping column, so the plan is still
                        //combinable
                        continue;
                    }else{
                        //not combinable
                        return null;
                    }
                }

                // The algebraic udf can have more than one input. Add the udf only once
                boolean exist = false;
                for (Pair<PhysicalOperator, PhysicalPlan> pair : algebraicOps) {
                    if (pair.first.equals(combineUdf)) {
                        exist = true;
                        break;
                    }
                }
                if (!exist)
                    algebraicOps.add(new Pair<PhysicalOperator, PhysicalPlan>(combineUdf, pplan));
            }
        }

        return algebraicOps;
    }

    /**
     * Look for a algebraic POUserFunc as successor to this project, called
     * recursively to skip any other projects seen on the way.  
     * @param proj project
     * @param pplan physical plan
     * @return null if any operator other POProject or algebraic POUserFunc is
     * found while going down the plan, otherwise algebraic POUserFunc is returned
     */
    private POUserFunc getAlgebraicSuccessor(POProject proj, PhysicalPlan pplan) {
        //check if root is followed by combinable operator
        List<PhysicalOperator> succs = pplan.getSuccessors(proj);
        if(succs == null || succs.size() == 0){
            return null;
        }
        if(succs.size() > 1){
            //project shared by more than one operator - does not happen 
            // in plans generated today
            // won't try to combine this
            return null;
        }


        PhysicalOperator succ = succs.get(0);
        if(succ instanceof POProject){
            return getAlgebraicSuccessor((POProject) succ, pplan);
        }

        if(succ instanceof POUserFunc && ((POUserFunc)succ).combinable() ){
            return (POUserFunc)succ;
        }

        //some other operator ? can't combine
        return null;
    }
    

    /**
     * Create a new foreach with same scope,alias as given foreach
     * add an inner plan that projects the group column, which is going to be
     * the first input
     * @param foreach source foreach
     * @param keyType type for group-by key
     * @return new POForeach
     */
    private POForEach createForEachWithGrpProj(POForEach foreach, byte keyType) {
        String scope = foreach.getOperatorKey().scope;
        POForEach newFE = new POForEach(createOperatorKey(scope), new ArrayList<PhysicalPlan>());
        newFE.addOriginalLocation(foreach.getAlias(), foreach.getOriginalLocations());
        newFE.setResultType(foreach.getResultType());
        //create plan that projects the group column 
        PhysicalPlan grpProjPlan = new PhysicalPlan();
        //group by column is the first column
        POProject proj = new POProject(createOperatorKey(scope), 1, 0);
        proj.setResultType(keyType);
        grpProjPlan.add(proj);

        newFE.addInputPlan(grpProjPlan, false);
        return newFE;
    }
    
    /**
     * Create new plan and  add to it the clones of operator algeOp  and its 
     * predecessors from the physical plan pplan .
     * @param algeOp algebraic operator 
     * @param pplan physical plan that has algeOp
     * @return new plan
     * @throws CloneNotSupportedException
     * @throws PlanException
     */
    private PhysicalPlan createPlanWithPredecessors(PhysicalOperator algeOp, PhysicalPlan pplan)
    throws CloneNotSupportedException, PlanException {
        PhysicalPlan newplan = new PhysicalPlan();
        addPredecessorsToPlan(algeOp, pplan, newplan);
        return newplan;
    }

    /**
     * Recursively clone op and its predecessors from pplan and add them to newplan
     * @param op
     * @param pplan
     * @param newplan
     * @return
     * @throws CloneNotSupportedException
     * @throws PlanException
     */
    private PhysicalOperator addPredecessorsToPlan(PhysicalOperator op, PhysicalPlan pplan,
            PhysicalPlan newplan)
    throws CloneNotSupportedException, PlanException {
        PhysicalOperator newOp = op.clone();
        newplan.add(newOp);
        if(pplan.getPredecessors(op) == null || pplan.getPredecessors(op).size() == 0){
            return newOp;
        }        
        for(PhysicalOperator pred : pplan.getPredecessors(op)){
            PhysicalOperator newPred = addPredecessorsToPlan(pred, pplan, newplan);
            newplan.connect(newPred, newOp);
        }
        return newOp;
    }
    



    /**
     * add algebraic functions with appropriate projection to new foreach in combiner
     * @param cfe - the new foreach in combiner 
     * @param op2newpos - mapping of physical operator to position in input
     * @throws CloneNotSupportedException
     * @throws PlanException
     */
    private void addAlgebraicFuncToCombineFE(POForEach cfe, Map<PhysicalOperator, Integer> op2newpos)
    throws CloneNotSupportedException, PlanException {

        //an array that we will first populate with physical operators in order 
        //of their position in input. Used while adding plans to combine foreach
        // just so that output of combine foreach same positions as input. That
        // means the same operator to position mapping can be used by reduce as well
        PhysicalOperator[] opsInOrder = new PhysicalOperator[op2newpos.size() + 1];
        for(Map.Entry<PhysicalOperator, Integer> op2pos : op2newpos.entrySet()){
            opsInOrder[op2pos.getValue()] = op2pos.getKey();
        }

        // first position is used by group column and a plan has been added for it,
        //so start with 1
        for(int i=1; i < opsInOrder.length; i++){
            //create new inner plan for foreach
            //add cloned copy of given physical operator and a new project.
            // Even if the udf in query takes multiple input, only one project
            // needs to be added because input to this udf
            //will be the INITIAL version of udf evaluated in map. 
            PhysicalPlan newPlan = new PhysicalPlan();
            PhysicalOperator newOp = opsInOrder[i].clone();
            newPlan.add(newOp);
            POProject proj = new POProject(
                    createOperatorKey(cfe.getOperatorKey().getScope()),
                    1, i
            );
            proj.setResultType(DataType.BAG);
            newPlan.add(proj);
            newPlan.connect(proj, newOp);
            cfe.addInputPlan(newPlan, false);
        }
    }

    /**
     * Replace old POLocalRearrange with new pre-combine LR,
     * add new map foreach, new map-local-rearrange, and connect them
     * 
     * @param mapPlan
     * @param preCombinerLR
     * @param mfe
     * @param mapAgg 
     * @param mlr
     * @throws PlanException 
     */
    private void patchUpMap(PhysicalPlan mapPlan, POPreCombinerLocalRearrange preCombinerLR,
            POForEach mfe, POPartialAgg mapAgg, POLocalRearrange mlr)
                    throws PlanException {

        POLocalRearrange oldLR = (POLocalRearrange)mapPlan.getLeaves().get(0);
        mapPlan.replace(oldLR, preCombinerLR);

        mapPlan.add(mfe);
        mapPlan.connect(preCombinerLR, mfe);

        //the operator before local rearrange
        PhysicalOperator opBeforeLR = mfe;

        if(mapAgg != null){
            mapPlan.add(mapAgg);
            mapPlan.connect(mfe, mapAgg);
            opBeforeLR = mapAgg;
        }

        mapPlan.add(mlr);
        mapPlan.connect(opBeforeLR, mlr);
    }

    /**
     * @param rearrange
     * @return
     */
    private POPreCombinerLocalRearrange getPreCombinerLR(POLocalRearrange rearrange) {

        String scope = rearrange.getOperatorKey().scope;
        POPreCombinerLocalRearrange pclr = new POPreCombinerLocalRearrange(
                createOperatorKey(scope),
                rearrange.getRequestedParallelism(), rearrange.getInputs());
        pclr.setPlans(rearrange.getPlans());
        return pclr;
    }

    private OperatorKey createOperatorKey(String scope) {
        return new OperatorKey(scope, NodeIdGenerator.getGenerator().getNextNodeId(scope));
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
     * Change the algebriac function type for algebraic functions in map and combine
     * In map and combine the algebraic functions will be leaf of the plan
     * @param fe
     * @param type
     * @throws PlanException
     */
    private void changeFunc(POForEach fe, byte type) throws PlanException {
        for(PhysicalPlan plan : fe.getInputPlans()){
            List<PhysicalOperator> leaves = plan.getLeaves();
            if (leaves == null || leaves.size() != 1) {
                int errCode = 2019;
                String msg = "Expected to find plan with single leaf. Found " + leaves.size() + " leaves.";
                throw new PlanException(msg, errCode, PigException.BUG);
            }

            PhysicalOperator leaf = leaves.get(0);
            if(leaf instanceof POProject){
                continue;
            }
            if (!(leaf instanceof POUserFunc)) {
                int errCode = 2020;
                String msg = "Expected to find plan with UDF or project leaf. Found " + leaf.getClass().getSimpleName();
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
    }


    /**
     * create new Local rearrange by cloning existing rearrange and 
     * add plan for projecting the key
     * @param rearrange
     * @return
     * @throws PlanException
     * @throws CloneNotSupportedException
     */
    private POLocalRearrange getNewRearrange(POLocalRearrange rearrange)
    throws PlanException, CloneNotSupportedException {
        
        POLocalRearrange newRearrange = rearrange.clone();
        
        // Set the projection to be the key
        PhysicalPlan newPlan = new PhysicalPlan();
        String scope = newRearrange.getOperatorKey().scope;
        POProject proj = new POProject(new OperatorKey(scope, 
                NodeIdGenerator.getGenerator().getNextNodeId(scope)), -1, 0);
        proj.setResultType(newRearrange.getKeyType());
        newPlan.add(proj);
        
        List<PhysicalPlan> plans = new ArrayList<PhysicalPlan>(1);
        plans.add(newPlan);
        newRearrange.setPlansFromCombiner(plans);
        
        return newRearrange;
    }

    /**
     * Checks if there is something that prevents the use of algebraic interface,
     * and looks for the PODistinct that can be used as algebraic
     * 
     */
    private static class AlgebraicPlanChecker extends PhyPlanVisitor {
        boolean sawNonAlgebraic = false;
        boolean sawDistinctAgg = false;
        private boolean sawForeach = false;
        private PODistinct distinct = null;


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
            this.distinct = distinct;
            if(sawDistinctAgg) {
                // we want to combine only in the case where there is only
                // one PODistinct which is the only input to an agg
                // we apparently have seen a PODistinct before, so lets not
                // combine.
                sawNonAlgebraic = true;
                return;
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

                // we want to combine only in the case where there is only
                // one PODistinct which is the only input to an agg.
                // Do not combine if there are additional inputs.
                List<PhysicalOperator> preds = mPlan.getPredecessors(leaf);
                if (preds.size() > 1) {
                    sawNonAlgebraic = true;
                    return;
                }

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

        /**
         * @return the distinct
         */
        public PODistinct getDistinct() {
            if(sawNonAlgebraic)
                return null;
            return distinct;
        }

        @Override
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
    private static class DistinctPatcher extends PhyPlanVisitor {

        private POUserFunc distinct = null;
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
                if(distinct != null) {
                    // we should not already have been patched since the
                    // Project-Distinct pair should occur only once
                    int errCode = 2076;
                    String msg = "Unexpected Project-Distinct pair while trying to set up plans for use with combiner.";
                    throw new OptimizerException(msg, errCode, PigException.BUG);
                }
                // we have stick in the POUserfunc(org.apache.pig.builtin.Distinct)[DataBag]
                // in place of the Project-PODistinct pair
                PhysicalOperator distinctPredecessor = mPlan.getPredecessors(pred).get(0);

                POUserFunc func = null;

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
                    func = new POUserFunc(new OperatorKey(scope, 
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
                distinct = func;
            } 
        }

        POUserFunc getDistinct(){
            return distinct;
        }


    }

    private static class fixMapProjects extends PhyPlanVisitor {

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

}
