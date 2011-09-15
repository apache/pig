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
package org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans;

import java.util.List;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.*;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.*;
import org.apache.pig.impl.plan.PlanVisitor;
import org.apache.pig.impl.plan.PlanWalker;
import org.apache.pig.impl.plan.VisitorException;

/**
 * The visitor class for the Physical Plan. To use this,
 * create the visitor with the plan to be visited. Call 
 * the visit() method to traverse the plan in a depth first
 * fashion.
 * 
 * This class also visits the nested plans inside the operators.
 * One has to extend this class to modify the nature of each visit
 * and to maintain any relevant state information between the visits
 * to two different operators.
 *
 */
public class PhyPlanVisitor extends PlanVisitor<PhysicalOperator,PhysicalPlan> {

    public PhyPlanVisitor(PhysicalPlan plan, PlanWalker<PhysicalOperator, PhysicalPlan> walker) {
        super(plan, walker);
    }

    public void visitLoad(POLoad ld) throws VisitorException{
        //do nothing
    }
    
    public void visitStore(POStore st) throws VisitorException{
        //do nothing
    }
    
    public void visitNative(PONative nat) throws VisitorException{
        //do nothing
    }
    
    public void visitFilter(POFilter fl) throws VisitorException{
        pushWalker(mCurrentWalker.spawnChildWalker(fl.getPlan()));
        visit();
        popWalker();
    }


    public void visitCollectedGroup(POCollectedGroup mg) throws VisitorException{
        List<PhysicalPlan> inpPlans = mg.getPlans();
        for (PhysicalPlan plan : inpPlans) {
            pushWalker(mCurrentWalker.spawnChildWalker(plan));
            visit();
            popWalker();
        }
    }
    
    public void visitLocalRearrange(POLocalRearrange lr) throws VisitorException{
        List<PhysicalPlan> inpPlans = lr.getPlans();
        for (PhysicalPlan plan : inpPlans) {
            pushWalker(mCurrentWalker.spawnChildWalker(plan));
            visit();
            popWalker();
        }
    }

    public void visitGlobalRearrange(POGlobalRearrange gr) throws VisitorException{
        //do nothing
    }
    
    public void visitPackage(POPackage pkg) throws VisitorException{
        //do nothing
    }
    
    public void visitCombinerPackage(POCombinerPackage pkg) throws VisitorException{
        //do nothing
    }
 
    public void visitMultiQueryPackage(POMultiQueryPackage pkg) throws VisitorException{
        //do nothing
    }
    
    public void visitPOForEach(POForEach nfe) throws VisitorException {
        List<PhysicalPlan> inpPlans = nfe.getInputPlans();
        for (PhysicalPlan plan : inpPlans) {
            pushWalker(mCurrentWalker.spawnChildWalker(plan));
            visit();
            popWalker();
        }
    }
    
    public void visitUnion(POUnion un) throws VisitorException{
        //do nothing
    }
    
    public void visitSplit(POSplit spl) throws VisitorException{
        List<PhysicalPlan> plans = spl.getPlans();
        for (PhysicalPlan plan : plans) {
            pushWalker(mCurrentWalker.spawnChildWalker(plan));
            visit();
            popWalker();
        }
    }

    public void visitDemux(PODemux demux) throws VisitorException{
        List<PhysicalPlan> plans = demux.getPlans();
        for (PhysicalPlan plan : plans) {
            pushWalker(mCurrentWalker.spawnChildWalker(plan));
            visit();
            popWalker();
        }
    }
    
	public void visitDistinct(PODistinct distinct) throws VisitorException {
        //do nothing		
	}

	public void visitSort(POSort sort) throws VisitorException {
        List<PhysicalPlan> inpPlans = sort.getSortPlans();
        for (PhysicalPlan plan : inpPlans) {
            pushWalker(mCurrentWalker.spawnChildWalker(plan));
            visit();
            popWalker();
        }
	}
    
    public void visitConstant(ConstantExpression cnst) throws VisitorException{
        //do nothing
    }
    
    public void visitProject(POProject proj) throws VisitorException{
        //do nothing
    }
    
    public void visitGreaterThan(GreaterThanExpr grt) throws VisitorException{
        //do nothing
    }
    
    public void visitLessThan(LessThanExpr lt) throws VisitorException{
        //do nothing
    }
    
    public void visitGTOrEqual(GTOrEqualToExpr gte) throws VisitorException{
        //do nothing
    }
    
    public void visitLTOrEqual(LTOrEqualToExpr lte) throws VisitorException{
        //do nothing
    }
    
    public void visitEqualTo(EqualToExpr eq) throws VisitorException{
        //do nothing
    }
    
    public void visitNotEqualTo(NotEqualToExpr eq) throws VisitorException{
        //do nothing
    }
    
    public void visitRegexp(PORegexp re) throws VisitorException{
        //do nothing
    }

    public void visitIsNull(POIsNull isNull) throws VisitorException {
    }
    
    public void visitAdd(Add add) throws VisitorException{
        //do nothing
    }
    
    public void visitSubtract(Subtract sub) throws VisitorException {
        //do nothing
    }
    
    public void visitMultiply(Multiply mul) throws VisitorException {
        //do nothing
    }
    
    public void visitDivide(Divide dv) throws VisitorException {
        //do nothing
    }
    
    public void visitMod(Mod mod) throws VisitorException {
        //do nothing
    }
    
    public void visitAnd(POAnd and) throws VisitorException {
        //do nothing
    }

    public void visitOr(POOr or) throws VisitorException {
        //do nothing
    }

    public void visitNot(PONot not) throws VisitorException {
        //do nothing
    }

    public void visitBinCond(POBinCond binCond) {
        // do nothing
        
    }

    public void visitNegative(PONegative negative) {
        //do nothing
        
    }
    
    public void visitUserFunc(POUserFunc userFunc) throws VisitorException {
        //do nothing
    }
    
    public void visitComparisonFunc(POUserComparisonFunc compFunc) throws VisitorException {
        //do nothing
    }

    public void visitMapLookUp(POMapLookUp mapLookUp) {
        // TODO Auto-generated method stub
        
    }
    
    public void visitJoinPackage(POJoinPackage joinPackage) throws VisitorException{
        //do nothing
    }

    public void visitCast(POCast cast) {
        // TODO Auto-generated method stub
        
    }
    
    public void visitLimit(POLimit lim) throws VisitorException{
        //do nothing
    }
    
    public void visitCross(POCross cross) throws VisitorException{
        //do nothing
    }
    
    public void visitFRJoin(POFRJoin join) throws VisitorException {
        //do nothing
    }
    
    public void visitMergeJoin(POMergeJoin join) throws VisitorException {
        //do nothing
    }
    
    public void visitMergeCoGroup(POMergeCogroup mergeCoGrp) throws VisitorException{
        
    }
    /**
     * @param stream
     * @throws VisitorException 
     */
    public void visitStream(POStream stream) throws VisitorException {
        // TODO Auto-generated method stub
        
    }

	public void visitSkewedJoin(POSkewedJoin sk) throws VisitorException {

	}

	public void visitPartitionRearrange(POPartitionRearrange pr) throws VisitorException {
        List<PhysicalPlan> inpPlans = pr.getPlans();
        for (PhysicalPlan plan : inpPlans) {
            pushWalker(mCurrentWalker.spawnChildWalker(plan));
            visit();
            popWalker();
        }
	}

    /**
     * @param optimizedForEach
     */
    public void visitPOOptimizedForEach(POOptimizedForEach optimizedForEach) throws VisitorException {
        // TODO Auto-generated method stub
        
    }

    /**
     * @param preCombinerLocalRearrange
     */
    public void visitPreCombinerLocalRearrange(
            POPreCombinerLocalRearrange preCombinerLocalRearrange) {
        // TODO Auto-generated method stub
    }

    public void visitPartialAgg(POPartialAgg poPartialAgg) {
    }


}
