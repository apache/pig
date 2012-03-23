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

import java.util.List;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.*;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.*;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.VisitorException;

/**
 * Sets the parent plan for all Physical Operators. Note: parentPlan
 * is a bit of a misnomer. We actually want all the operators to point
 * to the same plan - not necessarily the one they're a member of.
 */
public class PhyPlanSetter extends PhyPlanVisitor {
    PhysicalPlan parent;

    public PhyPlanSetter(PhysicalPlan plan) {
        super(plan, new DependencyOrderWalker<PhysicalOperator, PhysicalPlan>(plan));
        parent = plan;
    }

    @Override
    public void visitLoad(POLoad ld) throws VisitorException{
        ld.setParentPlan(parent);
    }
    
    @Override
    public void visitNative(PONative nt) throws VisitorException{
        nt.setParentPlan(parent);
    }
 
    @Override
    public void visitStore(POStore st) throws VisitorException{
        st.setParentPlan(parent);
    }
    
    @Override
    public void visitFilter(POFilter fl) throws VisitorException{
        super.visitFilter(fl);
        fl.setParentPlan(parent);
    }
    
    @Override
    public void visitLocalRearrange(POLocalRearrange lr) throws VisitorException{
        super.visitLocalRearrange(lr);
        lr.setParentPlan(parent);
    }

    @Override
    public void visitCollectedGroup(POCollectedGroup mg) throws VisitorException{
        super.visitCollectedGroup(mg);
        mg.setParentPlan(parent);
    }

    @Override
    public void visitGlobalRearrange(POGlobalRearrange gr) throws VisitorException{
        gr.setParentPlan(parent);
    }
    
    @Override
    public void visitPackage(POPackage pkg) throws VisitorException{
        pkg.setParentPlan(parent);
    }
    
    @Override
    public void visitCombinerPackage(POCombinerPackage pkg) throws VisitorException{
        pkg.setParentPlan(parent);
    }
    
    @Override
    public void visitPOForEach(POForEach nfe) throws VisitorException {
        super.visitPOForEach(nfe);
        nfe.setParentPlan(parent);
    }
    
    @Override
    public void visitUnion(POUnion un) throws VisitorException{
        un.setParentPlan(parent);
    }
    
    @Override
    public void visitSplit(POSplit spl) throws VisitorException{
        PhysicalPlan oldPlan = parent;
        List<PhysicalPlan> plans = spl.getPlans();
        for (PhysicalPlan plan : plans) {
            parent = plan;
            pushWalker(mCurrentWalker.spawnChildWalker(plan));
            visit();
            popWalker();
        }
        parent=oldPlan;
        spl.setParentPlan(parent);
    }
    
    @Override
    public void visitDemux(PODemux demux) throws VisitorException{
        super.visitDemux(demux);
        demux.setParentPlan(parent);
    }

    @Override
    public void visitDistinct(PODistinct distinct) throws VisitorException {
        distinct.setParentPlan(parent);		
    }
    
    @Override
    public void visitSort(POSort sort) throws VisitorException {
        super.visitSort(sort);
        sort.setParentPlan(parent);
    }
    
    @Override
    public void visitConstant(ConstantExpression cnst) throws VisitorException{
        cnst.setParentPlan(parent);
    }
    
    @Override
    public void visitProject(POProject proj) throws VisitorException{
        proj.setParentPlan(parent);
    }
    
    @Override
    public void visitGreaterThan(GreaterThanExpr grt) throws VisitorException{
        grt.setParentPlan(parent);
    }
    
    @Override
    public void visitLessThan(LessThanExpr lt) throws VisitorException{
        lt.setParentPlan(parent);
    }
    
    @Override
    public void visitGTOrEqual(GTOrEqualToExpr gte) throws VisitorException{
        gte.setParentPlan(parent);
    }
    
    @Override
    public void visitLTOrEqual(LTOrEqualToExpr lte) throws VisitorException{
        lte.setParentPlan(parent);
    }
    
    @Override
    public void visitEqualTo(EqualToExpr eq) throws VisitorException{
        eq.setParentPlan(parent);
    }
    
    @Override
    public void visitNotEqualTo(NotEqualToExpr eq) throws VisitorException{
        eq.setParentPlan(parent);
    }
    
    @Override
    public void visitRegexp(PORegexp re) throws VisitorException{
        re.setParentPlan(parent);
    }

    @Override
    public void visitIsNull(POIsNull isNull) throws VisitorException {
        isNull.setParentPlan(parent);
    }
    
    @Override
    public void visitAdd(Add add) throws VisitorException{
        add.setParentPlan(parent);
    }
    
    @Override
    public void visitSubtract(Subtract sub) throws VisitorException {
        sub.setParentPlan(parent);
    }
    
    @Override
    public void visitMultiply(Multiply mul) throws VisitorException {
        mul.setParentPlan(parent);
    }
    
    @Override
    public void visitDivide(Divide dv) throws VisitorException {
        dv.setParentPlan(parent);
    }
    
    @Override
    public void visitMod(Mod mod) throws VisitorException {
        mod.setParentPlan(parent);
    }
    
    @Override
    public void visitAnd(POAnd and) throws VisitorException {
        and.setParentPlan(parent);
    }

    @Override
    public void visitOr(POOr or) throws VisitorException {
        or.setParentPlan(parent);
    }

    @Override
    public void visitNot(PONot not) throws VisitorException {
        not.setParentPlan(parent);
    }

    @Override
    public void visitBinCond(POBinCond binCond) {
        binCond.setParentPlan(parent);
    }

    @Override
    public void visitNegative(PONegative negative) {
        negative.setParentPlan(parent);
    }
    
    @Override
    public void visitUserFunc(POUserFunc userFunc) throws VisitorException {
        userFunc.setParentPlan(parent);
    }
    
    @Override
    public void visitComparisonFunc(POUserComparisonFunc compFunc) throws VisitorException {
        compFunc.setParentPlan(parent);
    }

    @Override
    public void visitMapLookUp(POMapLookUp mapLookUp) {
        mapLookUp.setParentPlan(parent);
    }
    
    @Override
    public void visitJoinPackage(POJoinPackage joinPackage) throws VisitorException{
        joinPackage.setParentPlan(parent);
    }

    @Override
    public void visitCast(POCast cast) {
        cast.setParentPlan(parent);
    }
    
    @Override
    public void visitLimit(POLimit lim) throws VisitorException{
        lim.setParentPlan(parent);
    }
    
    @Override
    public void visitFRJoin(POFRJoin join) throws VisitorException {
        join.setParentPlan(parent);
    }
    
    @Override
    public void visitMergeJoin(POMergeJoin join) throws VisitorException {
        join.setParentPlan(parent);
    }

    @Override
    public void visitSkewedJoin(POSkewedJoin join) throws VisitorException {
        join.setParentPlan(parent);
    }   

    @Override
    public void visitStream(POStream stream) throws VisitorException {
        stream.setParentPlan(parent);
    }

/*
    @Override
    public void visitPartitionRearrange(POPartitionRearrange lrfi) throws VisitorException {
        super.visitPartitionRearrange(lrfi);
        lrfi.setParentPlan(parent);
    }
*/

    @Override
    public void visitPartialAgg(POPartialAgg poPartialAgg) {
       poPartialAgg.setParentPlan(parent);
    }

    @Override
    public void visitPOOptimizedForEach(POOptimizedForEach optimizedForEach) {
        optimizedForEach.setParentPlan(parent);
    }

    @Override
    public void visitPreCombinerLocalRearrange(
            POPreCombinerLocalRearrange preCombinerLocalRearrange) {
        preCombinerLocalRearrange.setParentPlan(parent);
    }


    @Override
    public void visitMergeCoGroup(POMergeCogroup mergeCoGrp)
            throws VisitorException {
        mergeCoGrp.setParentPlan(parent);
    }
}
