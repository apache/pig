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
package org.apache.pig.impl.physicalLayer.plans;

import java.util.List;

import org.apache.pig.impl.physicalLayer.PhysicalOperator;
import org.apache.pig.impl.physicalLayer.expressionOperators.Add;
import org.apache.pig.impl.physicalLayer.expressionOperators.ConstantExpression;
import org.apache.pig.impl.physicalLayer.expressionOperators.Divide;
import org.apache.pig.impl.physicalLayer.expressionOperators.EqualToExpr;
import org.apache.pig.impl.physicalLayer.expressionOperators.GTOrEqualToExpr;
import org.apache.pig.impl.physicalLayer.expressionOperators.GreaterThanExpr;
import org.apache.pig.impl.physicalLayer.expressionOperators.LTOrEqualToExpr;
import org.apache.pig.impl.physicalLayer.expressionOperators.LessThanExpr;
import org.apache.pig.impl.physicalLayer.expressionOperators.Mod;
import org.apache.pig.impl.physicalLayer.expressionOperators.Multiply;
import org.apache.pig.impl.physicalLayer.expressionOperators.NotEqualToExpr;
import org.apache.pig.impl.physicalLayer.expressionOperators.POAnd;
import org.apache.pig.impl.physicalLayer.expressionOperators.POBinCond;
import org.apache.pig.impl.physicalLayer.expressionOperators.POCast;
import org.apache.pig.impl.physicalLayer.expressionOperators.POMapLookUp;
import org.apache.pig.impl.physicalLayer.expressionOperators.PONegative;
import org.apache.pig.impl.physicalLayer.expressionOperators.PONot;
import org.apache.pig.impl.physicalLayer.expressionOperators.POOr;
import org.apache.pig.impl.physicalLayer.expressionOperators.POProject;
import org.apache.pig.impl.physicalLayer.expressionOperators.PORegexp;
import org.apache.pig.impl.physicalLayer.expressionOperators.POUserFunc;
import org.apache.pig.impl.physicalLayer.expressionOperators.Subtract;
import org.apache.pig.impl.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.impl.physicalLayer.relationalOperators.PODistinct;
import org.apache.pig.impl.physicalLayer.relationalOperators.POFilter;
import org.apache.pig.impl.physicalLayer.relationalOperators.POGlobalRearrange;
import org.apache.pig.impl.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.impl.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.impl.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.impl.physicalLayer.relationalOperators.PORead;
import org.apache.pig.impl.physicalLayer.relationalOperators.POSort;
import org.apache.pig.impl.physicalLayer.relationalOperators.POSplit;
import org.apache.pig.impl.physicalLayer.relationalOperators.POStore;
import org.apache.pig.impl.physicalLayer.relationalOperators.POUnion;
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
    
    public void visitFilter(POFilter fl) throws VisitorException{
        pushWalker(mCurrentWalker.spawnChildWalker(fl.getPlan()));
        visit();
        popWalker();
    }
    
    public void visitLocalRearrange(POLocalRearrange lr) throws VisitorException{
        List<PhysicalPlan> inpPlans = lr.getPlans();
        for (PhysicalPlan plan : inpPlans) {
            pushWalker(mCurrentWalker.spawnChildWalker(plan));
            visit();
        }
    }
    
    public void visitGlobalRearrange(POGlobalRearrange gr) throws VisitorException{
        //do nothing
    }
    
    public void visitPackage(POPackage pkg) throws VisitorException{
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
        //do nothing
    }

	public void visitDistinct(PODistinct distinct) throws VisitorException {
        //do nothing		
	}

	public void visitRead(PORead read) throws VisitorException {
        //do nothing		
	}

	public void visitSort(POSort sort) throws VisitorException {
        List<PhysicalPlan> inpPlans = sort.getSortPlans();
        for (PhysicalPlan plan : inpPlans) {
            pushWalker(mCurrentWalker.spawnChildWalker(plan));
            visit();
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

    public void visitMapLookUp(POMapLookUp mapLookUp) {
        // TODO Auto-generated method stub
        
    }

    public void visitCast(POCast cast) {
        // TODO Auto-generated method stub
        
    }
}
