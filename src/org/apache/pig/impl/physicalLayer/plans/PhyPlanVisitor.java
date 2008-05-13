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

import org.apache.pig.impl.physicalLayer.topLevelOperators.POFilter;
import org.apache.pig.impl.physicalLayer.topLevelOperators.POForEach;
import org.apache.pig.impl.physicalLayer.topLevelOperators.POGenerate;
import org.apache.pig.impl.physicalLayer.topLevelOperators.POGlobalRearrange;
import org.apache.pig.impl.physicalLayer.topLevelOperators.POLoad;
import org.apache.pig.impl.physicalLayer.topLevelOperators.POLocalRearrange;
import org.apache.pig.impl.physicalLayer.topLevelOperators.POPackage;
import org.apache.pig.impl.physicalLayer.topLevelOperators.POSplit;
import org.apache.pig.impl.physicalLayer.topLevelOperators.PORead;
import org.apache.pig.impl.physicalLayer.topLevelOperators.POSort;
import org.apache.pig.impl.physicalLayer.topLevelOperators.PODistinct;
import org.apache.pig.impl.physicalLayer.topLevelOperators.POStore;
import org.apache.pig.impl.physicalLayer.topLevelOperators.POUnion;
import org.apache.pig.impl.physicalLayer.topLevelOperators.POUserFunc;
import org.apache.pig.impl.physicalLayer.topLevelOperators.PhysicalOperator;
import org.apache.pig.impl.physicalLayer.topLevelOperators.POUnion;
import org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators.ExpressionOperator;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.PlanWalker;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.plan.PlanVisitor;

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
 * @param <O>
 * @param <P>
 */
public class PhyPlanVisitor<O extends PhysicalOperator, P extends PhysicalPlan<O>> extends PlanVisitor<O,P> {

    public PhyPlanVisitor(P plan, PlanWalker<O, P> walker) {
        super(plan, walker);
    }

    public void visitLoad(POLoad ld) throws VisitorException{
        //do nothing
    }
 
    public void visitStore(POStore st) throws VisitorException{
        //do nothing
    }
    
    public void visitFilter(POFilter fl) throws VisitorException{
        ExprPlanVisitor epv = new ExprPlanVisitor(fl.getPlan(),
            new DepthFirstWalker<ExpressionOperator, ExprPlan>(fl.getPlan()));
        epv.visit();
    }
    
    public void visitLocalRearrange(POLocalRearrange lr) throws VisitorException{
        pushWalker(mCurrentWalker.spawnChildWalker((P)lr.getPlan()));
        // this causes the current walker (the new one we created)
        // to walk the nested plan
        visit();
        popWalker();
    }
    
    public void visitForEach(POForEach fe) throws VisitorException{
        pushWalker(mCurrentWalker.spawnChildWalker((P)fe.getPlan()));
        // this causes the current walker (the new one we created)
        // to walk the nested plan
        visit();
        popWalker();
    }
    
    public void visitGlobalRearrange(POGlobalRearrange gr) throws VisitorException{
        //do nothing
    }
    
    public void visitPackage(POPackage pkg) throws VisitorException{
        //do nothing
    }
    
    public void visitGenerate(POGenerate pogen) throws VisitorException {
        List<ExprPlan> inpPlans = pogen.getInputPlans();
        for (ExprPlan plan : inpPlans) {
            ExprPlanVisitor epv = new ExprPlanVisitor(plan,new DependencyOrderWalker<ExpressionOperator, ExprPlan>(plan));
            epv.visit();
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
        List<ExprPlan> inpPlans = sort.getSortPlans();
        for (ExprPlan plan : inpPlans) {
            ExprPlanVisitor epv = new ExprPlanVisitor(plan,new DependencyOrderWalker<ExpressionOperator, ExprPlan>(plan));
            epv.visit();
        }
	}

	public void visitUserFunc(POUserFunc userFunc) throws VisitorException {
	    //do nothing
	}

}
