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

import org.apache.pig.impl.logicalLayer.parser.ParseException;
import org.apache.pig.impl.physicalLayer.topLevelOperators.POFilter;
import org.apache.pig.impl.physicalLayer.topLevelOperators.POGenerate;
//import org.apache.pig.impl.physicalLayer.topLevelOperators.POGenerate;
//import org.apache.pig.impl.physicalLayer.topLevelOperators.POGlobalRearrange;
//import org.apache.pig.impl.physicalLayer.topLevelOperators.POLoad;
//import org.apache.pig.impl.physicalLayer.topLevelOperators.POLocalRearrange;
//import org.apache.pig.impl.physicalLayer.topLevelOperators.POPackage;
//import org.apache.pig.impl.physicalLayer.topLevelOperators.POStore;
import org.apache.pig.impl.physicalLayer.topLevelOperators.PhysicalOperator;
//import org.apache.pig.impl.physicalLayer.topLevelOperators.StartMap;
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
public abstract class PhyPlanVisitor<O extends PhysicalOperator, P extends PhysicalPlan<O>> extends PlanVisitor<O,P> {

    public PhyPlanVisitor(P plan) {
        super(plan);
    }

    @Override
    public void visit() throws ParseException {
        depthFirst();
    }
    
//    public void visitLoad(POLoad ld){
//        //do nothing
//    }
//    
//    public void visitStore(POStore st){
//        //do nothing
//    }
//    
    public void visitFilter(POFilter fl) throws ParseException{
        ExprPlanVisitor epv = new ExprPlanVisitor(fl.getPlan());
        epv.visit();
    }
//    
//    public void visitLocalRearrange(POLocalRearrange lr){
//        //do nothing
//    }
//    
//    public void visitGlobalRearrange(POGlobalRearrange gr){
//        //do nothing
//    }
//    
//    public void visitStartMap(StartMap sm){
//        //do nothing
//    }
//    
//    public void visitPackage(POPackage pkg){
//        //do nothing
//    }
    
    public void visitGenerate(POGenerate pogen) {
        //do nothing
    }

}
