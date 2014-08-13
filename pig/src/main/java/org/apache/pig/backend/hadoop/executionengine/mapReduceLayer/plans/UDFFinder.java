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
package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans;

import java.util.ArrayList;
import java.util.List;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POCast;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POUserComparisonFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POUserFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFilter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSort;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.PlanWalker;
import org.apache.pig.impl.plan.VisitorException;

public class UDFFinder extends PhyPlanVisitor {
    List<String> UDFs;
    DepthFirstWalker<PhysicalOperator, PhysicalPlan> dfw;
    
    public UDFFinder(){
        this(null, null);
    }
    
    public UDFFinder(PhysicalPlan plan, PlanWalker<PhysicalOperator, PhysicalPlan> walker) {
        super(plan, walker);
        UDFs = new ArrayList<String>();
        dfw = new DepthFirstWalker<PhysicalOperator, PhysicalPlan>(null);
    }

    public List<String> getUDFs() {
        return UDFs;
    }
    
    public void setPlan(PhysicalPlan plan){
        mPlan = plan;
        dfw.setPlan(plan);
        mCurrentWalker = dfw;
        UDFs.clear();
    }
    
    /*private void addUDFsIn(PhysicalPlan ep) throws VisitorException{
        udfFinderForExpr.setPlan(ep);
        udfFinderForExpr.visit();
        UDFs.addAll(udfFinderForExpr.getUDFs());
    }

    @Override
    public void visitFilter(POFilter op) throws VisitorException {
        addUDFsIn(op.getPlan());
    }

    @Override
    public void visitGenerate(POGenerate op) throws VisitorException {
        List<PhysicalPlan> eps = op.getInputPlans();
        for (PhysicalPlan ep : eps) {
            addUDFsIn(ep);
        }
    }*/

    @Override
    public void visitSort(POSort op) throws VisitorException {
        if(op.getMSortFunc()!=null)
            UDFs.add(op.getMSortFunc().getFuncSpec().toString());
    }
    
    @Override
    public void visitUserFunc(POUserFunc userFunc) throws VisitorException {
        UDFs.add(userFunc.getFuncSpec().toString());
    }

    @Override
    public void visitComparisonFunc(POUserComparisonFunc compFunc) throws VisitorException {
        UDFs.add(compFunc.getFuncSpec().toString());
    }
    
    @Override
    public void visitCast(POCast op) {
        if (op.getFuncSpec()!=null)
        UDFs.add(op.getFuncSpec().toString());
    }
    
}
