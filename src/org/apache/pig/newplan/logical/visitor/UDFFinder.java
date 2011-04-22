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
package org.apache.pig.newplan.logical.visitor;

import java.util.ArrayList;
import java.util.List;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.DependencyOrderWalker;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanWalker;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.LogicalExpressionVisitor;
import org.apache.pig.newplan.logical.expression.UserFuncExpression;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOGenerate;
import org.apache.pig.newplan.logical.relational.LogicalRelationalNodesVisitor;

public class UDFFinder extends LogicalRelationalNodesVisitor {

    private List<UserFuncExpression> mUDFList = new ArrayList<UserFuncExpression>();
    
    public UDFFinder(OperatorPlan plan)
            throws FrontendException {
        super(plan, new DependencyOrderWalker(plan));
    }

    @Override
    public void visit(LOForEach foreach) throws FrontendException {
        OperatorPlan innerPlan = foreach.getInnerPlan();
        PlanWalker newWalker = currentWalker.spawnChildWalker(innerPlan);
        pushWalker(newWalker);
        currentWalker.walk(this);
        popWalker();
    }
    
    @Override
    public void visit(LOGenerate generate) throws FrontendException {
        for (LogicalExpressionPlan plan : generate.getOutputPlans()) {
            UDFExpFinder udfExpFinder = new UDFExpFinder(plan);
            udfExpFinder.visit();
            mUDFList.addAll(udfExpFinder.getUDFList());
        }
    }
    
    /**
     * 
     * @return true if the plan had any UDFs; false otherwise
     */
    public List<UserFuncExpression> getUDFList() {
        return mUDFList;
    }
}

class UDFExpFinder extends LogicalExpressionVisitor {
    
    List<UserFuncExpression> mUDFList = new ArrayList<UserFuncExpression>();
    
    UDFExpFinder(OperatorPlan plan)
            throws FrontendException {
        super(plan, new DependencyOrderWalker(plan));
    }
    
    @Override
    public void visit(UserFuncExpression userFunc) {
        mUDFList.add(userFunc);
    }
    
    public List<UserFuncExpression> getUDFList() {
        return mUDFList;
    }
}
