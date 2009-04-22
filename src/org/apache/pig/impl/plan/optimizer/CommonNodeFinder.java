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
package org.apache.pig.impl.plan.optimizer;

import java.util.ArrayList;
import java.util.List;

import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.VisitorException;

public class CommonNodeFinder extends
        RulePlanVisitor {

    private List<RuleOperator> mCommonNodes = null;
    
    public CommonNodeFinder(RulePlan plan) {
        super(plan, new DependencyOrderWalker<RuleOperator, RulePlan>(plan));
    }
    
    public int getCount() {
        return ((mCommonNodes == null) ? 0 : mCommonNodes.size());
    }
    
    public List<RuleOperator> getCommonNodes() {
        return mCommonNodes;
    }
    
    private void reset() {
        mCommonNodes = new ArrayList<RuleOperator>();
    }

    @Override
    public void visit() throws VisitorException {
        reset();
        super.visit();
    }
    
    /**
     * @param ruleOp
     *            the rule operator that has to be visited
     * @throws VisitorException
     */
    protected void visit(RuleOperator ruleOp)
            throws VisitorException {
        /**
         * A common node is a node that appears in the common path of two nodes in the rule plan
         * Any node that has more than one predecessor is a common node
         * Any node that has a predecessor which is a common node is a common node
         * Any node that has more than one successor is a common node
         */
        if(ruleOp.getNodeType().equals(RuleOperator.NodeType.ANY_NODE)) {
           return; 
        }
        List<RuleOperator> predecessors = mPlan.getPredecessors(ruleOp);
        List<RuleOperator> successors = mPlan.getSuccessors(ruleOp);
        
        if(predecessors != null) {
            if(predecessors.size() > 1) {
                ruleOp.setNodeType(RuleOperator.NodeType.COMMON_NODE);
                mCommonNodes.add(ruleOp);
                return;
            } else {
                //has to be one predecessor
                //check if the predecessor is a common node then this node is
                //also a common node
                RuleOperator ruleOperatorPredecessor = predecessors.get(0);
                if(ruleOperatorPredecessor.getNodeType().equals(RuleOperator.NodeType.COMMON_NODE)) {
                    ruleOp.setNodeType(RuleOperator.NodeType.COMMON_NODE);
                    mCommonNodes.add(ruleOp);
                    return;
                }
            }
        }
        
        if(successors != null) {
            if (successors.size() > 1) {
                ruleOp.setNodeType(RuleOperator.NodeType.COMMON_NODE);
                mCommonNodes.add(ruleOp);
                return;
            }
        }
    }

}