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

package org.apache.pig.pen.util;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.pig.newplan.DepthFirstWalker;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanVisitor;
import org.apache.pig.newplan.PlanWalker;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.Utils;

public class PreOrderDepthFirstWalker extends PlanWalker {
  
    private boolean branchFlag = false;
    
    /**
     * @param plan
     *            Plan for this walker to traverse.
     */
    public PreOrderDepthFirstWalker(OperatorPlan plan) {
        super(plan);
    }

    public void setBranchFlag() {
        branchFlag = true;
    }
    
    public boolean getBranchFlag() {
        return branchFlag;
    }
    
    /**
     * Begin traversing the graph.
     * 
     * @param visitor
     *            Visitor this walker is being used by.
     * @throws VisitorException
     *             if an error is encountered while walking.
     */
    public void walk(PlanVisitor visitor) throws FrontendException {
        List<Operator> leaves = plan.getSinks();
        Set<Operator> seen = new HashSet<Operator>();

        depthFirst(null, leaves, seen, visitor);
    }

    public PlanWalker spawnChildWalker(OperatorPlan plan) {
        return new DepthFirstWalker(plan);
    }

    private void depthFirst(Operator node, Collection<Operator> predecessors, Set<Operator> seen,
            PlanVisitor visitor) throws FrontendException {
        if (predecessors == null)
            return;

        boolean thisBranchFlag = branchFlag;
        for (Operator pred : predecessors) {
            if (seen.add(pred)) {
                branchFlag = thisBranchFlag;
                pred.accept(visitor);
                Collection<Operator> newPredecessors = Utils.mergeCollection(plan.getPredecessors(pred), plan.getSoftLinkPredecessors(pred));
                depthFirst(pred, newPredecessors, seen, visitor);
            }
        }
    }
}
