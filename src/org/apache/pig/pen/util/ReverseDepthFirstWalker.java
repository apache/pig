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
import java.util.Set;
import java.util.Map;

import org.apache.pig.impl.plan.Operator;
import org.apache.pig.impl.plan.OperatorPlan;
import org.apache.pig.impl.plan.PlanVisitor;
import org.apache.pig.impl.plan.PlanWalker;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;

@SuppressWarnings("unchecked")
public class ReverseDepthFirstWalker<O extends Operator, P extends OperatorPlan<O>>
        extends PlanWalker<O, P> {
    private HashSet<O> stoppers = null;
    private LogicalOperator lo;
    private LogicalPlan lp;
    Map<LogicalOperator, O> l2pMap;
    Set<O> seen;
    
    /**
     * @param plan
     *            Plan for this walker to traverse.
     */
    public ReverseDepthFirstWalker(P plan, LogicalOperator op, LogicalPlan lp, Map<LogicalOperator, O> l2pMap, Set<O> seen) {
        super(plan);
        if (lp != null && lp.getPredecessors(op) != null)
        {
            stoppers = new HashSet<O>();
            for (LogicalOperator lo :  lp.getPredecessors(op))
                stoppers.add(l2pMap.get(lo));
        }
        lo = op;
        this.lp = lp;
        this.l2pMap = l2pMap;
        if (seen == null)
            this.seen = new HashSet<O>();
        else
            this.seen = seen;
    }

    public ReverseDepthFirstWalker(P plan, LogicalOperator op, LogicalPlan lp, Map<LogicalOperator, O> l2pMap)
    {
        this(plan, op, lp, l2pMap, null);
    }
    
    public PlanWalker<O, P> spawnChildWalker(P plan) {
        return new ReverseDepthFirstWalker<O, P>(plan, lo, lp, l2pMap, seen);
    }
        
    /**
     * Begin traversing the graph.
     * 
     * @param visitor
     *            Visitor this walker is being used by.
     * @throws VisitorException
     *             if an error is encountered while walking.
     */
    public void walk(PlanVisitor<O, P> visitor) throws VisitorException {
        O po = l2pMap.get(lo);
        // TODO: After use new LO, this should be removed. Found necessary in the testForeach test.
        if (po == null)
            return;
        depthFirst(null, mPlan.getPredecessors(po), visitor);
        if (seen.add(po))
            po.visit(visitor);
    }

    private void depthFirst(O node, Collection<O> predecessors,
            PlanVisitor<O, P> visitor) throws VisitorException {
        if (predecessors == null)
            return;

        for (O pred : predecessors) {
            if (seen.add(pred)) {
                Collection<O> newPredecessors = Utils.mergeCollection(mPlan.getPredecessors(pred), mPlan.getSoftLinkPredecessors(pred));
                depthFirst(pred, newPredecessors, visitor);
                pred.visit(visitor);
            }
            if (stoppers.contains(pred))
                continue;
        }
    }
}
