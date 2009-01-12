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

import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.Operator;
import org.apache.pig.impl.plan.OperatorPlan;
import org.apache.pig.impl.plan.PlanVisitor;
import org.apache.pig.impl.plan.PlanWalker;
import org.apache.pig.impl.plan.VisitorException;

public class PreOrderDepthFirstWalker<O extends Operator, P extends OperatorPlan<O>>
        extends PlanWalker<O, P> {
    /**
     * @param plan
     *            Plan for this walker to traverse.
     */
    public PreOrderDepthFirstWalker(P plan) {
        super(plan);
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
        List<O> leaves = mPlan.getLeaves();
        Set<O> seen = new HashSet<O>();

        depthFirst(null, leaves, seen, visitor);
    }

    public PlanWalker<O, P> spawnChildWalker(P plan) {
        return new DepthFirstWalker<O, P>(plan);
    }

    private void depthFirst(O node, Collection<O> predecessors, Set<O> seen,
            PlanVisitor<O, P> visitor) throws VisitorException {
        if (predecessors == null)
            return;

        for (O pred : predecessors) {
            if (seen.add(pred)) {
                pred.visit(visitor);
                Collection<O> newPredecessors = mPlan.getPredecessors(pred);
                depthFirst(pred, newPredecessors, seen, visitor);
            }
        }
    }
}
