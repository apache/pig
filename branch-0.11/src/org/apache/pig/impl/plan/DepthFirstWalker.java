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
package org.apache.pig.impl.plan;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.pig.impl.util.Utils;


/**
 * DepthFirstWalker traverses a plan in a depth first manner.  One important note
 * is that, in compliance with the PlanWalker contract, it only visits each node in
 * the graph once.
 */
public class DepthFirstWalker <O extends Operator, P extends OperatorPlan<O>>
    extends PlanWalker<O, P> {

    /**
     * @param plan Plan for this walker to traverse.
     */
    public DepthFirstWalker(P plan) {
        super(plan);
    }

    /**
     * Begin traversing the graph.
     * @param visitor Visitor this walker is being used by.
     * @throws VisitorException if an error is encountered while walking.
     */
    public void walk(PlanVisitor<O, P> visitor) throws VisitorException {
        List<O> roots = mPlan.getRoots();
        Set<O> seen = new HashSet<O>();

        depthFirst(null, roots, seen, visitor);
    }

    public PlanWalker<O, P> spawnChildWalker(P plan) {
        return new DepthFirstWalker<O, P>(plan);
    }

    // Suppress "unchecked" warnings for all logical plan related classes. Will revisit in logical plan rework
    @SuppressWarnings("unchecked")
    private void depthFirst(O node,
                            Collection<O> successors,
                            Set<O> seen,
                            PlanVisitor<O, P> visitor) throws VisitorException {
        if (successors == null) return;

        for (O suc : successors) {
            if (seen.add(suc)) {
                suc.visit(visitor);
                Collection<O> newSuccessors = Utils.mergeCollection(mPlan.getSuccessors(suc), mPlan.getSoftLinkSuccessors(suc));
                depthFirst(suc, newSuccessors, seen, visitor);
            }
        }
    }
}
