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

package org.apache.pig.experimental.plan;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Do a depth first traversal of the graph.
 */
public class DepthFirstWalker extends PlanWalker {

    public DepthFirstWalker(OperatorPlan plan) {
        super(plan);
    }

    @Override
    public PlanWalker spawnChildWalker(OperatorPlan plan) {
        return new DepthFirstWalker(plan);
    }

    /**
     * Begin traversing the graph.
     * @param visitor Visitor this walker is being used by.
     * @throws IOException if an error is encountered while walking.
     */
    @Override
    public void walk(PlanVisitor visitor) throws IOException {
        List<Operator> roots = plan.getRoots();
        Set<Operator> seen = new HashSet<Operator>();

        depthFirst(null, roots, seen, visitor);
    }

    private void depthFirst(Operator node,
                            Collection<Operator> successors,
                            Set<Operator> seen,
                            PlanVisitor visitor) throws IOException {
        if (successors == null) return;

        for (Operator suc : successors) {
            if (seen.add(suc)) {
                suc.accept(visitor);
                Collection<Operator> newSuccessors = plan.getSuccessors(suc);
                depthFirst(suc, newSuccessors, seen, visitor);
            }
        }
    }
}
