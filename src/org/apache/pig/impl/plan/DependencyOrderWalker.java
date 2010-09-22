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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.pig.impl.util.Utils;


/**
 * DependencyOrderWalker traverses the graph in such a way that no node is visited
 * before all the nodes it depends on have been visited.  Beyond this, it does not
 * guarantee any particular order.  So, you have a graph with node 1 2 3 4, and
 * edges 1->3, 2->3, and 3->4, this walker guarnatees that 1 and 2 will be visited
 * before 3 and 3 before 4, but it does not guarantee whether 1 or 2 will be
 * visited first.
 */
public class DependencyOrderWalker <O extends Operator, P extends OperatorPlan<O>>
    extends PlanWalker<O, P> {

    /**
     * @param plan Plan for this walker to traverse.
     */
    public DependencyOrderWalker(P plan) {
        super(plan);
    }

    /**
     * Begin traversing the graph.
     * @param visitor Visitor this walker is being used by.
     * @throws VisitorException if an error is encountered while walking.
     */
    @SuppressWarnings("unchecked")
    public void walk(PlanVisitor<O, P> visitor) throws VisitorException {
        // This is highly inefficient, but our graphs are small so it should be okay.
        // The algorithm works by starting at any node in the graph, finding it's
        // predecessors and calling itself for each of those predecessors.  When it
        // finds a node that has no unfinished predecessors it puts that node in the
        // list.  It then unwinds itself putting each of the other nodes in the list.
        // It keeps track of what nodes it's seen as it goes so it doesn't put any
        // nodes in the graph twice.

        List<O> fifo = new ArrayList<O>();
        Set<O> seen = new HashSet<O>();
        List<O> leaves = mPlan.getLeaves();
        if (leaves == null) return;
        for (O op : leaves) {
            doAllPredecessors(op, seen, fifo);
        }
        for (O op: fifo) {
            op.visit(visitor);
        }
    }

    public PlanWalker<O, P> spawnChildWalker(P plan) { 
        return new DependencyOrderWalker<O, P>(plan);
    }

    protected void doAllPredecessors(O node,
                                   Set<O> seen,
                                   Collection<O> fifo) throws VisitorException {
        if (!seen.contains(node)) {
            // We haven't seen this one before.
            Collection<O> preds = Utils.mergeCollection(mPlan.getPredecessors(node), mPlan.getSoftLinkPredecessors(node));
            if (preds != null && preds.size() > 0) {
                // Do all our predecessors before ourself
                for (O op : preds) {
                    doAllPredecessors(op, seen, fifo);
                }
            }
            // Now do ourself
            seen.add(node);
            fifo.add(node);
        }
    }
}
