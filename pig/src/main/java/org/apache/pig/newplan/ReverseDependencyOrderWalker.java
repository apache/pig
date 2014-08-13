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

package org.apache.pig.newplan;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.Utils;

/**
 * Visit a plan in the reverse of the dependency order.  That is, every node
 * after every node that depends on it is visited.  Thus this is equivalent to
 * doing a reverse topilogical sort on the graph and then visiting it in order.
 */
public class ReverseDependencyOrderWalker extends PlanWalker {

    public ReverseDependencyOrderWalker(OperatorPlan plan) {
        super(plan);
    }

    @Override
    public PlanWalker spawnChildWalker(OperatorPlan plan) {
        return new ReverseDependencyOrderWalker(plan);
    }

    /**
     * Begin traversing the graph.
     * @param visitor Visitor this walker is being used by.
     * @throws VisitorException if an error is encountered while walking.
     */
    @Override
    public void walk(PlanVisitor visitor) throws FrontendException {
        // This is highly inefficient, but our graphs are small so it should be okay.
        // The algorithm works by starting at any node in the graph, finding it's
        // successors and calling itself for each of those successors.  When it
        // finds a node that has no unfinished successors it puts that node in the
        // list.  It then unwinds itself putting each of the other nodes in the list.
        // It keeps track of what nodes it's seen as it goes so it doesn't put any
        // nodes in the graph twice.

        List<Operator> fifo = new ArrayList<Operator>();
        Set<Operator> seen = new HashSet<Operator>();
        List<Operator> roots = plan.getSources();
        if (roots == null) return;
        for (Operator op : roots) {
            doAllSuccessors(op, seen, fifo);
        }

        for (Operator op: fifo) {
            op.accept(visitor);
        }
    }

    protected void doAllSuccessors(Operator node,
                                   Set<Operator> seen,
                                   Collection<Operator> fifo) throws FrontendException {
        if (!seen.contains(node)) {
            // We haven't seen this one before.
            Collection<Operator> succs = Utils.mergeCollection(plan.getSuccessors(node), plan.getSoftLinkSuccessors(node));
            if (succs != null && succs.size() > 0) {
                // Do all our successors before ourself
                for (Operator op : succs) {
                    doAllSuccessors(op, seen, fifo);
                }
            }
            // Now do ourself
            seen.add(node);
            fifo.add(node);
        }
    }
}
