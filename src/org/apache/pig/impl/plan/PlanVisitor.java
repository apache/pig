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
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.pig.impl.logicalLayer.parser.ParseException;

/**
 * A visitor mechanism for navigating and operating on a plan of 
 * Operators.  This class contains the logic to traverse the plan.  It does not
 * visit individual nodes.  That is left to implementing classes (such as
 * LOVisitor).
 *
 */
abstract public class PlanVisitor <O extends Operator, P extends OperatorPlan<O>> {

    protected P mPlan;

    /**
     * Entry point for visiting the plan.
     * @throws ParseException if an error is encountered while visiting.
     */
    public abstract void visit() throws ParseException;

    /**
     * @param plan OperatorPlan this visitor will visit.
     */
    protected PlanVisitor(P plan) {
        mPlan = plan;
    }

    /**
     * Visit the graph in a depth first traversal.
     * @throws ParseException if the underlying visitor has a problem.
     */
    protected void depthFirst() throws ParseException {
        List<O> roots = mPlan.getRoots();
        Set<O> seen = new HashSet<O>();

        depthFirst(null, roots, seen);
    }

    private void depthFirst(O node,
                            Collection<O> successors,
                            Set<O> seen) throws ParseException {
        if (successors == null) return;

        for (O suc : successors) {
            if (seen.add(suc)) {
                suc.visit(this);
                Collection<O> newSuccessors = mPlan.getSuccessors(suc);
                depthFirst(suc, newSuccessors, seen);
            }
        }
    }

    /**
     * Visit the graph in a way that guarantees that no node is visited before
     * all the nodes it depends on (that is, all those giving it input) have
     * already been visited.
     * @throws ParseException if the underlying visitor has a problem.
     */
    protected void dependencyOrder() throws ParseException {
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
            op.visit(this);
        }
    }

    private void doAllPredecessors(O node,
                                   Set<O> seen,
                                   Collection<O> fifo) throws ParseException {
        if (!seen.contains(node)) {
            // We haven't seen this one before.
            Collection<O> preds = mPlan.getPredecessors(node);
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
