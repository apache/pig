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
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.experimental.logical.optimizer.PlanPrinter;
import org.apache.pig.impl.logicalLayer.DotLOPrinter;
import org.apache.pig.impl.logicalLayer.LOPrinter;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.Pair;

public abstract class BaseOperatorPlan implements OperatorPlan {

    protected Set<Operator> ops;
    protected PlanEdge fromEdges;
    protected PlanEdge toEdges;

    private List<Operator> roots;
    private List<Operator> leaves;
    protected static final Log log =
        LogFactory.getLog(BaseOperatorPlan.class);
 
    public BaseOperatorPlan() {
        ops = new HashSet<Operator>();
        roots = new ArrayList<Operator>();
        leaves = new ArrayList<Operator>();
        fromEdges = new PlanEdge();
        toEdges = new PlanEdge();
    }
    
    /**
     * Get number of nodes in the plan.
     */
    public int size() {
        return ops.size();
    }

    /**
     * Get all operators in the plan that have no predecessors.
     * @return all operators in the plan that have no predecessors, or
     * an empty list if the plan is empty.
     */
    public List<Operator> getSources() {
        if (roots.size() == 0 && ops.size() > 0) {
            for (Operator op : ops) {               
                if (toEdges.get(op) == null) {
                    roots.add(op);
                }
            }
        }
        return roots;
    }

    /**
     * Get all operators in the plan that have no successors.
     * @return all operators in the plan that have no successors, or
     * an empty list if the plan is empty.
     */
    public List<Operator> getSinks() {
        if (leaves.size() == 0 && ops.size() > 0) {
            for (Operator op : ops) {
                if (fromEdges.get(op) == null) {
                    leaves.add(op);
                }
            }
        }
        return leaves;
    }

    /**
     * For a given operator, get all operators immediately before it in the
     * plan.
     * @param op operator to fetch predecessors of
     * @return list of all operators imeediately before op, or an empty list
     * if op is a root.
     * @throws IOException if op is not in the plan.
     */
    public List<Operator> getPredecessors(Operator op) throws IOException {
        return (List<Operator>)toEdges.get(op);
    }
    
    /**
     * For a given operator, get all operators immediately after it.
     * @param op operator to fetch successors of
     * @return list of all operators imeediately after op, or an empty list
     * if op is a leaf.
     * @throws IOException if op is not in the plan.
     */
    public List<Operator> getSuccessors(Operator op) throws IOException {
        return (List<Operator>)fromEdges.get(op);
    }

    /**
     * Add a new operator to the plan.  It will not be connected to any
     * existing operators.
     * @param op operator to add
     */
    public void add(Operator op) {
        markDirty();
        ops.add(op);
    }

    /**
     * Remove an operator from the plan.
     * @param op Operator to be removed
     * @throws IOException if the remove operation attempts to 
     * remove an operator that is still connected to other operators.
     */
    public void remove(Operator op) throws IOException {
        
        if (fromEdges.containsKey(op) || toEdges.containsKey(op)) {
            throw new IOException("Attempt to remove operator " + op.getName()
                    + " that is still connected in the plan");
        }
        markDirty();
        ops.remove(op);
    }
    
    /**
     * Connect two operators in the plan, controlling which position in the
     * edge lists that the from and to edges are placed.
     * @param from Operator edge will come from
     * @param fromPos Position in the array for the from edge
     * @param to Operator edge will go to
     * @param toPos Position in the array for the to edge
     */
    public void connect(Operator from,
                        int fromPos,
                        Operator to,
                        int toPos) {
        markDirty();
        fromEdges.put(from, to, fromPos);
        toEdges.put(to, from, toPos);
    }
    
    /**
     * Connect two operators in the plan.
     * @param from Operator edge will come from
     * @param to Operator edge will go to
     */
    public void connect(Operator from, Operator to) {
        markDirty();
        fromEdges.put(from, to);
        toEdges.put(to, from);
    }
    
    /**
     * Disconnect two operators in the plan.
     * @param from Operator edge is coming from
     * @param to Operator edge is going to
     * @return pair of positions, indicating the position in the from and
     * to arrays.
     * @throws IOException if the two operators aren't connected.
     */
    public Pair<Integer, Integer> disconnect(Operator from,
                                             Operator to) throws IOException {
        Pair<Operator, Integer> f = fromEdges.removeWithPosition(from, to);
        if (f == null) { 
            throw new IOException("Attempt to disconnect operators " + 
                from.getName() + " and " + to.getName() +
                " which are not connected.");
        }
        
        Pair<Operator, Integer> t = toEdges.removeWithPosition(to, from);
        if (t == null) { 
            throw new IOException("Plan in inconssistent state " + 
                from.getName() + " and " + to.getName() +
                " connected in fromEdges but not toEdges.");
        }
        
        markDirty();
        return new Pair<Integer, Integer>(f.second, t.second);
    }

    private void markDirty() {
        roots.clear();
        leaves.clear();
    }

    public Iterator<Operator> getOperators() {
        return ops.iterator();
    }
   
    public boolean isEqual(OperatorPlan other) {
        return isEqual(this, other);
    }
    
    private static boolean checkPredecessors(Operator op1,
                                      Operator op2) {
        try {
            List<Operator> preds = op1.getPlan().getPredecessors(op1);
            List<Operator> otherPreds = op2.getPlan().getPredecessors(op2);
            if (preds == null && otherPreds == null) {
                // intentionally blank
            } else if (preds == null || otherPreds == null) {
                return false;
            } else {
                if (preds.size() != otherPreds.size()) return false;
                for (int i = 0; i < preds.size(); i++) {
                    Operator p1 = preds.get(i);
                    Operator p2 = otherPreds.get(i);
                    if (!p1.isEqual(p2)) return false;
                    if (!checkPredecessors(p1, p2)) return false;
                }
            }
            return true;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }   
    
    protected static boolean isEqual(OperatorPlan p1, OperatorPlan p2) {
        if (p1 == p2) {
            return true;
        }
        
        if (p1 != null && p2 != null) {
            List<Operator> leaves = p1.getSinks();
            List<Operator> otherLeaves = p2.getSinks();
            if (leaves.size() != otherLeaves.size()) return false;
            // Must find some leaf that is equal to each leaf.  There is no
            // guarantee leaves will be returned in any particular order.
            boolean foundAll = true;
            for (Operator op1 : leaves) {
                boolean foundOne = false;
                for (Operator op2 : otherLeaves) {
                    if (op1.isEqual(op2) && checkPredecessors(op1, op2)) {
                        foundOne = true;
                        break;
                    }
                }
                foundAll &= foundOne;
                if (!foundAll) return false;
            }
            return foundAll;
        }
        
        return false;
    }
    
    public void explain(PrintStream ps, String format, boolean verbose) 
    throws IOException {
        ps.println("#-----------------------------------------------");
        ps.println("# New Logical Plan:");
        ps.println("#-----------------------------------------------");

        PlanPrinter npp = new PlanPrinter(this, ps);
        npp.visit();
}
}
