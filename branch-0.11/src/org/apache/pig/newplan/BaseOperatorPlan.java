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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.Pair;

public abstract class BaseOperatorPlan implements OperatorPlan {

    protected List<Operator> ops;
    protected PlanEdge fromEdges;
    protected PlanEdge toEdges;
    protected PlanEdge softFromEdges;
    protected PlanEdge softToEdges;

    private List<Operator> roots;
    private List<Operator> leaves;
    protected static final Log log =
        LogFactory.getLog(BaseOperatorPlan.class);
 
    public BaseOperatorPlan() {
        ops = new ArrayList<Operator>();
        roots = new ArrayList<Operator>();
        leaves = new ArrayList<Operator>();
        fromEdges = new PlanEdge();
        toEdges = new PlanEdge();
        softFromEdges = new PlanEdge();
        softToEdges = new PlanEdge();
    }
    
    @SuppressWarnings("unchecked")
    public BaseOperatorPlan(BaseOperatorPlan other) {
        // (shallow) copy constructor
        ops = (List<Operator>) ((ArrayList<Operator>) other.ops).clone();
        roots = (List<Operator>) ((ArrayList) other.roots).clone();
        leaves = (List<Operator>) ((ArrayList) other.leaves).clone();
        fromEdges = other.fromEdges.shallowClone();
        toEdges = other.toEdges.shallowClone();
        softFromEdges = other.softFromEdges.shallowClone();
        softToEdges = other.softToEdges.shallowClone();
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
     */
    public List<Operator> getPredecessors(Operator op) {
        return toEdges.get(op);
    }
    
    /**
     * For a given operator, get all operators immediately after it.
     * @param op operator to fetch successors of
     * @return list of all operators imeediately after op, or an empty list
     * if op is a leaf.
     */
    public List<Operator> getSuccessors(Operator op) {
        return fromEdges.get(op);
    }
    
    /**
     * For a given operator, get all operators softly immediately before it in the
     * plan.
     * @param op operator to fetch predecessors of
     * @return list of all operators immediately before op, or an empty list
     * if op is a root.
     */
    public List<Operator> getSoftLinkPredecessors(Operator op) {
        return softToEdges.get(op);
    }
    
    /**
     * For a given operator, get all operators softly immediately after it.
     * @param op operator to fetch successors of
     * @return list of all operators immediately after op, or an empty list
     * if op is a leaf.
     */
    public List<Operator> getSoftLinkSuccessors(Operator op) {
        return softFromEdges.get(op);
    }

    /**
     * Add a new operator to the plan.  It will not be connected to any
     * existing operators.
     * @param op operator to add
     */
    public void add(Operator op) {
        markDirty();
        if (!ops.contains(op))
            ops.add(op);
    }

    /**
     * Remove an operator from the plan.
     * @param op Operator to be removed
     * @throws FrontendException if the remove operation attempts to 
     * remove an operator that is still connected to other operators.
     */
    public void remove(Operator op) throws FrontendException {
        
        if (fromEdges.containsKey(op) || toEdges.containsKey(op)) {
            throw new FrontendException("Attempt to remove operator " + op.getName()
                    + " that is still connected in the plan", 2243);
        }
        if (softFromEdges.containsKey(op) || softToEdges.containsKey(op)) {
            throw new FrontendException("Attempt to remove operator " + op.getName()
                    + " that is still softly connected in the plan", 2243);
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
     * Check if given two operators are directly connected.
     * @param from Operator edge will come from
     * @param to Operator edge will go to
     */
    public boolean isConnected(Operator from, Operator to) {
    	List<Operator> preds = getPredecessors( to );
    	return ( preds != null ) && preds.contains( from );
    }
    
    /**
     * Connect two operators in the plan.
     * @param from Operator edge will come from
     * @param to Operator edge will go to
     */
    public void connect(Operator from, Operator to) {
    	if( isConnected( from, to ) )
    		return;
    	
        markDirty();
        fromEdges.put(from, to);
        toEdges.put(to, from);
    }
    
    /**
     * Create an soft edge between two nodes.
     * @param from Operator dependent upon
     * @param to Operator having the dependency
     */
    public void createSoftLink(Operator from, Operator to) {
        softFromEdges.put(from, to);
        softToEdges.put(to, from);
    }
    
    /**
     * Remove an soft edge
     * @param from Operator dependent upon
     * @param to Operator having the dependency
     */
    public void removeSoftLink(Operator from, Operator to) {
        softFromEdges.remove(from, to);
        softToEdges.remove(to, from);
    }
    
    /**
     * Disconnect two operators in the plan.
     * @param from Operator edge is coming from
     * @param to Operator edge is going to
     * @return pair of positions, indicating the position in the from and
     * to arrays.
     * @throws FrontendException if the two operators aren't connected.
     */
    public Pair<Integer, Integer> disconnect(Operator from,
                                             Operator to) throws FrontendException {
        Pair<Operator, Integer> f = fromEdges.removeWithPosition(from, to);
        if (f == null) { 
            throw new FrontendException("Attempt to disconnect operators " + 
                from.getName() + " and " + to.getName() +
                " which are not connected.", 2219);
        }
        
        Pair<Operator, Integer> t = toEdges.removeWithPosition(to, from);
        if (t == null) { 
            throw new FrontendException("Plan in inconssistent state " + 
                from.getName() + " and " + to.getName() +
                " connected in fromEdges but not toEdges.", 2220);
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
   
    public boolean isEqual(OperatorPlan other) throws FrontendException {
        return isEqual(this, other);
    }
    
    private static boolean checkPredecessors(Operator op1,
                                      Operator op2) throws FrontendException {
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
    }   
    
    protected static boolean isEqual(OperatorPlan p1, OperatorPlan p2) throws FrontendException {
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
    
    public void explain(PrintStream ps, String format, boolean verbose) throws FrontendException {
    }
    
    @Override
    public String toString() {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(os);
        try {
            explain(ps,"",false);
        } catch (FrontendException e) {
            return "";
        }
        return os.toString();
    }
    
    @Override
    public void replace(Operator oldOperator, Operator newOperator) throws FrontendException {
        add(newOperator);
        
        List<Operator> preds = getPredecessors(oldOperator);
        if (preds!=null) {
            List<Operator> predsCopy = new ArrayList<Operator>();
            predsCopy.addAll(preds);
            for (int i=0;i<predsCopy.size();i++) {
                Operator pred = predsCopy.get(i);
                Pair<Integer, Integer> pos = disconnect(pred, oldOperator);
                connect(pred, pos.first, newOperator, i);
            }
        }
        
        List<Operator> succs = getSuccessors(oldOperator);
        if (succs!=null) {
            List<Operator> succsCopy = new ArrayList<Operator>();
            succsCopy.addAll(succs);
            for (int i=0;i<succsCopy.size();i++) {
                Operator succ = succsCopy.get(i);
                Pair<Integer, Integer> pos = disconnect(oldOperator, succ);
                connect(newOperator, i, succ, pos.second);
            }
        }
        
        remove(oldOperator);
    }
    
    // We assume if node has multiple inputs, it only has one output;
    // if node has multiple outputs, it only has one input.
    // Otherwise, we don't know how to connect inputs to outputs.
    // This assumption is true for logical plan/physical plan, and most MR plan
    @Override
    public void removeAndReconnect(Operator operatorToRemove) throws FrontendException {
        List<Operator> predsCopy = null;
        if (getPredecessors(operatorToRemove)!=null && getPredecessors(operatorToRemove).size()!=0) {
            predsCopy = new ArrayList<Operator>();
            predsCopy.addAll(getPredecessors(operatorToRemove));
        }
        
        List<Operator> succsCopy = null;
        if (getSuccessors(operatorToRemove)!=null && getSuccessors(operatorToRemove).size()!=0) {
            succsCopy = new ArrayList<Operator>();
            succsCopy.addAll(getSuccessors(operatorToRemove));
        }
        
        if (predsCopy!=null && predsCopy.size()>1 && succsCopy!=null && succsCopy.size()>1) {
            throw new FrontendException("Cannot remove and reconnect node with multiple inputs/outputs", 2256);
        }
        
        if (predsCopy!=null && predsCopy.size()>1) {
            // node has multiple inputs, it can only has one output (or no output)
            // reconnect inputs to output
            Operator succ = null;
            Pair<Integer, Integer> pos2 = null;
            if (succsCopy!=null) {
                succ = succsCopy.get(0);
                pos2 = disconnect(operatorToRemove, succ);
            }
            for (Operator pred : predsCopy) {
                Pair<Integer, Integer> pos1 = disconnect(pred, operatorToRemove);
                if (succ!=null) {
                    connect(pred, pos1.first, succ, pos2.second);
                }
            }
        } else if (succsCopy!=null && succsCopy.size()>1) {
            // node has multiple outputs, it can only has one output (or no output)
            // reconnect input to outputs
            Operator pred = null;
            Pair<Integer, Integer> pos1 = null;
            if (predsCopy!=null) {
                pred = predsCopy.get(0);
                pos1 = disconnect(pred, operatorToRemove);
            }
            for (Operator succ : succsCopy) {
                Pair<Integer, Integer> pos2 = disconnect(operatorToRemove, succ);
                if (pred!=null) {
                    connect(pred, pos1.first, succ, pos2.second);
                }
            }
        } else {
            // Only have one input/output
            Operator pred = null;
            Pair<Integer, Integer> pos1 = null;
            if (predsCopy!=null) {
                pred = predsCopy.get(0);
                pos1 = disconnect(pred, operatorToRemove);
            }
            
            Operator succ = null;
            Pair<Integer, Integer> pos2 = null;
            if (succsCopy!=null) {
                succ = succsCopy.get(0);
                pos2 = disconnect(operatorToRemove, succ);
            }
            
            if (pred!=null && succ!=null) {
                connect(pred, pos1.first, succ, pos2.second);
            }
        }
        
        remove(operatorToRemove);
    }
    
    @Override
    public void insertBetween(Operator pred, Operator operatorToInsert, Operator succ) throws FrontendException {
        add(operatorToInsert);
        Pair<Integer, Integer> pos = disconnect(pred, succ);
        connect(pred, pos.first, operatorToInsert, 0);
        connect(operatorToInsert, 0, succ, pos.second);
    }

    /**
     * A method to check if there is a path from a given node to another node
     * @param from the start node for checking
     * @param to the end node for checking
     * @return true if path exists, false otherwise
     */
    public boolean pathExists(Operator from, Operator to) {
        List<Operator> successors = getSuccessors( from );
        if(successors == null || successors.size() == 0) {
            return false;
        }
        for (Operator successor : successors) {
            if( successor.equals( to ) || pathExists( successor, to ) ) {
                return true;
            }
        }
        return false;
    }

}
