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

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.pig.impl.util.MultiMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


//import org.apache.commons.collections.map.MultiValueMap;

/**
 * A generic graphing class for use by LogicalPlan, PhysicalPlan, etc.  One
 * important aspect of this package is that it guarantees that once a graph is
 * constructed, manipulations on that graph will maintain the ordering of
 * inputs and outputs for a given node.  That is, if a node has two inputs, 0
 * and 1, it is guaranteed that everytime it asks for its inputs, it will
 * receive them in the same order.  This allows operators that need to
 * distinguish their inputs (such as binary operators that need to know left
 * from right) to work without needing to store their inputs themselves.  This
 * is an extra burden on the graph package and not in line with the way graphs
 * are generally understood mathematically.  But it greatly reducing the need
 * for graph manipulators (such as the validators and optimizers) to
 * understand the internals of various nodes.
 */
public abstract class OperatorPlan<E extends Operator> implements Iterable, Serializable, Cloneable {
    protected Map<E, OperatorKey> mOps;
    protected Map<OperatorKey, E> mKeys;
    protected MultiMap<E, E> mFromEdges;
    protected MultiMap<E, E> mToEdges;

    private List<E> mRoots;
    private List<E> mLeaves;
    protected static Log log = LogFactory.getLog(OperatorPlan.class);
    
    public OperatorPlan() {
        mRoots = new ArrayList<E>();
        mLeaves = new ArrayList<E>();
        mOps = new HashMap<E, OperatorKey>();
        mKeys = new HashMap<OperatorKey, E>();
        mFromEdges = new MultiMap<E, E>();
        mToEdges = new MultiMap<E, E>();
    }

    /**
     * Get a list of all nodes in the graph that are roots.  A root is defined to
     * be a node that has no input.
     */
    public List<E> getRoots() {
        if (mRoots.size() == 0 && mOps.size() > 0) {
            for (E op : mOps.keySet()) {
                if (mToEdges.get(op) == null) {
                    mRoots.add(op);
                }
            }
        }
        return mRoots;
    }

    /**
     * Get a list of all nodes in the graph that are leaves.  A leaf is defined to
     * be a node that has no output.
     */
    public List<E> getLeaves() {
        if (mLeaves.size() == 0 && mOps.size() > 0) {
            for (E op : mOps.keySet()) {
                if (mFromEdges.get(op) == null) {
                    mLeaves.add(op);
                }
            }
        }
        return mLeaves;
    }

    /**
     * Given an operator, find its OperatorKey.
     * @param op Logical operator.
     * @return associated OperatorKey
     */
    public OperatorKey getOperatorKey(E op) {
        return mOps.get(op);
    }

    /**
     * Given an OperatorKey, find the associated operator.
     * @param opKey OperatorKey
     * @return associated operator.
     */
    public E getOperator(OperatorKey opKey) {
        return mKeys.get(opKey);
    }

    /**
     * Get the map of operator key and associated operators
     * @return map of operator key and operators.
     */
    public Map<OperatorKey, E> getKeys() {
        return mKeys;
    }

    /**
     * Insert an operator into the plan.  This only inserts it as a node in
     * the graph, it does not connect it to any other operators.  That should
     * be done as a separate step using connect.
     * @param op Operator to add to the plan.
     */
    public void add(E op) {
        markDirty();
        mOps.put(op, op.getOperatorKey());
        mKeys.put(op.getOperatorKey(), op);
    }

    /**
     * Create an edge between two nodes.  The direction of the edge implies data
     * flow.
     * @param from Operator data will flow from.
     * @param to Operator data will flow to.
     * @throws PlanException if this edge will create multiple inputs for an
     * operator that does not support multiple inputs or create multiple outputs
     * for an operator that does not support multiple outputs.
     */
    public void connect(E from, E to) throws PlanException {
        markDirty();

        // Check that both nodes are in the plan.
        checkInPlan(from);
        checkInPlan(to);

        // Check to see if the from operator already has outputs, and if so
        // whether it supports multiple outputs.
        if (mFromEdges.get(from) != null &&
                !from.supportsMultipleOutputs()) {
            PlanException pe = new PlanException("Attempt to give operator of type " +
                from.getClass().getName() + " multiple outputs.  This operator does "
                + "not support multiple outputs.");
            log.error(pe.getMessage());
            throw pe;
        }

        // Check to see if the to operator already has inputs, and if so
        // whether it supports multiple inputs.
        if (mToEdges.get(to) != null &&
                !to.supportsMultipleInputs()) {
            PlanException pe =  new PlanException("Attempt to give operator of type " +
                to.getClass().getName() + " multiple inputs.  This operator does "
                + "not support multiple inputs.");
            log.error(pe.getMessage());
            throw pe;
        }
        mFromEdges.put(from, to);
        mToEdges.put(to, from);
    }

    /**
     * Remove an edge from between two nodes. 
     * Use {@link org.apache.pig.impl.plan.OperatorPlan#insertBetween(Operator, Operator, Operator)} 
     * if disconnect is used in the process of inserting a new node between two nodes 
     * by calling disconnect followed by a connect.
     * @param from Operator data would flow from.
     * @param to Operator data would flow to.
     * @return true if the nodes were connected according to the specified data
     * flow, false otherwise.
     */
    public boolean disconnect(E from, E to) {
        markDirty();
        
        boolean sawNull = false;
        if (mFromEdges.remove(from, to) == null) sawNull = true;
        if (mToEdges.remove(to, from) == null) sawNull = true;

        return !sawNull;
    }

    /**
     * Remove an operator from the plan.  Any edges that the node has will
     * be removed as well.
     * @param op Operator to remove.
     */
    public void remove(E op) {
        markDirty();

        removeEdges(op, mFromEdges, mToEdges);
        removeEdges(op, mToEdges, mFromEdges);

        // Remove the operator from nodes
        mOps.remove(op);
        mKeys.remove(op.getOperatorKey());
    }

    /**
     * Trim everything below a given operator.  The specified operator will
     * NOT be removed.
     * @param op Operator to trim everything after.
     */
    public void trimBelow(E op) {
        trimBelow(getSuccessors(op));
    }

    private void trimBelow(List<E> ops) {
        if (ops != null) {
            // Make a copy because we'll be messing with the underlying list.
            List<E> copy = new ArrayList<E>(ops);
            for (E op : copy) {
                trimBelow(getSuccessors(op));
                remove(op);
            }
        }
    }

    /**
     * Trim everything above a given operator.  The specified operator will
     * NOT be removed.
     * @param op Operator to trim everything before.
     */
    public void trimAbove(E op) {
        trimAbove(getPredecessors(op));
    }

    private void trimAbove(List<E> ops) {
        if (ops != null) {
            // Make a copy because we'll be messing with the underlying list.
            List<E> copy = new ArrayList<E>(ops);
            for (E op : copy) {
                trimAbove(getPredecessors(op));
                remove(op);
            }
        }
    }


    /**
     * Find all of the nodes that have edges to the indicated node from
     * themselves.
     * @param op Node to look to
     * @return Collection of nodes.
     */
    public List<E> getPredecessors(E op) {
        return (List<E>)mToEdges.get(op);
    }


    /**
     * Find all of the nodes that have edges from the indicated node to
     * themselves.
     * @param op Node to look from
     * @return Collection of nodes.
     */
    public List<E> getSuccessors(E op) {
        return (List<E>)mFromEdges.get(op);
    }

    public Iterator<E> iterator() { 
        return mOps.keySet().iterator();
    }

    private void markDirty() {
        mRoots.clear();
        mLeaves.clear();
    }

    private void removeEdges(E op,
                             MultiMap<E, E> fromMap,
                             MultiMap<E, E> toMap) {
        // Find all of the from edges, as I have to remove all the associated to
        // edges.  Need to make a copy so we can delete from the map without
        // screwing up our iterator.
        Collection c = fromMap.get(op);
        if (c == null) return;

        ArrayList al = new ArrayList(c);
        Iterator i = al.iterator();
        while (i.hasNext()) {
            E to = (E)i.next();
            toMap.remove(to, op);
            fromMap.remove(op, to);
        }
    }

    private void checkInPlan(E op) throws PlanException {
        if (mOps.get(op) == null) {
            PlanException pe = new PlanException("Attempt to connect operator " +
                op.name() + " which is not in the plan.");
            log.error(pe.getMessage());
            throw pe;
        }
    }
    
    /**
     * Merges the operators in the incoming operPlan with
     * this plan's operators. By merging I mean just making
     * a combined graph with each one as a component
     * It doesn't support merging of shared plans
     * @param inpPlan
     * @return this pointer
     * @throws PlanException
     */
    public OperatorPlan<E> merge(OperatorPlan<E> inpPlan) throws PlanException {
        Map<E, OperatorKey> inpOps = inpPlan.mOps;
        Set<E> curOpsKeySet = mOps.keySet();
        for (Map.Entry<E, OperatorKey> mapEnt : inpOps.entrySet()) {
            if (curOpsKeySet.contains(mapEnt.getKey())) {
                PlanException pe = new PlanException(
                        "There are operators that are shared across the plans. Merge of "
                                + "mutually exclusive plans is the only supported merge.");
                log.error(pe.getMessage());
                throw pe;
            }
            mOps.put(mapEnt.getKey(), mapEnt.getValue());
        }

        Map<OperatorKey, E> inpKeys = inpPlan.mKeys;
        Set<OperatorKey> curOKKeySet = mKeys.keySet();
        for (Map.Entry<OperatorKey, E> mapEnt : inpKeys.entrySet()) {
            if (curOKKeySet.contains(mapEnt.getKey())) {
                PlanException pe = new PlanException(
                        "There are operators that are shared across the plans. Merge of "
                                + "mutually exclusive plans is the only supported merge.");
                log.error(pe.getMessage());
                throw pe;
            }
            mKeys.put(mapEnt.getKey(), mapEnt.getValue());
        }

        MultiMap<E, E> inpFromEdges = inpPlan.mFromEdges;
        Set<E> curFEKeySet = mFromEdges.keySet();
        for (E fromEdg : inpFromEdges.keySet()) {
            if (curFEKeySet.contains(fromEdg)) {
                PlanException pe = new PlanException(
                        "There are operators that are shared across the plans. Merge of "
                                + "mutually exclusive plans is the only supported merge.");
                log.error(pe.getMessage());
                throw pe;
            }
            for (E e : inpFromEdges.get(fromEdg)) {
                mFromEdges.put(fromEdg, e);
            }
        }

        MultiMap<E, E> inpToEdges = inpPlan.mToEdges;
        Set<E> curTEKeySet = mToEdges.keySet();
        for (E toEdg : inpToEdges.keySet()) {
            if (curTEKeySet.contains(toEdg)) {
                PlanException pe = new PlanException(
                        "There are operators that are shared across the plans. Merge of "
                                + "mutually exclusive plans is the only supported merge.");
                log.error(pe.getMessage());
                throw pe;
            }
            for (E e : inpToEdges.get(toEdg)) {
                mToEdges.put(toEdg, e);
            }
        }

        markDirty();
        return this;
    }
    
    /**
     * Utility method heavily used in the MRCompiler
     * Adds the leaf operator to the plan and connects
     * all existing leaves to the new leaf
     * @param leaf
     * @throws PlanException 
     */
    public void addAsLeaf(E leaf) throws PlanException {
        List<E> ret = new ArrayList<E>();
        for (E operator : getLeaves()) {
            ret.add(operator);
        }
        add(leaf);
        for (E oper : ret) {
            connect(oper, leaf);
        }
    }
    
    public boolean isSingleLeafPlan() {
        List<E> tmpList = getLeaves() ;
        return tmpList.size() == 1 ;
    }

    public int size() {
        return mKeys.size() ;
    }

    /**
     * Given two connected nodes add another node between them.
     * 'newNode' will be placed in same position in predecessor list as 'before' (old node).
     * @param after Node to insert this node after
     * @param newNode new node to insert.  This node must have already been
     * added to the plan.
     * @param before Node to insert this node before
     * @throws PlanException if it encounters trouble disconecting or
     * connecting nodes.
     */
    public void insertBetween(
            E after,
            E newNode,
            E before) throws PlanException {
        checkInPlan(newNode);
        if (!replaceNode(after, newNode, before, mFromEdges) || !replaceNode(before, newNode, after, mToEdges)) {
            PlanException pe = new PlanException("Attempt to insert between two nodes " +
                "that were not connected.");
            log.error(pe.getMessage());
            throw pe;
        }
        mFromEdges.put(newNode, before);
        mToEdges.put(newNode, after);
    }

    // replaces (src -> dst) entry in multiMap with (src -> replacement)
    private boolean replaceNode(E src, E replacement, E dst, MultiMap<E, E> multiMap) {
        Collection c = multiMap.get(src);
        if (c == null) return false;

        ArrayList al = new ArrayList(c);
        for(int i = 0; i < al.size(); ++i) {
            E to = (E)al.get(i);
            if(to.equals(dst)) {
                al.set(i, replacement);
                multiMap.removeKey(src);
                multiMap.put(src, al);
                return true;
            }
        }
        return false;
    }

    /**
     * Replace an existing node in the graph with a new node.  The new node
     * will be connected to all the nodes the old node was.  The old node will
     * be removed.
     * @param oldNode Node to be replaced
     * @param newNode Node to add in place of oldNode
     * @throws PlanException
     */
    public void replace(E oldNode, E newNode) throws PlanException {
        checkInPlan(oldNode);
        add(newNode);
        mToEdges = generateNewMap(oldNode, newNode, mToEdges);
        mFromEdges = generateNewMap(oldNode, newNode, mFromEdges);
        remove(oldNode);
    }

    private MultiMap<E, E> generateNewMap(
            E oldNode,
            E newNode,
            MultiMap<E, E> mm) {
        // First, replace the key
        Collection<E> targets = mm.get(oldNode);
        if (targets != null) {
            mm.removeKey(oldNode);
            mm.put(newNode, targets);
        }

        // We can't just do a remove and add in the map because of our
        // guarantee of not changing orders.  So we need to walk the lists and
        // put the new node in the same slot as the old.

        // Walk all the other keys and replace any references to the oldNode
        // in their targets.
        MultiMap<E, E> newMap = new MultiMap<E, E>(mm.size());
        for (E key : mm.keySet()) {
            Collection<E> c = mm.get(key);
            ArrayList<E> al = new ArrayList<E>(c);
            for (int i = 0; i < al.size(); i++) {
                if (al.get(i) == oldNode) al.set(i, newNode);
            }
            newMap.put(key, al);
        }
        return newMap;
    }

    /**
     * Remove a node in a way that connects the node's predecessor (if any)
     * with the node's successor (if any).  This function does not handle the
     * case where the node has multiple predecessors or successors.
     * @param node Node to be removed
     * @throws PlanException if the node has more than one predecessor or
     * successor.
     */
    public void removeAndReconnect(E node) throws PlanException {
        List<E> preds = getPredecessors(node);
        E pred = null;
        if (preds != null) {
            if (preds.size() > 1) {
                PlanException pe = new PlanException("Attempt to remove " +
                    " and reconnect for node with multiple predecessors.");
                log.error(pe.getMessage());
                throw pe;
            }
            pred = preds.get(0);
            disconnect(pred, node);
        }

        List<E> succs = getSuccessors(node);
        E succ = null;
        if (succs != null) {
            if (succs.size() > 1) {
                PlanException pe = new PlanException("Attempt to remove " +
                    " and reconnect for node with multiple successors.");
                log.error(pe.getMessage());
                throw pe;
            }
            succ = succs.get(0);
            disconnect(node, succ);
        }

        remove(node);
        if (pred != null && succ != null) connect(pred, succ);
    }

    /**
     * Remove a node in a way that connects the node's predecessor (if any)
     * with the node's successors (if any).  This function handles the
     * case where the node has *one* predecessor and one or more successors.
     * It replaces the predecessor in same position as node was in
     * each of the successors predecessor list(getPredecessors()), to 
     * preserve input ordering 
     * for eg, it is used to remove redundant project(*) from plan
     * which will have only one predecessor,but can have multiple success
     * @param node Node to be removed
     * @throws PlanException if the node has more than one predecessor
     */
    public void removeAndReconnectMultiSucc(E node) throws PlanException {

        // Before:
        //    A (predecessor (only one) )
        //  / |
        // X  B(nodeB)  Y(some predecessor of a Cn)
        //  / | \     / 
        // C1 C2  C3 ... (Successors)
        // should become
        // After:
        //    ___ A     Y
        //   /  / | \  /
        //  X  C1 C2 C3 ...
        // the variable names are from above example

    	E nodeB = node;
        List<E> preds = getPredecessors(nodeB);
        //checking pre-requisite conditions
        if (preds == null || preds.size() != 1) {
            Integer size = null;
            if(preds != null)
                size = preds.size();

            PlanException pe = new PlanException("Attempt to remove " +
                    " and reconnect for node with  " + size +
            " predecessors.");
            log.error(pe.getMessage());
            throw pe;
        }

        //A and C
        E nodeA = preds.get(0);
        Collection<E> nodeC = mFromEdges.get(nodeB);

        //checking pre-requisite conditions
        if (nodeC == null || nodeC.size() == 0) {
            PlanException pe = new PlanException("Attempt to remove " +
            " and reconnect for node with no successors.");
            log.error(pe.getMessage());
            throw pe;
        }   


        // replace B in A.succesors and add B.successors(ie C) to it
        replaceAndAddSucessors(nodeA, nodeB);
        
        // for all C(succs) , replace B(node) in predecessors, with A(pred)
        for(E c: nodeC) {
            Collection<E> sPreds = mToEdges.get(c);
            ArrayList<E> newPreds = new ArrayList<E>(sPreds.size());
            for(E p: sPreds){
                if(p == nodeB){
                    //replace
                    newPreds.add(nodeA);
                }
                else{
                    newPreds.add(p);
                }
            }
            mToEdges.removeKey(c);
            mToEdges.put(c,newPreds);
            
        }
        remove(nodeB); 
    }
    
    //removes entry  for succ in list of successors of nd, and adds successors
    // of succ in its place
    // @param nd - parent node whose entry for succ needs to be replaced
    // @param succ - see above
    private void replaceAndAddSucessors(E nd, E succ) throws PlanException {
       Collection<E> oldSuccs = mFromEdges.get(nd);
       Collection<E> repSuccs = mFromEdges.get(succ);
       ArrayList<E> newSuccs = new ArrayList<E>(oldSuccs.size() - 1 + repSuccs.size() );
       for(E s: oldSuccs){
           if(s == succ){
               newSuccs.addAll(repSuccs);
           }else{
               newSuccs.add(s);
           }
       }
       mFromEdges.removeKey(nd);
       mFromEdges.put(nd,newSuccs);
    }
    

    public void dump(PrintStream ps) {
        ps.println("Ops");
        for (E op : mOps.keySet()) {
            ps.println(op.name());
        }
        ps.println("from edges");
        for (E op : mFromEdges.keySet()) {
            for (E to : mFromEdges.get(op)) {
                ps.println(op.name() + " -> " + to.name());
            }
        }
        ps.println("to edges");
        for (E op : mToEdges.keySet()) {
            for (E to : mToEdges.get(op)) {
                ps.println(op.name() + " -> " + to.name());
            }
        }
    }
    
    public void explain(
            OutputStream out,
            PrintStream ps) throws VisitorException, IOException {
        PlanPrinter pp = new PlanPrinter(ps, this);
        pp.print(out);
    }


}
