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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.pig.impl.logicalLayer.OperatorKey;

//import org.apache.commons.collections.map.MultiValueMap;

/**
 * A generic graphing class for use by LogicalPlan, PhysicalPlan, etc.
 */
public abstract class OperatorPlan<E extends Operator> implements Iterable, Serializable {
    protected Map<E, OperatorKey> mOps;
    protected Map<OperatorKey, E> mKeys;
    protected MultiMap<E, E> mFromEdges;
    protected MultiMap<E, E> mToEdges;

    private List<E> mRoots;
    private List<E> mLeaves;

    
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
            throw new PlanException("Attempt to give operator of type " +
                from.getClass().getName() + " multiple outputs.  This operator does "
                + "not support multiple outputs.");
        }

        // Check to see if the to operator already has inputs, and if so
        // whether it supports multiple inputs.
        if (mToEdges.get(to) != null &&
                !to.supportsMultipleInputs()) {
            throw new PlanException("Attempt to give operator of type " +
                from.getClass().getName() + " multiple inputs.  This operator does "
                + "not support multiple inputs.");
        }

        mFromEdges.put(from, to);
        mToEdges.put(to, from);
    }

    /**
     * Remove an edge from between two nodes.
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
            throw new PlanException("Attempt to connect operator " +
                op.name() + " which is not in the plan.");
        }
    }
    
    public boolean isSingleLeafPlan() {
        List<E> tmpList = getLeaves() ;
        return tmpList.size() == 1 ;
    }


}
