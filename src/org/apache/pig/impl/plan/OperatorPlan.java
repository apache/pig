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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.pig.impl.logicalLayer.OperatorKey;

import org.apache.commons.collections.map.MultiValueMap;

/**
 * A generic graphing class for use by LogicalPlan, PhysicalPlan, etc.
 */
public abstract class OperatorPlan implements Iterable {
    protected Map<Operator, OperatorKey> mOps;
    protected Map<OperatorKey, Operator> mKeys;
    protected MultiValueMap mFromEdges;
    protected MultiValueMap mToEdges;

    private List<Operator> mRoots;
    private List<Operator> mLeaves;

    
    public OperatorPlan() {
        mRoots = new ArrayList<Operator>();
        mLeaves = new ArrayList<Operator>();
        mOps = new HashMap<Operator, OperatorKey>();
        mKeys = new HashMap<OperatorKey, Operator>();
        mFromEdges = new MultiValueMap();
        mToEdges = new MultiValueMap();
    }

    /**
     * Get a list of all nodes in the graph that are roots.  A root is defined to
     * be a node that has no input.
     */
    public List<Operator> getRoots() {
        if (mRoots.size() == 0 && mOps.size() > 0) {
            for (Operator op : mOps.keySet()) {
                if (mToEdges.getCollection(op) == null) {
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
    public List<Operator> getLeaves() {
        if (mLeaves.size() == 0 && mOps.size() > 0) {
            for (Operator op : mOps.keySet()) {
                if (mFromEdges.getCollection(op) == null) {
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
    public OperatorKey getOperatorKey(Operator op) {
        return mOps.get(op);
    }

    /**
     * Given an OperatorKey, find the associated operator.
     * @param opKey OperatorKey
     * @return associated operator.
     */
    public Operator getOperator(OperatorKey opKey) {
        return mKeys.get(opKey);
    }

    /**
     * Insert an operator into the plan.  This only inserts it as a node in
     * the graph, it does not connect it to any other operators.  That should
     * be done as a separate step using connect.
     * @param op Operator to add to the plan.
     */
    public void add(Operator op) {
        markDirty();
        mOps.put(op, op.getOperatorKey());
        mKeys.put(op.getOperatorKey(), op);
    }

    /**
     * Create an edge between two nodes.  The direction of the edge implies data
     * flow.
     * @param from Operator data will flow from.
     * @param to Operator data will flow to.
     * @throws IOException if this edge will create multiple inputs for an
     * operator that does not support multiple inputs or create multiple outputs
     * for an operator that does not support multiple outputs.
     */
    public void connect(Operator from, Operator to) throws IOException {
        markDirty();

        // Check that both nodes are in the plan.
        checkInPlan(from);
        checkInPlan(to);

        // Check to see if the from operator already has outputs, and if so
        // whether it supports multiple outputs.
        if (mFromEdges.getCollection(from) != null &&
                !from.supportsMultipleOutputs()) {
            throw new IOException("Attempt to give operator of type " +
                from.typeName() + " multiple outputs.  This operator does "
                + "not support multiple outputs.");
        }

        // Check to see if the to operator already has inputs, and if so
        // whether it supports multiple inputs.
        if (mToEdges.getCollection(to) != null &&
                !to.supportsMultipleInputs()) {
            throw new IOException("Attempt to give operator of type " +
                from.typeName() + " multiple inputs.  This operator does "
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
    public boolean disconnect(Operator from, Operator to) {
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
    public void remove(Operator op) {
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
    public Collection<Operator> getPredecessors(Operator op) {
        return (Collection<Operator>)mToEdges.getCollection(op);
    }


    /**
     * Find all of the nodes that have edges from the indicated node to
     * themselves.
     * @param op Node to look from
     * @return Collection of nodes.
     */
    public Collection<Operator> getSuccessors(Operator op) {
        return (Collection<Operator>)mFromEdges.getCollection(op);
    }

    public Iterator<Operator> iterator() { 
        return mOps.keySet().iterator();
    }

    private void markDirty() {
        mRoots.clear();
        mLeaves.clear();
    }

    private void removeEdges(Operator op,
                             MultiValueMap fromMap,
                             MultiValueMap toMap) {
        // Find all of the from edges, as I have to remove all the associated to
        // edges.  Need to make a copy so we can delete from the map without
        // screwing up our iterator.
        Collection c = fromMap.getCollection(op);
        if (c == null) return;

        ArrayList al = new ArrayList(c);
        Iterator i = al.iterator();
        while (i.hasNext()) {
            Operator to = (Operator)i.next();
            toMap.remove(to, op);
            fromMap.remove(op, to);
        }
    }

    private void checkInPlan(Operator op) throws IOException {
        if (mOps.get(op) == null) {
            throw new IOException("Attempt to connect operator " +
                op.name() + " which is not in the plan.");
        }
    }


}
