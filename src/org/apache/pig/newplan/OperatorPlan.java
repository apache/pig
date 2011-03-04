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

import java.util.Iterator;
import java.util.List;

import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LOStore;

/**
 * An interface that defines graph operations on plans.  Plans are modeled as
 * graphs with restrictions on the types of connections and operations allowed.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface OperatorPlan {
    
    /**
     * Get number of nodes in the plan.
     * @return number of nodes in the plan
     */
    public int size();

    /**
     * Get all operators in the plan that have no predecessors.
     * @return all operators in the plan that have no predecessors, or
     * an empty list if the plan is empty.
     */
    public List<Operator> getSources();

    /**
     * Get all operators in the plan that have no successors.
     * @return all operators in the plan that have no successors, or
     * an empty list if the plan is empty.
     */
    public List<Operator> getSinks();

    /**
     * For a given operator, get all operators immediately before it in the
     * plan.
     * @param op operator to fetch predecessors of
     * @return list of all operators immediately before op, or an empty list
     * if op is a root.
     */
    public List<Operator> getPredecessors(Operator op);
    
    /**
     * For a given operator, get all operators immediately after it.
     * @param op operator to fetch successors of
     * @return list of all operators immediately after op, or an empty list
     * if op is a leaf.
     */
    public List<Operator> getSuccessors(Operator op);

    /**
     * For a given operator, get all operators softly immediately before it in the
     * plan.
     * @param op operator to fetch predecessors of
     * @return list of all operators immediately before op, or an empty list
     * if op is a root.
     */
    public List<Operator> getSoftLinkPredecessors(Operator op);
    
    /**
     * For a given operator, get all operators softly immediately after it.
     * @param op operator to fetch successors of
     * @return list of all operators immediately after op, or an empty list
     * if op is a leaf.
     */
    public List<Operator> getSoftLinkSuccessors(Operator op);
    
    /**
     * Add a new operator to the plan.  It will not be connected to any
     * existing operators.
     * @param op operator to add
     */
    public void add(Operator op);

    /**
     * Remove an operator from the plan.
     * @param op Operator to be removed
     * @throws FrontendException if the remove operation attempts to 
     * remove an operator that is still connected to other operators.
     */
    public void remove(Operator op) throws FrontendException;
    
    /**
     * Connect two operators in the plan, controlling which position in the
     * edge lists that the from and to edges are placed.
     * @param from Operator edge will come from
     * @param fromPos Position in the array for the from edge
     * @param to Operator edge will go to
     * @param toPos Position in the array for the to edge
     */
    public void connect(Operator from, int fromPos, Operator to, int toPos);
    
    /**
     * Connect two operators in the plan.
     * @param from Operator edge will come from
     * @param to Operator edge will go to
     */
    public void connect(Operator from, Operator to);
    
    /**
     * Create an soft edge between two nodes.
     * @param from Operator dependent upon
     * @param to Operator having the dependency
     */
    public void createSoftLink(Operator from, Operator to);
    
    /**
     * Remove an soft edge
     * @param from Operator dependent upon
     * @param to Operator having the dependency
     */
    public void removeSoftLink(Operator from, Operator to);
    
    /**
     * Disconnect two operators in the plan.
     * @param from Operator edge is coming from
     * @param to Operator edge is going to
     * @return pair of positions, indicating the position in the from and
     * to arrays.
     * @throws FrontendException if the two operators aren't connected.
     */
    public Pair<Integer, Integer> disconnect(Operator from, Operator to) throws FrontendException;


    /**
     * Get an iterator of all operators in this plan
     * @return an iterator of all operators in this plan
     */
    public Iterator<Operator> getOperators();
    
    /**
     * This is like a shallow comparison.
     * Two plans are equal if they have equivalent operators and equivalent 
     * structure.
     * @param other  object to compare
     * @return boolean if both the plans are equivalent
     * @throws FrontendException
     */
    public boolean isEqual( OperatorPlan other ) throws FrontendException;
    
    /**
     * This method replace the oldOperator with the newOperator, make all connection
     * to the new operator in the place of old operator
     * @param oldOperator operator to be replaced
     * @param newOperator operator to replace
     * @throws FrontendException
     */
    public void replace(Operator oldOperator, Operator newOperator) throws FrontendException;
    
    /**
     * This method remove a node operatorToRemove. It also Connect all its successors to 
     * predecessor/connect all it's predecessors to successor
     * @param operatorToRemove operator to remove
     * @throws FrontendException
     */
    public void removeAndReconnect(Operator operatorToRemove) throws FrontendException;

    /**
     * This method insert node operatorToInsert between pred and succ. Both pred and succ cannot be null
     * @param pred predecessor of inserted node after this method
     * @param operatorToInsert operato to insert
     * @param succ successor of inserted node after this method
     * @throws FrontendException
     */
    public void insertBetween(Operator pred, Operator operatorToInsert, Operator succ) throws FrontendException;

    /**
     * check if there is a path in the plan graph between the load operator to the store operator.
     * @param load load operator
     * @param store store operator
     * @return true if yes, no otherwise.
     */
    public boolean pathExists(Operator load, Operator store);
}
