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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LOStore;

/**
 * Class to represent a view of a plan. The view contains a subset of the plan.
 * All the operators returned from the view are the same objects to the operators
 * in its base plan. It is used to represent match results. 
 *
 */
public class OperatorSubPlan implements OperatorPlan {

    private OperatorPlan basePlan;
    private List<Operator> roots;
    private List<Operator> leaves;
    private Set<Operator> operators;

    public OperatorSubPlan(OperatorPlan base) {
        basePlan = base;
        roots = new ArrayList<Operator>();
        leaves = new ArrayList<Operator>();
        operators = new HashSet<Operator>();
    }                
    
    public OperatorPlan getBasePlan() {
        return basePlan;
    }
    
    @Override
    public void add(Operator op) {
        operators.add(op);
        leaves.clear();
        roots.clear();
    }

    @Override
    public void connect(Operator from, int fromPos, Operator to, int toPos) {
        throw new UnsupportedOperationException("connect() can not be called on OperatorSubPlan");
    }

    @Override
    public void connect(Operator from, Operator to) {
        throw new UnsupportedOperationException("connect() can not be called on OperatorSubPlan");
    }

    @Override
    public Pair<Integer, Integer> disconnect(Operator from, Operator to) throws FrontendException {
        throw new UnsupportedOperationException("disconnect() can not be called on OperatorSubPlan");
    }

    @Override
    public List<Operator> getSinks() {
        if (leaves.size() == 0 && operators.size() > 0) {
            for (Operator op : operators) {       
                if (getSuccessors(op) == null) {
                    leaves.add(op);
                }
            }
        }
        return leaves;
    }

    @Override
    public Iterator<Operator> getOperators() {
        return operators.iterator();
    }

    @Override
    public List<Operator> getPredecessors(Operator op) {
        List<Operator> l = basePlan.getPredecessors(op);
        List<Operator> list = null;
        if (l != null) {
            for(Operator oper: l) {
                if (operators.contains(oper)) {
                    if (list == null) {
                        list = new ArrayList<Operator>();
                    }
                    list.add(oper);
                }
            }
        }
        
        return list;
    }

    @Override
    public List<Operator> getSources() {
        if (roots.size() == 0 && operators.size() > 0) {
            for (Operator op : operators) {       
                if (getPredecessors(op) == null) {
                    roots.add(op);
                }
            }
        }
        return roots;
    }

    @Override
    public List<Operator> getSuccessors(Operator op) {
        List<Operator> l = basePlan.getSuccessors(op);
        List<Operator> list = null;
        if (l != null) {
            for(Operator oper: l) {
                if (operators.contains(oper)) {
                    if (list == null) {
                        list = new ArrayList<Operator>();
                    }
                    list.add(oper);
                }
            }
        }
        
        return list;
    }

    @Override
    public void remove(Operator op) throws FrontendException {
        operators.remove(op);
        leaves.clear();
        roots.clear();
    }

    @Override
    public int size() {
        return operators.size();
    }

    @Override
    public boolean isEqual(OperatorPlan other) throws FrontendException {        
        return BaseOperatorPlan.isEqual(this, other);
    }

    @Override
    public void createSoftLink(Operator from, Operator to) {
        throw new UnsupportedOperationException("connect() can not be called on OperatorSubPlan");
    }
    
    @Override
    public void removeSoftLink(Operator from, Operator to) {
        throw new UnsupportedOperationException("removeSoftLink() can not be called on OperatorSubPlan");
    }

    @Override
    public List<Operator> getSoftLinkPredecessors(Operator op) {
        return basePlan.getSoftLinkPredecessors(op);
    }

    @Override
    public List<Operator> getSoftLinkSuccessors(Operator op) {
        return basePlan.getSoftLinkSuccessors(op);
    }

    @Override
    public void insertBetween(Operator pred, Operator operatorToInsert, Operator succ)
            throws FrontendException {
        throw new UnsupportedOperationException("insertBetween() can not be called on OperatorSubPlan");
        
    }

    @Override
    public void removeAndReconnect(Operator operatorToRemove)
            throws FrontendException {
        throw new UnsupportedOperationException("removeAndReconnect() can not be called on OperatorSubPlan");
        
    }

    @Override
    public void replace(Operator oldOperator, Operator newOperator)
            throws FrontendException {
        throw new UnsupportedOperationException("replace() can not be called on OperatorSubPlan");
        
    }

    @Override
    public boolean pathExists(Operator from, Operator to) {
        // Not needed. Not implemented
        return false;
    }
}
