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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.pig.impl.util.Pair;

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
    
    public void add(Operator op) {
        operators.add(op);
    }

    public void connect(Operator from, int fromPos, Operator to, int toPos) {
        throw new UnsupportedOperationException("connect() can not be called on OperatorSubPlan");
    }

    public void connect(Operator from, Operator to) {
        throw new UnsupportedOperationException("connect() can not be called on OperatorSubPlan");
    }

    public Pair<Integer, Integer> disconnect(Operator from, Operator to) throws IOException {
        throw new UnsupportedOperationException("disconnect() can not be called on OperatorSubPlan");
    }

    public List<Operator> getSinks() {
        if (leaves.size() == 0 && operators.size() > 0) {
            for (Operator op : operators) {       
                try {
                    if (getSuccessors(op) == null) {
                        leaves.add(op);
                    }
                }catch(Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return leaves;
    }

    public Iterator<Operator> getOperators() {
        return operators.iterator();
    }

    public List<Operator> getPredecessors(Operator op) throws IOException {
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

    public List<Operator> getSources() {
        if (roots.size() == 0 && operators.size() > 0) {
            for (Operator op : operators) {       
                try {
                    if (getPredecessors(op) == null) {
                        roots.add(op);
                    }
                }catch(Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return roots;
    }

    public List<Operator> getSuccessors(Operator op) throws IOException {
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

    public void remove(Operator op) throws IOException {
        operators.remove(op);
        leaves.clear();
        roots.clear();
    }

    public int size() {
        return operators.size();
    }

    @Override
    public boolean isEqual(OperatorPlan other) {		
        return BaseOperatorPlan.isEqual(this, other);
    }    
}
