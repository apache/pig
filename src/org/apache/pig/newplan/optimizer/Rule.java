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

package org.apache.pig.newplan.optimizer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.OperatorSubPlan;

/**
 * Rules describe a pattern of operators.  They also reference a Transformer.
 * If the pattern of operators is found one or more times in the provided plan,
 * then the optimizer will use the associated Transformer to transform the
 * plan.
 * Note: the pattern matching logic implemented here has a limitation 
 * that it assumes that all the leaves in the pattern are siblings. See more 
 * detailed description here - https://issues.apache.org/jira/browse/PIG-1742
 * If new rules use patterns that don't work with this limitation, the pattern
 * match logic will need to be updated. 
 */
public abstract class Rule {

    protected String name = null;
    protected OperatorPlan pattern;
    transient protected OperatorPlan currentPlan;
    protected static final Log log = LogFactory.getLog(Rule.class);
    private transient Set<Operator> matchedNodes = new HashSet<Operator>();
    private boolean mandatory;
    private boolean skipListener = false;
    
    /**
     * Create this rule by using the default pattern that this rule provided
     * @param n Name of this rule
     * @param mandatory if it is set to false, this rule can be disabled by user
     */
    public Rule(String n, boolean mandatory) {
        name = n;    
        pattern = buildPattern();
        this.mandatory = mandatory;
    }
    
    /**
     * @param n Name of this rule
     * @param p Pattern to look for.
     */
    public Rule(String n, OperatorPlan p) {
        name = n;    
        pattern = p;
    }

    /**
     * Build the pattern that this rule will look for
     * @return  the pattern to look for by this rule
     */
    abstract protected OperatorPlan buildPattern();
    
    /**
     * Get the transformer for this rule.  Abstract because the rule
     * may want to choose how to instantiate the transformer.  
     * This should never return a cached transformer, it should
     * always return a fresh one with no state.
     * @return Transformer to use with this rule
     */
    abstract public Transformer getNewTransformer(); 
    
    /**
     * Return the pattern to be matched for this rule
     * @return the pattern to be matched for this rule
     */
    public OperatorPlan getPattern() {
        return pattern;
    }
    
    protected boolean isSkipListener() {
        return this.skipListener;
    }
    
    protected void setSkipListener(boolean skip) {
        this.skipListener = skip;
    }
    
    /**
     * Search for all the sub-plans that matches the pattern
     * defined by this rule. 
     * See class description above for limitations on the patterns supported.
     * @return A list of all matched sub-plans. The returned plans are
     *        partial views of the original OperatorPlan. Each is a 
     *        sub-set of the original plan and represents the same
     *        topology as the pattern, but operators in the returned plan  
     *        are the same objects as the original plan. Therefore, 
     *        a call getPlan() from any node in the return plan would 
     *        return the original plan.
     *        
     * @param plan the OperatorPlan to look for matches to the pattern
     */
    public List<OperatorPlan> match(OperatorPlan plan) throws FrontendException {
        currentPlan = plan;
        
        List<Operator> leaves = pattern.getSinks();
        
        Iterator<Operator> iter = plan.getOperators();
        List<OperatorPlan> matchedList = new ArrayList<OperatorPlan>();       
        matchedNodes.clear();
       
        while(iter.hasNext()) {
            Operator op = iter.next();
           
            // find a node that matches the first leaf of the pattern
            if (match(op, leaves.get(0))) {
                List<Operator> planOps = new ArrayList<Operator>();
                planOps.add(op);
                                
                // if there is more than 1 leaves in the pattern, we check 
                // if other leaves match the siblings of this node
                if (leaves.size()>1) {
                    boolean matched = true;
                    
                    
                    List<Operator> preds = null;
                    preds = plan.getPredecessors(op);
                    
                    // if this node has no predecessor, it must be a root
                    if (preds == null) {
                        preds = new ArrayList<Operator>();
                        preds.add(null);
                    }
                    
                    for(Operator s: preds) {
                        matched = true;
                        List<Operator> siblings = null;
                        if (s != null) {
                            siblings = plan.getSuccessors(s);
                        }else{
                            // for a root, we get its siblings by getting all roots
                            siblings = plan.getSources();
                        }

                        int index = siblings.indexOf(op);
                        if (siblings.size()-index < leaves.size()) {
                            continue;
                        }
                    
                        
                        for(int j=1; j<leaves.size(); j++) {
                            if (!match(siblings.get(index+j), leaves.get(j))) {
                                matched = false;
                                break;
                            }
                        }     
                        
                        if (matched) {
                            for(int j=1; j<leaves.size(); j++) {
                                planOps.add(siblings.get(index+j));
                            }
                        }
                    
                    }
                   
                    // we have move on to next operator as this one doesn't have siblings to
                    // match all the leaves
                    if (!matched) {
                        continue;
                    }
                }
                
              
                PatternMatchOperatorPlan match = new PatternMatchOperatorPlan(plan);
                if (match.check(planOps)) {
                    // we find a matched pattern,
                    // add the operators into matchedNodes
                    Iterator<Operator> iter2 = match.getOperators();                      
                    while(iter2.hasNext()) {
                        Operator opt = iter2.next();
                        matchedNodes.add(opt);                        
                    }
                    
                    // add pattern
                    matchedList.add(match);                                                
                }
            }
        }
        
        return matchedList;
    }     

    public String getName() {
        return name;
    }
    
    public boolean isMandatory() {
        return mandatory;
    }
    
    /** 
     * Check if two operators match each other, we want to find matches
     * that don't share nodes
     */
    private boolean match(Operator planNode, Operator patternNode) {
        return planNode.getClass().equals(patternNode.getClass()) && !matchedNodes.contains(planNode);
    }
    
 
    private class PatternMatchOperatorPlan extends OperatorSubPlan {
        
        public PatternMatchOperatorPlan(OperatorPlan basePlan) {
            super(basePlan);
        }    	    	
        
        protected boolean check(List<Operator> planOps) throws FrontendException {
            List<Operator> patternOps = pattern.getSinks();
            if (planOps.size() != patternOps.size()) {
                return false;
            }
            
            for(int i=0; i<planOps.size(); i++) {
                Stack<Operator> s = new Stack<Operator>();
                if (!check(planOps.get(i), patternOps.get(i), s)) {
                    return false;
                }
                Iterator<Operator> iter = s.iterator();
                while(iter.hasNext()) {
                    add(iter.next());
                }
            }
            
            if (size() == pattern.size()) {
                return true;
            }
            
            return false;
        }
        
        /**
         * Check if the plan operator and its sub-tree has a match to the pattern 
         * operator and its sub-tree. This algorithm only search and return one match.
         * It doesn't recursively search for all possible matches. For example,
         * for a plan that looks like
         *                   join
         *                  /     \
         *                 load   load
         * if we are looking for join->load pattern, only one match will be returned instead
         * of two, so that the matched subsets don't share nodes.
         */ 
        private boolean check(Operator planOp, Operator patternOp, Stack<Operator> opers) throws FrontendException {
            if (!match(planOp, patternOp)) {
                return false;
            }
                 
            // check if their predecessors match
            List<Operator> preds1 = getBasePlan().getPredecessors(planOp);
            List<Operator> preds2 = pattern.getPredecessors(patternOp);
            if (preds1 == null && preds2 != null) {
                return false;
            }
            
            if (preds1 != null && preds2 != null && preds1.size() < preds2.size()) {
                return false;
            }
            
            // we've reached the root of the pattern, so a match is found
            if (preds2 == null || preds2.size() == 0) {       
                opers.push(planOp);
                return true;
            }
            
            int index = 0;            
            // look for predecessors 
            while(index < preds1.size()) {
                boolean match = true;
                if (match(preds1.get(index), preds2.get(0))) {
                    if ( (preds1.size() - index) < preds2.size()) {
                        return false;
                    }
                             
                    int oldSize = opers.size();
                    for(int i=0; i<preds2.size(); i++) {
                        if (!check(preds1.get(index+i), preds2.get(i), opers)) {
                            for(int j=opers.size(); j>oldSize; j--) {
                                opers.pop();                                
                            }
                            match = false;
                            break;
                        }
                    }
                    if (match) {
                        opers.push(planOp);
                        return true;
                    }
                }
                index++;
            }
            
            return false;
        }    
    }
}
