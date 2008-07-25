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
package org.apache.pig.impl.plan.optimizer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.apache.pig.impl.plan.Operator;
import org.apache.pig.impl.plan.OperatorPlan;
import org.apache.pig.impl.plan.VisitorException;

/**
 * RuleMatcher contains the logic to determine whether a given rule matches.
 * This alone does not mean the rule will be applied.  Transformer.check()
 * still has to pass before Transfomer.transform() is called. 
 *
 */

public class RuleMatcher<O extends Operator, P extends OperatorPlan<O>> {

    private Rule<O, P> mRule;
    private List<O> mMatch;
    private List<List<O>> mMatches = new ArrayList<List<O>>();
    private P mPlan; // for convience.
    Set<O> seen = new HashSet<O>();

    /**
     * Test a rule to see if it matches the current plan. Save all matched nodes using BFS
     * @param rule Rule to test for a match.
     * @return true if the plan matches.
     */
    public boolean match(Rule<O, P> rule) {
        mRule = rule;
        
        mPlan = mRule.transformer.getPlan();
        mMatches.clear();
        
        if (mRule.algo == Rule.WalkerAlgo.DependencyOrderWalker)
        	DependencyOrderWalker();
        else if (mRule.algo == Rule.WalkerAlgo.DepthFirstWalker)
        	DepthFirstWalker();        
        
        return (mMatches.size()!=0);
    }
    
    private void DependencyOrderWalker()
    {
        List<O> fifo = new ArrayList<O>();
        Set<O> seen = new HashSet<O>();
        List<O> leaves = mPlan.getLeaves();
        if (leaves == null) return;
        for (O op : leaves) {
        	BFSDoAllPredecessors(op, seen, fifo);
        }

        for (O op: fifo) {
        	if (beginMatch(op))
			{
        		mMatches.add(mMatch);
			}
        }
    }
    
    private void BFSDoAllPredecessors(O node, Set<O> seen, Collection<O> fifo)  {
		if (!seen.contains(node)) {
		// We haven't seen this one before.
		Collection<O> preds = mPlan.getPredecessors(node);
		if (preds != null && preds.size() > 0) {
		// Do all our predecessors before ourself
			for (O op : preds) {
				BFSDoAllPredecessors(op, seen, fifo);
			}
		}
		// Now do ourself
		seen.add(node);
		fifo.add(node);
		}
    }
    
    private void DepthFirstWalker()
    {
    	Set<O> seen = new HashSet<O>();
        DFSVisit(null, mPlan.getRoots(), seen);
    }
    
    private void DFSVisit(O node, Collection<O> successors,Set<O> seen)
    {
        if (successors == null) return;
        for (O suc : successors) {
            if (seen.add(suc)) {
            	if (beginMatch(suc))
            		mMatches.add(mMatch);
                Collection<O> newSuccessors = mPlan.getSuccessors(suc);
                DFSVisit(suc, newSuccessors, seen);
            }
        }
    }

    /**
     * @return first occurrence of matched list of nodes that with the instances of nodes that matched the
     * pattern defined by
     * the rule.  The nodes will be in the vector in the order they are
     * specified in the rule.
     */
    List<O> getMatches() {
        if (mMatches.size()>=1)
            return mMatches.get(0);
        return null;

    }

    /**
     * @return all occurrences of matches. lists of nodes that with the instances of nodes that matched the
     * pattern defined by
     * the rule.  The nodes will be in the vector in the order they are
     * specified in the rule.
     */
    List<List<O>> getAllMatches() {
        return mMatches;
    }

    /*
     * This pattern matching is fairly simple and makes some important
     * assumptions.
     * 1)  The pattern to be matched must be expressable as a simple list.  That
     *     is it can match patterns like: 1->2, 2->3.  It cannot match patterns
     *     like 1->2, 1->3.
     * 2)  The pattern must always begin with the first node in the nodes array.
     *     After that it can go where it wants.  So 1->3, 3->4 is legal.  A
     *     pattern of 2->1 is not.
     *
     */
    private boolean beginMatch(O node) {
        if (node == null) return false;
        
        int sz = mRule.nodes.size();
        mMatch = new ArrayList<O>(sz);
        // Add sufficient slots in the matches array
        for (int i = 0; i < sz; i++) mMatch.add(null);
        
        List<O> successors = new ArrayList<O>();
        if (node.getClass().getName().equals(mRule.nodes.get(0))) {
            mMatch.set(0, node);
            // Follow the edge to see the next node we should be looking for.
            Integer nextOpNum = mRule.edges.get(0);
            if (nextOpNum == null) {
                // This was looking for a single node
                return true;
            }
            successors = mPlan.getSuccessors(node);
            if (successors == null) return false;
            for (O successorOp : successors) {
                if (continueMatch(successorOp, nextOpNum)) return true;
            }
        }
        // If we get here we haven't found it.
        return false;
    }

    private boolean continueMatch(O current, Integer nodeNumber) {
        if (current.getClass().getName() == mRule.nodes.get(nodeNumber)) {
            mMatch.set(nodeNumber, current);

            // Follow the edge to see the next node we should be looking for.
            Integer nextOpNum = mRule.edges.get(nodeNumber);
            if (nextOpNum == null) {
                // We've comleted the match
                return true;
            }
            List<O> successors = new ArrayList<O>();
            successors = mPlan.getSuccessors(current);
            if (successors == null) return false;
            for (O successorOp : successors) {
                if (continueMatch(successorOp, nextOpNum)) return true;
            }
        } else if (!mRule.required.get(nodeNumber)) {
            // This node was optional, so it's okay if we don't match, keep
            // going anyway.  Keep looking for the current node (don't find our
            // successors, but look for the next edge.
            Integer nextOpNum = mRule.edges.get(nodeNumber);
            if (nextOpNum == null) {
                // We've comleted the match
                return true;
            }
            if (continueMatch(current, nextOpNum)) return true;
        }

        // We can arrive here either because we didn't match at this node or
        // further down the line.  One way or another we need to remove ourselves
        // from the match vector and return false.
        mMatch.set(nodeNumber, null);
        return false;
    }

}
