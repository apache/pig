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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.pig.impl.plan.Operator;
import org.apache.pig.impl.plan.OperatorPlan;

/**
 * RuleMatcher contains the logic to determine whether a given rule matches.
 * This alone does not mean the rule will be applied.  Transformer.check()
 * still has to pass before Transfomer.transform() is called. 
 *
 */

public class RuleMatcher<O extends Operator, P extends OperatorPlan<O>> {

    private Rule<O, P> mRule;
	private List<O> mMatches;
    private P mPlan; // for convience.

    /**
     * Test a rule to see if it matches the current plan.
     * @param rule Rule to test for a match.
     * @return true if the plan matches.
     */
    public boolean match(Rule<O, P> rule) {
        mRule = rule;
        int sz = mRule.nodes.size();
        mMatches = new ArrayList<O>(sz);
        mPlan = mRule.transformer.getPlan();
        // Add sufficient slots in the matches array
        for (int i = 0; i < sz; i++) mMatches.add(null);
	    return beginMatch(mPlan.getRoots());
    }

	/**
	 * @return a list that with the instances of nodes that matched the
     * pattern defined by
	 * the rule.  The nodes will be in the vector in the order they are
	 * specified in the rule.
	 */
	List<O> getMatches() {
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
    private boolean beginMatch(List<O> nodes) {
        if (nodes == null) return false;
        for (O op : nodes) {
		    List<O> successors = new ArrayList<O>();
		    if (op.getClass().getName().equals(mRule.nodes.get(0))) {
			    mMatches.set(0, op);
			    // Follow the edge to see the next node we should be looking for.
			    Integer nextOpNum = mRule.edges.get(0);
                if (nextOpNum == null) {
				    // This was looking for a single node
				    return true;
			    }
			    successors = mPlan.getSuccessors(op);
                if (successors == null) return false;
                for (O successorOp : successors) {
				    if (continueMatch(successorOp, nextOpNum)) return true;
			    }
		    }

		    // That node didn't match.  Go to this nodes successors and see if
		    // any of them match.
		    successors = mPlan.getSuccessors(op);
		    if (beginMatch(successors)) return true;
	    }
	    // If we get here we haven't found it.
	    return false;
    }

	private boolean continueMatch(O current, Integer nodeNumber) {
	    if (current.getClass().getName() == mRule.nodes.get(nodeNumber)) {
		    mMatches.set(nodeNumber, current);

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
	    mMatches.set(nodeNumber, null);
	    return false;
    }

}
