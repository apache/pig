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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.apache.pig.PigException;
import org.apache.pig.impl.plan.Operator;
import org.apache.pig.impl.plan.OperatorPlan;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.plan.optimizer.RuleOperator.NodeType;
import org.apache.pig.impl.util.Pair;

/**
 * RuleMatcher contains the logic to determine whether a given rule matches.
 * This alone does not mean the rule will be applied.  Transformer.check()
 * still has to pass before Transfomer.transform() is called. 
 *
 */

public class RuleMatcher<O extends Operator, P extends OperatorPlan<O>> {

    private Rule<O, P> mRule;
    private List<Pair<O, RuleOperator.NodeType>> mMatch;
    private List<List<Pair<O, RuleOperator.NodeType>>> mPrelimMatches = new ArrayList<List<Pair<O, RuleOperator.NodeType>>>();
    private List<List<O>> mMatches = new ArrayList<List<O>>();
    private P mPlan; // for convenience.
    private int mNumCommonNodes = 0;
    private List<RuleOperator> mCommonNodes = null;

    /**
     * Test a rule to see if it matches the current plan. Save all matched nodes using BFS
     * @param rule Rule to test for a match.
     * @return true if the plan matches.
     */
    public boolean match(Rule<O, P> rule) throws OptimizerException {
        mRule = rule;
        CommonNodeFinder commonNodeFinder = new CommonNodeFinder(mRule.getPlan());
        try {
            commonNodeFinder.visit();
            mNumCommonNodes = commonNodeFinder.getCount();
            mCommonNodes = commonNodeFinder.getCommonNodes();
        } catch (VisitorException ve) {
            int errCode = 2125;
            String msg = "Internal error. Problem in computing common nodes in the Rule Plan.";
            throw new OptimizerException(msg, errCode, PigException.BUG, ve);
        }
        mPlan = mRule.getTransformer().getPlan();
        mMatches.clear();
        mPrelimMatches.clear();
        
        if (mRule.getWalkerAlgo() == Rule.WalkerAlgo.DependencyOrderWalker)
        	DependencyOrderWalker();
        else if (mRule.getWalkerAlgo() == Rule.WalkerAlgo.DepthFirstWalker)
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
        		mPrelimMatches.add(mMatch);
			}
        }
        
        if(mPrelimMatches.size() > 0) {
            processPreliminaryMatches();
        }
    }
    
    /**
     * A method to compute the final matches
     */
    private void processPreliminaryMatches() {
        //The preliminary matches contain paths that match
        //the specification in the RulePlan. However, if there
        //are twigs and DAGs, then a further computation is required
        //to extract the nodes in the mPlan that correspond to the
        //roots of the RulePlan
        
        //compute the number of common nodes in each preliminary match
        
        List<List<O>> commonNodesPerMatch = new ArrayList<List<O>>();
        for(int i = 0; i < mPrelimMatches.size(); ++i) {
            commonNodesPerMatch.add(getCommonNodesFromMatch(mPrelimMatches.get(i)));
        }
        
        if(mNumCommonNodes == 0) {
            //the rule plan had simple paths
            
            //verification step
            //if any of the preliminary matches had common nodes 
            //then its an anomaly
            
            for(int i = 0; i < commonNodesPerMatch.size(); ++i) {
                if(commonNodesPerMatch.get(i) != null) {
                    //we have found common nodes when there should be none
                    //just return as mMatches will be empty
                    return;
                }
            }
            
            //pick the first node of each match and put them into individual lists
            //put the lists inside the list of lists mMatches
            
            for(int i = 0; i < mPrelimMatches.size(); ++i) {
                List<O> match = new ArrayList<O>();
                match.add(mPrelimMatches.get(i).get(0).first);
                mMatches.add(match);
            }
            //all the matches have been computed for the simple path
            return;
        } else {
            for(int i = 0; i < commonNodesPerMatch.size(); ++i) {
                int commonNodes = (commonNodesPerMatch.get(i) == null? 0 : commonNodesPerMatch.get(i).size());
                if(commonNodes != mNumCommonNodes) {
                    //if there are is a mismatch in the common nodes then we have a problem
                    //the rule plan states that we have mNumCommonNodes but we have commonNodes 
                    //in the match. Just return
                    
                    return;
                }
            }
        }
        
        //keep track of the matches that have been processed
        List<Boolean> processedMatches = new ArrayList<Boolean>();
        for(int i = 0; i < mPrelimMatches.size(); ++i) {
            processedMatches.add(false);
        }
        
        //a do while loop to handle single matches
        int outerIndex = 0;
        do {
            
            if(processedMatches.get(outerIndex)) {
               ++outerIndex;
               continue;
            }
            
            List<Pair<O, RuleOperator.NodeType>> outerMatch = mPrelimMatches.get(outerIndex);
            List<O> outerCommonNodes = commonNodesPerMatch.get(outerIndex);
            Set<O> outerSetCommonNodes = new HashSet<O>(outerCommonNodes);
            Set<O> finalIntersection = new HashSet<O>(outerCommonNodes);
            Set<O> cumulativeIntersection = new HashSet<O>(outerCommonNodes);
            List<O> patternMatchingRoots = new ArrayList<O>();
            Set<O> unionOfRoots = new HashSet<O>();
            boolean innerMatchProcessed = false;
            unionOfRoots.add(outerMatch.get(0).first);
            
            
            for(int innerIndex = outerIndex + 1; 
                (innerIndex < mPrelimMatches.size()) && (!processedMatches.get(innerIndex)); 
                ++innerIndex) {
                List<Pair<O, RuleOperator.NodeType>> innerMatch = mPrelimMatches.get(innerIndex);
                List<O> innerCommonNodes = commonNodesPerMatch.get(innerIndex);
                Set<O> innerSetCommonNodes = new HashSet<O>(innerCommonNodes);
                
                //we need to compute the intersection of the common nodes
                //the size of the intersection should be equal to the number
                //of common nodes and the type of each rule node class
                //if there is no match then it could be that we hit a match
                //for a different path, i.e., another pattern that matched
                //with a different set of nodes. In this case, we mark this
                //match as not processed and move onto the next match
                
                outerSetCommonNodes.retainAll(innerSetCommonNodes);
                
                if(outerSetCommonNodes.size() != mNumCommonNodes) {
                    //there was no match
                    //continue to the next match
                    continue;
                } else {
                    Set<O> tempCumulativeIntersection = new HashSet<O>(cumulativeIntersection);
                    tempCumulativeIntersection.retainAll(outerSetCommonNodes);
                    if(tempCumulativeIntersection.size() != mNumCommonNodes) {
                        //problem - there was a set intersection with a size mismatch
                        //between the cumulative intersection and the intersection of the
                        //inner and outer common nodes 
                        //set mMatches to empty and return
                        mMatches = new ArrayList<List<O>>();
                        return;
                    } else {
                        processedMatches.set(innerIndex, true);
                        innerMatchProcessed = true;
                        cumulativeIntersection = tempCumulativeIntersection;
                        unionOfRoots.add(innerMatch.get(0).first);
                    }
                }
            }
            
            cumulativeIntersection.retainAll(finalIntersection);
            if(cumulativeIntersection.size() != mNumCommonNodes) {
                //the cumulative and final intersections did not intersect
                //this could happen when each of the matches are disjoint
                //check if the innerMatches were processed at all
                if(innerMatchProcessed) {
                    //problem - the inner matches were processed and we did
                    //not find common intersections
                    mMatches = new ArrayList<List<O>>();
                    return;
                }
            }
            processedMatches.set(outerIndex, true);
            for(O node: unionOfRoots) {
                patternMatchingRoots.add(node);
            }
            mMatches.add(patternMatchingRoots);
            ++outerIndex;
        } while (outerIndex < mPrelimMatches.size() - 1);        
    }

    private List<O> getCommonNodesFromMatch(List<Pair<O, NodeType>> match) {
        List<O> commonNodes = null;
        //A lookup table to weed out duplicates
        Map<O, Boolean> lookup = new HashMap<O, Boolean>();
        for(int index = 0; index < match.size(); ++index) {
            if(match.get(index).second.equals(RuleOperator.NodeType.COMMON_NODE)) {
                if(commonNodes == null) {
                    commonNodes = new ArrayList<O>();
                }
                O node = match.get(index).first;
                //lookup the node under question
                //if the node is not found in the table
                //then we are examining it for the first time
                //add it to the output list and mark it as seen
                //else continue to the next iteration
                if(lookup.get(node) == null) {
                    commonNodes.add(node);
                    lookup.put(node, true);
                }
            }
        }
        return commonNodes;
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
            		mPrelimMatches.add(mMatch);
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
     * 1)  The pattern to be matched must be expressible as a graph.
     * 2)  The pattern must always begin with one of the root nodes in the rule plan.
     *     After that it can go where it wants.
     *
     */
    private boolean beginMatch(O node) {
        if (node == null) return false;
        
        mMatch = new ArrayList<Pair<O, RuleOperator.NodeType>>();
        
        List<O> nodeSuccessors;
        List<RuleOperator> ruleRoots = mRule.getPlan().getRoots();
        for(RuleOperator ruleRoot: ruleRoots) {
            if (node.getClass().getName().equals(ruleRoot.getNodeClass().getName()) || 
                    ruleRoot.getNodeType().equals(RuleOperator.NodeType.ANY_NODE)) {
                mMatch.add(new Pair<O, RuleOperator.NodeType>(node, ruleRoot.getNodeType()));
                // Follow the edge to see the next node we should be looking for.
                List<RuleOperator> ruleRootSuccessors = mRule.getPlan().getSuccessors(ruleRoot);
                if (ruleRootSuccessors == null) {
                    // This was looking for a single node
                    return true;
                }
                nodeSuccessors = mPlan.getSuccessors(node);
                if ((nodeSuccessors == null) || (nodeSuccessors.size() != ruleRootSuccessors.size())) {
                    //the ruleRoot has successors but the node does not
                    //OR
                    //the number of successors for the ruleRoot does not match 
                    //the number of successors for the node
                    return false; 
                }
                boolean foundMatch = false;
                for (O nodeSuccessor : nodeSuccessors) {
                    foundMatch |= continueMatch(nodeSuccessor, ruleRootSuccessors);
                }
                return foundMatch;
            }
        }
        // If we get here we haven't found it.
        return false;
    }

    private boolean continueMatch(O node, List<RuleOperator> ruleOperators) {
        for(RuleOperator ruleOperator: ruleOperators) {
            if (node.getClass().getName().equals(ruleOperator.getNodeClass().getName()) || 
                    ruleOperator.getNodeType().equals(RuleOperator.NodeType.ANY_NODE)) {
                mMatch.add(new Pair<O, RuleOperator.NodeType>(node,ruleOperator.getNodeType()));
    
                // Follow the edge to see the next node we should be looking for.
                List<RuleOperator> ruleOperatorSuccessors = mRule.getPlan().getSuccessors(ruleOperator);
                if (ruleOperatorSuccessors == null) {
                    // We've completed the match
                    return true;
                }
                List<O> nodeSuccessors;
                nodeSuccessors = mPlan.getSuccessors(node);
                if ((nodeSuccessors == null) || 
                        (nodeSuccessors.size() != ruleOperatorSuccessors.size())) {
                    //the ruleOperator has successors but the node does not
                    //OR
                    //the number of successors for the ruleOperator does not match 
                    //the number of successors for the node
                    return false;
                }
                boolean foundMatch = false;
                for (O nodeSuccessor : nodeSuccessors) {
                    foundMatch |= continueMatch(nodeSuccessor, ruleOperatorSuccessors);
                }
                return foundMatch;
            }
    
            // We can arrive here either because we didn't match at this node or
            // further down the line.  One way or another we need to remove ourselves
            // from the match vector and return false.
            //SMS - I don't think we need this as mMatch will be discarded anyway
            //mMatch.set(nodeNumber, null);
            return false;
        }
        return false;
    }

}
