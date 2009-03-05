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
package org.apache.pig.impl.logicalLayer.optimizer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.PlanWalker;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.plan.optimizer.Transformer;
import org.apache.pig.impl.plan.optimizer.Transformer;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.LOFRJoin;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.logicalLayer.LOCogroup;
import org.apache.pig.impl.logicalLayer.LOFilter;
import org.apache.pig.impl.logicalLayer.LOForEach;
import org.apache.pig.impl.logicalLayer.LOGenerate;
import org.apache.pig.impl.logicalLayer.LOProject;
import org.apache.pig.impl.logicalLayer.LOSort;
import org.apache.pig.impl.logicalLayer.LOSplit;
import org.apache.pig.impl.logicalLayer.LOSplitOutput;
import org.apache.pig.impl.logicalLayer.LOVisitor;

public abstract class LogicalTransformer extends Transformer<LogicalOperator, LogicalPlan> {

    private final Log log = LogFactory.getLog(getClass());

    protected LogicalTransformer(
            LogicalPlan plan,
            PlanWalker<LogicalOperator, LogicalPlan> walker) {
        super(plan, walker);
    }

    /**
     * Rebuild schemas after a rule has transformed the tree.  This will first
     * null out existing schemas and then call getSchema to rebuild them.
     * @throws VisitorException, FrontendException
     */
    protected void rebuildSchemas() throws VisitorException, FrontendException {
        SchemaRemover sr = new SchemaRemover(mPlan);
        sr.visit();
        SchemaCalculator sc = new SchemaCalculator(mPlan);
        sc.visit();
        
    }

    /**
     * A class to visit all the projects and change them to attach to a new
     * node.  This class overrides all of the relational operators visit
     * methods because it does not want to visit contained plans.
     */
    private class ProjectFixerUpper extends LOVisitor {

        private LogicalOperator mNewNode;
        private LogicalOperator mOldNode;
        private Map<Integer, Integer> mProjectionMapping;

        ProjectFixerUpper(
                LogicalPlan plan,
                LogicalOperator oldNode,
                LogicalOperator newNode, Map<Integer, Integer> projectionMapping) {
            super(plan,
                new DepthFirstWalker<LogicalOperator, LogicalPlan>(plan));
            mNewNode = newNode;
            mOldNode = oldNode;
            mProjectionMapping = projectionMapping;
        }

        protected void visit(LOCogroup cg) throws VisitorException {
        }

        protected void visit(LOSort s) throws VisitorException {
        }

        protected void visit(LOFilter f) throws VisitorException {
        }

        protected void visit(LOSplit s) throws VisitorException {
        }

        protected void visit(LOSplitOutput s) throws VisitorException {
        }

        protected void visit(LOForEach f) throws VisitorException {
        }

        protected void visit(LOProject p) throws VisitorException {
            // Only switch the expression if this is a top level projection,
            // that is, this project is pointing to a relational operator
            // outside the plan).
            List<LogicalOperator> preds = mPlan.getPredecessors(p);
            if (preds == null || preds.size() == 0) {
                if(p.getExpression().equals(mOldNode))
                    // Change the expression
                    p.setExpression(mNewNode);

                // Remap the projection column if necessary
                if (mProjectionMapping != null && !p.isStar()) {
                    List<Integer> oldProjection = p.getProjection();
                    List<Integer> newProjection =
                        new ArrayList<Integer>(oldProjection.size());
                    for (Integer i : oldProjection) {
                        Integer n = mProjectionMapping.get(i);
                        assert(n != null);
                        newProjection.add(n);
                    }
                }
            } else {
                p.getExpression().visit(this);
            }
        }
    }

    /**
     * Insert a node in between two existing nodes.  This includes inserting
     * the node into the correct place in the plan and finding any projects in
     * successors and reconnecting them to the new node as well as rebuilding
     * all of the schemas.
     * @param after Node to insert the new node after
     * @param newNode New node to insert
     * @param before Node to insert this node before
     * @param projectionMapping A map that defines how projections in after
     * relate to projections in newnode.  Keys are the projection offsets in
     * after, values are the new offsets in newnode.  If this field is null,
     * then it will be assumed that the mapping is 1-1.
     * @throws VisitorException, FrontendException
     */
    protected void insertBetween(
            LogicalOperator after,
            LogicalOperator newNode,
            LogicalOperator before,
            Map<Integer, Integer> projectionMapping)
            throws VisitorException, FrontendException {
        // Insert it into the plan.
        mPlan.add(newNode);
        mPlan.insertBetween(after, newNode, before);
        fixUpContainedPlans(after, newNode, before, projectionMapping);
    }
    
    /**
     * Once a node has been inserted, inner plans associated with other nodes
     * may have references to the node that has been replaced or moved.  This
     * function walks those inner plans and patches up references.
     * @param after Node that has had a new node inserted after it.
     * @param newNode node that has been inserted
     * @param before Node that has had a new node inserted before it.
     * @param projectionMapping A map that defines how projections in after
     * relate to projections in newNode.  Keys are the projection offsets in
     * after, values are the new offsets in newNode.  If this field is null,
     * then it will be assumed that the mapping is 1-1.
     * @throws VisitorException, FrontendException
     */
    protected void fixUpContainedPlans(
            LogicalOperator after,
            LogicalOperator newNode,
            LogicalOperator before,
            Map<Integer, Integer> projectionMapping) 
    throws VisitorException, FrontendException {
        // Fix up COGroup internal wiring
        if (before instanceof LOCogroup) {
            LOCogroup cg = (LOCogroup) before ;
            cg.switchGroupByPlanOp(after, newNode);
        }
        if (before instanceof LOFRJoin) {
            LOFRJoin frj = (LOFRJoin) before ;
            frj.switchJoinColPlanOp(after, newNode);
        }

        // Visit all the inner plans of before and change their projects to
        // connect to newNode instead of after.
        // Find right inner plan(s) to visit
        List<LogicalPlan> plans = new ArrayList<LogicalPlan>();
        if (before instanceof LOCogroup) {
            plans.addAll((((LOCogroup)before).getGroupByPlans()).values());
        } else if (before instanceof LOFRJoin) {
            plans.addAll((((LOFRJoin)before).getJoinColPlans()).values());
        }
        else if (before instanceof LOSort) {
            plans.addAll(((LOSort)before).getSortColPlans());
        } else if (before instanceof LOFilter) {
            plans.add(((LOFilter)before).getComparisonPlan());
        } else if (before instanceof LOSplit) {
            // In this case we have to find each of the Split outputs, and
            // add their plans.
            List<LogicalOperator> splitOutputs = mPlan.getSuccessors(before);
            for (LogicalOperator so : splitOutputs) {
                if (!(so instanceof LOSplitOutput)) {
                    String msg = "Found an LOSplit with an operator other " 
                        + "than LOSplitOutput after it!";
                    log.error(msg);
                    throw new VisitorException(msg);
                }
                plans.add(((LOSplitOutput)so).getConditionPlan());
            }
        } else if (before instanceof LOForEach) {
            plans.addAll(((LOForEach)before).getForEachPlans());
        }
        
        for (LogicalPlan lp : plans) {
            ProjectFixerUpper pfu =
                new ProjectFixerUpper(lp, after, newNode, projectionMapping);
            pfu.visit();
        }
    }

    /**
     * Insert a node in after an existing nodes.  This includes inserting
     * the node into the correct place in the plan and finding any projects in
     * successors and reconnecting them to the new node as well as rebuilding
     * all of the schemas.  This function
     * assumes that the node has only one predecessor.
     * @param after Node to insert the new node after
     * @param newNode New node to insert
     * @param projectionMapping A map that defines how projections in after
     * relate to projections in newnode.  Keys are the projection offsets in
     * after, values are the new offsets in newnode.  If this field is null,
     * then it will be assumed that the mapping is 1-1.
     * @throws VisitorException, FrontendException
     */
    protected void insertAfter(
            LogicalOperator after,
            LogicalOperator newNode,
            Map<Integer, Integer> projectionMapping)
            throws VisitorException, FrontendException {
        List<LogicalOperator> successors = mPlan.getSuccessors(after);
        if(successors==null) {
            mPlan.addAsLeaf(newNode);
            return;
        }
        if (successors.size() != 1) {
            throw new RuntimeException("insertAfter only valid to insert " + 
                "after a node with single output.");
        }
        insertBetween(after, newNode, successors.get(0), projectionMapping);
    }

    /**
     * Remove a node in the middle of a linear chain. This includes removing the node 
     * from the plan and reconnect the nodes before and after the node as well as rebuilding
     * all of the schemas. This node should have one and only one predecessor and zero or one 
     * successor
     * @param nodeToRemove Node to remove
     * @param projectionMapping A map that defines how projections in after
     * relate to projections in nodeToRemove.  Keys are the projection offsets in
     * after, values are the new offsets in nodeToRemove.  If this field is null,
     * then it will be assumed that the mapping is 1-1.
     * @throws VisitorException, FrontendException
     */
    protected void removeFromChain(
            LogicalOperator nodeToRemove,
            Map<Integer, Integer> projectionMapping)
            throws VisitorException, FrontendException {
    	
    	List<LogicalOperator> afterNodes = mPlan.getPredecessors(nodeToRemove);
    	if (afterNodes.size()!=1)
    		throw new RuntimeException("removeFromChain only valid to remove " + 
            	"node has one predecessor.");
    	List<LogicalOperator> beforeNodes = mPlan.getSuccessors(nodeToRemove);
    	if (beforeNodes!=null && beforeNodes.size()!=1)
    		throw new RuntimeException("removeFromChain only valid to remove " + 
        		"node has one successor.");
    	
    	// Get after and before node
    	LogicalOperator after = mPlan.getPredecessors(nodeToRemove).get(0);
    	LogicalOperator before = null;
    	if (beforeNodes!=null)
    		before = mPlan.getSuccessors(nodeToRemove).get(0);
    	
        // Remove nodeToRemove from plan
    	mPlan.remove(nodeToRemove);
    	if (before!=null)
    	{
	    	// Shortcut nodeToRemove.
	        mPlan.connect(after, before);
	
	        // Visit all the inner plans of before and change their projects to
	        // connect to after instead of nodeToRemove.
	        // Find right inner plan(s) to visit
	        List<LogicalPlan> plans = new ArrayList<LogicalPlan>();
	        if (before instanceof LOCogroup) {
	            plans.addAll((((LOCogroup)before).getGroupByPlans()).values());
	        } else if (before instanceof LOSort) {
	            plans.addAll(((LOSort)before).getSortColPlans());
	        } else if (before instanceof LOFilter) {
	            plans.add(((LOFilter)before).getComparisonPlan());
	        } else if (before instanceof LOSplitOutput) {
	            plans.add(((LOSplitOutput)before).getConditionPlan());
	        } else if (before instanceof LOForEach) {
	            plans.addAll(((LOForEach)before).getForEachPlans());
	        }
	        
	        for (LogicalPlan lp : plans) {
	            ProjectFixerUpper pfu =
	                new ProjectFixerUpper(lp, nodeToRemove, after, projectionMapping);
	            pfu.visit();
	        }
	
    	}

    	// Now rebuild the schemas
        // rebuildSchemas();
    }
    
}
