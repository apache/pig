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
package org.apache.pig.impl.logicalLayer;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;

import org.apache.pig.PigException;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.ProjectionMap;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.impl.util.Pair;

/**
 * A class to visit all the projects and change them to attach to a new
 * node.  This class overrides all of the relational operators visit
 * methods because it does not want to visit contained plans.
 */
public class ProjectFixerUpper extends LOVisitor {

    private LogicalOperator mNewNode;
    private LogicalOperator mOldNode;
    private int mPredecessorIndex;
    private boolean mUseOldNode;
    private LogicalOperator mContainingNode;
    private Map<Integer, Integer> mProjectionMapping;

    public ProjectFixerUpper(
            LogicalPlan plan,
            LogicalOperator oldNode,
            int oldNodeIndex,
            LogicalOperator newNode,
            boolean useOldNode,
            LogicalOperator containingNode) throws VisitorException {
        this(plan, oldNode, newNode, (Map<Integer, Integer>)null);
        if(containingNode == null) {
            int errCode = 1097;
            String msg = "Containing node cannot be null.";
            throw new VisitorException(msg, errCode, PigException.INPUT);
        }
        if(oldNodeIndex < 0) {
            int errCode = 1098;
            String msg = "Node index cannot be negative.";
            throw new VisitorException(msg, errCode, PigException.INPUT);
        }
        mContainingNode = containingNode;
        mPredecessorIndex = oldNodeIndex;
        mUseOldNode = useOldNode;
    }
    
    public ProjectFixerUpper(
            LogicalPlan plan,
            LogicalOperator oldNode,
            LogicalOperator newNode, Map<Integer, Integer> projectionMapping) throws VisitorException {
        super(plan,
            new DepthFirstWalker<LogicalOperator, LogicalPlan>(plan));
        if(oldNode == null) {
            int errCode = 1099;
            String msg = "Node to be replaced cannot be null.";
            throw new VisitorException(msg, errCode, PigException.INPUT);
        }
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
            if (p.getExpression().equals(mOldNode)) {
                if (mNewNode == null) {
                    int errCode = 1100;
                    String msg = "Replacement node cannot be null.";
                    throw new VisitorException(msg, errCode, PigException.INPUT);
                }

                // Change the expression
                p.setExpression(mNewNode);
                if(p.isStar()) {
                    //its a project(*)
                    //no need of changing it
                    return;
                }
                if (mContainingNode != null) {
                    // use the projection mapping of mOldNode or mNewNode along
                    // with that of mContainingNode
                    // to figure out the mapping and replace the columns
                    // appropriately
                    int oldNodeColumn = p.getCol();

                    if (mUseOldNode) {
                        // use mOldNode's projection map and find the column number
                        // from the input that
                        ProjectionMap oldNodeMap = mOldNode.getProjectionMap();

                        if (oldNodeMap == null) {
                            // bail out if the projection map is null
                            int errCode = 2156;
                            String msg = "Error while fixing projections. Projection map of node to be replaced is null.";
                            throw new VisitorException(msg, errCode,
                                    PigException.BUG);
                        }

                        if (!oldNodeMap.changes()) {
                            // no change required
                            return;
                        }

                        MultiMap<Integer, ProjectionMap.Column> oldNodeMappedFields = oldNodeMap
                                .getMappedFields();
                        if (oldNodeMappedFields == null) {
                            // there is no mapping available bail out
                            int errCode = 2157;
                            String msg = "Error while fixing projections. No mapping available in old predecessor to replace column.";
                            throw new VisitorException(msg, errCode,
                                    PigException.BUG);

                        }

                        List<ProjectionMap.Column> columns = (List<ProjectionMap.Column>) oldNodeMappedFields.get(oldNodeColumn);
                        
                        if (columns == null) {
                            // there is no mapping for oldNodeColumn
                            // it could be an added field; bail out
                            int errCode = 2158;
                            String msg = "Error during fixing projections. No mapping available in old predecessor for column to be replaced.";
                            throw new VisitorException(msg, errCode,
                                    PigException.BUG);
                        }

                        boolean foundMapping = false;
                        for (ProjectionMap.Column column : columns) {
                        	Pair<Integer, Integer> pair = column.getInputColumn();
                            if (pair.first.equals(mPredecessorIndex)) {
                                ArrayList<Integer> newColumns = new ArrayList<Integer>();
                                newColumns.add(pair.second);
                                p.setProjection(newColumns);
                                foundMapping = true;
                                break;
                            }
                        }
                        if (!foundMapping) {
                            // did not find a mapping - bail out
                            int errCode = 2159;
                            String msg = "Error during fixing projections. Could not locate replacement column from the old predecessor.";
                            throw new VisitorException(msg, errCode,
                                    PigException.BUG);
                        } else {
                            // done with adjusting the column number of the
                            // project
                            return;
                        }
                    } else {
                        // here the projection mapping of new node has to be
                        // used to figure out
                        // the reverse mapping. From the newNode projection
                        // mapping search for
                        // the key whose value contains the pair (mOldNodeIndex,
                        // oldNodeColumn)

                        ProjectionMap newNodeMap = mNewNode.getProjectionMap();
                        if (newNodeMap == null) {
                            // did not find a mapping - bail out
                            int errCode = 2160;
                            String msg = "Error during fixing projections. Projection map of new predecessor is null.";
                            throw new VisitorException(msg, errCode,
                                    PigException.BUG);
                        }
                        
                        if (!newNodeMap.changes()) {
                            // no change required
                            return;
                        }

                        MultiMap<Integer, ProjectionMap.Column> newNodeMappedFields = newNodeMap
                                .getMappedFields();
                        if (newNodeMappedFields == null) {
                            // there is no mapping available bail out
                            int errCode = 2161;
                            String msg = "Error during fixing projections. No mapping available in new predecessor to replace column.";
                            throw new VisitorException(msg, errCode,
                                    PigException.BUG);

                        }

                        boolean foundMapping = false;
                        for (Integer key : newNodeMappedFields.keySet()) {

                            List<ProjectionMap.Column> columns = (List<ProjectionMap.Column>) newNodeMappedFields
                                    .get(key);
                            if (columns == null) {
                                // should not happen
                                int errCode = 2162;
                                String msg = "Error during fixing projections. Could not locate mapping for column: "
                                        + key + " in new predecessor.";
                                throw new VisitorException(msg, errCode,
                                        PigException.BUG);
                            }

                            for (ProjectionMap.Column column : columns) {
                            	Pair<Integer, Integer> pair = column.getInputColumn();
                                if (pair.first.equals(mPredecessorIndex)
                                        && pair.second.equals(oldNodeColumn)) {
                                    ArrayList<Integer> newColumns = new ArrayList<Integer>();
                                    newColumns.add(key);
                                    p.setProjection(newColumns);
                                    foundMapping = true;
                                    break;
                                }
                            }

                            if (foundMapping) {
                                // done with adjusting the column number of the
                                // project
                                return;
                            }
                        }
                        if (!foundMapping) {
                            // did not find a mapping - bail out
                            int errCode = 2163;
                            String msg = "Error during fixing projections. Could not locate replacement column for column: "
                                    + oldNodeColumn
                                    + " in the new predecessor.";
                            throw new VisitorException(msg, errCode,
                                    PigException.BUG);
                        }
                    }
                }// end if for containing node != null
            }// end if for projection expression equals mOldNode

            // Remap the projection column if necessary
            if (mProjectionMapping != null && !p.isStar()) {
                List<Integer> oldProjection = p.getProjection();
                List<Integer> newProjection = new ArrayList<Integer>(
                        oldProjection.size());
                for (Integer i : oldProjection) {
                    Integer n = mProjectionMapping.get(i);
                    assert (n != null);
                    newProjection.add(n);
                }
            }
        } else {
            // TODO
            // not sure if we need this. the walker should take care of this
            p.getExpression().visit(this);
        }
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("old node: " + mOldNode);
        sb.append(" old node index: " + mPredecessorIndex);
        sb.append(" new node: " + mNewNode);
        sb.append(" containing node: " + mContainingNode);
        sb.append(" mProjectionMapping: " + mProjectionMapping);
        return sb.toString();
    }
}
