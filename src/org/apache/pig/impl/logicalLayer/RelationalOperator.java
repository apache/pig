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

import java.util.ArrayList;
import java.util.List;

import org.apache.pig.PigException;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.ProjectionMap;
import org.apache.pig.impl.plan.RequiredFields;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.plan.ProjectionMap.Column;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.impl.util.Pair;

public abstract class RelationalOperator extends LogicalOperator {
    private static final long serialVersionUID = 2L;
    /**
     * @param plan
     *            Logical plan this operator is a part of.
     * @param k
     *            Operator key to assign to this node.
     * @param rp
     *            degree of requested parallelism with which to execute this
     *            node.
     */
    public RelationalOperator(LogicalPlan plan, OperatorKey k, int rp) {
        super(plan, k, rp);
    }

    /**
     * @param plan
     *            Logical plan this operator is a part of.
     * @param k
     *            Operator key to assign to this node.
     */
    public RelationalOperator(LogicalPlan plan, OperatorKey k) {
        super(plan, k);
    }
    
    /**
     * Produce a map describing how this operator modifies its projection.
     * @return ProjectionMap null indicates it does not know how the projection
     * changes, for example a join of two inputs where one input does not have
     * a schema.
     */
    @Override
    public ProjectionMap getProjectionMap() {
        return null;
    };
    
    /**
     * Unset the projection map as if it had not been calculated.  This is used by
     * anyone who reorganizes the tree and needs to have projection maps recalculated.
     */
    @Override
    public void unsetProjectionMap() {
        mIsProjectionMapComputed = false;
        mProjectionMap = null;
    }

    /**
     * Regenerate the projection map by unsetting and getting the projection map
     */
    @Override
    public ProjectionMap regenerateProjectionMap() {
        try {
            regenerateSchema();
        } catch (Exception e) {
            
        }
        unsetProjectionMap();
        return getProjectionMap();
    }


    /**
     * Get a list of fields that this operator requires. This is not necessarily
     * equivalent to the list of fields the operator projects. For example, a
     * filter will project anything passed to it, but requires only the fields
     * explicitly referenced in its filter expression.
     * 
     * @return list of RequiredFields null indicates that the operator does not need any
     *         fields from its input.
     */
    public List<RequiredFields> getRequiredFields() {
        return null;
    }
    
    /**
     * Get relevant input columns of a particular output column. The resulting input columns 
     * are necessary components only to the output column. Input columns needed by the entire 
     * RelationalOperator thus indirectly contribute to the output columns are not counted. Those
     * are required columns.
     * eg1:
     * A = load 'a' AS (a0, a1, a2);
     * B = filter a by a0=='1';
     * 
     * Relevant input columns for B.$1 is A.a1 because A.a1 direct generate B.$1. A.a0 is needed
     * by the filter operator and it is considered as required fields for the relational operator.
     * 
     * eg2:
     * A = load 'a' AS (a0, a1);
     * B = load 'b' AS (b0, b1);
     * C = join A by a0, B by b0;
     * 
     * Relevant input columns for C.$0 is A.a0. Relevant input columns for C.$1 is A.a1.
     * 
     * eg3:
     * A = load 'a' AS (a0, a1);
     * B = load 'b' AS (b0, b1);
     * C = cogroup A by a0, B by b0;
     * 
     * Relevant input columns for C.$0 is A.a0, B.b0. Relevant input columns for C.$1 is A.*. Relevant input columns for C.$2 is B.*.
     * 
     * eg4:
     * A = load 'a' AS (a0, a1, a2);
     * B = foreach A generate a1, a0+a2;
     * 
     * Relevant input columns for B.$0 is A.a1. Relevant input columns for B.$1 is A.a0 and A.a2.
     * 
     * eg5:
     * A = load 'a' AS (a0, a1, a2);
     * B = foreach A generate a1, *;
     * 
     * Relevant input columns for B.$0 is A.a1. Relevant input columns for B.$1 is A.a0. 
     * Relevant input columns for B.$2 is A.a1. Relevant input columns for B.$3 is A.a2.
     * 
     * @param output output index. Only LOSplit have output other than 0 currently 
     * @param column output column
     * @return List of relevant input columns. null if Pig cannot determine relevant inputs or any error occurs
     */
    abstract public List<RequiredFields> getRelevantInputs(int output, int column) throws FrontendException; 

    public boolean pruneColumns(List<Pair<Integer, Integer>> columns)
            throws FrontendException {
        unsetSchema();
        getSchema();
        mIsProjectionMapComputed = false;
        getProjectionMap();
        return true;
    }

    public void pruneColumnInPlan(LogicalPlan plan, int column)
            throws FrontendException {
        TopLevelProjectFinder projectFinder = new TopLevelProjectFinder(plan);
        try {
            projectFinder.visit();
        } catch (VisitorException ve) {
            int errCode = 2196;
            throw new FrontendException("Exception when traversing inner plan",
                    errCode, PigException.BUG, ve);
        }
        for (LOProject loProject : projectFinder.getProjectSet()) {
            if (loProject.isStar()) {
                int errCode = 2197;
                throw new FrontendException(
                        "Cannot drop column which require *", errCode,
                        PigException.BUG);
            }
            int col = loProject.getCol();
            if (column < col) {
                loProject.getProjection().set(0, col - 1);
            }
        }
    }
    
    // insert a forEach after the operator. This forEach map columns in columnsToProject directly, and remove the rest
    public LogicalOperator insertPlainForEachAfter(List<Integer> columnsToProject) throws FrontendException {
        ArrayList<Boolean> flattenList = new ArrayList<Boolean>();
        ArrayList<LogicalPlan> generatePlans = new ArrayList<LogicalPlan>();
        String scope = getOperatorKey().scope;
        for (int pos : columnsToProject) {
            LogicalPlan projectPlan = new LogicalPlan();
            ExpressionOperator column = new LOProject(projectPlan, new OperatorKey(scope, NodeIdGenerator.getGenerator().getNextNodeId(scope)), this, pos);
            flattenList.add(false);
            projectPlan.add(column);
            generatePlans.add(projectPlan);
        }
        LOForEach forEach = new LOForEach(mPlan, new OperatorKey(scope, NodeIdGenerator.getGenerator().getNextNodeId(scope)), generatePlans, flattenList);
        LogicalOperator succ = mPlan.getSuccessors(this).get(0);

        MultiMap<Integer, Column> mappedFields = new MultiMap<Integer, Column>();
        List<Column> columns;
        for (int i=0;i<=getSchema().size();i++) {
            columns = new ArrayList<Column>();
            columns.add(new Column(new Pair<Integer, Integer>(0, i)));
            mappedFields.put(i, columns);
        }
        mPlan.add(forEach);
        mPlan.doInsertBetween(this, forEach, succ, false);
        forEach.getProjectionMap().setMappedFields(mappedFields);
        succ.rewire(this, 0, forEach, false);
        return forEach;
    }
}
