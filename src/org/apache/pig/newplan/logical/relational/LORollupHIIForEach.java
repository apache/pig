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
package org.apache.pig.newplan.logical.relational;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Stack;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanVisitor;
import org.apache.pig.newplan.ReverseDependencyOrderWalker;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.optimizer.AllSameRalationalNodesVisitor;

/**
 * Rollup operator implementation for data rollup computation. This class
 * provides a new ForEach logical operator to implement IRG and Hybrid IRG
 * approaches for rollup computation.
 */

public class LORollupHIIForEach extends LOForEach {

    private static final long serialVersionUID = 2L;

    private LogicalPlan innerPlan;
    //the pivot value
    private int pivot = -1;
    //the index of the first field involves in ROLLUP
    private int rollupFieldIndex = 0;
    //the original index of the first field involves in ROLLUP in case it was moved to the end
    //(if we have the combination of cube and rollup)
    private int rollupOldFieldIndex = 0;
    //number of fields that involve in ROLLUP
    private int rollupSize = 0;
    //if we only use IRG, not Hybrid IRG+IRG
    private boolean isOnlyIRG = false;
    //the size of total fields that involve in CUBE clause
    private int dimensionSize = 0;

    public LORollupHIIForEach(OperatorPlan plan) {
        super(plan);
    }

    public LORollupHIIForEach(LOForEach foreach) throws FrontendException {
        super(foreach.getPlan());
        this.setInnerPlan(foreach.getInnerPlan());
        this.setRequestedParallelism(foreach.getRequestedParallelism());
        this.setAlias(foreach.getAlias());
        this.setSchema(foreach.getSchema());
        this.setLocation(foreach.getLocation());
        Iterator<Operator> its = foreach.getInnerPlan().getOperators();
        while (its.hasNext()) {
            Operator opr = its.next();
            AttachPrjToNew(opr, this);
        }
    }

    public void setOnlyIRG() {
        isOnlyIRG = true;
    }

    public boolean getOnlyIRG() {
        return isOnlyIRG;
    }

    public void setRollupOldFieldIndex(int rofi) {
        this.rollupOldFieldIndex = rofi;
    }

    public int getRollupOldFieldIndex() {
        return this.rollupOldFieldIndex;
    }

    public void setRollupSize(int rs) {
        this.rollupSize = rs;
    }

    public int getRollupSize() {
        return this.rollupSize;
    }

    public void setRollupFieldIndex(int rfi) {
        this.rollupFieldIndex = rfi;
    }

    public int getRollupFieldIndex() {
        return this.rollupFieldIndex;
    }

    public void setPivot(int pvt) {
        this.pivot = pvt;
    }

    public int getPivot() {
        return this.pivot;
    }

    public void setDimensionSize(int ds) {
        this.dimensionSize = ds;
    }

    public int getDimensionSize() {
        return this.dimensionSize;
    }

    /**
     * Attach ProjectExpression from old LOForEach operator to new
     * LORollupHIIForEach operator
     *
     * @param opr
     * @param hfe
     * @throws FrontendException
     */
    private void AttachPrjToNew(Operator opr, LORollupHIIForEach hfe) throws FrontendException {

        if (opr instanceof LOGenerate) {
            LOGenerate log = (LOGenerate) opr;
            List<LogicalExpressionPlan> leps = log.getOutputPlans();
            for (LogicalExpressionPlan lep : leps) {
                Iterator<Operator> its = lep.getOperators();
                while (its.hasNext()) {
                    Operator opr2 = its.next();
                    if (opr2 instanceof ProjectExpression) {
                        if (((ProjectExpression) opr2).getAttachedRelationalOp() instanceof LOForEach) {
                            ((ProjectExpression) opr2).setAttachedRelationalOp(hfe);
                        }
                        Pair<List<LOInnerLoad>, Boolean> a = findReacheableInnerLoadFromBoundaryProject((ProjectExpression) opr2);
                        List<LOInnerLoad> innerLoads = a.first;
                        boolean needNewUid = a.second;
                    }
                }
            }
        } else if (opr instanceof LOInnerLoad) {
            LOInnerLoad loi = (LOInnerLoad) opr;

            if (loi.getProjection().getAttachedRelationalOp() instanceof LOForEach)
                loi.getProjection().setAttachedRelationalOp(hfe);
        }
    }

    public LogicalPlan getInnerPlan() {
        return innerPlan;
    }

    public void setInnerPlan(LogicalPlan p) {
        innerPlan = p;
    }

    @Override
    public boolean isEqual(Operator other) throws FrontendException {
        if (!(other instanceof LORollupHIIForEach)) {
            return false;
        }

        return innerPlan.isEqual(((LORollupHIIForEach) other).innerPlan);
    }

    @Override
    public LogicalSchema getSchema() throws FrontendException {
        List<Operator> ll = innerPlan.getSinks();
        if (ll != null) {
            schema = ((LogicalRelationalOperator) ll.get(0)).getSchema();
        }

        return schema;
    }

    @Override
    public void accept(PlanVisitor v) throws FrontendException {
        if (!(v instanceof LogicalRelationalNodesVisitor)) {
            throw new FrontendException("Expected LogicalPlanVisitor", 2222);
        }
        ((LogicalRelationalNodesVisitor) v).visit(this);
    }

    public LogicalSchema dumpNestedSchema(String alias, String nestedAlias) throws FrontendException {
        NestedRelationalOperatorFinder opFinder = new NestedRelationalOperatorFinder(innerPlan, nestedAlias);
        opFinder.visit();

        if (opFinder.getMatchedOperator() != null) {
            LogicalSchema nestedSc = opFinder.getMatchedOperator().getSchema();
            return nestedSc;
        }
        return null;
    }

    private static class NestedRelationalOperatorFinder extends AllSameRalationalNodesVisitor {
        String aliasOfOperator;
        LogicalRelationalOperator opFound = null;

        public NestedRelationalOperatorFinder(LogicalPlan plan, String alias) throws FrontendException {
            super(plan, new ReverseDependencyOrderWalker(plan));
            aliasOfOperator = alias;
        }

        public LogicalRelationalOperator getMatchedOperator() {
            return opFound;
        }

        @Override
        public void execute(LogicalRelationalOperator op) throws FrontendException {
            if (op.getAlias() != null && op.getAlias().equals(aliasOfOperator))
                opFound = op;
        }
    }
}
