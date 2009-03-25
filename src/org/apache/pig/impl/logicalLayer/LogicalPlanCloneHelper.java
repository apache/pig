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

import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;

import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.MultiMap;

/**
 * LogicalPlanCloneHelper implements a visitor mechanism to clone a logical plan
 * and then patch up the connections held within the operators of the logical plan.
 * This class should not be used for cloning the logical plan. Use {@link LogicalPlanCloner}
 * instead.
 */
public class LogicalPlanCloneHelper extends LOVisitor {
    
    public static Map<LogicalOperator, LogicalOperator> mOpToCloneMap;
    private LogicalPlan mOriginalPlan;

    /**
     * @param plan logical plan to be cloned
     */
    public LogicalPlanCloneHelper(LogicalPlan plan) throws CloneNotSupportedException {
        super(plan, new DependencyOrderWalker<LogicalOperator, LogicalPlan>(plan));
        mOriginalPlan = plan;
        //LOVisitor does not have a default constructor and super needs to be the first
        //statement in the constructor. As a result, mPlan and mCurrentWalker are being
        //re-initialized here
        mPlan = plan.clone();
        mCurrentWalker = new DependencyOrderWalker<LogicalOperator, LogicalPlan>(mPlan);
    }
    
    /**
     * @param plan
     * @param origCloneMap the lookup table used for tracking operators cloned in the plan
     */
    public LogicalPlanCloneHelper(LogicalPlan plan,
            Map<LogicalOperator, LogicalOperator> origCloneMap) throws CloneNotSupportedException {
        super(plan, new DependencyOrderWalker<LogicalOperator, LogicalPlan>(plan));
        mOpToCloneMap = origCloneMap;
        mOriginalPlan = plan;
        //LOVisitor does not have a default constructor and super needs to be the first
        //statement in the constructor. As a result, mPlan and mCurrentWalker are being
        //re-initialized here
        mPlan = plan.clone();
        mCurrentWalker = new DependencyOrderWalker<LogicalOperator, LogicalPlan>(mPlan);
    }
    
    public LogicalPlan getClonedPlan() throws CloneNotSupportedException {
        
        // set the "mPlan" member of all the Logical operators 
        // in the cloned plan to the cloned plan
        PlanSetter ps = new PlanSetter(mPlan);
        try {
            ps.visit();
            //patch up the connections
            this.visit();
        
        } catch (VisitorException e) {
            CloneNotSupportedException cnse = new CloneNotSupportedException("Unable to set plan correctly during cloning");
            cnse.initCause(e);
            throw cnse;
        }

        return mPlan;
    }

    public static void resetState() {
        mOpToCloneMap.clear();
    }
    
    @Override
    public void visit(BinaryExpressionOperator binOp) {
    }

    /**
     * @see org.apache.pig.impl.logicalLayer.LOVisitor#visit(org.apache.pig.impl.logicalLayer.LOAdd)
     */
    @Override
    public void visit(LOAdd op) throws VisitorException {
        this.visit((BinaryExpressionOperator)op);
    }

    /**
     * @see org.apache.pig.impl.logicalLayer.LOVisitor#visit(org.apache.pig.impl.logicalLayer.LOAnd)
     */
    @Override
    public void visit(LOAnd binOp) throws VisitorException {
        this.visit((BinaryExpressionOperator)binOp);
    }

    /**
     * @see org.apache.pig.impl.logicalLayer.LOVisitor#visit(org.apache.pig.impl.logicalLayer.LOBinCond)
     */
    @Override
    protected void visit(LOBinCond binCond) throws VisitorException {
    }

    /**
     * @see org.apache.pig.impl.logicalLayer.LOVisitor#visit(org.apache.pig.impl.logicalLayer.LOCast)
     */
    @Override
    protected void visit(LOCast cast) throws VisitorException {
    }

    /**
     * @see org.apache.pig.impl.logicalLayer.LOVisitor#visit(org.apache.pig.impl.logicalLayer.LOCogroup)
     */
    @Override
    protected void visit(LOCogroup cg) throws VisitorException {
        MultiMap<LogicalOperator, LogicalPlan> groupByPlans = cg.getGroupByPlans();
        MultiMap<LogicalOperator, LogicalPlan> groupByPlansClone = new MultiMap<LogicalOperator, LogicalPlan>();
        for(LogicalOperator cgInput: groupByPlans.keySet()) {
            LogicalOperator cgInputClone = mOpToCloneMap.get(cgInput);
            if(cgInputClone != null) {
                groupByPlansClone.put(cgInputClone, groupByPlans.get(cgInput));
            } else {
                groupByPlansClone.put(cgInput, groupByPlans.get(cgInput));
            }
        }
        cg.setGroupByPlans(groupByPlansClone);
        super.visit(cg);
    }

    /**
     * @see org.apache.pig.impl.logicalLayer.LOVisitor#visit(org.apache.pig.impl.logicalLayer.LOConst)
     */
    @Override
    protected void visit(LOConst constant) throws VisitorException {
    }

    /**
     * @see org.apache.pig.impl.logicalLayer.LOVisitor#visit(org.apache.pig.impl.logicalLayer.LOCross)
     */
    @Override
    protected void visit(LOCross cs) throws VisitorException {
        super.visit(cs);
    }

    /**
     * @see org.apache.pig.impl.logicalLayer.LOVisitor#visit(org.apache.pig.impl.logicalLayer.LODistinct)
     */
    @Override
    protected void visit(LODistinct dt) throws VisitorException {
        super.visit(dt);
    }

    /**
     * @see org.apache.pig.impl.logicalLayer.LOVisitor#visit(org.apache.pig.impl.logicalLayer.LODivide)
     */
    @Override
    public void visit(LODivide op) throws VisitorException {
        this.visit((BinaryExpressionOperator)op);
    }

    /**
     * @see org.apache.pig.impl.logicalLayer.LOVisitor#visit(org.apache.pig.impl.logicalLayer.LOEqual)
     */
    @Override
    public void visit(LOEqual op) throws VisitorException {
        this.visit((BinaryExpressionOperator)op);
    }

    /**
     * @see org.apache.pig.impl.logicalLayer.LOVisitor#visit(org.apache.pig.impl.logicalLayer.LOFilter)
     */
    @Override
    protected void visit(LOFilter filter) throws VisitorException {
        super.visit(filter);
    }

    /**
     * @see org.apache.pig.impl.logicalLayer.LOVisitor#visit(org.apache.pig.impl.logicalLayer.LOForEach)
     */
    @Override
    protected void visit(LOForEach forEach) throws VisitorException {
        super.visit(forEach);
    }

    /**
     * @see org.apache.pig.impl.logicalLayer.LOVisitor#visit(org.apache.pig.impl.logicalLayer.LOGenerate)
     */
    @Override
    protected void visit(LOGenerate g) throws VisitorException {
        super.visit(g);
    }

    /**
     * @see org.apache.pig.impl.logicalLayer.LOVisitor#visit(org.apache.pig.impl.logicalLayer.LogicalOperator)
     */
    @Override
    protected void visit(LogicalOperator op) throws VisitorException {
        super.visit(op);
    }

    /**
     * @see org.apache.pig.impl.logicalLayer.LOVisitor#visit(org.apache.pig.impl.logicalLayer.LOGreaterThan)
     */
    @Override
    public void visit(LOGreaterThan op) throws VisitorException {
        this.visit((BinaryExpressionOperator)op);
    }

    /**
     * @see org.apache.pig.impl.logicalLayer.LOVisitor#visit(org.apache.pig.impl.logicalLayer.LOGreaterThanEqual)
     */
    @Override
    public void visit(LOGreaterThanEqual op) throws VisitorException {
        this.visit((BinaryExpressionOperator)op);
    }

    /**
     * @see org.apache.pig.impl.logicalLayer.LOVisitor#visit(org.apache.pig.impl.logicalLayer.LOIsNull)
     */
    @Override
    public void visit(LOIsNull uniOp) throws VisitorException {
        this.visit((UnaryExpressionOperator) uniOp);
    }

    /**
     * @see org.apache.pig.impl.logicalLayer.LOVisitor#visit(org.apache.pig.impl.logicalLayer.LOLesserThan)
     */
    @Override
    public void visit(LOLesserThan op) throws VisitorException {
        this.visit((BinaryExpressionOperator)op);
    }

    /**
     * @see org.apache.pig.impl.logicalLayer.LOVisitor#visit(org.apache.pig.impl.logicalLayer.LOLesserThanEqual)
     */
    @Override
    public void visit(LOLesserThanEqual op) throws VisitorException {
        this.visit((BinaryExpressionOperator)op);
    }

    /**
     * @see org.apache.pig.impl.logicalLayer.LOVisitor#visit(org.apache.pig.impl.logicalLayer.LOLimit)
     */
    @Override
    protected void visit(LOLimit limOp) throws VisitorException {
        super.visit(limOp);
    }

    /**
     * @see org.apache.pig.impl.logicalLayer.LOVisitor#visit(org.apache.pig.impl.logicalLayer.LOLoad)
     */
    @Override
    protected void visit(LOLoad load) throws VisitorException {
        //TODO
        //LOLoad cloning is not implemented
    }

    /**
     * @see org.apache.pig.impl.logicalLayer.LOVisitor#visit(org.apache.pig.impl.logicalLayer.LOMapLookup)
     */
    @Override
    public void visit(LOMapLookup op) throws VisitorException {
    }

    /**
     * @see org.apache.pig.impl.logicalLayer.LOVisitor#visit(org.apache.pig.impl.logicalLayer.LOMod)
     */
    @Override
    public void visit(LOMod op) throws VisitorException {
        this.visit((BinaryExpressionOperator)op);
    }

    /**
     * @see org.apache.pig.impl.logicalLayer.LOVisitor#visit(org.apache.pig.impl.logicalLayer.LOMultiply)
     */
    @Override
    public void visit(LOMultiply op) throws VisitorException {
        this.visit((BinaryExpressionOperator)op);
    }

    /**
     * @see org.apache.pig.impl.logicalLayer.LOVisitor#visit(org.apache.pig.impl.logicalLayer.LONegative)
     */
    @Override
    public void visit(LONegative op) throws VisitorException {
        this.visit((UnaryExpressionOperator) op);
    }

    /**
     * @see org.apache.pig.impl.logicalLayer.LOVisitor#visit(org.apache.pig.impl.logicalLayer.LONot)
     */
    @Override
    public void visit(LONot uniOp) throws VisitorException {
        this.visit((UnaryExpressionOperator) uniOp);
    }

    /**
     * @see org.apache.pig.impl.logicalLayer.LOVisitor#visit(org.apache.pig.impl.logicalLayer.LONotEqual)
     */
    @Override
    public void visit(LONotEqual op) throws VisitorException {
        this.visit((BinaryExpressionOperator)op);
    }

    /**
     * @see org.apache.pig.impl.logicalLayer.LOVisitor#visit(org.apache.pig.impl.logicalLayer.LOOr)
     */
    @Override
    public void visit(LOOr binOp) throws VisitorException {
        this.visit((BinaryExpressionOperator)binOp);
    }

    /**
     * @see org.apache.pig.impl.logicalLayer.LOVisitor#visit(org.apache.pig.impl.logicalLayer.LOProject)
     */
    @Override
    protected void visit(LOProject project) throws VisitorException {
        LogicalOperator projectInputClone = mOpToCloneMap.get(project.getExpression());
        if(projectInputClone != null) {
            project.setExpression(projectInputClone);
        }
    }

    /**
     * @see org.apache.pig.impl.logicalLayer.LOVisitor#visit(org.apache.pig.impl.logicalLayer.LORegexp)
     */
    @Override
    protected void visit(LORegexp binOp) throws VisitorException {
        this.visit((BinaryExpressionOperator)binOp);
    }

    /**
     * @see org.apache.pig.impl.logicalLayer.LOVisitor#visit(org.apache.pig.impl.logicalLayer.LOSort)
     */
    @Override
    protected void visit(LOSort s) throws VisitorException {
        super.visit(s);
    }

    /**
     * @see org.apache.pig.impl.logicalLayer.LOVisitor#visit(org.apache.pig.impl.logicalLayer.LOSplit)
     */
    @Override
    protected void visit(LOSplit split) throws VisitorException {
        List<LogicalOperator> splitOutputs = split.getOutputs();
        ArrayList<LogicalOperator> splitOutputClones = new ArrayList<LogicalOperator>(splitOutputs.size());
        for(LogicalOperator splitOutput: splitOutputs) {
            LogicalOperator splitOutputClone = mOpToCloneMap.get(splitOutput);
            if(splitOutputClone != null) {
                splitOutputClones.add(splitOutputClone);
            } else {
                splitOutputClones.add(splitOutput);
            }
        }
        split.setOutputs(splitOutputClones);
    }

    /**
     * @see org.apache.pig.impl.logicalLayer.LOVisitor#visit(org.apache.pig.impl.logicalLayer.LOSplitOutput)
     */
    @Override
    protected void visit(LOSplitOutput sop) throws VisitorException {
        super.visit(sop);
    }

    /**
     * @see org.apache.pig.impl.logicalLayer.LOVisitor#visit(org.apache.pig.impl.logicalLayer.LOStore)
     */
    @Override
    protected void visit(LOStore store) throws VisitorException {
        //TODO
        //LOStore cloning is not implemented
    }

    /**
     * @see org.apache.pig.impl.logicalLayer.LOVisitor#visit(org.apache.pig.impl.logicalLayer.LOStream)
     */
    @Override
    protected void visit(LOStream stream) throws VisitorException {
        //TODO
        //LOStream cloning is not implemented
    }

    /**
     * @see org.apache.pig.impl.logicalLayer.LOVisitor#visit(org.apache.pig.impl.logicalLayer.LOSubtract)
     */
    @Override
    public void visit(LOSubtract op) throws VisitorException {
        this.visit((BinaryExpressionOperator)op);
    }

    /**
     * @see org.apache.pig.impl.logicalLayer.LOVisitor#visit(org.apache.pig.impl.logicalLayer.LOUnion)
     */
    @Override
    protected void visit(LOUnion u) throws VisitorException {
        super.visit(u);
    }

    /**
     * @see org.apache.pig.impl.logicalLayer.LOVisitor#visit(org.apache.pig.impl.logicalLayer.LOUserFunc)
     */
    @Override
    protected void visit(LOUserFunc func) throws VisitorException {
    }

    /**
     * @see org.apache.pig.impl.logicalLayer.LOVisitor#visit(org.apache.pig.impl.logicalLayer.UnaryExpressionOperator)
     */
    @Override
    protected void visit(UnaryExpressionOperator uniOp) throws VisitorException {
    }
    
}
