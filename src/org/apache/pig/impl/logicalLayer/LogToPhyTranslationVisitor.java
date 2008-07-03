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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.ComparisonFunc;
import org.apache.pig.EvalFunc;
import org.apache.pig.LoadFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.physicalLayer.PhysicalOperator;
import org.apache.pig.impl.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.impl.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.physicalLayer.relationalOperators.*;
import org.apache.pig.impl.physicalLayer.expressionOperators.*;
import org.apache.pig.impl.physicalLayer.expressionOperators.ExpressionOperator;
import org.apache.pig.impl.physicalLayer.expressionOperators.BinaryExpressionOperator;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.DependencyOrderWalkerWOSeenChk;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.PlanWalker;
import org.apache.pig.impl.plan.VisitorException;

public class LogToPhyTranslationVisitor extends LOVisitor {

    Map<LogicalOperator, PhysicalOperator> LogToPhyMap;

    Random r = new Random();

    Stack<PhysicalPlan> currentPlans;

    PhysicalPlan currentPlan;

    NodeIdGenerator nodeGen = NodeIdGenerator.getGenerator();

    private Log log = LogFactory.getLog(getClass());

    PigContext pc;

    LoadFunc load;

    public LogToPhyTranslationVisitor(LogicalPlan plan) {
        super(plan, new DependencyOrderWalker<LogicalOperator, LogicalPlan>(
                plan));

        currentPlans = new Stack<PhysicalPlan>();
        currentPlan = new PhysicalPlan();
        LogToPhyMap = new HashMap<LogicalOperator, PhysicalOperator>();
    }

    public void setPigContext(PigContext pc) {
        this.pc = pc;
    }

    public PhysicalPlan getPhysicalPlan() {

        return currentPlan;
    }

    @Override
    public void visit(LOGreaterThan op) throws VisitorException {
        String scope = op.getOperatorKey().scope;
        BinaryComparisonOperator exprOp = new GreaterThanExpr(new OperatorKey(
                scope, nodeGen.getNextNodeId(scope)), op
                .getRequestedParallelism());
        exprOp.setOperandType(op.getLhsOperand().getType());
        exprOp.setLhs((ExpressionOperator) LogToPhyMap.get(op.getLhsOperand()));
        exprOp.setRhs((ExpressionOperator) LogToPhyMap.get(op.getRhsOperand()));
        LogicalPlan lp = op.mPlan;

        currentPlan.add(exprOp);
        LogToPhyMap.put(op, exprOp);

        List<LogicalOperator> predecessors = lp.getPredecessors(op);

        if (predecessors == null)
            return;
        for (LogicalOperator lo : predecessors) {
            PhysicalOperator from = LogToPhyMap.get(lo);
            try {
                // currentExprPlan.connect(from, exprOp);
                currentPlan.connect(from, exprOp);
            } catch (PlanException e) {
                log.error("Invalid physical operators in the physical plan"
                        + e.getMessage());
            }
        }
    }

    @Override
    public void visit(LOLesserThan op) throws VisitorException {
        String scope = op.getOperatorKey().scope;
        BinaryComparisonOperator exprOp = new LessThanExpr(new OperatorKey(
                scope, nodeGen.getNextNodeId(scope)), op
                .getRequestedParallelism());
        exprOp.setOperandType(op.getLhsOperand().getType());
        exprOp.setLhs((ExpressionOperator) LogToPhyMap.get(op.getLhsOperand()));
        exprOp.setRhs((ExpressionOperator) LogToPhyMap.get(op.getRhsOperand()));
        LogicalPlan lp = op.mPlan;

        currentPlan.add(exprOp);
        LogToPhyMap.put(op, exprOp);

        List<LogicalOperator> predecessors = lp.getPredecessors(op);

        if (predecessors == null)
            return;
        for (LogicalOperator lo : predecessors) {
            PhysicalOperator from = LogToPhyMap.get(lo);
            try {
                currentPlan.connect(from, exprOp);
            } catch (PlanException e) {
                log.error("Invalid physical operators in the physical plan"
                        + e.getMessage());
            }
        }
    }

    @Override
    public void visit(LOGreaterThanEqual op) throws VisitorException {
        String scope = op.getOperatorKey().scope;
        BinaryComparisonOperator exprOp = new GTOrEqualToExpr(new OperatorKey(
                scope, nodeGen.getNextNodeId(scope)), op
                .getRequestedParallelism());
        exprOp.setOperandType(op.getLhsOperand().getType());
        exprOp.setLhs((ExpressionOperator) LogToPhyMap.get(op.getLhsOperand()));
        exprOp.setRhs((ExpressionOperator) LogToPhyMap.get(op.getRhsOperand()));
        LogicalPlan lp = op.mPlan;

        currentPlan.add(exprOp);
        LogToPhyMap.put(op, exprOp);

        List<LogicalOperator> predecessors = lp.getPredecessors(op);
        if (predecessors == null)
            return;
        for (LogicalOperator lo : predecessors) {
            PhysicalOperator from = LogToPhyMap.get(lo);
            try {
                currentPlan.connect(from, exprOp);
            } catch (PlanException e) {
                log.error("Invalid physical operators in the physical plan"
                        + e.getMessage());
            }
        }
    }

    @Override
    public void visit(LOLesserThanEqual op) throws VisitorException {
        String scope = op.getOperatorKey().scope;
        BinaryComparisonOperator exprOp = new LTOrEqualToExpr(new OperatorKey(
                scope, nodeGen.getNextNodeId(scope)), op
                .getRequestedParallelism());
        exprOp.setOperandType(op.getLhsOperand().getType());
        exprOp.setLhs((ExpressionOperator) LogToPhyMap.get(op.getLhsOperand()));
        exprOp.setRhs((ExpressionOperator) LogToPhyMap.get(op.getRhsOperand()));
        LogicalPlan lp = op.mPlan;

        currentPlan.add(exprOp);
        LogToPhyMap.put(op, exprOp);

        List<LogicalOperator> predecessors = lp.getPredecessors(op);
        if (predecessors == null)
            return;
        for (LogicalOperator lo : predecessors) {
            PhysicalOperator from = LogToPhyMap.get(lo);
            try {
                currentPlan.connect(from, exprOp);
            } catch (PlanException e) {
                log.error("Invalid physical operators in the physical plan"
                        + e.getMessage());
            }
        }
    }

    @Override
    public void visit(LOEqual op) throws VisitorException {
        String scope = op.getOperatorKey().scope;
        BinaryComparisonOperator exprOp = new EqualToExpr(new OperatorKey(
                scope, nodeGen.getNextNodeId(scope)), op
                .getRequestedParallelism());
        exprOp.setOperandType(op.getLhsOperand().getType());
        exprOp.setLhs((ExpressionOperator) LogToPhyMap.get(op.getLhsOperand()));
        exprOp.setRhs((ExpressionOperator) LogToPhyMap.get(op.getRhsOperand()));
        LogicalPlan lp = op.mPlan;

        currentPlan.add(exprOp);
        LogToPhyMap.put(op, exprOp);

        List<LogicalOperator> predecessors = lp.getPredecessors(op);
        if (predecessors == null)
            return;
        for (LogicalOperator lo : predecessors) {
            PhysicalOperator from = LogToPhyMap.get(lo);
            try {
                currentPlan.connect(from, exprOp);
            } catch (PlanException e) {
                log.error("Invalid physical operators in the physical plan"
                        + e.getMessage());
            }
        }
    }

    @Override
    public void visit(LONotEqual op) throws VisitorException {
        String scope = op.getOperatorKey().scope;
        BinaryComparisonOperator exprOp = new NotEqualToExpr(new OperatorKey(
                scope, nodeGen.getNextNodeId(scope)), op
                .getRequestedParallelism());
        exprOp.setOperandType(op.getLhsOperand().getType());
        exprOp.setLhs((ExpressionOperator) LogToPhyMap.get(op.getLhsOperand()));
        exprOp.setRhs((ExpressionOperator) LogToPhyMap.get(op.getRhsOperand()));
        LogicalPlan lp = op.mPlan;

        currentPlan.add(exprOp);
        LogToPhyMap.put(op, exprOp);

        List<LogicalOperator> predecessors = lp.getPredecessors(op);
        if (predecessors == null)
            return;
        for (LogicalOperator lo : predecessors) {
            PhysicalOperator from = LogToPhyMap.get(lo);
            try {
                currentPlan.connect(from, exprOp);
            } catch (PlanException e) {
                log.error("Invalid physical operators in the physical plan"
                        + e.getMessage());
            }
        }
    }

    @Override
    public void visit(LORegexp op) throws VisitorException {
        String scope = op.getOperatorKey().scope;
        BinaryComparisonOperator exprOp =
            new PORegexp(new OperatorKey(scope, nodeGen.getNextNodeId(scope)),
            op.getRequestedParallelism());
        exprOp.setLhs((ExpressionOperator)LogToPhyMap.get(op.getLhsOperand()));
        exprOp.setRhs((ExpressionOperator)LogToPhyMap.get(op.getRhsOperand()));
        LogicalPlan lp = op.mPlan;

        currentPlan.add(exprOp);
        LogToPhyMap.put(op, exprOp);

        List<LogicalOperator> predecessors = lp.getPredecessors(op);
        if (predecessors == null)
            return;
        for (LogicalOperator lo : predecessors) {
            PhysicalOperator from = LogToPhyMap.get(lo);
            try {
                currentPlan.connect(from, exprOp);
            } catch (PlanException e) {
                log.error("Invalid physical operators in the physical plan"
                        + e.getMessage());
            }
        }
    }

    @Override
    public void visit(LOAdd op) throws VisitorException {
        String scope = op.getOperatorKey().scope;
        BinaryExpressionOperator exprOp = new Add(new OperatorKey(scope,
                nodeGen.getNextNodeId(scope)), op.getRequestedParallelism());
        exprOp.setResultType(op.getType());
        exprOp.setLhs((ExpressionOperator) LogToPhyMap.get(op.getLhsOperand()));
        exprOp.setRhs((ExpressionOperator) LogToPhyMap.get(op.getRhsOperand()));
        LogicalPlan lp = op.mPlan;

        currentPlan.add(exprOp);
        LogToPhyMap.put(op, exprOp);

        List<LogicalOperator> predecessors = lp.getPredecessors(op);
        if (predecessors == null)
            return;
        for (LogicalOperator lo : predecessors) {
            PhysicalOperator from = LogToPhyMap.get(lo);
            try {
                currentPlan.connect(from, exprOp);
            } catch (PlanException e) {
                log.error("Invalid physical operators in the physical plan"
                        + e.getMessage());
            }
        }
    }

    @Override
    public void visit(LOSubtract op) throws VisitorException {
        String scope = op.getOperatorKey().scope;
        BinaryExpressionOperator exprOp = new Subtract(new OperatorKey(scope,
                nodeGen.getNextNodeId(scope)), op.getRequestedParallelism());
        exprOp.setResultType(op.getType());
        exprOp.setLhs((ExpressionOperator) LogToPhyMap.get(op.getLhsOperand()));
        exprOp.setRhs((ExpressionOperator) LogToPhyMap.get(op.getRhsOperand()));
        LogicalPlan lp = op.mPlan;

        currentPlan.add(exprOp);
        LogToPhyMap.put(op, exprOp);

        List<LogicalOperator> predecessors = lp.getPredecessors(op);
        if (predecessors == null)
            return;
        for (LogicalOperator lo : predecessors) {
            PhysicalOperator from = LogToPhyMap.get(lo);
            try {
                currentPlan.connect(from, exprOp);
            } catch (PlanException e) {
                log.error("Invalid physical operators in the physical plan"
                        + e.getMessage());
            }
        }
    }

    @Override
    public void visit(LOMultiply op) throws VisitorException {
        String scope = op.getOperatorKey().scope;
        BinaryExpressionOperator exprOp = new Multiply(new OperatorKey(scope,
                nodeGen.getNextNodeId(scope)), op.getRequestedParallelism());
        exprOp.setResultType(op.getType());
        exprOp.setLhs((ExpressionOperator) LogToPhyMap.get(op.getLhsOperand()));
        exprOp.setRhs((ExpressionOperator) LogToPhyMap.get(op.getRhsOperand()));
        LogicalPlan lp = op.mPlan;

        currentPlan.add(exprOp);
        LogToPhyMap.put(op, exprOp);

        List<LogicalOperator> predecessors = lp.getPredecessors(op);
        if (predecessors == null)
            return;
        for (LogicalOperator lo : predecessors) {
            PhysicalOperator from = LogToPhyMap.get(lo);
            try {
                currentPlan.connect(from, exprOp);
            } catch (PlanException e) {
                log.error("Invalid physical operators in the physical plan"
                        + e.getMessage());
            }
        }
    }

    @Override
    public void visit(LODivide op) throws VisitorException {
        String scope = op.getOperatorKey().scope;
        BinaryExpressionOperator exprOp = new Divide(new OperatorKey(scope,
                nodeGen.getNextNodeId(scope)), op.getRequestedParallelism());
        exprOp.setResultType(op.getType());
        exprOp.setLhs((ExpressionOperator) LogToPhyMap.get(op.getLhsOperand()));
        exprOp.setRhs((ExpressionOperator) LogToPhyMap.get(op.getRhsOperand()));
        LogicalPlan lp = op.mPlan;

        currentPlan.add(exprOp);
        LogToPhyMap.put(op, exprOp);

        List<LogicalOperator> predecessors = lp.getPredecessors(op);
        if (predecessors == null)
            return;
        for (LogicalOperator lo : predecessors) {
            PhysicalOperator from = LogToPhyMap.get(lo);
            try {
                currentPlan.connect(from, exprOp);
            } catch (PlanException e) {
                log.error("Invalid physical operators in the physical plan"
                        + e.getMessage());
            }
        }
    }

    @Override
    public void visit(LOMod op) throws VisitorException {
        String scope = op.getOperatorKey().scope;
        BinaryExpressionOperator exprOp = new Mod(new OperatorKey(scope,
                nodeGen.getNextNodeId(scope)), op.getRequestedParallelism());
        exprOp.setResultType(op.getType());
        exprOp.setLhs((ExpressionOperator) LogToPhyMap.get(op.getLhsOperand()));
        exprOp.setRhs((ExpressionOperator) LogToPhyMap.get(op.getRhsOperand()));
        LogicalPlan lp = op.mPlan;

        currentPlan.add(exprOp);
        LogToPhyMap.put(op, exprOp);

        List<LogicalOperator> predecessors = lp.getPredecessors(op);
        if (predecessors == null)
            return;
        for (LogicalOperator lo : predecessors) {
            PhysicalOperator from = LogToPhyMap.get(lo);
            try {
                currentPlan.connect(from, exprOp);
            } catch (PlanException e) {
                log.error("Invalid physical operators in the physical plan"
                        + e.getMessage());
            }
        }
    }
    
    @Override
    public void visit(LOAnd op) throws VisitorException {
        String scope = op.getOperatorKey().scope;
        BinaryComparisonOperator exprOp = new POAnd(new OperatorKey(scope, nodeGen.getNextNodeId(scope)), op.getRequestedParallelism());
        exprOp.setLhs((ExpressionOperator)LogToPhyMap.get(op.getLhsOperand()));
        exprOp.setRhs((ExpressionOperator)LogToPhyMap.get(op.getRhsOperand()));
        LogicalPlan lp = op.mPlan;
        
        currentPlan.add(exprOp);
        LogToPhyMap.put(op, exprOp);
        
        List<LogicalOperator> predecessors = lp.getPredecessors(op);
        if(predecessors == null) return;
        for(LogicalOperator lo : predecessors) {
            PhysicalOperator from = LogToPhyMap.get(lo);
            try {
                currentPlan.connect(from, exprOp);
            } catch (PlanException e) {
                log.error("Invalid physical operators in the physical plan" + e.getMessage());
            }
        }
    }

    @Override
    public void visit(LOOr op) throws VisitorException {
        String scope = op.getOperatorKey().scope;
        BinaryComparisonOperator exprOp = new POOr(new OperatorKey(scope, nodeGen.getNextNodeId(scope)), op.getRequestedParallelism());
        exprOp.setLhs((ExpressionOperator)LogToPhyMap.get(op.getLhsOperand()));
        exprOp.setRhs((ExpressionOperator)LogToPhyMap.get(op.getRhsOperand()));
        LogicalPlan lp = op.mPlan;
        
        currentPlan.add(exprOp);
        LogToPhyMap.put(op, exprOp);
        
        List<LogicalOperator> predecessors = lp.getPredecessors(op);
        if(predecessors == null) return;
        for(LogicalOperator lo : predecessors) {
            PhysicalOperator from = LogToPhyMap.get(lo);
            try {
                currentPlan.connect(from, exprOp);
            } catch (PlanException e) {
                log.error("Invalid physical operators in the physical plan" + e.getMessage());
            }
        }
    }

    @Override
    public void visit(LONot op) throws VisitorException {
        String scope = op.getOperatorKey().scope;
        UnaryComparisonOperator exprOp = new PONot(new OperatorKey(scope, nodeGen.getNextNodeId(scope)), op.getRequestedParallelism());
        exprOp.setExpr((ExpressionOperator)LogToPhyMap.get(op.getOperand()));
        LogicalPlan lp = op.mPlan;
        
        currentPlan.add(exprOp);
        LogToPhyMap.put(op, exprOp);
        
        List<LogicalOperator> predecessors = lp.getPredecessors(op);
        if(predecessors == null) return;
        PhysicalOperator from = LogToPhyMap.get(predecessors.get(0));
        try {
            currentPlan.connect(from, exprOp);
        } catch (PlanException e) {
            log.error("Invalid physical operators in the physical plan" + e.getMessage());
        }
    }

    @Override
    public void visit(LOCogroup cg) throws VisitorException {
        boolean currentPhysicalPlan = false;
        String scope = cg.getOperatorKey().scope;
        List<LogicalOperator> inputs = cg.getInputs();

        POGlobalRearrange poGlobal = new POGlobalRearrange(new OperatorKey(
                scope, nodeGen.getNextNodeId(scope)), cg
                .getRequestedParallelism());
        POPackage poPackage = new POPackage(new OperatorKey(scope, nodeGen
                .getNextNodeId(scope)), cg.getRequestedParallelism());

        currentPlan.add(poGlobal);
        currentPlan.add(poPackage);

        try {
            currentPlan.connect(poGlobal, poPackage);
        } catch (PlanException e1) {
            log.error("Invalid physical operators in the physical plan"
                    + e1.getMessage());
        }

        int count = 0;
        Byte type = null;
        for (LogicalOperator op : inputs) {
            List<LogicalPlan> plans = (List<LogicalPlan>) cg.getGroupByPlans()
                    .get(op);
            POLocalRearrange physOp = new POLocalRearrange(new OperatorKey(
                    scope, nodeGen.getNextNodeId(scope)), cg
                    .getRequestedParallelism());
            List<PhysicalPlan> exprPlans = new ArrayList<PhysicalPlan>();
            currentPlans.push(currentPlan);
            for (LogicalPlan lp : plans) {
                currentPlan = new PhysicalPlan();
                PlanWalker<LogicalOperator, LogicalPlan> childWalker = mCurrentWalker
                        .spawnChildWalker(lp);
                pushWalker(childWalker);
                mCurrentWalker.walk(this);
                exprPlans.add((PhysicalPlan) currentPlan);
                popWalker();

            }
            currentPlan = currentPlans.pop();
            physOp.setPlans(exprPlans);
            physOp.setIndex(count++);
            if (plans.size() > 1) {
                type = DataType.TUPLE;
                physOp.setKeyType(type);
            } else {
                type = exprPlans.get(0).getLeaves().get(0).getResultType();
                physOp.setKeyType(type);
            }
            physOp.setResultType(DataType.TUPLE);

            currentPlan.add(physOp);

            try {
                currentPlan.connect(LogToPhyMap.get(op), physOp);
                currentPlan.connect(physOp, poGlobal);
            } catch (PlanException e) {
                log.error("Invalid physical operators in the physical plan"
                        + e.getMessage());
            }

        }
        poPackage.setKeyType(type);
        poPackage.setResultType(DataType.TUPLE);
        poPackage.setNumInps(count);
        poPackage.setInner(cg.getInner());
        LogToPhyMap.put(cg, poPackage);
    }

    @Override
    public void visit(LOFilter filter) throws VisitorException {
        String scope = filter.getOperatorKey().scope;
        POFilter poFilter = new POFilter(new OperatorKey(scope, nodeGen
                .getNextNodeId(scope)), filter.getRequestedParallelism());
        poFilter.setResultType(filter.getType());
        currentPlan.add(poFilter);
        LogToPhyMap.put(filter, poFilter);
        currentPlans.push(currentPlan);

        currentPlan = new PhysicalPlan();

        PlanWalker<LogicalOperator, LogicalPlan> childWalker = mCurrentWalker
                .spawnChildWalker(filter.getComparisonPlan());
        pushWalker(childWalker);
        mCurrentWalker.walk(this);
        popWalker();

        poFilter.setPlan((PhysicalPlan) currentPlan);
        currentPlan = currentPlans.pop();

        List<LogicalOperator> op = filter.getPlan().getPredecessors(filter);

        PhysicalOperator from = LogToPhyMap.get(op.get(0));
        try {
            currentPlan.connect(from, poFilter);
        } catch (PlanException e) {
            log.error("Invalid physical operators in the physical plan"
                    + e.getMessage());
        }
    }

    @Override
    public void visit(LOProject op) throws VisitorException {
        String scope = op.getOperatorKey().scope;
        POProject exprOp = new POProject(new OperatorKey(scope, nodeGen
                .getNextNodeId(scope)), op.getRequestedParallelism());
        exprOp.setResultType(op.getType());
        exprOp.setColumns((ArrayList)op.getProjection());
        exprOp.setStar(op.isStar());
        LogicalPlan lp = op.mPlan;
        LogToPhyMap.put(op, exprOp);
        currentPlan.add(exprOp);

        List<LogicalOperator> predecessors = lp.getPredecessors(op);

        // Project might not have any predecessors
        if (predecessors == null)
            return;

        for (LogicalOperator lo : predecessors) {
            PhysicalOperator from = LogToPhyMap.get(lo);
            try {
                currentPlan.connect(from, exprOp);
            } catch (PlanException e) {

                log.error("Invalid physical operators in the physical plan"
                        + e.getMessage());
            }
        }
    }

    @Override
    public void visit(LOForEach g) throws VisitorException {
        boolean currentPhysicalPlan = false;
        String scope = g.getOperatorKey().scope;
        List<PhysicalPlan> innerPlans = new ArrayList<PhysicalPlan>();
        List<LogicalPlan> plans = g.getForEachPlans();

        currentPlans.push(currentPlan);
        for (LogicalPlan plan : plans) {
            currentPlan = new PhysicalPlan();
            PlanWalker<LogicalOperator, LogicalPlan> childWalker = new DependencyOrderWalkerWOSeenChk<LogicalOperator, LogicalPlan>(
                    plan);
            pushWalker(childWalker);
            childWalker.walk(this);
            innerPlans.add(currentPlan);
            popWalker();
        }
        currentPlan = currentPlans.pop();

        // PhysicalOperator poGen = new POGenerate(new OperatorKey("",
        // r.nextLong()), inputs, toBeFlattened);
        POForEach poFE = new POForEach(new OperatorKey(scope, nodeGen
                .getNextNodeId(scope)), g.getRequestedParallelism(), innerPlans,
                g.getFlatten());
        poFE.setResultType(g.getType());
        LogToPhyMap.put(g, poFE);
        currentPlan.add(poFE);

        // generate cannot have multiple inputs
        List<LogicalOperator> op = g.getPlan().getPredecessors(g);

        // generate may not have any predecessors
        if (op == null)
            return;

        PhysicalOperator from = LogToPhyMap.get(op.get(0));
        try {
            currentPlan.connect(from, poFE);
        } catch (PlanException e) {
            log.error("Invalid physical operators in the physical plan"
                    + e.getMessage());
        }

    }

    @Override
    public void visit(LOSort s) throws VisitorException {
        String scope = s.getOperatorKey().scope;
        List<LogicalPlan> logPlans = s.getSortColPlans();
        List<PhysicalPlan> sortPlans = new ArrayList<PhysicalPlan>(logPlans.size());

        // convert all the logical expression plans to physical expression plans
        currentPlans.push(currentPlan);
        for (LogicalPlan plan : logPlans) {
            currentPlan = new PhysicalPlan();
            PlanWalker<LogicalOperator, LogicalPlan> childWalker = mCurrentWalker
                    .spawnChildWalker(plan);
            pushWalker(childWalker);
            childWalker.walk(this);
            sortPlans.add((PhysicalPlan) currentPlan);
            popWalker();
        }
        currentPlan = currentPlans.pop();

        // get the physical operator for sort
        POSort sort;
        if (s.getUserFunc() == null) {
            sort = new POSort(new OperatorKey(scope, nodeGen
                    .getNextNodeId(scope)), s.getRequestedParallelism(), null,
                    sortPlans, s.getAscendingCols(), null);
        } else {
            POUserFunc comparator = new POUserComparisonFunc(new OperatorKey(
                    scope, nodeGen.getNextNodeId(scope)), s
                    .getRequestedParallelism(), null, s.getUserFunc());
            sort = new POSort(new OperatorKey(scope, nodeGen
                    .getNextNodeId(scope)), s.getRequestedParallelism(), null,
                    sortPlans, s.getAscendingCols(), comparator);
        }
        // sort.setRequestedParallelism(s.getType());
        LogToPhyMap.put(s, sort);
        currentPlan.add(sort);
        PhysicalOperator from = LogToPhyMap.get(s.mPlan
                .getPredecessors(s).get(0));
        try {
            currentPlan.connect(from, sort);
        } catch (PlanException e) {
            log.error("Invalid physical operator in the plan" + e.getMessage());
        }

        sort.setResultType(s.getType());

    }

    @Override
    public void visit(LODistinct op) throws VisitorException {
        String scope = op.getOperatorKey().scope;
        // This is simpler. No plans associated with this. Just create the
        // physical operator,
        // push it in the current plan and make the connections
        PhysicalOperator physOp = new PODistinct(new OperatorKey(scope, nodeGen
                .getNextNodeId(scope)), op.getRequestedParallelism());
        physOp.setResultType(op.getType());
        LogToPhyMap.put(op, physOp);
        currentPlan.add(physOp);
        // Distinct will only have a single input
        PhysicalOperator from = LogToPhyMap.get(op.mPlan.getPredecessors(op)
                .get(0));
        try {
            currentPlan.connect(from, physOp);
        } catch (PlanException e) {
            log.error("Invalid physical operator in the plan" + e.getMessage());
        }
    }

    @Override
    public void visit(LOSplit split) throws VisitorException {
        String scope = split.getOperatorKey().scope;
        PhysicalOperator physOp = new POSplit(new OperatorKey(scope, nodeGen
                .getNextNodeId(scope)), split.getRequestedParallelism());
        LogToPhyMap.put(split, physOp);

        currentPlan.add(physOp);
        PhysicalOperator from = LogToPhyMap.get(split.getPlan()
                .getPredecessors(split).get(0));
        try {
            currentPlan.connect(from, physOp);
        } catch (PlanException e) {
            log.error("Invalid physical operator in the plan" + e.getMessage());
        }
    }

    @Override
    public void visit(LOSplitOutput split) throws VisitorException {
        String scope = split.getOperatorKey().scope;
        PhysicalOperator physOp = new POFilter(new OperatorKey(scope, nodeGen
                .getNextNodeId(scope)), split.getRequestedParallelism());
        LogToPhyMap.put(split, physOp);

        currentPlan.add(physOp);
        currentPlans.push(currentPlan);
        currentPlan = new PhysicalPlan();
        PlanWalker<LogicalOperator, LogicalPlan> childWalker = mCurrentWalker
                .spawnChildWalker(split.getConditionPlan());
        pushWalker(childWalker);
        mCurrentWalker.walk(this);
        popWalker();

        ((POFilter) physOp).setPlan((PhysicalPlan) currentPlan);
        currentPlan = currentPlans.pop();
        currentPlan.add(physOp);
        PhysicalOperator from = LogToPhyMap.get(split.getPlan()
                .getPredecessors(split).get(0));
        try {
            currentPlan.connect(from, physOp);
        } catch (PlanException e) {
            log.error("Invalid physical operator in the plan" + e.getMessage());
        }
    }

    @Override
    public void visit(LOUserFunc func) throws VisitorException {
        String scope = func.getOperatorKey().scope;
        Object f = PigContext.instantiateFuncFromSpec(func.getFuncSpec());
        PhysicalOperator p;
        if (f instanceof EvalFunc) {
            p = new POUserFunc(new OperatorKey(scope, nodeGen
                    .getNextNodeId(scope)), func.getRequestedParallelism(),
                    null, func.getFuncSpec(), (EvalFunc) f);
        } else {
            p = new POUserComparisonFunc(new OperatorKey(scope, nodeGen
                    .getNextNodeId(scope)), func.getRequestedParallelism(),
                    null, func.getFuncSpec(), (ComparisonFunc) f);
        }
        p.setResultType(func.getType());
        currentPlan.add(p);
        List<LogicalOperator> fromList = func.getPlan().getPredecessors(func);
        for (LogicalOperator op : fromList) {
            PhysicalOperator from = LogToPhyMap.get(op);
            try {
                currentPlan.connect(from, p);
            } catch (PlanException e) {
                log.error("Invalid physical operator in the plan"
                        + e.getMessage());
            }
        }
        LogToPhyMap.put(func, p);

    }

    @Override
    public void visit(LOLoad loLoad) throws VisitorException {
        String scope = loLoad.getOperatorKey().scope;
        // This would be a root operator. We don't need to worry about finding
        // its predecessors
        POLoad load = new POLoad(new OperatorKey(scope, nodeGen
                .getNextNodeId(scope)));
        load.setLFile(loLoad.getInputFile());
        load.setPc(pc);
        load.setResultType(loLoad.getType());
        currentPlan.add(load);
        LogToPhyMap.put(loLoad, load);
        this.load = loLoad.getLoadFunc();
    }

    @Override
    public void visit(LOStore loStore) throws VisitorException {
        String scope = loStore.getOperatorKey().scope;
        POStore store = new POStore(new OperatorKey(scope, nodeGen
                .getNextNodeId(scope)));
        store.setSFile(loStore.getOutputFile());
        store.setPc(pc);
        currentPlan.add(store);
        PhysicalOperator from = LogToPhyMap.get(loStore
                .getPlan().getPredecessors(loStore).get(0));
        try {
            currentPlan.connect(from, store);
        } catch (PlanException e) {
            log.error("Invalid physical operator in the plan" + e.getMessage());
        }
        LogToPhyMap.put(loStore, store);
    }

    @Override
    public void visit(LOConst op) throws VisitorException {
        String scope = op.getOperatorKey().scope;
        ConstantExpression ce = new ConstantExpression(new OperatorKey(scope,
                nodeGen.getNextNodeId(scope)));
        ce.setValue(op.getValue());
        ce.setResultType(op.getType());
        //this operator doesn't have any predecessors
        currentPlan.add(ce);
        LogToPhyMap.put(op, ce);
    }

    @Override
    public void visit(LOBinCond op) throws VisitorException {
        String scope = op.getOperatorKey().scope;
        ExpressionOperator physOp = new POBinCond(new OperatorKey(scope,
                nodeGen.getNextNodeId(scope)), op.getRequestedParallelism());
        LogToPhyMap.put(op, physOp);
        POBinCond phy = (POBinCond) physOp;
        ExpressionOperator cond = (ExpressionOperator) LogToPhyMap.get(op
                .getCond());
        phy.setCond(cond);
        ExpressionOperator lhs = (ExpressionOperator) LogToPhyMap.get(op
                .getLhsOp());
        phy.setLhs(lhs);
        ExpressionOperator rhs = (ExpressionOperator) LogToPhyMap.get(op
                .getRhsOp());
        phy.setRhs(rhs);
        phy.setResultType(op.getType());
        currentPlan.add(physOp);

        List<LogicalOperator> ops = op.getPlan().getPredecessors(op);

        for (LogicalOperator l : ops) {
            ExpressionOperator from = (ExpressionOperator) LogToPhyMap.get(l);
            try {
                currentPlan.connect(from, physOp);
            } catch (PlanException e) {
                log.error("Invalid physical operator in the plan"
                        + e.getMessage());
            }
        }

    }

    @Override
    public void visit(LONegative op) throws VisitorException {
        String scope = op.getOperatorKey().scope;
        ExpressionOperator physOp = new PONegative(new OperatorKey(scope,
                nodeGen.getNextNodeId(scope)), op.getRequestedParallelism(),
                null);
        currentPlan.add(physOp);

        LogToPhyMap.put(op, physOp);
        ExpressionOperator from = (ExpressionOperator) LogToPhyMap.get(op
                .getPlan().getPredecessors(op).get(0));
        ((PONegative) physOp).setExpr(from);
        try {
            currentPlan.connect(from, physOp);
        } catch (PlanException e) {
            log.error("Invalid physical operator in the plan" + e.getMessage());
        }

    }

    @Override
    public void visit(LOIsNull op) throws VisitorException {
        String scope = op.getOperatorKey().scope;
        ExpressionOperator physOp = new POIsNull(new OperatorKey(scope, nodeGen
                .getNextNodeId(scope)), op.getRequestedParallelism(), null);
        currentPlan.add(physOp);

        LogToPhyMap.put(op, physOp);
        ExpressionOperator from = (ExpressionOperator) LogToPhyMap.get(op
                .getPlan().getPredecessors(op).get(0));
        ((POIsNull) physOp).setExpr(from);
        try {
            currentPlan.connect(from, physOp);
        } catch (PlanException e) {
            log.error("Invalid physical operator in the plan" + e.getMessage());
        }

    }

    @Override
    public void visit(LOMapLookup op) throws VisitorException {
        String scope = ((OperatorKey) op.getOperatorKey()).scope;
        ExpressionOperator physOp = new POMapLookUp(new OperatorKey(scope,
                nodeGen.getNextNodeId(scope)), op.getRequestedParallelism(), op
                .getLookUpKey());
        physOp.setResultType(op.getType());
        currentPlan.add(physOp);

        LogToPhyMap.put(op, physOp);

        ExpressionOperator from = (ExpressionOperator) LogToPhyMap.get(op
                .getMap());
        try {
            currentPlan.connect(from, physOp);
        } catch (PlanException e) {
            log.error("Invalid physical operator in the plan" + e.getMessage());
        }

    }

    @Override
    public void visit(LOCast op) throws VisitorException {
        String scope = op.getOperatorKey().scope;
        ExpressionOperator physOp = new POCast(new OperatorKey(scope, nodeGen
                .getNextNodeId(scope)), op.getRequestedParallelism());
        currentPlan.add(physOp);

        LogToPhyMap.put(op, physOp);
        ExpressionOperator from = (ExpressionOperator) LogToPhyMap.get(op
                .getExpression());
        physOp.setResultType(op.getType());
        ((POCast) physOp).setLoadFSpec(load.getClass().getName());
        try {
            currentPlan.connect(from, physOp);
        } catch (PlanException e) {
            log.error("Invalid physical operator in the plan" + e.getMessage());
        }

    }

    @Override
    public void visit(LOUnion op) throws VisitorException {
        String scope = op.getOperatorKey().scope;
        POUnion physOp = new POUnion(new OperatorKey(scope, nodeGen
                .getNextNodeId(scope)), op.getRequestedParallelism());
        currentPlan.add(physOp);
        physOp.setResultType(op.getType());
        LogToPhyMap.put(op, physOp);
        List<LogicalOperator> ops = op.getInputs();

        for (LogicalOperator l : ops) {
            PhysicalOperator from = LogToPhyMap.get(l);
            try {
                currentPlan.connect(from, physOp);
            } catch (PlanException e) {
                log.error("Invalid physical operator in the plan"
                        + e.getMessage());
            }
        }
    }

}
