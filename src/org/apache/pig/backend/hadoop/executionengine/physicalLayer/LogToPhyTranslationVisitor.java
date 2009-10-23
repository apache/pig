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
package org.apache.pig.backend.hadoop.executionengine.physicalLayer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.ComparisonFunc;
import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.NonSpillableDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.*;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.*;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.ExpressionOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.BinaryExpressionOperator;
import org.apache.pig.builtin.BinStorage;
import org.apache.pig.builtin.IsEmpty;
import org.apache.pig.impl.builtin.GFCross;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.logicalLayer.*;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.DependencyOrderWalkerWOSeenChk;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.PlanWalker;
import org.apache.pig.impl.plan.VisitorException;


import org.apache.pig.impl.util.LinkedMultiMap;
import org.apache.pig.impl.util.MultiMap;

public class LogToPhyTranslationVisitor extends LOVisitor {

    protected Map<LogicalOperator, PhysicalOperator> logToPhyMap;

    Random r = new Random();

    protected Stack<PhysicalPlan> currentPlans;

    protected PhysicalPlan currentPlan;

    protected NodeIdGenerator nodeGen = NodeIdGenerator.getGenerator();

    private Log log = LogFactory.getLog(getClass());

    protected PigContext pc;

    LoadFunc load;

    public LogToPhyTranslationVisitor(LogicalPlan plan) {
        super(plan, new DependencyOrderWalker<LogicalOperator, LogicalPlan>(
                plan));

        currentPlans = new Stack<PhysicalPlan>();
        currentPlan = new PhysicalPlan();
        logToPhyMap = new HashMap<LogicalOperator, PhysicalOperator>();
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
        exprOp.setLhs((ExpressionOperator) logToPhyMap.get(op.getLhsOperand()));
        exprOp.setRhs((ExpressionOperator) logToPhyMap.get(op.getRhsOperand()));
        LogicalPlan lp = op.getPlan();

        currentPlan.add(exprOp);
        logToPhyMap.put(op, exprOp);

        List<LogicalOperator> predecessors = lp.getPredecessors(op);

        if (predecessors == null)
            return;
        for (LogicalOperator lo : predecessors) {
            PhysicalOperator from = logToPhyMap.get(lo);
            try {
                // currentExprPlan.connect(from, exprOp);
                currentPlan.connect(from, exprOp);
            } catch (PlanException e) {
                int errCode = 2015;
                String msg = "Invalid physical operators in the physical plan" ;
                throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
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
        exprOp.setLhs((ExpressionOperator) logToPhyMap.get(op.getLhsOperand()));
        exprOp.setRhs((ExpressionOperator) logToPhyMap.get(op.getRhsOperand()));
        LogicalPlan lp = op.getPlan();

        currentPlan.add(exprOp);
        logToPhyMap.put(op, exprOp);

        List<LogicalOperator> predecessors = lp.getPredecessors(op);

        if (predecessors == null)
            return;
        for (LogicalOperator lo : predecessors) {
            PhysicalOperator from = logToPhyMap.get(lo);
            try {
                currentPlan.connect(from, exprOp);
            } catch (PlanException e) {
                int errCode = 2015;
                String msg = "Invalid physical operators in the physical plan" ;
                throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
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
        exprOp.setLhs((ExpressionOperator) logToPhyMap.get(op.getLhsOperand()));
        exprOp.setRhs((ExpressionOperator) logToPhyMap.get(op.getRhsOperand()));
        LogicalPlan lp = op.getPlan();

        currentPlan.add(exprOp);
        logToPhyMap.put(op, exprOp);

        List<LogicalOperator> predecessors = lp.getPredecessors(op);
        if (predecessors == null)
            return;
        for (LogicalOperator lo : predecessors) {
            PhysicalOperator from = logToPhyMap.get(lo);
            try {
                currentPlan.connect(from, exprOp);
            } catch (PlanException e) {
                int errCode = 2015;
                String msg = "Invalid physical operators in the physical plan" ;
                throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
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
        exprOp.setLhs((ExpressionOperator) logToPhyMap.get(op.getLhsOperand()));
        exprOp.setRhs((ExpressionOperator) logToPhyMap.get(op.getRhsOperand()));
        LogicalPlan lp = op.getPlan();

        currentPlan.add(exprOp);
        logToPhyMap.put(op, exprOp);

        List<LogicalOperator> predecessors = lp.getPredecessors(op);
        if (predecessors == null)
            return;
        for (LogicalOperator lo : predecessors) {
            PhysicalOperator from = logToPhyMap.get(lo);
            try {
                currentPlan.connect(from, exprOp);
            } catch (PlanException e) {
                int errCode = 2015;
                String msg = "Invalid physical operators in the physical plan" ;
                throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
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
        exprOp.setLhs((ExpressionOperator) logToPhyMap.get(op.getLhsOperand()));
        exprOp.setRhs((ExpressionOperator) logToPhyMap.get(op.getRhsOperand()));
        LogicalPlan lp = op.getPlan();

        currentPlan.add(exprOp);
        logToPhyMap.put(op, exprOp);

        List<LogicalOperator> predecessors = lp.getPredecessors(op);
        if (predecessors == null)
            return;
        for (LogicalOperator lo : predecessors) {
            PhysicalOperator from = logToPhyMap.get(lo);
            try {
                currentPlan.connect(from, exprOp);
            } catch (PlanException e) {
                int errCode = 2015;
                String msg = "Invalid physical operators in the physical plan" ;
                throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
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
        exprOp.setLhs((ExpressionOperator) logToPhyMap.get(op.getLhsOperand()));
        exprOp.setRhs((ExpressionOperator) logToPhyMap.get(op.getRhsOperand()));
        LogicalPlan lp = op.getPlan();

        currentPlan.add(exprOp);
        logToPhyMap.put(op, exprOp);

        List<LogicalOperator> predecessors = lp.getPredecessors(op);
        if (predecessors == null)
            return;
        for (LogicalOperator lo : predecessors) {
            PhysicalOperator from = logToPhyMap.get(lo);
            try {
                currentPlan.connect(from, exprOp);
            } catch (PlanException e) {
                int errCode = 2015;
                String msg = "Invalid physical operators in the physical plan" ;
                throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
            }
        }
    }

    @Override
    public void visit(LORegexp op) throws VisitorException {
        String scope = op.getOperatorKey().scope;
        BinaryComparisonOperator exprOp =
            new PORegexp(new OperatorKey(scope, nodeGen.getNextNodeId(scope)),
            op.getRequestedParallelism());
        exprOp.setLhs((ExpressionOperator)logToPhyMap.get(op.getLhsOperand()));
        exprOp.setRhs((ExpressionOperator)logToPhyMap.get(op.getRhsOperand()));
        LogicalPlan lp = op.getPlan();

        currentPlan.add(exprOp);
        logToPhyMap.put(op, exprOp);

        List<LogicalOperator> predecessors = lp.getPredecessors(op);
        if (predecessors == null)
            return;
        for (LogicalOperator lo : predecessors) {
            PhysicalOperator from = logToPhyMap.get(lo);
            try {
                currentPlan.connect(from, exprOp);
            } catch (PlanException e) {
                int errCode = 2015;
                String msg = "Invalid physical operators in the physical plan" ;
                throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
            }
        }
    }

    @Override
    public void visit(LOAdd op) throws VisitorException {
        String scope = op.getOperatorKey().scope;
        BinaryExpressionOperator exprOp = new Add(new OperatorKey(scope,
                nodeGen.getNextNodeId(scope)), op.getRequestedParallelism());
        exprOp.setResultType(op.getType());
        exprOp.setLhs((ExpressionOperator) logToPhyMap.get(op.getLhsOperand()));
        exprOp.setRhs((ExpressionOperator) logToPhyMap.get(op.getRhsOperand()));
        LogicalPlan lp = op.getPlan();

        currentPlan.add(exprOp);
        logToPhyMap.put(op, exprOp);

        List<LogicalOperator> predecessors = lp.getPredecessors(op);
        if (predecessors == null)
            return;
        for (LogicalOperator lo : predecessors) {
            PhysicalOperator from = logToPhyMap.get(lo);
            try {
                currentPlan.connect(from, exprOp);
            } catch (PlanException e) {
                int errCode = 2015;
                String msg = "Invalid physical operators in the physical plan" ;
                throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
            }
        }
    }

    @Override
    public void visit(LOSubtract op) throws VisitorException {
        String scope = op.getOperatorKey().scope;
        BinaryExpressionOperator exprOp = new Subtract(new OperatorKey(scope,
                nodeGen.getNextNodeId(scope)), op.getRequestedParallelism());
        exprOp.setResultType(op.getType());
        exprOp.setLhs((ExpressionOperator) logToPhyMap.get(op.getLhsOperand()));
        exprOp.setRhs((ExpressionOperator) logToPhyMap.get(op.getRhsOperand()));
        LogicalPlan lp = op.getPlan();

        currentPlan.add(exprOp);
        logToPhyMap.put(op, exprOp);

        List<LogicalOperator> predecessors = lp.getPredecessors(op);
        if (predecessors == null)
            return;
        for (LogicalOperator lo : predecessors) {
            PhysicalOperator from = logToPhyMap.get(lo);
            try {
                currentPlan.connect(from, exprOp);
            } catch (PlanException e) {
                int errCode = 2015;
                String msg = "Invalid physical operators in the physical plan" ;
                throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
            }
        }
    }

    @Override
    public void visit(LOMultiply op) throws VisitorException {
        String scope = op.getOperatorKey().scope;
        BinaryExpressionOperator exprOp = new Multiply(new OperatorKey(scope,
                nodeGen.getNextNodeId(scope)), op.getRequestedParallelism());
        exprOp.setResultType(op.getType());
        exprOp.setLhs((ExpressionOperator) logToPhyMap.get(op.getLhsOperand()));
        exprOp.setRhs((ExpressionOperator) logToPhyMap.get(op.getRhsOperand()));
        LogicalPlan lp = op.getPlan();

        currentPlan.add(exprOp);
        logToPhyMap.put(op, exprOp);

        List<LogicalOperator> predecessors = lp.getPredecessors(op);
        if (predecessors == null)
            return;
        for (LogicalOperator lo : predecessors) {
            PhysicalOperator from = logToPhyMap.get(lo);
            try {
                currentPlan.connect(from, exprOp);
            } catch (PlanException e) {
                int errCode = 2015;
                String msg = "Invalid physical operators in the physical plan" ;
                throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
            }
        }
    }

    @Override
    public void visit(LODivide op) throws VisitorException {
        String scope = op.getOperatorKey().scope;
        BinaryExpressionOperator exprOp = new Divide(new OperatorKey(scope,
                nodeGen.getNextNodeId(scope)), op.getRequestedParallelism());
        exprOp.setResultType(op.getType());
        exprOp.setLhs((ExpressionOperator) logToPhyMap.get(op.getLhsOperand()));
        exprOp.setRhs((ExpressionOperator) logToPhyMap.get(op.getRhsOperand()));
        LogicalPlan lp = op.getPlan();

        currentPlan.add(exprOp);
        logToPhyMap.put(op, exprOp);

        List<LogicalOperator> predecessors = lp.getPredecessors(op);
        if (predecessors == null)
            return;
        for (LogicalOperator lo : predecessors) {
            PhysicalOperator from = logToPhyMap.get(lo);
            try {
                currentPlan.connect(from, exprOp);
            } catch (PlanException e) {
                int errCode = 2015;
                String msg = "Invalid physical operators in the physical plan" ;
                throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
            }
        }
    }

    @Override
    public void visit(LOMod op) throws VisitorException {
        String scope = op.getOperatorKey().scope;
        BinaryExpressionOperator exprOp = new Mod(new OperatorKey(scope,
                nodeGen.getNextNodeId(scope)), op.getRequestedParallelism());
        exprOp.setResultType(op.getType());
        exprOp.setLhs((ExpressionOperator) logToPhyMap.get(op.getLhsOperand()));
        exprOp.setRhs((ExpressionOperator) logToPhyMap.get(op.getRhsOperand()));
        LogicalPlan lp = op.getPlan();

        currentPlan.add(exprOp);
        logToPhyMap.put(op, exprOp);

        List<LogicalOperator> predecessors = lp.getPredecessors(op);
        if (predecessors == null)
            return;
        for (LogicalOperator lo : predecessors) {
            PhysicalOperator from = logToPhyMap.get(lo);
            try {
                currentPlan.connect(from, exprOp);
            } catch (PlanException e) {
                int errCode = 2015;
                String msg = "Invalid physical operators in the physical plan" ;
                throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
            }
        }
    }
    
    @Override
    public void visit(LOAnd op) throws VisitorException {
        String scope = op.getOperatorKey().scope;
        BinaryComparisonOperator exprOp = new POAnd(new OperatorKey(scope, nodeGen.getNextNodeId(scope)), op.getRequestedParallelism());
        exprOp.setLhs((ExpressionOperator)logToPhyMap.get(op.getLhsOperand()));
        exprOp.setRhs((ExpressionOperator)logToPhyMap.get(op.getRhsOperand()));
        LogicalPlan lp = op.getPlan();
        
        currentPlan.add(exprOp);
        logToPhyMap.put(op, exprOp);
        
        List<LogicalOperator> predecessors = lp.getPredecessors(op);
        if(predecessors == null) return;
        for(LogicalOperator lo : predecessors) {
            PhysicalOperator from = logToPhyMap.get(lo);
            try {
                currentPlan.connect(from, exprOp);
            } catch (PlanException e) {
                int errCode = 2015;
                String msg = "Invalid physical operators in the physical plan" ;
                throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
            }
        }
    }

    @Override
    public void visit(LOOr op) throws VisitorException {
        String scope = op.getOperatorKey().scope;
        BinaryComparisonOperator exprOp = new POOr(new OperatorKey(scope, nodeGen.getNextNodeId(scope)), op.getRequestedParallelism());
        exprOp.setLhs((ExpressionOperator)logToPhyMap.get(op.getLhsOperand()));
        exprOp.setRhs((ExpressionOperator)logToPhyMap.get(op.getRhsOperand()));
        LogicalPlan lp = op.getPlan();
        
        currentPlan.add(exprOp);
        logToPhyMap.put(op, exprOp);
        
        List<LogicalOperator> predecessors = lp.getPredecessors(op);
        if(predecessors == null) return;
        for(LogicalOperator lo : predecessors) {
            PhysicalOperator from = logToPhyMap.get(lo);
            try {
                currentPlan.connect(from, exprOp);
            } catch (PlanException e) {
                int errCode = 2015;
                String msg = "Invalid physical operators in the physical plan" ;
                throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
            }
        }
    }

    @Override
    public void visit(LONot op) throws VisitorException {
        String scope = op.getOperatorKey().scope;
        UnaryComparisonOperator exprOp = new PONot(new OperatorKey(scope, nodeGen.getNextNodeId(scope)), op.getRequestedParallelism());
        exprOp.setExpr((ExpressionOperator)logToPhyMap.get(op.getOperand()));
        LogicalPlan lp = op.getPlan();
        
        currentPlan.add(exprOp);
        logToPhyMap.put(op, exprOp);
        
        List<LogicalOperator> predecessors = lp.getPredecessors(op);
        if(predecessors == null) return;
        PhysicalOperator from = logToPhyMap.get(predecessors.get(0));
        try {
            currentPlan.connect(from, exprOp);
        } catch (PlanException e) {
            int errCode = 2015;
            String msg = "Invalid physical operators in the physical plan" ;
            throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
        }
    }

    @Override
    protected void visit(LOCross cs) throws VisitorException {
        String scope = cs.getOperatorKey().scope;
        List<LogicalOperator> inputs = cs.getInputs();
        
        POGlobalRearrange poGlobal = new POGlobalRearrange(new OperatorKey(
                scope, nodeGen.getNextNodeId(scope)), cs
                .getRequestedParallelism());
        POPackage poPackage = new POPackage(new OperatorKey(scope, nodeGen
                .getNextNodeId(scope)), cs.getRequestedParallelism());

        currentPlan.add(poGlobal);
        currentPlan.add(poPackage);
        
        int count = 0;
        
        try {
            currentPlan.connect(poGlobal, poPackage);
            List<Boolean> flattenLst = Arrays.asList(true, true);
            
            for (LogicalOperator op : inputs) {
                List<PhysicalOperator> pop = Arrays.asList(logToPhyMap.get(op));
                PhysicalPlan fep1 = new PhysicalPlan();
                ConstantExpression ce1 = new ConstantExpression(new OperatorKey(scope, nodeGen.getNextNodeId(scope)),cs.getRequestedParallelism());
                ce1.setValue(inputs.size());
                ce1.setResultType(DataType.INTEGER);
                fep1.add(ce1);
                
                ConstantExpression ce2 = new ConstantExpression(new OperatorKey(scope, nodeGen.getNextNodeId(scope)),cs.getRequestedParallelism());
                ce2.setValue(count);
                ce2.setResultType(DataType.INTEGER);
                fep1.add(ce2);
                /*Tuple ce1val = TupleFactory.getInstance().newTuple(2);
                ce1val.set(0,inputs.size());
                ce1val.set(1,count);
                ce1.setValue(ce1val);
                ce1.setResultType(DataType.TUPLE);*/
                
                

                POUserFunc gfc = new POUserFunc(new OperatorKey(scope, nodeGen.getNextNodeId(scope)),cs.getRequestedParallelism(), Arrays.asList((PhysicalOperator)ce1,(PhysicalOperator)ce2), new FuncSpec(GFCross.class.getName()));
                gfc.setResultType(DataType.BAG);
                fep1.addAsLeaf(gfc);
                gfc.setInputs(Arrays.asList((PhysicalOperator)ce1,(PhysicalOperator)ce2));
                /*fep1.add(gfc);
                fep1.connect(ce1, gfc);
                fep1.connect(ce2, gfc);*/
                
                PhysicalPlan fep2 = new PhysicalPlan();
                POProject feproj = new POProject(new OperatorKey(scope, nodeGen.getNextNodeId(scope)), cs.getRequestedParallelism());
                feproj.setResultType(DataType.TUPLE);
                feproj.setStar(true);
                feproj.setOverloaded(false);
                fep2.add(feproj);
                List<PhysicalPlan> fePlans = Arrays.asList(fep1, fep2);
                
                POForEach fe = new POForEach(new OperatorKey(scope, nodeGen.getNextNodeId(scope)), cs.getRequestedParallelism(), fePlans, flattenLst );
                currentPlan.add(fe);
                currentPlan.connect(logToPhyMap.get(op), fe);
                
                POLocalRearrange physOp = new POLocalRearrange(new OperatorKey(
                        scope, nodeGen.getNextNodeId(scope)), cs
                        .getRequestedParallelism());
                List<PhysicalPlan> lrPlans = new ArrayList<PhysicalPlan>();
                for(int i=0;i<inputs.size();i++){
                    PhysicalPlan lrp1 = new PhysicalPlan();
                    POProject lrproj1 = new POProject(new OperatorKey(scope, nodeGen.getNextNodeId(scope)), cs.getRequestedParallelism(), i);
                    lrproj1.setOverloaded(false);
                    lrproj1.setResultType(DataType.INTEGER);
                    lrp1.add(lrproj1);
                    lrPlans.add(lrp1);
                }
                
                physOp.setCross(true);
                physOp.setIndex(count++);
                physOp.setKeyType(DataType.TUPLE);
                physOp.setPlans(lrPlans);
                physOp.setResultType(DataType.TUPLE);
                
                currentPlan.add(physOp);
                currentPlan.connect(fe, physOp);
                currentPlan.connect(physOp, poGlobal);
            }
        } catch (PlanException e1) {
            int errCode = 2015;
            String msg = "Invalid physical operators in the physical plan" ;
            throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e1);
        } catch (ExecException e) {
        	int errCode = 2058;
        	String msg = "Unable to set index on newly create POLocalRearrange.";
            throw new VisitorException(msg, errCode, PigException.BUG, e);
        }
        
        poPackage.setKeyType(DataType.TUPLE);
        poPackage.setResultType(DataType.TUPLE);
        poPackage.setNumInps(count);
        boolean inner[] = new boolean[count];
        for (int i=0;i<count;i++) {
            inner[i] = true;
        }
        poPackage.setInner(inner);
        
        List<PhysicalPlan> fePlans = new ArrayList<PhysicalPlan>();
        List<Boolean> flattenLst = new ArrayList<Boolean>();
        for(int i=1;i<=count;i++){
            PhysicalPlan fep1 = new PhysicalPlan();
            POProject feproj1 = new POProject(new OperatorKey(scope, nodeGen.getNextNodeId(scope)), cs.getRequestedParallelism(), i);
            feproj1.setResultType(DataType.BAG);
            feproj1.setOverloaded(false);
            fep1.add(feproj1);
            fePlans.add(fep1);
            flattenLst.add(true);
        }
        
        POForEach fe = new POForEach(new OperatorKey(scope, nodeGen.getNextNodeId(scope)), cs.getRequestedParallelism(), fePlans, flattenLst );
        currentPlan.add(fe);
        try{
            currentPlan.connect(poPackage, fe);
        }catch (PlanException e1) {
            int errCode = 2015;
            String msg = "Invalid physical operators in the physical plan" ;
            throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e1);
        }
        logToPhyMap.put(cs, fe);
    }
    
    @Override
    public void visit(LOCogroup cg) throws VisitorException {
            
        if (cg.getGroupType() == LOCogroup.GROUPTYPE.COLLECTED) {

            translateCollectedCogroup(cg);

        } else {
            
            translateRegularCogroup(cg);
        }
    }
    
    private void translateRegularCogroup(LOCogroup cg) throws VisitorException {
        String scope = cg.getOperatorKey().scope;
        List<LogicalOperator> inputs = cg.getInputs();
        
        POGlobalRearrange poGlobal = new POGlobalRearrange(new OperatorKey(
                scope, nodeGen.getNextNodeId(scope)), cg.getRequestedParallelism());
        POPackage poPackage = new POPackage(new OperatorKey(scope, nodeGen
                .getNextNodeId(scope)), cg.getRequestedParallelism());

        currentPlan.add(poGlobal);
        currentPlan.add(poPackage);

        try {
            currentPlan.connect(poGlobal, poPackage);
        } catch (PlanException e1) {
            int errCode = 2015;
            String msg = "Invalid physical operators in the physical plan" ;
            throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e1);
        }

        int count = 0;
        Byte type = null;
        for (LogicalOperator op : inputs) {
            List<LogicalPlan> plans = (List<LogicalPlan>)cg.getGroupByPlans().get(op);
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
                exprPlans.add(currentPlan);
                popWalker();
            }
            currentPlan = currentPlans.pop();
            try {
                physOp.setPlans(exprPlans);
            } catch (PlanException pe) {
                int errCode = 2071;
                String msg = "Problem with setting up local rearrange's plans.";
                throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, pe);
            }
            try {
                physOp.setIndex(count++);
            } catch (ExecException e1) {
                int errCode = 2058;
                String msg = "Unable to set index on newly create POLocalRearrange.";
                throw new VisitorException(msg, errCode, PigException.BUG, e1);
            }
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
                currentPlan.connect(logToPhyMap.get(op), physOp);
                currentPlan.connect(physOp, poGlobal);
            } catch (PlanException e) {
                int errCode = 2015;
                String msg = "Invalid physical operators in the physical plan" ;
                throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
            }
        }
        
        poPackage.setKeyType(type);
        poPackage.setResultType(DataType.TUPLE);
        poPackage.setNumInps(count);
        poPackage.setInner(cg.getInner());
        logToPhyMap.put(cg, poPackage);
    }
    
    private void translateCollectedCogroup(LOCogroup cg) throws VisitorException {
        String scope = cg.getOperatorKey().scope;
        List<LogicalOperator> inputs = cg.getInputs();
        
        // can have only one input
        LogicalOperator op = inputs.get(0);
        List<LogicalPlan> plans = (List<LogicalPlan>) cg.getGroupByPlans().get(op);
        POCollectedGroup physOp = new POCollectedGroup(new OperatorKey(
                scope, nodeGen.getNextNodeId(scope)));
        
        List<PhysicalPlan> exprPlans = new ArrayList<PhysicalPlan>();
        currentPlans.push(currentPlan);
        for (LogicalPlan lp : plans) {
            currentPlan = new PhysicalPlan();
            PlanWalker<LogicalOperator, LogicalPlan> childWalker = 
                mCurrentWalker.spawnChildWalker(lp);
            pushWalker(childWalker);
            mCurrentWalker.walk(this);
            exprPlans.add(currentPlan);
            popWalker();
        }
        currentPlan = currentPlans.pop();
        
        try {
            physOp.setPlans(exprPlans);
        } catch (PlanException pe) {
            int errCode = 2071;
            String msg = "Problem with setting up map group's plans.";
            throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, pe);
        }
        Byte type = null;
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
            currentPlan.connect(logToPhyMap.get(op), physOp);
        } catch (PlanException e) {
            int errCode = 2015;
            String msg = "Invalid physical operators in the physical plan" ;
            throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
        }

        logToPhyMap.put(cg, physOp);
    }
    
	@Override
	protected void visit(LOJoin loj) throws VisitorException {

	    String scope = loj.getOperatorKey().scope;
	    
	    // List of join predicates 
	    List<LogicalOperator> inputs = loj.getInputs();
	    
	    // mapping of inner join physical plans corresponding to inner physical operators.
        MultiMap<PhysicalOperator, PhysicalPlan> joinPlans = new LinkedMultiMap<PhysicalOperator, PhysicalPlan>();
        
        // Outer list corresponds to join predicates. Inner list is inner physical plan of each predicate.
        List<List<PhysicalPlan>> ppLists = new ArrayList<List<PhysicalPlan>>();
        
        // List of physical operator corresponding to join predicates.
        List<PhysicalOperator> inp = new ArrayList<PhysicalOperator>();
        
        // Outer list corresponds to join predicates and inner list corresponds to type of keys for each predicate.
        List<List<Byte>> keyTypes = new ArrayList<List<Byte>>();

        for (LogicalOperator op : inputs) {
			PhysicalOperator physOp = logToPhyMap.get(op);
            inp.add(physOp);
            List<LogicalPlan> plans = (List<LogicalPlan>) loj.getJoinPlans().get(op);
            List<PhysicalPlan> exprPlans = new ArrayList<PhysicalPlan>();
            currentPlans.push(currentPlan);
            for (LogicalPlan lp : plans) {
                currentPlan = new PhysicalPlan();
                PlanWalker<LogicalOperator, LogicalPlan> childWalker = mCurrentWalker.spawnChildWalker(lp);
                pushWalker(childWalker);
                mCurrentWalker.walk(this);
                exprPlans.add(currentPlan);
                popWalker();
            }
            currentPlan = currentPlans.pop();
            ppLists.add(exprPlans);
            joinPlans.put(physOp, exprPlans);
            
            // Key could potentially be a tuple. So, we visit all exprPlans to get types of members of tuples.
            List<Byte> tupleKeyMemberTypes = new ArrayList<Byte>();
            for(PhysicalPlan exprPlan : exprPlans)
                tupleKeyMemberTypes.add(exprPlan.getLeaves().get(0).getResultType());
            keyTypes.add(tupleKeyMemberTypes);
		}

		if (loj.getJoinType() == LOJoin.JOINTYPE.SKEWED) {
			POSkewedJoin skj;
			try {
				skj = new POSkewedJoin(new OperatorKey(scope,nodeGen.getNextNodeId(scope)),loj.getRequestedParallelism(),
											inp);
				skj.setJoinPlans(joinPlans);
			}
			catch (Exception e) {
				int errCode = 2015;
				String msg = "Skewed Join creation failed";
				throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
			}
			skj.setResultType(DataType.TUPLE);
			currentPlan.add(skj);

			for (LogicalOperator op : inputs) {
				try {
					currentPlan.connect(logToPhyMap.get(op), skj);
				} catch (PlanException e) {
					int errCode = 2015;
					String msg = "Invalid physical operators in the physical plan" ;
					throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
				}
			}
			logToPhyMap.put(loj, skj);
		} 
		
		else if(loj.getJoinType() == LOJoin.JOINTYPE.REPLICATED) {
	        
	        int fragment = 0;
	        POFRJoin pfrj;
	        try {
	            pfrj = new POFRJoin(new OperatorKey(scope,nodeGen.getNextNodeId(scope)),loj.getRequestedParallelism(),
	                                        inp, ppLists, keyTypes, null, fragment);
	        } catch (ExecException e1) {
	            int errCode = 2058;
	            String msg = "Unable to set index on newly create POLocalRearrange.";
	            throw new VisitorException(msg, errCode, PigException.BUG, e1);
	        }
	        pfrj.setResultType(DataType.TUPLE);
	        currentPlan.add(pfrj);
	        for (LogicalOperator op : inputs) {
	            try {
	                currentPlan.connect(logToPhyMap.get(op), pfrj);
	            } catch (PlanException e) {
	                int errCode = 2015;
	                String msg = "Invalid physical operators in the physical plan" ;
	                throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
	            }
	        }
	        logToPhyMap.put(loj, pfrj);
		}
		
		else if (loj.getJoinType() == LOJoin.JOINTYPE.MERGE && validateMergeJoin(loj)) {
            
		    POMergeJoin smj;
            try {
                smj = new POMergeJoin(new OperatorKey(scope,nodeGen.getNextNodeId(scope)),loj.getRequestedParallelism(),inp,joinPlans,keyTypes);
            }
            catch (Exception e) {
                int errCode = 2042;
                String msg = "Merge Join creation failed";
                throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
            }

            smj.setResultType(DataType.TUPLE);
            currentPlan.add(smj);

            for (LogicalOperator op : inputs) {
                try {
                    currentPlan.connect(logToPhyMap.get(op), smj);
                } catch (PlanException e) {
                    int errCode = 2015;
                    String msg = "Invalid physical operators in the physical plan" ;
                    throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
                }
            }
            logToPhyMap.put(loj, smj);
            return;
        }
		else if (loj.getJoinType() == LOJoin.JOINTYPE.REGULAR){
	        POGlobalRearrange poGlobal = new POGlobalRearrange(new OperatorKey(
	                scope, nodeGen.getNextNodeId(scope)), loj
	                .getRequestedParallelism());
	        POPackage poPackage = new POPackage(new OperatorKey(scope, nodeGen
	                .getNextNodeId(scope)), loj.getRequestedParallelism());

	        currentPlan.add(poGlobal);
	        currentPlan.add(poPackage);
	        
	        int count = 0;
            Byte type = null;
	        
	        try {
	            currentPlan.connect(poGlobal, poPackage);
	            for (LogicalOperator op : inputs) {
	                List<LogicalPlan> plans = (List<LogicalPlan>) loj.getJoinPlans()
	                        .get(op);
	                POLocalRearrange physOp = new POLocalRearrange(new OperatorKey(
	                        scope, nodeGen.getNextNodeId(scope)), loj
	                        .getRequestedParallelism());
	                List<PhysicalPlan> exprPlans = new ArrayList<PhysicalPlan>();
	                currentPlans.push(currentPlan);
	                for (LogicalPlan lp : plans) {
	                    currentPlan = new PhysicalPlan();
	                    PlanWalker<LogicalOperator, LogicalPlan> childWalker = mCurrentWalker
	                            .spawnChildWalker(lp);
	                    pushWalker(childWalker);
	                    mCurrentWalker.walk(this);
	                    exprPlans.add(currentPlan);
	                    popWalker();

	                }
	                currentPlan = currentPlans.pop();
	                try {
	                    physOp.setPlans(exprPlans);
	                } catch (PlanException pe) {
	                    int errCode = 2071;
	                    String msg = "Problem with setting up local rearrange's plans.";
	                    throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, pe);
	                }
	                try {
	                    physOp.setIndex(count++);
	                } catch (ExecException e1) {
	                    int errCode = 2058;
	                    String msg = "Unable to set index on newly create POLocalRearrange.";
	                    throw new VisitorException(msg, errCode, PigException.BUG, e1);
	                }
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
	                    currentPlan.connect(logToPhyMap.get(op), physOp);
	                    currentPlan.connect(physOp, poGlobal);
	                } catch (PlanException e) {
	                    int errCode = 2015;
	                    String msg = "Invalid physical operators in the physical plan" ;
	                    throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
	                }

	            }
	            
	        } catch (PlanException e1) {
	            int errCode = 2015;
	            String msg = "Invalid physical operators in the physical plan" ;
	            throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e1);
	        }

            poPackage.setKeyType(type);
            poPackage.setResultType(DataType.TUPLE);
            poPackage.setNumInps(count);
            
            boolean[] innerFlags = loj.getInnerFlags();
            poPackage.setInner(innerFlags);
            
	        List<PhysicalPlan> fePlans = new ArrayList<PhysicalPlan>();
	        List<Boolean> flattenLst = new ArrayList<Boolean>();
	        
	        try{
    	        for(int i=0;i< count;i++){
    	            PhysicalPlan fep1 = new PhysicalPlan();
    	            POProject feproj1 = new POProject(new OperatorKey(scope, nodeGen.getNextNodeId(scope)), 
    	                    loj.getRequestedParallelism(), i+1); //i+1 since the first column is the "group" field
    	            feproj1.setResultType(DataType.BAG);
    	            feproj1.setOverloaded(false);
    	            fep1.add(feproj1);
    	            fePlans.add(fep1);
    	            // the parser would have marked the side
    	            // where we need to keep empty bags on
    	            // non matched as outer (innerFlags[i] would be
    	            // false)
    	            if(!(innerFlags[i])) {
    	                LogicalOperator joinInput = inputs.get(i);
                        // for outer join add a bincond
                        // which will project nulls when bag is
                        // empty
                        updateWithEmptyBagCheck(fep1, joinInput);
    	            }
    	            flattenLst.add(true);
    	        }
    	        
    	        POForEach fe = new POForEach(new OperatorKey(scope, nodeGen.getNextNodeId(scope)), 
    	                loj.getRequestedParallelism(), fePlans, flattenLst );
    	        currentPlan.add(fe);
	            currentPlan.connect(poPackage, fe);
	            logToPhyMap.put(loj, fe);
	        }catch (PlanException e1) {
	            int errCode = 2015;
	            String msg = "Invalid physical operators in the physical plan" ;
	            throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e1);
	        }
	        
		}
	}

	/**
	 * updates plan with check for empty bag and if bag is empty to flatten a bag
	 * with as many null's as dictated by the schema
	 * @param fePlan the plan to update
	 * @param joinInput the relation for which the corresponding bag is being checked
	 * @throws PlanException
	 * @throws LogicalToPhysicalTranslatorException
	 */
    public static void updateWithEmptyBagCheck(PhysicalPlan fePlan, LogicalOperator joinInput) throws PlanException, LogicalToPhysicalTranslatorException {
        Schema inputSchema = null;
        try {
            inputSchema = joinInput.getSchema();
            
            
            if(inputSchema == null) {
                int errCode = 1105;
                String msg = "Input (" + joinInput.getAlias() + ") " +
                        "on which outer join is desired should have a valid schema";
                throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.INPUT);
            }
        } catch (FrontendException e) {
            int errCode = 2014;
            String msg = "Error while determining the schema of input";
            throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
        }
        
        // we currently have POProject[bag] as the only operator in the plan
        // If the bag is an empty bag, we should replace
        // it with a bag with one tuple with null fields so that when we flatten
        // we do not drop records (flatten will drop records if the bag is left
        // as an empty bag) and actually project nulls for the fields in 
        // the empty bag
        
        // So we need to get to the following state:
        // POProject[Bag]
        //         \     
        //    POUserFunc["IsEmpty()"] Const[Bag](bag with null fields)   
        //                        \      |    POProject[Bag]             
        //                         \     |    /
        //                          POBinCond
        
        POProject relationProject = (POProject) fePlan.getRoots().get(0);
        try {
            
            // condition of the bincond
            POProject relationProjectForIsEmpty = relationProject.clone();
            fePlan.add(relationProjectForIsEmpty);
            String scope = relationProject.getOperatorKey().scope;
            FuncSpec isEmptySpec = new FuncSpec(IsEmpty.class.getName());
            Object f = PigContext.instantiateFuncFromSpec(isEmptySpec);
            POUserFunc isEmpty = new POUserFunc(new OperatorKey(scope, NodeIdGenerator.getGenerator().
                        getNextNodeId(scope)), -1, null, isEmptySpec, (EvalFunc) f);
            isEmpty.setResultType(DataType.BOOLEAN);
            fePlan.add(isEmpty);
            fePlan.connect(relationProjectForIsEmpty, isEmpty);
            
            // lhs of bincond (const bag with null fields)
            ConstantExpression ce = new ConstantExpression(new OperatorKey(scope,
                    NodeIdGenerator.getGenerator().getNextNodeId(scope)));
            // the following should give a tuple with the
            // required number of nulls
            Tuple t = TupleFactory.getInstance().newTuple(inputSchema.size());
            for(int i = 0; i < inputSchema.size(); i++) {
                t.set(i, null);
            }
            List<Tuple> bagContents = new ArrayList<Tuple>(1);
            bagContents.add(t);
            DataBag bg = new NonSpillableDataBag(bagContents);
            ce.setValue(bg);
            ce.setResultType(DataType.BAG);
            //this operator doesn't have any predecessors
            fePlan.add(ce);
            
            //rhs of bincond is the original project
            // let's set up the bincond now
            POBinCond bincond = new POBinCond(new OperatorKey(scope,
                    NodeIdGenerator.getGenerator().getNextNodeId(scope)));
            bincond.setCond(isEmpty);
            bincond.setLhs(ce);
            bincond.setRhs(relationProject);
            bincond.setResultType(DataType.BAG);
            fePlan.add(bincond);

            fePlan.connect(isEmpty, bincond);
            fePlan.connect(ce, bincond);
            fePlan.connect(relationProject, bincond);

        } catch (Exception e) {
            throw new PlanException("Error setting up outerjoin", e);
        }
        
        
    }

    private boolean validateMergeJoin(LOJoin loj) throws LogicalToPhysicalTranslatorException{
	    
	    List<LogicalOperator> preds = loj.getInputs();

	    int errCode = 1101;
	    String errMsg = "Merge Join must have exactly two inputs.";
	    if(preds.size() != 2)
            throw new LogicalToPhysicalTranslatorException(errMsg+" Found: "+preds.size(),errCode);
        
	    return mergeJoinValidator(preds,loj.getPlan());
	}
	
	private boolean mergeJoinValidator(List<LogicalOperator> preds,LogicalPlan lp) throws LogicalToPhysicalTranslatorException{
	    
	    int errCode = 1103;
	    String errMsg = "Merge join only supports Filter, Foreach, filter and Load as its predecessor. Found : ";
	    if(preds != null && !preds.isEmpty()){
	        for(LogicalOperator lo : preds){
	            if (!(lo instanceof LOFilter || lo instanceof LOForEach || lo instanceof LOLoad))
	             throw new LogicalToPhysicalTranslatorException(errMsg, errCode);
	            // All is good at this level. Visit predecessors now.
	            mergeJoinValidator(lp.getPredecessors(lo),lp);
	        }
	    }
	    // We visited everything and all is good.
	    return true;
    }
	
	@Override
    public void visit(LOFilter filter) throws VisitorException {
        String scope = filter.getOperatorKey().scope;
        POFilter poFilter = new POFilter(new OperatorKey(scope, nodeGen
                .getNextNodeId(scope)), filter.getRequestedParallelism());
        poFilter.setResultType(filter.getType());
        currentPlan.add(poFilter);
        logToPhyMap.put(filter, poFilter);
        currentPlans.push(currentPlan);

        currentPlan = new PhysicalPlan();

        PlanWalker<LogicalOperator, LogicalPlan> childWalker = mCurrentWalker
                .spawnChildWalker(filter.getComparisonPlan());
        pushWalker(childWalker);
        mCurrentWalker.walk(this);
        popWalker();

        poFilter.setPlan(currentPlan);
        currentPlan = currentPlans.pop();

        List<LogicalOperator> op = filter.getPlan().getPredecessors(filter);

        PhysicalOperator from;
        if(op != null) {
            from = logToPhyMap.get(op.get(0));
        } else {
            int errCode = 2051;
            String msg = "Did not find a predecessor for Filter." ;
            throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG);
        }
        
        try {
            currentPlan.connect(from, poFilter);
        } catch (PlanException e) {
            int errCode = 2015;
            String msg = "Invalid physical operators in the physical plan" ;
            throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
        }
    }

    @Override
    public void visit(LOStream stream) throws VisitorException {
        String scope = stream.getOperatorKey().scope;
        POStream poStream = new POStream(new OperatorKey(scope, nodeGen
                .getNextNodeId(scope)), stream.getExecutableManager(), 
                stream.getStreamingCommand(), this.pc.getProperties());
        currentPlan.add(poStream);
        logToPhyMap.put(stream, poStream);
        
        List<LogicalOperator> op = stream.getPlan().getPredecessors(stream);

        PhysicalOperator from;
        if(op != null) {
            from = logToPhyMap.get(op.get(0));
        } else {                
            int errCode = 2051;
            String msg = "Did not find a predecessor for Stream." ;
            throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG);
        }
        
        try {
            currentPlan.connect(from, poStream);
        } catch (PlanException e) {
            int errCode = 2015;
            String msg = "Invalid physical operators in the physical plan" ;
            throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
        }
    }

    @Override
    public void visit(LOProject op) throws VisitorException {
        String scope = op.getOperatorKey().scope;
        POProject exprOp;
        if(op.isSendEmptyBagOnEOP()) {
            exprOp = new PORelationToExprProject(new OperatorKey(scope, nodeGen
                .getNextNodeId(scope)), op.getRequestedParallelism());
        } else {
            exprOp = new POProject(new OperatorKey(scope, nodeGen
                .getNextNodeId(scope)), op.getRequestedParallelism());
        }
        exprOp.setResultType(op.getType());
        exprOp.setColumns((ArrayList)op.getProjection());
        exprOp.setStar(op.isStar());
        exprOp.setOverloaded(op.getOverloaded());
        LogicalPlan lp = op.getPlan();
        logToPhyMap.put(op, exprOp);
        currentPlan.add(exprOp);

        List<LogicalOperator> predecessors = lp.getPredecessors(op);

        // Project might not have any predecessors
        if (predecessors == null)
            return;

        for (LogicalOperator lo : predecessors) {
            PhysicalOperator from = logToPhyMap.get(lo);
            try {
                currentPlan.connect(from, exprOp);
            } catch (PlanException e) {
                int errCode = 2015;
                String msg = "Invalid physical operators in the physical plan" ;
                throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
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
        logToPhyMap.put(g, poFE);
        currentPlan.add(poFE);

        // generate cannot have multiple inputs
        List<LogicalOperator> op = g.getPlan().getPredecessors(g);

        // generate may not have any predecessors
        if (op == null)
            return;

        PhysicalOperator from = logToPhyMap.get(op.get(0));
        try {
            currentPlan.connect(from, poFE);
        } catch (PlanException e) {
            int errCode = 2015;
            String msg = "Invalid physical operators in the physical plan" ;
            throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
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
            sortPlans.add(currentPlan);
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
            POUserComparisonFunc comparator = new POUserComparisonFunc(new OperatorKey(
                    scope, nodeGen.getNextNodeId(scope)), s
                    .getRequestedParallelism(), null, s.getUserFunc());
            sort = new POSort(new OperatorKey(scope, nodeGen
                    .getNextNodeId(scope)), s.getRequestedParallelism(), null,
                    sortPlans, s.getAscendingCols(), comparator);
        }
        sort.setLimit(s.getLimit());
        // sort.setRequestedParallelism(s.getType());
        logToPhyMap.put(s, sort);
        currentPlan.add(sort);
        List<LogicalOperator> op = s.getPlan().getPredecessors(s); 
        PhysicalOperator from;
        
        if(op != null) {
            from = logToPhyMap.get(op.get(0));
        } else {
            int errCode = 2051;
            String msg = "Did not find a predecessor for Sort." ;
            throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG);            
        }
        
        try {
            currentPlan.connect(from, sort);
        } catch (PlanException e) {
            int errCode = 2015;
            String msg = "Invalid physical operators in the physical plan" ;
            throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
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
        logToPhyMap.put(op, physOp);
        currentPlan.add(physOp);
        // Distinct will only have a single input
        List<LogicalOperator> inputs = op.getPlan().getPredecessors(op);
        PhysicalOperator from; 
        
        if(inputs != null) {
            from = logToPhyMap.get(inputs.get(0));
        } else {
            int errCode = 2051;
            String msg = "Did not find a predecessor for Distinct." ;
            throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG);            
        }

        try {
            currentPlan.connect(from, physOp);
        } catch (PlanException e) {
            int errCode = 2015;
            String msg = "Invalid physical operators in the physical plan" ;
            throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
        }
    }

    @Override
    public void visit(LOSplit split) throws VisitorException {
        String scope = split.getOperatorKey().scope;
        PhysicalOperator physOp = new POSplit(new OperatorKey(scope, nodeGen
                .getNextNodeId(scope)), split.getRequestedParallelism());
        FileSpec splStrFile;
        try {
            splStrFile = new FileSpec(FileLocalizer.getTemporaryPath(null, pc).toString(),new FuncSpec(BinStorage.class.getName()));
        } catch (IOException e1) {
            byte errSrc = pc.getErrorSource();
            int errCode = 0;
            switch(errSrc) {
            case PigException.BUG:
                errCode = 2016;
                break;
            case PigException.REMOTE_ENVIRONMENT:
                errCode = 6002;
                break;
            case PigException.USER_ENVIRONMENT:
                errCode = 4003;
                break;
            }
            String msg = "Unable to obtain a temporary path." ;
            throw new LogicalToPhysicalTranslatorException(msg, errCode, errSrc, e1);

        }
        ((POSplit)physOp).setSplitStore(splStrFile);
        logToPhyMap.put(split, physOp);

        currentPlan.add(physOp);

        List<LogicalOperator> op = split.getPlan().getPredecessors(split); 
        PhysicalOperator from;
        
        if(op != null) {
            from = logToPhyMap.get(op.get(0));
        } else {
            int errCode = 2051;
            String msg = "Did not find a predecessor for Split." ;
            throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG);            
        }        

        try {
            currentPlan.connect(from, physOp);
        } catch (PlanException e) {
            int errCode = 2015;
            String msg = "Invalid physical operators in the physical plan" ;
            throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
        }
    }

    @Override
    public void visit(LOSplitOutput split) throws VisitorException {
        String scope = split.getOperatorKey().scope;
        PhysicalOperator physOp = new POFilter(new OperatorKey(scope, nodeGen
                .getNextNodeId(scope)), split.getRequestedParallelism());
        logToPhyMap.put(split, physOp);

        currentPlan.add(physOp);
        currentPlans.push(currentPlan);
        currentPlan = new PhysicalPlan();
        PlanWalker<LogicalOperator, LogicalPlan> childWalker = mCurrentWalker
                .spawnChildWalker(split.getConditionPlan());
        pushWalker(childWalker);
        mCurrentWalker.walk(this);
        popWalker();

        ((POFilter) physOp).setPlan(currentPlan);
        currentPlan = currentPlans.pop();
        currentPlan.add(physOp);

        List<LogicalOperator> op = split.getPlan().getPredecessors(split); 
        PhysicalOperator from;
        
        if(op != null) {
            from = logToPhyMap.get(op.get(0));
        } else {
            int errCode = 2051;
            String msg = "Did not find a predecessor for Split Output." ;
            throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG);            
        }        
        
        try {
            currentPlan.connect(from, physOp);
        } catch (PlanException e) {
            int errCode = 2015;
            String msg = "Invalid physical operators in the physical plan" ;
            throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
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
        List<org.apache.pig.impl.logicalLayer.ExpressionOperator> fromList = func.getArguments();
        if(fromList!=null){
            for (LogicalOperator op : fromList) {
                PhysicalOperator from = logToPhyMap.get(op);
                try {
                    currentPlan.connect(from, p);
                } catch (PlanException e) {
                    int errCode = 2015;
                    String msg = "Invalid physical operators in the physical plan" ;
                    throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
                }
            }
        }
        logToPhyMap.put(func, p);

    }

    @Override
    public void visit(LOLoad loLoad) throws VisitorException {
        String scope = loLoad.getOperatorKey().scope;
        POLoad load = new POLoad(new OperatorKey(scope, nodeGen
                .getNextNodeId(scope)), loLoad.isSplittable());
        load.setLFile(loLoad.getInputFile());
        load.setPc(pc);
        load.setResultType(loLoad.getType());
        currentPlan.add(load);
        logToPhyMap.put(loLoad, load);
        this.load = loLoad.getLoadFunc();

        // Load is typically a root operator, but in the multiquery
        // case it might have a store as a predecessor.
        List<LogicalOperator> op = loLoad.getPlan().getPredecessors(loLoad); 
        PhysicalOperator from;
        
        if(op != null) {
            from = logToPhyMap.get(op.get(0));

            try {
                currentPlan.connect(from, load);
            } catch (PlanException e) {
                int errCode = 2015;
                String msg = "Invalid physical operators in the physical plan" ;
                throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
            }
        }
    }

    @Override
    public void visit(LOStore loStore) throws VisitorException {
        String scope = loStore.getOperatorKey().scope;
        POStore store = new POStore(new OperatorKey(scope, nodeGen
                .getNextNodeId(scope)));
        store.setSFile(loStore.getOutputFile());
        store.setInputSpec(loStore.getInputSpec());
        try {
            // create a new schema for ourselves so that when
            // we serialize we are not serializing objects that
            // contain the schema - apparently Java tries to
            // serialize the object containing the schema if
            // we are trying to serialize the schema reference in
            // the containing object. The schema here will be serialized
            // in JobControlCompiler
            store.setSchema(new Schema(loStore.getSchema()));
        } catch (FrontendException e1) {
            int errorCode = 1060;
            String message = "Cannot resolve Store output schema";  
            throw new VisitorException(message, errorCode, PigException.BUG, e1);    
        }
        currentPlan.add(store);
        
        List<LogicalOperator> op = loStore.getPlan().getPredecessors(loStore); 
        PhysicalOperator from;
        
        if(op != null) {
            from = logToPhyMap.get(op.get(0));
        } else {
            int errCode = 2051;
            String msg = "Did not find a predecessor for Store." ;
            throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG);            
        }        

        try {
            currentPlan.connect(from, store);
        } catch (PlanException e) {
            int errCode = 2015;
            String msg = "Invalid physical operators in the physical plan" ;
            throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
        }
        logToPhyMap.put(loStore, store);
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
        logToPhyMap.put(op, ce);
    }

    @Override
    public void visit(LOBinCond op) throws VisitorException {
        String scope = op.getOperatorKey().scope;
        ExpressionOperator physOp = new POBinCond(new OperatorKey(scope,
                nodeGen.getNextNodeId(scope)), op.getRequestedParallelism());
        logToPhyMap.put(op, physOp);
        POBinCond phy = (POBinCond) physOp;
        ExpressionOperator cond = (ExpressionOperator) logToPhyMap.get(op
                .getCond());
        phy.setCond(cond);
        ExpressionOperator lhs = (ExpressionOperator) logToPhyMap.get(op
                .getLhsOp());
        phy.setLhs(lhs);
        ExpressionOperator rhs = (ExpressionOperator) logToPhyMap.get(op
                .getRhsOp());
        phy.setRhs(rhs);
        phy.setResultType(op.getType());
        currentPlan.add(physOp);

        List<LogicalOperator> ops = op.getPlan().getPredecessors(op);

        for (LogicalOperator l : ops) {
            ExpressionOperator from = (ExpressionOperator) logToPhyMap.get(l);
            try {
                currentPlan.connect(from, physOp);
            } catch (PlanException e) {
                int errCode = 2015;
                String msg = "Invalid physical operators in the physical plan" ;
                throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
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

        logToPhyMap.put(op, physOp);

        List<LogicalOperator> inputs = op.getPlan().getPredecessors(op); 
        ExpressionOperator from;
        
        if(inputs != null) {
            from = (ExpressionOperator)logToPhyMap.get(inputs.get(0));
        } else {
            int errCode = 2051;
            String msg = "Did not find a predecessor for Negative." ;
            throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG);            
        }
   
        ((PONegative) physOp).setExpr(from);
        ((PONegative) physOp).setResultType(op.getType());
        try {
            currentPlan.connect(from, physOp);
        } catch (PlanException e) {
            int errCode = 2015;
            String msg = "Invalid physical operators in the physical plan" ;
            throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
        }

    }

    @Override
    public void visit(LOIsNull op) throws VisitorException {
        String scope = op.getOperatorKey().scope;
        UnaryComparisonOperator physOp = new POIsNull(new OperatorKey(scope, nodeGen
                .getNextNodeId(scope)), op.getRequestedParallelism(), null);

        List<LogicalOperator> inputs = op.getPlan().getPredecessors(op); 
        ExpressionOperator from;
        
        if(inputs != null) {
            from = (ExpressionOperator)logToPhyMap.get(inputs.get(0));
        } else {
            int errCode = 2051;
            String msg = "Did not find a predecessor for Null." ;
            throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG);            
        }

        
        physOp.setOperandType(op.getOperand().getType());
        currentPlan.add(physOp);

        logToPhyMap.put(op, physOp);

        
        ((POIsNull) physOp).setExpr(from);
        try {
            currentPlan.connect(from, physOp);
        } catch (PlanException e) {
            int errCode = 2015;
            String msg = "Invalid physical operators in the physical plan" ;
            throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
        }

    }

    @Override
    public void visit(LOMapLookup op) throws VisitorException {
        String scope = (op.getOperatorKey()).scope;
        ExpressionOperator physOp = new POMapLookUp(new OperatorKey(scope,
                nodeGen.getNextNodeId(scope)), op.getRequestedParallelism(), op
                .getLookUpKey());
        physOp.setResultType(op.getType());
        currentPlan.add(physOp);

        logToPhyMap.put(op, physOp);

        ExpressionOperator from = (ExpressionOperator) logToPhyMap.get(op
                .getMap());
        try {
            currentPlan.connect(from, physOp);
        } catch (PlanException e) {
            int errCode = 2015;
            String msg = "Invalid physical operators in the physical plan" ;
            throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
        }

    }

    @Override
    public void visit(LOCast op) throws VisitorException {
        String scope = op.getOperatorKey().scope;
        ExpressionOperator physOp = new POCast(new OperatorKey(scope, nodeGen
                .getNextNodeId(scope)), op.getRequestedParallelism());
        currentPlan.add(physOp);

        logToPhyMap.put(op, physOp);
        ExpressionOperator from = (ExpressionOperator) logToPhyMap.get(op
                .getExpression());
        physOp.setResultType(op.getType());
        FuncSpec lfSpec = op.getLoadFuncSpec();
        if(null != lfSpec) {
            ((POCast) physOp).setLoadFSpec(lfSpec);
        }
        try {
            currentPlan.connect(from, physOp);
        } catch (PlanException e) {
            int errCode = 2015;
            String msg = "Invalid physical operators in the physical plan" ;
            throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
        }

    }

    @Override
    public void visit(LOLimit limit) throws VisitorException {
            String scope = limit.getOperatorKey().scope;
            POLimit poLimit = new POLimit(new OperatorKey(scope, nodeGen.getNextNodeId(scope)), limit.getRequestedParallelism());
            poLimit.setResultType(limit.getType());
            poLimit.setLimit(limit.getLimit());
            currentPlan.add(poLimit);
            logToPhyMap.put(limit, poLimit);

            List<LogicalOperator> op = limit.getPlan().getPredecessors(limit);

            PhysicalOperator from;
            if(op != null) {
                from = logToPhyMap.get(op.get(0));
            } else {
                int errCode = 2051;
                String msg = "Did not find a predecessor for Limit." ;
                throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG);                
            }
            try {
                    currentPlan.connect(from, poLimit);
            } catch (PlanException e) {
                int errCode = 2015;
                String msg = "Invalid physical operators in the physical plan" ;
                throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
            }
    }

    @Override
    public void visit(LOUnion op) throws VisitorException {
        String scope = op.getOperatorKey().scope;
        POUnion physOp = new POUnion(new OperatorKey(scope, nodeGen
                .getNextNodeId(scope)), op.getRequestedParallelism());
        currentPlan.add(physOp);
        physOp.setResultType(op.getType());
        logToPhyMap.put(op, physOp);
        List<LogicalOperator> ops = op.getInputs();

        for (LogicalOperator l : ops) {
            PhysicalOperator from = logToPhyMap.get(l);
            try {
                currentPlan.connect(from, physOp);
            } catch (PlanException e) {
                int errCode = 2015;
                String msg = "Invalid physical operators in the physical plan" ;
                throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
            }
        }
    }

}
