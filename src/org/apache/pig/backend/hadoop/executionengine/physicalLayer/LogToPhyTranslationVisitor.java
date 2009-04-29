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
import org.apache.pig.data.DataType;
import org.apache.pig.impl.PigContext;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.*;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.*;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.ExpressionOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.BinaryExpressionOperator;
import org.apache.pig.builtin.BinStorage;
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

public class LogToPhyTranslationVisitor extends LOVisitor {

    protected Map<LogicalOperator, PhysicalOperator> LogToPhyMap;

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
        LogicalPlan lp = op.getPlan();

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
        exprOp.setLhs((ExpressionOperator) LogToPhyMap.get(op.getLhsOperand()));
        exprOp.setRhs((ExpressionOperator) LogToPhyMap.get(op.getRhsOperand()));
        LogicalPlan lp = op.getPlan();

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
        exprOp.setLhs((ExpressionOperator) LogToPhyMap.get(op.getLhsOperand()));
        exprOp.setRhs((ExpressionOperator) LogToPhyMap.get(op.getRhsOperand()));
        LogicalPlan lp = op.getPlan();

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
        exprOp.setLhs((ExpressionOperator) LogToPhyMap.get(op.getLhsOperand()));
        exprOp.setRhs((ExpressionOperator) LogToPhyMap.get(op.getRhsOperand()));
        LogicalPlan lp = op.getPlan();

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
        exprOp.setLhs((ExpressionOperator) LogToPhyMap.get(op.getLhsOperand()));
        exprOp.setRhs((ExpressionOperator) LogToPhyMap.get(op.getRhsOperand()));
        LogicalPlan lp = op.getPlan();

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
        exprOp.setLhs((ExpressionOperator) LogToPhyMap.get(op.getLhsOperand()));
        exprOp.setRhs((ExpressionOperator) LogToPhyMap.get(op.getRhsOperand()));
        LogicalPlan lp = op.getPlan();

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
        exprOp.setLhs((ExpressionOperator)LogToPhyMap.get(op.getLhsOperand()));
        exprOp.setRhs((ExpressionOperator)LogToPhyMap.get(op.getRhsOperand()));
        LogicalPlan lp = op.getPlan();

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
        exprOp.setLhs((ExpressionOperator) LogToPhyMap.get(op.getLhsOperand()));
        exprOp.setRhs((ExpressionOperator) LogToPhyMap.get(op.getRhsOperand()));
        LogicalPlan lp = op.getPlan();

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
        exprOp.setLhs((ExpressionOperator) LogToPhyMap.get(op.getLhsOperand()));
        exprOp.setRhs((ExpressionOperator) LogToPhyMap.get(op.getRhsOperand()));
        LogicalPlan lp = op.getPlan();

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
        exprOp.setLhs((ExpressionOperator) LogToPhyMap.get(op.getLhsOperand()));
        exprOp.setRhs((ExpressionOperator) LogToPhyMap.get(op.getRhsOperand()));
        LogicalPlan lp = op.getPlan();

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
        exprOp.setLhs((ExpressionOperator) LogToPhyMap.get(op.getLhsOperand()));
        exprOp.setRhs((ExpressionOperator) LogToPhyMap.get(op.getRhsOperand()));
        LogicalPlan lp = op.getPlan();

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
        exprOp.setLhs((ExpressionOperator) LogToPhyMap.get(op.getLhsOperand()));
        exprOp.setRhs((ExpressionOperator) LogToPhyMap.get(op.getRhsOperand()));
        LogicalPlan lp = op.getPlan();

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
        exprOp.setLhs((ExpressionOperator)LogToPhyMap.get(op.getLhsOperand()));
        exprOp.setRhs((ExpressionOperator)LogToPhyMap.get(op.getRhsOperand()));
        LogicalPlan lp = op.getPlan();
        
        currentPlan.add(exprOp);
        LogToPhyMap.put(op, exprOp);
        
        List<LogicalOperator> predecessors = lp.getPredecessors(op);
        if(predecessors == null) return;
        for(LogicalOperator lo : predecessors) {
            PhysicalOperator from = LogToPhyMap.get(lo);
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
        exprOp.setLhs((ExpressionOperator)LogToPhyMap.get(op.getLhsOperand()));
        exprOp.setRhs((ExpressionOperator)LogToPhyMap.get(op.getRhsOperand()));
        LogicalPlan lp = op.getPlan();
        
        currentPlan.add(exprOp);
        LogToPhyMap.put(op, exprOp);
        
        List<LogicalOperator> predecessors = lp.getPredecessors(op);
        if(predecessors == null) return;
        for(LogicalOperator lo : predecessors) {
            PhysicalOperator from = LogToPhyMap.get(lo);
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
        exprOp.setExpr((ExpressionOperator)LogToPhyMap.get(op.getOperand()));
        LogicalPlan lp = op.getPlan();
        
        currentPlan.add(exprOp);
        LogToPhyMap.put(op, exprOp);
        
        List<LogicalOperator> predecessors = lp.getPredecessors(op);
        if(predecessors == null) return;
        PhysicalOperator from = LogToPhyMap.get(predecessors.get(0));
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
                List<PhysicalOperator> pop = Arrays.asList(LogToPhyMap.get(op));
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
                currentPlan.connect(LogToPhyMap.get(op), fe);
                
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
        LogToPhyMap.put(cs, fe);
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
            int errCode = 2015;
            String msg = "Invalid physical operators in the physical plan" ;
            throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e1);
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
                currentPlan.connect(LogToPhyMap.get(op), physOp);
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
        LogToPhyMap.put(cg, poPackage);
    }
    
    
    /**
     * Create the inner plans used to configure the Local Rearrange operators(ppLists)
     * Extract the keytypes and create the POFRJoin operator.
     */
    @Override
    protected void visit(LOFRJoin frj) throws VisitorException {
        String scope = frj.getOperatorKey().scope;
        List<LogicalOperator> inputs = frj.getInputs();
        List<List<PhysicalPlan>> ppLists = new ArrayList<List<PhysicalPlan>>();
        List<Byte> keyTypes = new ArrayList<Byte>();
        
        int fragment = findFrag(inputs,frj.getFragOp());
        List<PhysicalOperator> inp = new ArrayList<PhysicalOperator>();
        for (LogicalOperator op : inputs) {
            inp.add(LogToPhyMap.get(op));
            List<LogicalPlan> plans = (List<LogicalPlan>) frj.getJoinColPlans()
                    .get(op);
            
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
            ppLists.add(exprPlans);
            
            if (plans.size() > 1) {
                keyTypes.add(DataType.TUPLE);
            } else {
                keyTypes.add(exprPlans.get(0).getLeaves().get(0).getResultType());
            }
        }
        POFRJoin pfrj;
        try {
            pfrj = new POFRJoin(new OperatorKey(scope,nodeGen.getNextNodeId(scope)),frj.getRequestedParallelism(),
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
                currentPlan.connect(LogToPhyMap.get(op), pfrj);
            } catch (PlanException e) {
                int errCode = 2015;
                String msg = "Invalid physical operators in the physical plan" ;
                throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
            }
        }
        LogToPhyMap.put(frj, pfrj);
    }

    private int findFrag(List<LogicalOperator> inputs, LogicalOperator fragOp) {
        int i=-1;
        for (LogicalOperator lop : inputs) {
            if(fragOp.getOperatorKey().equals(lop.getOperatorKey()))
                return ++i;
        }
        return -1;
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

        PhysicalOperator from;
        if(op != null) {
            from = LogToPhyMap.get(op.get(0));
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
        LogToPhyMap.put(stream, poStream);
        
        List<LogicalOperator> op = stream.getPlan().getPredecessors(stream);

        PhysicalOperator from;
        if(op != null) {
            from = LogToPhyMap.get(op.get(0));
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
            POUserComparisonFunc comparator = new POUserComparisonFunc(new OperatorKey(
                    scope, nodeGen.getNextNodeId(scope)), s
                    .getRequestedParallelism(), null, s.getUserFunc());
            sort = new POSort(new OperatorKey(scope, nodeGen
                    .getNextNodeId(scope)), s.getRequestedParallelism(), null,
                    sortPlans, s.getAscendingCols(), comparator);
        }
        sort.setLimit(s.getLimit());
        // sort.setRequestedParallelism(s.getType());
        LogToPhyMap.put(s, sort);
        currentPlan.add(sort);
        List<LogicalOperator> op = s.getPlan().getPredecessors(s); 
        PhysicalOperator from;
        
        if(op != null) {
            from = LogToPhyMap.get(op.get(0));
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
        LogToPhyMap.put(op, physOp);
        currentPlan.add(physOp);
        // Distinct will only have a single input
        List<LogicalOperator> inputs = op.getPlan().getPredecessors(op);
        PhysicalOperator from; 
        
        if(inputs != null) {
            from = LogToPhyMap.get(inputs.get(0));
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
        LogToPhyMap.put(split, physOp);

        currentPlan.add(physOp);

        List<LogicalOperator> op = split.getPlan().getPredecessors(split); 
        PhysicalOperator from;
        
        if(op != null) {
            from = LogToPhyMap.get(op.get(0));
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

        List<LogicalOperator> op = split.getPlan().getPredecessors(split); 
        PhysicalOperator from;
        
        if(op != null) {
            from = LogToPhyMap.get(op.get(0));
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
                PhysicalOperator from = LogToPhyMap.get(op);
                try {
                    currentPlan.connect(from, p);
                } catch (PlanException e) {
                    int errCode = 2015;
                    String msg = "Invalid physical operators in the physical plan" ;
                    throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
                }
            }
        }
        LogToPhyMap.put(func, p);

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
        LogToPhyMap.put(loLoad, load);
        this.load = loLoad.getLoadFunc();

        // Load is typically a root operator, but in the multiquery
        // case it might have a store as a predecessor.
        List<LogicalOperator> op = loLoad.getPlan().getPredecessors(loLoad); 
        PhysicalOperator from;
        
        if(op != null) {
            from = LogToPhyMap.get(op.get(0));

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
            from = LogToPhyMap.get(op.get(0));
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

        LogToPhyMap.put(op, physOp);

        List<LogicalOperator> inputs = op.getPlan().getPredecessors(op); 
        ExpressionOperator from;
        
        if(inputs != null) {
            from = (ExpressionOperator)LogToPhyMap.get(inputs.get(0));
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
            from = (ExpressionOperator)LogToPhyMap.get(inputs.get(0));
        } else {
            int errCode = 2051;
            String msg = "Did not find a predecessor for Null." ;
            throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG);            
        }

        
        physOp.setOperandType(op.getOperand().getType());
        currentPlan.add(physOp);

        LogToPhyMap.put(op, physOp);

        
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

        LogToPhyMap.put(op, physOp);
        ExpressionOperator from = (ExpressionOperator) LogToPhyMap.get(op
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
            LogToPhyMap.put(limit, poLimit);

            List<LogicalOperator> op = limit.getPlan().getPredecessors(limit);

            PhysicalOperator from;
            if(op != null) {
                from = LogToPhyMap.get(op.get(0));
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
        LogToPhyMap.put(op, physOp);
        List<LogicalOperator> ops = op.getInputs();

        for (LogicalOperator l : ops) {
            PhysicalOperator from = LogToPhyMap.get(l);
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
