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
package org.apache.pig.experimental.logical.expression;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.pig.FuncSpec;
import org.apache.pig.PigException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.LogicalToPhysicalTranslatorException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.Add;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.BinaryComparisonOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.BinaryExpressionOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.ConstantExpression;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.Divide;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.EqualToExpr;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.ExpressionOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.GreaterThanExpr;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.LessThanExpr;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.Mod;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.Multiply;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.NotEqualToExpr;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POAnd;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POCast;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POIsNull;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POMapLookUp;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.PONegative;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.PONot;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POOr;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POProject;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.PORelationToExprProject;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.Subtract;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.data.DataType;
import org.apache.pig.experimental.logical.relational.LogicalRelationalOperator;
import org.apache.pig.experimental.plan.DependencyOrderWalker;
import org.apache.pig.experimental.plan.Operator;
import org.apache.pig.experimental.plan.OperatorPlan;
import org.apache.pig.experimental.plan.PlanWalker;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanException;

public class ExpToPhyTranslationVisitor extends LogicalExpressionVisitor {

    // This value points to the current LogicalRelationalOperator we are working on
    protected LogicalRelationalOperator currentOp;
    
    public ExpToPhyTranslationVisitor(OperatorPlan plan, LogicalRelationalOperator op, PhysicalPlan phyPlan, Map<Operator, PhysicalOperator> map) {
        super(plan, new DependencyOrderWalker(plan));
        currentOp = op;
        logToPhyMap = map;
        currentPlan = phyPlan;
        currentPlans = new Stack<PhysicalPlan>();
    }
    
    public ExpToPhyTranslationVisitor(OperatorPlan plan, PlanWalker walker, LogicalRelationalOperator op, PhysicalPlan phyPlan, Map<Operator, PhysicalOperator> map) {
        super(plan, walker);
        currentOp = op;
        logToPhyMap = map;
        currentPlan = phyPlan;
        currentPlans = new Stack<PhysicalPlan>();
    }
    
    protected Map<Operator, PhysicalOperator> logToPhyMap;

    protected Stack<PhysicalPlan> currentPlans;

    protected PhysicalPlan currentPlan;

    protected NodeIdGenerator nodeGen = NodeIdGenerator.getGenerator();

    protected PigContext pc;
    
    public void setPigContext(PigContext pc) {
        this.pc = pc;
    }

    public PhysicalPlan getPhysicalPlan() {
        return currentPlan;
    }
    
    private void attachBinaryComparisonOperator( BinaryExpression op, 
            BinaryComparisonOperator exprOp ) throws IOException {
        // We dont have aliases in ExpressionOperators
        // exprOp.setAlias(op.getAlias());
        
        
        exprOp.setOperandType(op.getLhs().getType());
        exprOp.setLhs((ExpressionOperator) logToPhyMap.get(op.getLhs()));
        exprOp.setRhs((ExpressionOperator) logToPhyMap.get(op.getRhs()));
        OperatorPlan oPlan = op.getPlan();

        currentPlan.add(exprOp);
        logToPhyMap.put(op, exprOp);

        List<Operator> successors = oPlan.getSuccessors(op);
        if (successors == null) {
            return;
        }
        for (Operator lo : successors) {
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
    
    private void attachBinaryExpressionOperator( BinaryExpression op, 
            BinaryExpressionOperator exprOp ) throws IOException {
        // We dont have aliases in ExpressionOperators
        // exprOp.setAlias(op.getAlias());
        
        
        exprOp.setResultType(op.getLhs().getType());
        exprOp.setLhs((ExpressionOperator) logToPhyMap.get(op.getLhs()));
        exprOp.setRhs((ExpressionOperator) logToPhyMap.get(op.getRhs()));
        OperatorPlan oPlan = op.getPlan();

        currentPlan.add(exprOp);
        logToPhyMap.put(op, exprOp);

        List<Operator> successors = oPlan.getSuccessors(op);
        if (successors == null) {
            return;
        }
        for (Operator lo : successors) {
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
    public void visitAnd( AndExpression op ) throws IOException {
        
//        System.err.println("Entering And");
        BinaryComparisonOperator exprOp = new POAnd(new OperatorKey(DEFAULT_SCOPE, nodeGen.getNextNodeId(DEFAULT_SCOPE)));
        
        attachBinaryComparisonOperator(op, exprOp);
    }
    
    @Override
    public void visitOr( OrExpression op ) throws IOException {
        
//        System.err.println("Entering Or");
        BinaryComparisonOperator exprOp = new POOr(new OperatorKey(DEFAULT_SCOPE, nodeGen.getNextNodeId(DEFAULT_SCOPE)));
        
        attachBinaryComparisonOperator(op, exprOp);
    }
    
    @Override
    public void visitEqual( EqualExpression op ) throws IOException {
        
        BinaryComparisonOperator exprOp = new EqualToExpr(new OperatorKey(
                DEFAULT_SCOPE, nodeGen.getNextNodeId(DEFAULT_SCOPE)));
        
        attachBinaryComparisonOperator(op, exprOp);
    }
    
    @Override
    public void visitNotEqual( NotEqualExpression op ) throws IOException {
        
        BinaryComparisonOperator exprOp = new NotEqualToExpr(new OperatorKey(
                DEFAULT_SCOPE, nodeGen.getNextNodeId(DEFAULT_SCOPE)));
        
        attachBinaryComparisonOperator(op, exprOp);
    }
    
    @Override
    public void visitGreaterThan( GreaterThanExpression op ) throws IOException {
        
        BinaryComparisonOperator exprOp = new GreaterThanExpr(new OperatorKey(
                DEFAULT_SCOPE, nodeGen.getNextNodeId(DEFAULT_SCOPE)));
        
        attachBinaryComparisonOperator(op, exprOp);
    }
    
    @Override
    public void visitGreaterThanEqual( GreaterThanEqualExpression op ) throws IOException {
        
        BinaryComparisonOperator exprOp = new LessThanExpr(new OperatorKey(
                DEFAULT_SCOPE, nodeGen.getNextNodeId(DEFAULT_SCOPE)));
        
        attachBinaryComparisonOperator(op, exprOp);
    }
    
    @Override
    public void visitLessThan( LessThanExpression op ) throws IOException {
        
        BinaryComparisonOperator exprOp = new LessThanExpr(new OperatorKey(
                DEFAULT_SCOPE, nodeGen.getNextNodeId(DEFAULT_SCOPE)));
        
        attachBinaryComparisonOperator(op, exprOp);
    }
    
    
    @Override
    public void visitLessThanEqual( LessThanEqualExpression op ) throws IOException {
        
        BinaryComparisonOperator exprOp = new LessThanExpr(new OperatorKey(
                DEFAULT_SCOPE, nodeGen.getNextNodeId(DEFAULT_SCOPE)));
        
        attachBinaryComparisonOperator(op, exprOp);
    }
    
    @Override
    public void visitProject(ProjectExpression op) throws IOException {
//        System.err.println("Entering Project");
        POProject exprOp;
       
        if(op.getType() == DataType.BAG) {
            exprOp = new PORelationToExprProject(new OperatorKey(DEFAULT_SCOPE, nodeGen
                .getNextNodeId(DEFAULT_SCOPE)));
         } else {
            exprOp = new POProject(new OperatorKey(DEFAULT_SCOPE, nodeGen
                .getNextNodeId(DEFAULT_SCOPE)));
        }
        
        // We dont have aliases in ExpressionOperators
        // exprOp.setAlias(op.getAlias());
        exprOp.setResultType(op.getType());
        exprOp.setColumn(op.getColNum());
        exprOp.setStar(false);
        // TODO implement this
//        exprOp.setOverloaded(op.getOverloaded());
        logToPhyMap.put(op, exprOp);
        currentPlan.add(exprOp);
        
        // We only have one input so connection is required from only one predecessor
//        PhysicalOperator from = logToPhyMap.get(op.findReferent(currentOp));
//        currentPlan.connect(from, exprOp);
        
//        List<Operator> predecessors = lp.getPredecessors(op);
//
//        // Project might not have any predecessors
//        if (predecessors == null)
//            return;
//
//        for (Operator lo : predecessors) {
//            PhysicalOperator from = logToPhyMap.get(lo);
//            try {
//                currentPlan.connect(from, exprOp);
//            } catch (PlanException e) {
//                int errCode = 2015;
//                String msg = "Invalid physical operators in the physical plan" ;
//                throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
//            }
//        }
//        System.err.println("Exiting Project");
    }
    
    @Override
    public void visitMapLookup( MapLookupExpression op ) throws IOException {
        ExpressionOperator physOp = new POMapLookUp(new OperatorKey(DEFAULT_SCOPE,
                nodeGen.getNextNodeId(DEFAULT_SCOPE)));
        ((POMapLookUp)physOp).setLookUpKey(op.getLookupKey() );
        physOp.setResultType(op.getType());
        physOp.setAlias(op.getFieldSchema().alias);
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
    public void visitConstant(org.apache.pig.experimental.logical.expression.ConstantExpression op) throws IOException {
        
//        System.err.println("Entering Constant");
        ConstantExpression ce = new ConstantExpression(new OperatorKey(DEFAULT_SCOPE,
                nodeGen.getNextNodeId(DEFAULT_SCOPE)));
        // We dont have aliases in ExpressionOperators
        // ce.setAlias(op.getAlias());
        ce.setValue(op.getValue());
        ce.setResultType(op.getType());
        //this operator doesn't have any predecessors
        currentPlan.add(ce);
        logToPhyMap.put(op, ce);
//        System.err.println("Exiting Constant");
    }
    
    @Override
    public void visitCast( CastExpression op ) throws IOException {
        POCast pCast = new POCast(new OperatorKey(DEFAULT_SCOPE, nodeGen
                .getNextNodeId(DEFAULT_SCOPE)));
//        physOp.setAlias(op.getAlias());
        currentPlan.add(pCast);

        logToPhyMap.put(op, pCast);
        ExpressionOperator from = (ExpressionOperator) logToPhyMap.get(op
                .getExpression());
        pCast.setResultType(op.getType());
        FuncSpec lfSpec = op.getFuncSpec();
        if(null != lfSpec) {
            pCast.setFuncSpec(lfSpec);
        }
        try {
            currentPlan.connect(from, pCast);
        } catch (PlanException e) {
            int errCode = 2015;
            String msg = "Invalid physical operators in the physical plan" ;
            throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
        }
    }
    
    @Override
    public void visitNot( NotExpression op ) throws IOException {
        
        PONot pNot = new PONot(new OperatorKey(DEFAULT_SCOPE, nodeGen
                .getNextNodeId(DEFAULT_SCOPE)));
//        physOp.setAlias(op.getAlias());
        currentPlan.add(pNot);

        logToPhyMap.put(op, pNot);
        ExpressionOperator from = (ExpressionOperator) logToPhyMap.get(op
                .getExpression());
        pNot.setResultType(op.getType());        
        try {
            currentPlan.connect(from, pNot);
        } catch (PlanException e) {
            int errCode = 2015;
            String msg = "Invalid physical operators in the physical plan" ;
            throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
        }
    }
    
    @Override
    public void visitIsNull( IsNullExpression op ) throws IOException {
        POIsNull pIsNull = new POIsNull(new OperatorKey(DEFAULT_SCOPE, nodeGen
                .getNextNodeId(DEFAULT_SCOPE)));
//        physOp.setAlias(op.getAlias());
        currentPlan.add(pIsNull);

        logToPhyMap.put(op, pIsNull);
        ExpressionOperator from = (ExpressionOperator) logToPhyMap.get(op
                .getExpression());
        pIsNull.setResultType(op.getType());        
        try {
            currentPlan.connect(from, pIsNull);
        } catch (PlanException e) {
            int errCode = 2015;
            String msg = "Invalid physical operators in the physical plan" ;
            throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
        }
    }

    @Override
    public void visitNegative( NegativeExpression op ) throws IOException {
        PONegative pNegative = new PONegative(new OperatorKey(DEFAULT_SCOPE, nodeGen
                .getNextNodeId(DEFAULT_SCOPE)));
//        physOp.setAlias(op.getAlias());
        currentPlan.add(pNegative);

        logToPhyMap.put(op, pNegative);
        ExpressionOperator from = (ExpressionOperator) logToPhyMap.get(op
                .getExpression());
        pNegative.setResultType(op.getType());        
        try {
            currentPlan.connect(from, pNegative);
        } catch (PlanException e) {
            int errCode = 2015;
            String msg = "Invalid physical operators in the physical plan" ;
            throw new LogicalToPhysicalTranslatorException(msg, errCode, PigException.BUG, e);
        }
    }
    
    @Override
    public void visitAdd( AddExpression op ) throws IOException {        
        BinaryExpressionOperator exprOp = new Add(new OperatorKey(DEFAULT_SCOPE, nodeGen.getNextNodeId(DEFAULT_SCOPE)));        
        
        attachBinaryExpressionOperator(op, exprOp);
    }
    
    @Override
    public void visitSubtract( SubtractExpression op ) throws IOException {        
        BinaryExpressionOperator exprOp = new Subtract(new OperatorKey(DEFAULT_SCOPE, nodeGen.getNextNodeId(DEFAULT_SCOPE)));        
        
        attachBinaryExpressionOperator(op, exprOp);
    }
    
    @Override
    public void visitMultiply( MultiplyExpression op ) throws IOException {        
        BinaryExpressionOperator exprOp = new Multiply(new OperatorKey(DEFAULT_SCOPE, nodeGen.getNextNodeId(DEFAULT_SCOPE)));        
        
        attachBinaryExpressionOperator(op, exprOp);
    }
    
    @Override
    public void visitDivide( DivideExpression op ) throws IOException {        
        BinaryExpressionOperator exprOp = new Divide(new OperatorKey(DEFAULT_SCOPE, nodeGen.getNextNodeId(DEFAULT_SCOPE)));        
        
        attachBinaryExpressionOperator(op, exprOp);
    }
    
    @Override
    public void visitMod( ModExpression op ) throws IOException {        
        BinaryExpressionOperator exprOp = new Mod(new OperatorKey(DEFAULT_SCOPE, nodeGen.getNextNodeId(DEFAULT_SCOPE)));        
        
        attachBinaryExpressionOperator(op, exprOp);
    }
}
