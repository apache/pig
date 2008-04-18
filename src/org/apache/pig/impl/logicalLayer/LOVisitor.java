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
import java.util.Iterator;
import java.util.Set;

import org.apache.pig.impl.plan.PlanVisitor;
import org.apache.pig.impl.plan.PlanWalker;
import org.apache.pig.impl.plan.VisitorException;

/**
 * A visitor mechanism for navigating and operating on a tree of Logical
 * Operators. This class contains the logic to navigate the tree, but does not
 * do anything with or to the tree. In order to operate on or extract
 * information from the tree, extend this class. You only need to implement the
 * methods dealing with the logical operators you are concerned with. For
 * example, if you wish to find every LOEval in a logical plan and perform some
 * operation on it, your visitor would look like: class MyLOVisitor extends
 * LOVisitor { public void visitEval(LOEval e) { you're logic here } } Any
 * operators that you do not implement the visitX method for will then be
 * navigated through by this class.
 * 
 * *NOTE* When invoking a visitor, you should never call one of the methods in
 * this class. You should pass your visitor as an argument to visit() on the
 * object you want to visit. So: RIGHT: LOEval myEval; MyVisitor v;
 * myEval.visit(v); WRONG: LOEval myEval; MyVisitor v; v.visitEval(myEval);
 * These methods are only public to make them accessible to the LO* objects.
 */
abstract public class LOVisitor extends PlanVisitor<LogicalOperator, LogicalPlan> {

    public LOVisitor(LogicalPlan plan,
                     PlanWalker<LogicalOperator, LogicalPlan> walker) {
        super(plan, walker);
    }

    /**
     * @param lOp
     *            the logical operator that has to be visited
     * @throws VisitorException
     */
    void visit(LogicalOperator lOp)
            throws VisitorException {
        //
        // Do Nothing
        //
    }

    /**
     * @param eOp
     *            the logical expression operator that has to be visited
     * @throws VisitorException
     */
    void visit(ExpressionOperator eOp)
            throws VisitorException {
        //
        // Do Nothing
        //
    }

    /**
     * @param binOp
     *            the logical binary expression operator that has to be visited
     * @throws VisitorException
     */
    void visit(BinaryExpressionOperator binOp)
            throws VisitorException {
        //
        // Visit the left hand side operand followed by the right hand side
        // operand
        //

        binOp.getLhsOperand().visit(this);
        binOp.getRhsOperand().visit(this);
    }

    /**
     * 
     * @param uniOp
     *            the logical unary operator that has to be visited
     * @throws VisitorException
     */
    void visit(UnaryExpressionOperator uniOp) throws VisitorException {
        // Visit the operand

        uniOp.getOperand().visit(this);
    }

    /**
     * 
     * @param cg
     *            the logical cogroup operator that has to be visited
     * @throws VisitorException
     */
    void visit(LOCogroup cg) throws VisitorException {
        // Visit each of the inputs of cogroup.
        Iterator<ExpressionOperator> i = cg.getGroupByCols().iterator();
        while (i.hasNext()) {
            i.next().visit(this);
        }
    }

    /**
     * 
     * @param g
     *            the logical generate operator that has to be visited
     * @throws VisitorException
     */
    void visit(LOGenerate g) throws VisitorException {
        // Visit each of generates projection elements.
        Iterator<ExpressionOperator> i = g.getProjections().iterator();
        while (i.hasNext()) {
            i.next().visit(this);
        }
    }

    /**
     * 
     * @param s
     *            the logical sort operator that has to be visited
     * @throws VisitorException
     */
    void visit(LOSort s) throws VisitorException {
        // Visit the sort function
        s.getUserFunc().visit(this);
    }

    /**
     * 
     * @param filter
     *            the logical filter operator that has to be visited
     * @throws VisitorException
     */
    void visit(LOFilter filter) throws VisitorException {
        // Visit the condition for the filter followed by the input
        filter.getCondition().visit(this);
    }

    /**
     * 
     * @param split
     *            the logical split operator that has to be visited
     * @throws VisitorException
     */
    void visit(LOSplit split) throws VisitorException {
        // Visit each of split's conditions
        Iterator<ExpressionOperator> i = split.getConditions().iterator();
        while (i.hasNext()) {
            i.next().visit(this);
        }
    }

    /**
     * 
     * @param forEach
     *            the logical foreach operator that has to be visited
     * @throws VisitorException
     */
    void visit(LOForEach forEach) throws VisitorException {
        // Visit the operators that are part of the foreach
        Iterator<LogicalOperator> i = forEach.getOperators().iterator();
        while (i.hasNext()) {
            i.next().visit(this);
        }
    }

    /**
     * Iterate over each expression that is part of the function argument list
     * 
     * @param func
     *            the user defined function
     * @throws VisitorException
     */
    void visit(LOUserFunc func) throws VisitorException {
        // Visit each of the arguments
        Iterator<ExpressionOperator> i = func.getArguments().iterator();
        while (i.hasNext()) {
            i.next().visit(this);
        }
    }

    /**
     * @param binCond the logical binCond operator that has to be visited
     * @throws VisitorException
     */
    void visit(LOBinCond binCond) throws VisitorException {
        /**
         * Visit the conditional expression followed by the left hand operator
         * and the right hand operator respectively
         */

        binCond.getCond().visit(this);
        binCond.getLhsOp().visit(this);
        binCond.getRhsOp().visit(this);
    }

    /**
     * 
     * @param cast
     *            the logical cast operator that has to be visited
     * @throws VisitorException
     */
    void visit(LOCast cast) throws VisitorException {
        // Visit the expression to be cast

        cast.getExpression().visit(this);
    }

}
