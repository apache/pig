/**
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
package org.apache.pig.newplan.logical.rules;

import java.util.List;
import java.util.Collection;
import java.util.Set;
import java.util.HashSet;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.newplan.logical.expression.*;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanWalker;
import org.apache.pig.newplan.PlanVisitor;
import org.apache.pig.impl.util.Pair;

/**
 * A NOT conversion visitor that will traverse the expression tree in a depth-first
 * manner with post-order handling. A status of negativity for a NOT expression is
 * recorded in the depth-first traversal before subtree traversal and reversed after
 * traversing the subtree. All "reversible" expressions is replaced by its negated
 * counterpart for negative negativity. Currently equality op, and its 
 * non-equality counter part, all range comparisons, logical AND and OR are reversible.
 * 
 * Notably missing is the "is null" for lack of a "is not null" base expression;
 * UDF and regex are not reversed too for the same sake.
 *
 */
class NOTConversionVisitor extends LogicalExpressionVisitor {
    boolean not = false;

    NOTConversionVisitor(OperatorPlan plan) throws FrontendException {
        super(plan, new NotConversionWalker(plan));
    }

    public void flip() {
        not = !not;
    }

    private void reset(Operator newOp, Operator oldOp)
                    throws FrontendException {
        List<Operator> p = plan.getPredecessors(oldOp);
        if (p != null) {
            Operator[] preds = p.toArray(new Operator[0]);
            for (Operator pred : preds) {
                Pair<Integer, Integer> pos = plan.disconnect(pred, oldOp);
                plan.connect(pred, pos.first, newOp, pos.second);
            }
        }
        List<Operator> s = plan.getSuccessors(oldOp);
        if (s != null) {
            Operator[] sucs = s.toArray(new Operator[0]);
            for (Operator suc : sucs) {
                plan.disconnect(oldOp, suc);
            }
        }
        plan.remove(oldOp);
    }

    private void insert(Operator before, Operator after)
                    throws FrontendException {
        // assume after is already connected to before
        List<Operator> p = plan.getPredecessors(after);
        if (p != null) {
            Operator[] preds = p.toArray(new Operator[0]);
            for (Operator pred : preds) {
                if (pred != before) {
                    Pair<Integer, Integer> pos = plan.disconnect(pred, after);
                    plan.connect(pred, pos.first, before, pos.second);
                }
            }
        }
    }

    private void remove(Operator op) throws FrontendException {
        List<Operator> p = plan.getPredecessors(op);
        if (p != null) {
            Operator[] preds = p.toArray(new Operator[0]);
            for (Operator pred : preds) {
                Pair<Integer, Integer> pos = plan.disconnect(pred, op);
                List<Operator> s = plan.getSuccessors(op);
                if (s != null) {
                    Operator[] sucs = s.toArray(new Operator[0]);
                    for (int i = 0;  i < sucs.length; i++)
                        plan.connect(pred, pos.first+i, sucs[i], pos.second+i);
                }
            }
        }
        List<Operator> s = plan.getSuccessors(op);
        if (s != null) {
            Operator[] sucs = s.toArray(new Operator[0]);
            for (Operator suc : sucs) {
                plan.disconnect(op, suc);
            }
        }
        plan.remove(op);
    }

    @Override
    public void visit(NotExpression not) throws FrontendException {
        remove(not);
    }

    @Override
    public void visit(AndExpression andExpr) throws FrontendException {
        if (not) {
            // Application of DeMorgan's Law to AND
            LogicalExpression newExp = new OrExpression(plan,
                            andExpr.getLhs(), andExpr.getRhs());
            reset(newExp, andExpr);
        }
    }

    @Override
    public void visit(OrExpression orExpr) throws FrontendException {
        if (not) {
            // Application of DeMorgan's Law to OR
            LogicalExpression newExp = new AndExpression(plan,
                            orExpr.getLhs(), orExpr.getRhs());
            reset(newExp, orExpr);
        }
    }

    @Override
    public void visit(EqualExpression equal) throws FrontendException {
        if (not) {
            // Application of DeMorgan's Law to OR
            LogicalExpression newExp = new NotEqualExpression(plan,
                            equal.getLhs(), equal.getRhs());
            reset(newExp, equal);
        }
    }

    @Override
    public void visit(NotEqualExpression op) throws FrontendException {
        if (not) {
            // Application of DeMorgan's Law to OR
            LogicalExpression newExp = new EqualExpression(plan,
                            op.getLhs(), op.getRhs());
            reset(newExp, op);
        }
    }

    @Override
    public void visit(IsNullExpression op) throws FrontendException {
        if (not) {
            LogicalExpression newExp = new NotExpression(plan, op);
            insert(newExp, op);
        }
    }

    @Override
    public void visit(RegexExpression op) throws FrontendException {
        if (not) {
            LogicalExpression newExp = new NotExpression(plan, op);
            insert(newExp, op);
        }
    }
    
    @Override
    public void visit(UserFuncExpression op) throws FrontendException {
        if (not) {
            LogicalExpression newExp = new NotExpression(plan, op);
            insert(newExp, op);
        }
    }
    
    @Override
    public void visit(GreaterThanExpression greaterThanExpression)
                    throws FrontendException {
        if (not) {
            reset(new LessThanEqualExpression(plan,
                            greaterThanExpression.getLhs(),
                            greaterThanExpression.getRhs()),
                            greaterThanExpression);
        }
    }

    @Override
    public void visit(
                    GreaterThanEqualExpression greaterThanEqualExpression)
                    throws FrontendException {
        if (not) {
            reset(new LessThanExpression(plan,
                            greaterThanEqualExpression.getLhs(),
                            greaterThanEqualExpression.getRhs()),
                            greaterThanEqualExpression);
        }
    }

    @Override
    public void visit(LessThanExpression lessThanExpression)
                    throws FrontendException {
        if (not) {
            reset(new GreaterThanEqualExpression(plan,
                            lessThanExpression.getLhs(),
                            lessThanExpression.getRhs()),
                            lessThanExpression);
        }
    }

    @Override
    public void visit(LessThanEqualExpression lessThanEqualExpression)
                    throws FrontendException {
        if (not) {
            reset(new GreaterThanExpression(plan,
                            lessThanEqualExpression.getLhs(),
                            lessThanEqualExpression.getRhs()),
                            lessThanEqualExpression);
        }
    }

    private static class NotConversionWalker extends PlanWalker {
    
        public NotConversionWalker(OperatorPlan plan) {
            super(plan);
        }
    
        @Override
        public PlanWalker spawnChildWalker(OperatorPlan plan) {
            return new NotConversionWalker(plan);
        }
    
        /**
         * Begin traversing the graph.
         * 
         * @param visitor
         *            Visitor this walker is being used by.
         * @throws FrontendException
         *             if an error is encountered while walking.
         */
        @Override
        public void walk(PlanVisitor visitor) throws FrontendException {
            List<Operator> roots = plan.getSources();
            Set<Operator> seen = new HashSet<Operator>();
    
            depthFirst(null, roots, seen, visitor);
        }
    
        private void depthFirst(Operator node,
                        Collection<Operator> successors,
                        Set<Operator> seen, PlanVisitor visitor)
                        throws FrontendException {
            if (successors == null) return;
    
            Operator[] sucs = successors.toArray(new Operator[0]);
            for (Operator suc : sucs) {
                if (seen.add(suc)) {
                    if (suc instanceof NotExpression)
                        ((NOTConversionVisitor) visitor).flip();
                    if(suc instanceof AndExpression
                            || suc instanceof NotExpression
                            || suc instanceof OrExpression
                    ){
                        //visit successors of suc only if they are the boolean operators
                        // the NOT conversion should be propagated only for 
                        // their successors 
                        Collection<Operator> newSuccessors = Utils.mergeCollection(plan.getSuccessors(suc), plan.getSoftLinkSuccessors(suc));
                        depthFirst(suc, newSuccessors, seen, visitor);
                    }
                    suc.accept(visitor);
                    if (suc instanceof NotExpression)
                        ((NOTConversionVisitor) visitor).flip();
                }
            }
        }
    }
}
