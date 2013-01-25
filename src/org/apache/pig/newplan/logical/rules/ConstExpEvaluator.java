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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Stack;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.ReverseDependencyOrderWalker;
import org.apache.pig.newplan.logical.expression.AddExpression;
import org.apache.pig.newplan.logical.expression.AndExpression;
import org.apache.pig.newplan.logical.expression.BinCondExpression;
import org.apache.pig.newplan.logical.expression.CastExpression;
import org.apache.pig.newplan.logical.expression.ConstantExpression;
import org.apache.pig.newplan.logical.expression.DereferenceExpression;
import org.apache.pig.newplan.logical.expression.DivideExpression;
import org.apache.pig.newplan.logical.expression.EqualExpression;
import org.apache.pig.newplan.logical.expression.GreaterThanEqualExpression;
import org.apache.pig.newplan.logical.expression.GreaterThanExpression;
import org.apache.pig.newplan.logical.expression.IsNullExpression;
import org.apache.pig.newplan.logical.expression.LessThanExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionVisitor;
import org.apache.pig.newplan.logical.expression.MapLookupExpression;
import org.apache.pig.newplan.logical.expression.ModExpression;
import org.apache.pig.newplan.logical.expression.MultiplyExpression;
import org.apache.pig.newplan.logical.expression.NegativeExpression;
import org.apache.pig.newplan.logical.expression.NotExpression;
import org.apache.pig.newplan.logical.expression.OrExpression;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.expression.RegexExpression;
import org.apache.pig.newplan.logical.expression.SubtractExpression;
import org.apache.pig.newplan.logical.expression.UserFuncExpression;

/**
 *  a constant expression evaluation visitor that will evaluate the constant expressions.
 *  It works by traversing the expression tree in a bottom-up manner and evaluate all
 *   subexpressions that have all constant subexpressions. All results from constant
 *    children are pushed to a stack for the parent to digest for its own evaluation.
 *    Any non-constant expression will push a null to the stack and consequently will
 *     cause all of its ancestors not to be evaluated.
 *
 *  For simplicity, only constant binary expressions and constant unary expressions are
 *   evaluated. More complex expressions are not evaluated at this moment. For UDF,
 *   this evaluation is not planned at all for fear of possible stateful consequences
 *   resulting from the original evaluations
 *
 */
class ConstExpEvaluator extends LogicalExpressionVisitor {
    Deque<ConstantExpression> result;

    ConstExpEvaluator(OperatorPlan plan) throws FrontendException {
        super(plan, new ReverseDependencyOrderWalker(plan));
        result = new LinkedList<ConstantExpression>();
    }

    @SuppressWarnings("unchecked")
    private void binaryExpressionConstPrune(LogicalExpression parent)
                    throws FrontendException {
        ConstantExpression rhs = result.pop(), lhs = result.pop();
        if (rhs == null || lhs == null) {
            result.push(null);
        }
        else {
            ConstantExpression newExp = null;
            if (parent instanceof AndExpression) newExp = new ConstantExpression(
                            plan,
                            (Boolean) rhs.getValue() && (Boolean) lhs.getValue());
            else if (parent instanceof OrExpression) newExp = new ConstantExpression(
                            plan,
                            (Boolean) rhs.getValue() || (Boolean) lhs.getValue());
            else if (parent instanceof EqualExpression) newExp = new ConstantExpression(
                            plan, rhs.isEqual(lhs));
            else if (parent instanceof GreaterThanExpression) newExp = new ConstantExpression(
                            plan,
                            ((Comparable) rhs.getValue()).compareTo((Comparable) lhs.getValue()) > 0);
            else if (parent instanceof GreaterThanEqualExpression) newExp = new ConstantExpression(
                            plan,
                            ((Comparable) rhs.getValue()).compareTo((Comparable) lhs.getValue()) >= 0);
            else if (parent instanceof LessThanExpression) newExp = new ConstantExpression(
                            plan,
                            ((Comparable) rhs.getValue()).compareTo((Comparable) lhs.getValue()) < 0);
            else if (parent instanceof LessThanExpression) newExp = new ConstantExpression(
                            plan,
                            ((Comparable) rhs.getValue()).compareTo((Comparable) lhs.getValue()) <= 0);
            else if (parent instanceof AddExpression) {
                byte type = parent.getFieldSchema().type;
                switch (type) {
                    case DataType.INTEGER:
                        newExp = new ConstantExpression(
                                        plan,
                                        (Integer) lhs.getValue() + (Integer) rhs.getValue());
                        break;
                    case DataType.LONG:
                        newExp = new ConstantExpression(
                                        plan,
                                        (Long) lhs.getValue() + (Long) rhs.getValue());
                        break;
                    case DataType.FLOAT:
                        newExp = new ConstantExpression(
                                        plan,
                                        (Float) lhs.getValue() + (Float) rhs.getValue());
                        break;
                    case DataType.DOUBLE:
                        newExp = new ConstantExpression(
                                        plan,
                                        (Double) lhs.getValue() + (Double) rhs.getValue());
                        break;
                    case DataType.BIGINTEGER:
                        newExp = new ConstantExpression(
                                        plan,
                                        ((BigInteger) lhs.getValue()).add((BigInteger) rhs.getValue()));
                        break;
                    case DataType.BIGDECIMAL:
                        newExp = new ConstantExpression(
                                        plan,
                                        ((BigDecimal) lhs.getValue()).add((BigDecimal) rhs.getValue()));
                        break;
                    default:
                        throw new FrontendException("Invalid type");
                }
            }
            else if (parent instanceof SubtractExpression) {
                byte type = parent.getFieldSchema().type;
                switch (type) {
                    case DataType.INTEGER:
                        newExp = new ConstantExpression(
                                        plan,
                                        (Integer) lhs.getValue() - (Integer) rhs.getValue());
                        break;
                    case DataType.LONG:
                        newExp = new ConstantExpression(
                                        plan,
                                        (Long) lhs.getValue() - (Long) rhs.getValue());
                        break;
                    case DataType.FLOAT:
                        newExp = new ConstantExpression(
                                        plan,
                                        (Float) lhs.getValue() - (Float) rhs.getValue());
                        break;
                    case DataType.DOUBLE:
                        newExp = new ConstantExpression(
                                        plan,
                                        (Double) lhs.getValue() - (Double) rhs.getValue());
                        break;
                    case DataType.BIGINTEGER:
                        newExp = new ConstantExpression(
                                        plan,
                                        ((BigInteger) lhs.getValue()).subtract((BigInteger) rhs.getValue()));
                        break;
                    case DataType.BIGDECIMAL:
                        newExp = new ConstantExpression(
                                        plan,
                                        ((BigDecimal) lhs.getValue()).subtract((BigDecimal) rhs.getValue()));
                        break;
                    default:
                        throw new FrontendException("Invalid type");
                }
            }
            else if (parent instanceof MultiplyExpression) {
                byte type = parent.getFieldSchema().type;
                switch (type) {
                    case DataType.INTEGER:
                        newExp = new ConstantExpression(
                                        plan,
                                        (Integer) lhs.getValue() * (Integer) rhs.getValue());
                        break;
                    case DataType.LONG:
                        newExp = new ConstantExpression(
                                        plan,
                                        (Long) lhs.getValue() * (Long) rhs.getValue());
                        break;
                    case DataType.FLOAT:
                        newExp = new ConstantExpression(
                                        plan,
                                        (Float) lhs.getValue() * (Float) rhs.getValue());
                        break;
                    case DataType.DOUBLE:
                        newExp = new ConstantExpression(
                                        plan,
                                        (Double) lhs.getValue() * (Double) rhs.getValue());
                        break;
                    case DataType.BIGINTEGER:
                        newExp = new ConstantExpression(
                                        plan,
                                        ((BigInteger) lhs.getValue()).multiply((BigInteger) rhs.getValue()));
                        break;
                    case DataType.BIGDECIMAL:
                        newExp = new ConstantExpression(
                                        plan,
                                        ((BigDecimal) lhs.getValue()).multiply((BigDecimal) rhs.getValue()));
                        break;
                    default:
                        throw new FrontendException("Invalid type");
                }
            }
            else if (parent instanceof ModExpression) {
                byte type = parent.getFieldSchema().type;
                switch (type) {
                    case DataType.INTEGER:
                        newExp = new ConstantExpression(
                                        plan,
                                        (Integer) lhs.getValue() % (Integer) rhs.getValue());
                        break;
                    case DataType.LONG:
                        newExp = new ConstantExpression(
                                        plan,
                                        (Long) lhs.getValue() % (Long) rhs.getValue());
                        break;
                    case DataType.BIGINTEGER:
                        newExp = new ConstantExpression(
                                        plan,
                                        ((BigInteger) lhs.getValue()).mod((BigInteger) rhs.getValue()));
                        break;
                    default:
                        throw new FrontendException("Invalid type");
                }
            }
            else if (parent instanceof DivideExpression) {
                byte type = parent.getFieldSchema().type;
                switch (type) {
                    case DataType.INTEGER:
                        if ((Integer) rhs.getValue() != 0)
                            newExp = new ConstantExpression(
                                            plan,
                                            (Integer) lhs.getValue() / (Integer) rhs.getValue());
                        break;
                    case DataType.LONG:
                        if ((Long) rhs.getValue() != 0)
                            newExp = new ConstantExpression(
                                            plan,
                                            (Long) lhs.getValue() / (Long) rhs.getValue());
                        break;
                    case DataType.FLOAT:
                        if ((Float) rhs.getValue() != 0)
                            newExp = new ConstantExpression(
                                            plan,
                                            (Float) lhs.getValue() / (Float) rhs.getValue());
                        break;
                    case DataType.DOUBLE:
                        if ((Double) rhs.getValue() != 0)
                            newExp = new ConstantExpression(
                                            plan,
                                            (Double) lhs.getValue() / (Double) rhs.getValue());
                        break;
                    case DataType.BIGINTEGER:
                        newExp = new ConstantExpression(
                                        plan,
                                        ((BigInteger) lhs.getValue()).divide((BigInteger) rhs.getValue()));
                        break;
                    case DataType.BIGDECIMAL:
                        newExp = new ConstantExpression(
                                        plan,
                                        ((BigDecimal) lhs.getValue()).divide((BigDecimal) rhs.getValue()));
                        break;
                    default:
                        throw new FrontendException("Invalid type");
                }
            }
            else throw new FrontendException("Invalid instance type.");
            if (newExp != null) {
                plan.disconnect(parent, rhs);
                plan.remove(rhs);
                plan.disconnect(parent, lhs);
                plan.remove(lhs);
                plan.add(newExp);
                List<Operator> predList = plan.getPredecessors(parent);
                if (predList != null) {
                    Operator[] preds = predList.toArray(new Operator[0]);
                    for (Object p : preds) {
                        Operator pred = (Operator) p;
                        Pair<Integer, Integer> pos = plan.disconnect(pred, parent);
                        plan.connect(pred, pos.first, newExp, pos.second);
                    }
                }
                plan.remove(parent);
                result.push(newExp);
            }
            else result.push(null);
        }
    }

    private void unaryExpressionConstPrune(LogicalExpression op)
                    throws FrontendException {
        ConstantExpression operand = result.pop();
        if (operand == null) {
            result.push(null);
            return;
        }
        if (op instanceof CastExpression) {
            // no simplification on cast for now
            result.push(null);
        }
        else {
            plan.remove(operand);
            plan.remove(op);
            ConstantExpression newExp;
            if (op instanceof NotExpression) newExp = new ConstantExpression(
                            plan, !((Boolean) operand.getValue()));
            else if (op instanceof IsNullExpression) newExp = new ConstantExpression(
                            plan, operand.getValue() == null);
            else if (op instanceof NegativeExpression) {
                byte type = operand.getFieldSchema().type;
                switch (type) {
                    case DataType.INTEGER:
                        newExp = new ConstantExpression(
                                        plan,
                                        -1 * ((Integer) operand.getValue()));
                        break;
                    case DataType.LONG:
                        newExp = new ConstantExpression(
                                        plan,
                                        -1L * ((Long) operand.getValue()));
                        break;
                    case DataType.FLOAT:
                        newExp = new ConstantExpression(
                                        plan,
                                        -1.0F * ((Float) operand.getValue()));
                        break;
                    case DataType.DOUBLE:
                        newExp = new ConstantExpression(
                                        plan,
                                        -1.0D * ((Integer) operand.getValue()));
                        break;
                    case DataType.BIGINTEGER:
                        newExp = new ConstantExpression(
                                        plan,
                                        ((BigInteger) operand.getValue()).negate());
                        break;
                    case DataType.BIGDECIMAL:
                        newExp = new ConstantExpression(
                                        plan,
                                        ((BigDecimal) operand.getValue()).negate());
                        break;
                    default:
                        throw new FrontendException("Invalid type");
                }
            }
            else throw new FrontendException("Invalid instance type.");
            plan.add(newExp);
            result.push(newExp);
        }
    }

    private void noConstPrune(LogicalExpression op)
                    throws FrontendException {
        // universal handler of an expression that can not or currently does not prune constant
        // sub expressions after pre-calculation
        List<Operator> sucs = op.getPlan().getSuccessors(op);
        int nSucs = (sucs == null ? 0 : sucs.size());
        for (int i = 0; i < nSucs; i++)
            result.pop();
        result.push(null);
    }

    @Override
    public void visit(ConstantExpression constant)
                    throws FrontendException {
        result.push(constant);
    }

    @Override
    public void visit(AndExpression andExpr) throws FrontendException {
        binaryExpressionConstPrune(andExpr);
    }

    @Override
    public void visit(OrExpression orExpr) throws FrontendException {
        binaryExpressionConstPrune(orExpr);
    }

    @Override
    public void visit(EqualExpression equal) throws FrontendException {
        binaryExpressionConstPrune(equal);
    }

    @Override
    public void visit(ProjectExpression project)
                    throws FrontendException {
        noConstPrune(project);
    }

    @Override
    public void visit(CastExpression cast) throws FrontendException {
        unaryExpressionConstPrune(cast);
    }

    @Override
    public void visit(GreaterThanExpression greaterThanExpression)
                    throws FrontendException {
        binaryExpressionConstPrune(greaterThanExpression);
    }

    @Override
    public void visit(NotExpression op) throws FrontendException {
        unaryExpressionConstPrune(op);
    }

    @Override
    public void visit(IsNullExpression op) throws FrontendException {
        unaryExpressionConstPrune(op);
    }

    @Override
    public void visit(NegativeExpression op) throws FrontendException {
        unaryExpressionConstPrune(op);
    }

    @Override
    public void visit(AddExpression op) throws FrontendException {
        binaryExpressionConstPrune(op);
    }

    @Override
    public void visit(SubtractExpression op) throws FrontendException {
        binaryExpressionConstPrune(op);
    }

    @Override
    public void visit(ModExpression op) throws FrontendException {
        binaryExpressionConstPrune(op);
    }

    @Override
    public void visit(MultiplyExpression op) throws FrontendException {
        binaryExpressionConstPrune(op);
    }

    @Override
    public void visit(DivideExpression op) throws FrontendException {
        binaryExpressionConstPrune(op);
    }

    @Override
    public void visit(MapLookupExpression op) throws FrontendException {
        noConstPrune(op);
    }

    @Override
    public void visit(BinCondExpression op) throws FrontendException {
        noConstPrune(op);
    }

    @Override
    public void visit(UserFuncExpression op) throws FrontendException {
        noConstPrune(op);
    }

    @Override
    public void visit(DereferenceExpression bagDerefenceExpression)
                    throws FrontendException {
        noConstPrune(bagDerefenceExpression);
    }

    @Override
    public void visit(RegexExpression op) throws FrontendException {
        noConstPrune(op);
    }
}
