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

import java.util.Deque;
import java.util.LinkedList;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.logical.expression.*;
import org.apache.pig.newplan.ReverseDependencyOrderWalker;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;

/**
 * 
 * A utility class to generate a DNF plan from a base plan.
 * Not that DNF plan can be generated only AFTER all NOTs
 * are pushed below the OR/AND expression, namely, the
 * NOTConversionVisitor should be applied before DNF can be
 * generated.
 *
 */
class DNFPlanGenerator extends LogicalExpressionVisitor {
    private OperatorPlan dnfPlan = null;
    Deque<LogicalExpression> result;

    DNFPlanGenerator(OperatorPlan plan) throws FrontendException {
        super(plan, new ReverseDependencyOrderWalker(plan));
        result = new LinkedList<LogicalExpression>();
    }

    OperatorPlan getDNFPlan() {
        if (dnfPlan == null) dnfPlan = (result.isEmpty() ? plan : result.pop().getPlan());
        return dnfPlan;
    }

    @Override
    public void visit(AndExpression exp) throws FrontendException {
        LogicalExpression rhsExp = ((exp.getRhs() instanceof AndExpression || exp.getRhs() instanceof OrExpression ? result.pop() : exp.getRhs()));
        LogicalExpression lhsExp = ((exp.getLhs() instanceof AndExpression || exp.getLhs() instanceof OrExpression ? result.pop() : exp.getLhs()));
        if (!(lhsExp instanceof AndExpression) && !(lhsExp instanceof DNFExpression) && !(lhsExp instanceof OrExpression) && !(rhsExp instanceof AndExpression) && !(rhsExp instanceof OrExpression) && !(rhsExp instanceof DNFExpression)) result.push(exp);
        else {
            if (dnfPlan == null) dnfPlan = new DNFPlan();
            boolean isLhsAnd = lhsExp instanceof AndExpression || 
            (lhsExp instanceof DNFExpression && 
                ((DNFExpression) lhsExp).type == DNFExpression.DNFExpressionType.AND),
                isRhsAnd = rhsExp instanceof AndExpression || (rhsExp instanceof DNFExpression && ((DNFExpression) rhsExp).type == DNFExpression.DNFExpressionType.AND);

            LogicalExpression current;
            if (isLhsAnd && isRhsAnd) {
                current = new DNFExpression("dnfAnd", dnfPlan,
                                DNFExpression.DNFExpressionType.AND);
                ((DNFPlan) dnfPlan).safeAdd(current);
                result.push(current);
                addChildren(current, lhsExp);
                addChildren(current, rhsExp);
            }
            else {
                boolean isLhsOr = lhsExp instanceof OrExpression ||
                (lhsExp instanceof DNFExpression && 
                    ((DNFExpression) lhsExp).type == DNFExpression.DNFExpressionType.OR), 
                isRhsOr = rhsExp instanceof OrExpression || 
                (rhsExp instanceof DNFExpression && ((DNFExpression) rhsExp).type == DNFExpression.DNFExpressionType.OR);

                if (isLhsOr || isRhsOr) current = new DNFExpression(
                                "dnfOr", dnfPlan, DNFExpression.DNFExpressionType.OR);
                else current = new DNFExpression("dnfAnd", dnfPlan,
                                DNFExpression.DNFExpressionType.AND);

                ((DNFPlan) dnfPlan).safeAdd(current);
                result.push(current);
                if (!isLhsOr && !isRhsOr) {
                    if (isLhsAnd) addChildren(current, lhsExp);
                    else if (!isLhsOr) {
                        // lhs is a simple expression
                        ((DNFPlan) dnfPlan).safeAdd(lhsExp);
                        dnfPlan.connect(current, lhsExp);
                    }
                    if (isRhsAnd) addChildren(current, rhsExp);
                    else if (!isRhsOr) {
                        // rhs is a simple expression
                        ((DNFPlan) dnfPlan).safeAdd(rhsExp);
                        dnfPlan.connect(current, rhsExp);
                    }
                }
                else if (!isLhsOr) {
                    // rhs is OR
                    if (!isLhsAnd) {
                        // lhs is simple
                        mergeSimpleOr(current, lhsExp, rhsExp, true);
                    }
                    else {
                        // lhs is AND
                        mergeAndOr(current, lhsExp, rhsExp, true);
                    }
                }
                else if (!isRhsOr) {
                    // lhs is OR
                    if (!isRhsAnd) {
                        // rhs is simple
                        mergeSimpleOr(current, rhsExp, lhsExp, false);
                    }
                    else {
                        // rhs is AND
                        mergeAndOr(current, rhsExp, lhsExp, false);
                    }
                }
                else {
                    // both lhs and rhs are OR
                    Operator[] lhsChildren = lhsExp.getPlan().getSuccessors(
                                    lhsExp).toArray(new Operator[0]);
                    Operator[] rhsChildren = rhsExp.getPlan().getSuccessors(
                                    rhsExp).toArray(new Operator[0]);
                    boolean lhsDNF = lhsExp.getPlan() == dnfPlan, rhsDNF = rhsExp.getPlan() == dnfPlan;
                    int lsize = lhsChildren.length, rsize = rhsChildren.length;
                    LogicalExpression[][] grandChildrenL = new LogicalExpression[lsize][];;
                    for (int i = 0; i < lsize; i++) {
                        if (lhsChildren[i] instanceof AndExpression) {
                            grandChildrenL[i] = lhsChildren[i].getPlan().getSuccessors(
                              lhsChildren[i]).toArray(
                              new LogicalExpression[0]);
                        } else if (lhsChildren[i] instanceof DNFExpression) {
                            grandChildrenL[i] = dnfPlan.getSuccessors(
                              lhsChildren[i]).toArray(
                              new LogicalExpression[0]);
                        } else {
                            grandChildrenL[i] = new LogicalExpression[1];
                            grandChildrenL[i][0] = (LogicalExpression) lhsChildren[i];
                        }
                    }
                    LogicalExpression[][] grandChildrenR = new LogicalExpression[rsize][];;
                    for (int i = 0; i < rsize; i++) {
                        if (rhsChildren[i] instanceof AndExpression) {
                            grandChildrenR[i] = rhsChildren[i].getPlan().getSuccessors(
                              rhsChildren[i]).toArray(
                              new LogicalExpression[0]);
                        } else if (rhsChildren[i] instanceof DNFExpression) {
                            grandChildrenR[i] = dnfPlan.getSuccessors(
                              rhsChildren[i]).toArray(
                              new LogicalExpression[0]);
                        } else {
                            grandChildrenR[i] = new LogicalExpression[1];
                            grandChildrenR[i][0] = (LogicalExpression) rhsChildren[i];
                        }
                    }
                    if (lhsDNF) {
                        removeDescendants(dnfPlan, lhsExp);
                        dnfPlan.remove(lhsExp);
                    }
                    if (rhsDNF) {
                        removeDescendants(dnfPlan, rhsExp);
                        dnfPlan.remove(rhsExp);
                    }
                    for (int i = 0; i < lsize; i++)
                        for (LogicalExpression lgchild : grandChildrenL[i])
                            if (lgchild instanceof LogicalExpressionProxy)
                                ((LogicalExpressionProxy) lgchild).restoreSrc();
                    for (int i = 0; i < rsize; i++)
                        for (LogicalExpression rgchild : grandChildrenR[i])
                            if (rgchild instanceof LogicalExpressionProxy)
                                ((LogicalExpressionProxy) rgchild).restoreSrc();
                    for (int i = 0; i < lsize; i++) {
                        for (int j = 0; j < rsize; j++) {
                            LogicalExpression child = new DNFExpression(
                                            "dnfAnd", dnfPlan,
                                            DNFExpression.DNFExpressionType.AND);
                            ((DNFPlan) dnfPlan).safeAdd(child);
                            dnfPlan.connect(current, child);
                            for (LogicalExpression lgchild : grandChildrenL[i]) {
                                LogicalExpressionProxy lhsClone;
                                if (lgchild instanceof LogicalExpressionProxy) {
                                    lhsClone = new LogicalExpressionProxy(
                                                    dnfPlan,
                                                    ((LogicalExpressionProxy) lgchild).src);
                                }
                                else {
                                    lhsClone = new LogicalExpressionProxy(
                                                    dnfPlan, lgchild);
                                }
                                dnfPlan.add(lhsClone);
                                dnfPlan.connect(child, lhsClone);
                            }
                            for (LogicalExpression rgchild : grandChildrenR[j]) {
                                LogicalExpressionProxy rhsClone;
                                if (rgchild instanceof LogicalExpressionProxy) {
                                    rhsClone = new LogicalExpressionProxy(
                                                    dnfPlan,
                                                    ((LogicalExpressionProxy) rgchild).src);
                                }
                                else {
                                    rhsClone = new LogicalExpressionProxy(
                                                    dnfPlan,
                                                    rgchild);
                                }
                                dnfPlan.add(rhsClone);
                                dnfPlan.connect(child, rhsClone);
                            }
                        }
                    }
                }
            }
        }
    }

    private void removeDescendants(OperatorPlan plan, Operator op)
                    throws FrontendException {
        // remove recursively a operator and it descendants from the plan
        if (plan.getSuccessors(op) == null) return;

        Object[] children = plan.getSuccessors(op).toArray();
        if (children != null) {
            for (Object c : children) {
                Operator child = (Operator) c;
                removeDescendants(plan, child);
                plan.disconnect(op, child);
                plan.remove(child);
            }
        }
    }

    private void mergeSimpleOr(LogicalExpression current,
                    LogicalExpression simple, LogicalExpression or,
                    boolean simpleFirst) throws FrontendException {
        Operator[] orChildren = or.getPlan().getSuccessors(or).toArray(
                        new Operator[0]);
        int size = orChildren.length;
        LogicalExpression[][] grandChildrenOr = new LogicalExpression[size][];;
        for (int i = 0; i < size; i++) {
            if (orChildren[i] instanceof DNFExpression)
              grandChildrenOr[i] = dnfPlan.getSuccessors(
                            orChildren[i]).toArray(
                            new LogicalExpression[0]);
            else if (orChildren[i] instanceof AndExpression)
              grandChildrenOr[i] = orChildren[i].getPlan().getSuccessors(
                  orChildren[i]).toArray(
                  new LogicalExpression[0]);
            else {
                grandChildrenOr[i] = new LogicalExpression[1];
                grandChildrenOr[i][0] = (LogicalExpression) orChildren[i];
            }
        }
        boolean simpleDNF = simple.getPlan() == dnfPlan, orDNF = or.getPlan() == dnfPlan;
        if (simpleDNF) {
            if (simple instanceof LogicalExpressionProxy)
                ((LogicalExpressionProxy) simple).restoreSrc();
            dnfPlan.remove(simple);
        }
        if (orDNF) {
            removeDescendants(dnfPlan, or);
            dnfPlan.remove(or);
        }
        for (int i = 0; i < size; i++) {
            LogicalExpression child = new DNFExpression("dnfAnd",
                            dnfPlan, DNFExpression.DNFExpressionType.AND);
            ((DNFPlan) dnfPlan).safeAdd(child);
            dnfPlan.connect(current, child);
            LogicalExpressionProxy simpleClone;
            if (simple instanceof LogicalExpressionProxy) simpleClone = new LogicalExpressionProxy(
                            dnfPlan,
                            ((LogicalExpressionProxy) simple).src);
            else simpleClone = new LogicalExpressionProxy(dnfPlan, simple);
            dnfPlan.add(simpleClone);
            if (simpleFirst) dnfPlan.connect(child, simpleClone);
            for (Operator gchild : grandChildrenOr[i]) {
                LogicalExpression childClone;
                if (gchild instanceof LogicalExpressionProxy) childClone = (LogicalExpression) gchild;
                else childClone = new LogicalExpressionProxy(dnfPlan,
                                (LogicalExpression) gchild);
                dnfPlan.add(childClone);
                dnfPlan.connect(child, childClone);
            }
            if (!simpleFirst) dnfPlan.connect(child, simpleClone);
        }
    }

    private void mergeAndOr(LogicalExpression current,
                    LogicalExpression and, LogicalExpression or,
                    boolean andFirst) throws FrontendException {
        Operator[] andChildren = and.getPlan().getSuccessors(and).toArray(
                        new Operator[0]);
        Operator[] orChildren = or.getPlan().getSuccessors(or).toArray(
                        new Operator[0]);
        int orSize = orChildren.length;
        int andSize = andChildren.length;
        boolean andDNF = and.getPlan() == dnfPlan, orDNF = or.getPlan() == dnfPlan;
        LogicalExpression[][] grandChildrenOr = new LogicalExpression[orSize][];;
        for (int i = 0; i < orSize; i++) {
            if (orChildren[i] instanceof DNFExpression)
              grandChildrenOr[i] = dnfPlan.getSuccessors(
                            orChildren[i]).toArray(
                            new LogicalExpression[0]);
            else if (orChildren[i] instanceof AndExpression)
              grandChildrenOr[i] = orChildren[i].getPlan().getSuccessors(
                  orChildren[i]).toArray(
                      new LogicalExpression[0]);
            else {
                grandChildrenOr[i] = new LogicalExpression[1];
                grandChildrenOr[i][0] = (LogicalExpression) orChildren[i];
            }
        }
        for (Operator andChild : andChildren) {
            if (andChild instanceof LogicalExpressionProxy)
                ((LogicalExpressionProxy) andChild).restoreSrc();
        }
        if (andDNF) {
            removeDescendants(dnfPlan, and);
            dnfPlan.remove(and);
        }
        if (orDNF) {
            removeDescendants(dnfPlan, or);
            dnfPlan.remove(or);
        }
        for (int i = 0; i < orSize; i++) {
            LogicalExpression child = new DNFExpression("dnfAnd",
                            dnfPlan, DNFExpression.DNFExpressionType.AND);
            ((DNFPlan) dnfPlan).safeAdd(child);
            if (!andFirst) for (Operator gchild : grandChildrenOr[i]) {
                dnfPlan.connect(child, gchild);
            }
            for (int j = 0; j < andSize; j++) {
                LogicalExpressionProxy andChildClone;
                if (andChildren[j] instanceof LogicalExpressionProxy) andChildClone = new LogicalExpressionProxy(
                                dnfPlan,
                                ((LogicalExpressionProxy) andChildren[j]).src);
                else andChildClone = new LogicalExpressionProxy(
                                dnfPlan,
                                (LogicalExpression) andChildren[j]);
                dnfPlan.connect(child, andChildClone);
            }
            if (andFirst) for (Operator gchild : grandChildrenOr[i]) {
                dnfPlan.connect(child, gchild);
            }
            dnfPlan.connect(current, child);
        }
    }

    @Override
    public void visit(OrExpression exp) throws FrontendException {
        LogicalExpression rhsExp = ((exp.getRhs() instanceof AndExpression || exp.getRhs() instanceof OrExpression ? result.pop() : exp.getRhs()));
        LogicalExpression lhsExp = ((exp.getLhs() instanceof AndExpression || exp.getLhs() instanceof OrExpression ? result.pop() : exp.getLhs()));
        if (!(lhsExp instanceof OrExpression) && 
            (!(lhsExp instanceof DNFExpression) || 
                ((DNFExpression) lhsExp).type == DNFExpression.DNFExpressionType.AND) && !(rhsExp instanceof OrExpression) && (!(rhsExp instanceof DNFExpression) || ((DNFExpression) rhsExp).type == DNFExpression.DNFExpressionType.AND)) result.push(exp);
        else {
            if (dnfPlan == null) dnfPlan = new DNFPlan();
            LogicalExpression current = new DNFExpression("dnfOr",
                            dnfPlan, DNFExpression.DNFExpressionType.OR);
            result.push(current);
            ((DNFPlan) dnfPlan).safeAdd(current);
            if (lhsExp instanceof OrExpression || (lhsExp instanceof DNFExpression && ((DNFExpression) lhsExp).type == DNFExpression.DNFExpressionType.OR))
                addChildren(current, lhsExp);
            else
                dnfPlan.connect(current, lhsExp);
            if (rhsExp instanceof OrExpression || (rhsExp instanceof DNFExpression && ((DNFExpression) rhsExp).type == DNFExpression.DNFExpressionType.OR))
                addChildren(current, rhsExp);
            else
                dnfPlan.connect(current, rhsExp);
        }
    }

    private void addChildren(LogicalExpression current,
                    LogicalExpression exp) throws FrontendException {
        OperatorPlan childPlan = exp.getPlan();
        Operator[] children = childPlan.getSuccessors(exp).toArray(
                        new Operator[0]);
        int size = children.length;
        for (int i = 0; i < size; ++i) {
            ((DNFPlan) dnfPlan).safeAdd(children[i]);
            dnfPlan.connect(current, children[i]);
        }
    }
}
