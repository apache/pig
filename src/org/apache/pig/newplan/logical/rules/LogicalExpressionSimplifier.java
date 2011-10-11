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

package org.apache.pig.newplan.logical.rules;

import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.newplan.logical.expression.*;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.ReverseDependencyOrderWalker;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.OperatorSubPlan;
import org.apache.pig.newplan.optimizer.Rule;
import org.apache.pig.newplan.optimizer.Transformer;

/**
 * A FILTER logical expression simplifier
 * 
 */

public class LogicalExpressionSimplifier extends Rule {

    private List<LOFilter> processedFilters = new ArrayList<LOFilter>();
    
    enum DNFExpressionType {
        AND, OR
    }

    public LogicalExpressionSimplifier(String n) {
        super(n, false);
    }

    @Override
    public Transformer getNewTransformer() {
        return new LogicalExpressionSimplifierTransformer(processedFilters);
    }

    public static class LogicalExpressionSimplifierTransformer extends Transformer {
        private static final String dnfCountAnnotationKey = "dnfSplitCount";
        private OperatorPlan plan;
        
        private List<LOFilter> processedFilters;

        public LogicalExpressionSimplifierTransformer(List<LOFilter> processedFilters) {
            this.processedFilters = processedFilters;
        }
        @Override
        public boolean check(OperatorPlan matched) throws FrontendException {
            LOFilter filter = (LOFilter)matched.getOperators().next();
            
            // If the filter is already processed, we quit.
            if (processedFilters.contains(filter))
                return false;
            
            processedFilters.add(filter);
            return true;
        }

        @Override
        public void transform(OperatorPlan plan) throws FrontendException {
            Iterator<Operator> iter = plan.getOperators();
            while (iter.hasNext()) {
                Operator op = iter.next();
                if (op instanceof LOFilter) {
                    LOFilter filter = (LOFilter) op;
                    LogicalExpressionPlan filterPlan = filter.getFilterPlan();
                    this.plan = ((OperatorSubPlan) plan).getBasePlan();
                    try {
                        // 1: evaluate constant expressions
                        ConstExpEvaluator constExpEvaluator = new ConstExpEvaluator(
                                        filterPlan);
                        constExpEvaluator.visit();

                        NOTConversionVisitor notVisitor = new NOTConversionVisitor(
                                        filterPlan);
                        // 2: convert away NOT through the DeMorgan's Law
                        notVisitor.visit();

                        // 3: DNF generation
                        DNFPlanGenerator dnfVisitor = new DNFPlanGenerator(
                                        filterPlan);
                        dnfVisitor.visit();
                        OperatorPlan dnfPlan = dnfVisitor.getDNFPlan();

                        // 4: Trim the DNF tree
                        /**
                         * Then the DNF plan is trimmed according to the inference
                         * rules between the operands of the conjunctions first,
                         * and then between the operands of the disjunction in the
                         *  DNF plan. If a leaf is trimmed, the counter, DNFSpliCounter
                         * , of the source of the proxy will be decremented. Basically,
                         * the DNF plan is used as a utility to determine if an
                         * original leaf expression can be trimmed from the original
                         * filter plan or not. If all proxies of the original leaf
                         * expression have been trimmed from the DNF plan, the original
                         * leaf expression can be trimmed from the original plan then.
                         * The point is that the DNF plan is not intended to replace
                         * the original filer plan since the DNF plan in general tends 
                         * to be more expensive to evaluate than the original filter plan.
                         */
                        checkDNFLeaves(dnfPlan);

                        // 5: Trim the original filterPlan
                        /**
                         * The original filter plan is traversed in a bottom-up manner 
                         * so that if a leaf's DNFSpliCounter is zero, which means all 
                         * of its proxies on DNF has been trimmed, the leaf will be trimmed.
                         * For non-leafs of "AND" or "OR" expressions, if one child survives, 
                         * the child will be re-linked to the predecessor(s). If either or
                         * both children are trimmed, the non-leaf will be trimmed
                         * too. If the whole new filter plan is empty, the filter operator
                         * will be removed from the logical plan too.
                         */
                        trimLogicalExpressionPlan(filterPlan);
                    }
                    catch (FrontendException e) {
                        return;
                    }
                    if (filterPlan.size() == 0) {
                        // the whole expression is simplified away so there is no reason for the FILTER itself
                        List<Operator> predList = this.plan.getPredecessors(op), sucList = this.plan.getSuccessors(op);
                        Operator[] sucs = sucList == null ? null
                                        : sucList.toArray(new Operator[0]);
                        if (sucs != null) {
                            for (Operator suc : sucs)
                                this.plan.disconnect(op, suc);
                        }
                        if (predList != null) {
                            Operator[] preds = predList.toArray(new Operator[0]);
                            for (Operator pred : preds) {
                                this.plan.disconnect(pred, op);
                                if (sucs != null) {
                                    for (Operator suc : sucs)
                                        this.plan.connect(pred, suc);
                                }
                            }
                        }
                        this.plan.remove(filter);
                    }
                }
            }
        }
        
        static void incrDNFSplitCount(LogicalExpression le) {
            Integer cnt = (Integer) le.getAnnotation(dnfCountAnnotationKey);
            if (cnt == null)
              cnt = 1;
            le.annotate(dnfCountAnnotationKey, cnt.intValue()+1);
        }

        static void decrDNFSplitCount(LogicalExpression le) {
            Integer cnt = (Integer) le.getAnnotation(dnfCountAnnotationKey);
            if (cnt == null)
                le.annotate(dnfCountAnnotationKey, 0);
            else
                le.annotate(dnfCountAnnotationKey, cnt.intValue()-1);
        }
        
        static boolean dnfTrimmed(LogicalExpression le) {
            Integer cnt = (Integer) le.getAnnotation(dnfCountAnnotationKey);
            if (cnt == null)
              return false;
            else
              return (cnt == 0);
        }
        
        static int getSplitCount(LogicalExpression le) {
          Integer cnt = (Integer) le.getAnnotation(dnfCountAnnotationKey);
          if (cnt == null)
            return 1;
          return cnt;
        }
        
        private void checkDNFLeaves(OperatorPlan dnfPlan)
                        throws FrontendException {
            List<Operator> roots = dnfPlan.getSources();
            if (roots == null || roots.size() != 1)
                throw new FrontendException(
                                "DNF root size is expected to be one");
            Operator dnf = roots.get(0);
            if (dnf instanceof AndExpression || (dnf instanceof DNFExpression && ((DNFExpression) dnf).type == DNFExpression.DNFExpressionType.AND)) {
                handleDNFAnd(dnfPlan, dnf);
            }
            else if (dnf instanceof OrExpression || (dnf instanceof DNFExpression && ((DNFExpression) dnf).type == DNFExpression.DNFExpressionType.OR)) {
                handleDNFOr(dnfPlan, dnf);
            } else if (dnf instanceof ConstantExpression && (Boolean) (((ConstantExpression) dnf).getValue()))
                decrDNFSplitCount((ConstantExpression) dnf);
        }

        static final byte ImplyLeft = 1,
                        ImplyRight = 2,
                        Exclusive = 4,
                        Equal = 8,
                        Complementary = 16,
                        Unknown = ~(ImplyLeft | ImplyRight | Exclusive | Equal | Complementary);

        private void handleDNFAnd(OperatorPlan plan, Operator and)
                        throws FrontendException {
            // Of N^2 complexity: the reason to limit the DNF size
            // process leaves of an AND in DNF tree to remove implicated subexpressions from others
            List<Operator> children = plan.getSuccessors(and);
            if (children == null)
                return;
            byte relation;
            int size = children.size();
            for (int i = 0; i < size; i++) {
                if (children.get(i) instanceof ConstantExpression && ((Boolean) ((ConstantExpression) children.get(i)).getValue()))
                    decrDNFSplitCount((LogicalExpression) children.get(i));
            }
            for (int i = 0; i < size; i++) {
                LogicalExpression child1 = (LogicalExpression) children.get(i);
                for (int j = i + 1; j < size; j++) {
                    LogicalExpression child2 = (LogicalExpression) children.get(j);
                    relation = inferRelationship(
                                    child1 instanceof LogicalExpressionProxy ? ((LogicalExpressionProxy) child1).src
                                                    : child1,
                                    child2 instanceof LogicalExpressionProxy ? ((LogicalExpressionProxy) child2).src
                                                    : child2);
                    if ((relation & Unknown) != 0) {
                        // no-op
                    }
                    else if ((relation & Equal) != 0) {
                        if (getSplitCount(child1) < getSplitCount(child2) && getSplitCount(child1) > 0) {
                            if (getSplitCount(child1) > 0)
                                decrDNFSplitCount(child1);
                        }
                        else {
                            if (getSplitCount(child2) > 0)
                                decrDNFSplitCount(child2);
                        }
                    }
                    else if ((relation & Exclusive) != 0) {
                        // no-op now: in the future may desire to replace with a rudimentary false expression
                    }
                    else if ((relation & ImplyLeft) != 0) {
                        if (getSplitCount(child1) > 0)
                            decrDNFSplitCount(child1);
                    }
                    else if ((relation & ImplyRight) != 0) {
                        if (getSplitCount(child2) > 0)
                            decrDNFSplitCount(child2);
                    }
                }
            }
            cleanupDNFPlan(plan, and);
        }

        private void handleDNFOr(OperatorPlan plan, Operator or)
                        throws FrontendException {
            // Of N^2 complexity: the reason to limit the DNF size
            // process children of an OR in DNF tree to remove implicated subexpressions from others
            Operator[] children = plan.getSuccessors(or).toArray(
                            new Operator[0]);
            int size = children.length;
            for (int i = 0; i < size; i++) {
              if (children[i] instanceof ConstantExpression && !((Boolean) ((ConstantExpression) children[i]).getValue()))
                  decrDNFSplitCount((LogicalExpression) children[i]);
            }
            for (int ii = 0; ii < size; ii++) {
                LogicalExpression child = (LogicalExpression) children[ii];
                if (child instanceof AndExpression || (child instanceof DNFExpression && ((DNFExpression) child).type == DNFExpression.DNFExpressionType.AND)) {
                    handleDNFAnd(plan, child);
                }
            }
            byte relation;
            children = plan.getSuccessors(or).toArray(new Operator[0]);
            size = children.length;
            for (int i = 0; i < size; i++) {
                LogicalExpression child1 = (LogicalExpression) children[i];
                boolean proxy1 = (child1 instanceof LogicalExpressionProxy);
                for (int j = i + 1; j < size; j++) {
                    LogicalExpression child2 = (LogicalExpression) children[j];
                    boolean proxy2 = child2 instanceof LogicalExpressionProxy;
                    relation = inferRelationship(
                                    proxy1 ? ((LogicalExpressionProxy) child1).src
                                                    : child1,
                                    proxy2 ? ((LogicalExpressionProxy) child2).src
                                                    : child2);
                    if ((relation & Unknown) != 0) {
                        // no-op
                    }
                    else if ((relation & Equal) != 0) {
                        if (getSplitCount(child1) < getSplitCount(child2) && getSplitCount(child1) > 0) {
                            if (getSplitCount(child1) > 0)
                                decrDNFSplitCount(child1);
                        }
                        else {
                            if (getSplitCount(child2) > 0)
                                decrDNFSplitCount(child2);
                        }
                    }
                    else if ((relation & ImplyLeft) != 0) {
                        if (getSplitCount(child2) > 0)
                            decrDNFSplitCount(child2);
                    }
                    else if ((relation & ImplyRight) != 0) {
                        if (getSplitCount(child1) > 0)
                            decrDNFSplitCount(child1);
                    }
                    else if ((relation & Complementary) != 0) {
                        if (getSplitCount(child1) > 0)
                            decrDNFSplitCount(child1);
                        decrDNFSplitCount(child2);
                    }
                    else if ((relation & Exclusive) != 0) {
                        // no-op
                    }
                }
            }
            cleanupDNFPlan(plan, or);
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
                    if (child instanceof LogicalExpressionProxy) ((LogicalExpressionProxy) child).decrSrcDNFSplitCounter();
                    else decrDNFSplitCount((LogicalExpression) child);
                    plan.remove(child);
                }
            }
        }

        private void cleanupDNFPlan(OperatorPlan plan, Operator root)
                        throws FrontendException {
            // clean up the DNF subtree rooted at 'root'
            Object[] children = plan.getSuccessors(root).toArray();
            for (Object c : children) {
                LogicalExpression child = (LogicalExpression) c;
                if (dnfTrimmed(child)) {
                    removeDescendants(plan, child);
                    plan.disconnect(root, child);
                    if (child instanceof LogicalExpressionProxy)
                        ((LogicalExpressionProxy) child).decrSrcDNFSplitCounter();
                    plan.remove(child);
                }
            }
            if (plan.getSuccessors(root) != null && plan.getSuccessors(root).size() == 1) {
                Operator child = plan.getSuccessors(root).get(0);
                plan.disconnect(root, child);
                if (plan.getPredecessors(root) != null) {
                    Operator[] preds = plan.getPredecessors(root).toArray(
                                    new Operator[0]);
                    for (Operator pred : preds) {
                        plan.disconnect(pred, root);
                        plan.connect(pred, child);
                    }
                }
                if (root instanceof LogicalExpressionProxy) ((LogicalExpressionProxy) root).decrSrcDNFSplitCounter();
                else decrDNFSplitCount((LogicalExpression) root);
                plan.remove(root);
            }
            else if (plan.getSuccessors(root) == null) {
                plan.remove(root);
            }
        }

        private byte inferRelationship(LogicalExpression e1,
                        LogicalExpression e2) throws FrontendException {
            // process DNF subexpressions: both could be AND of Leaves, or LEAF
            byte result = 0;
            if (e1.isEqual(e2)) {
                result = Equal | ImplyLeft | ImplyRight;
                return result;
            }
            boolean and1 = (e1 instanceof AndExpression || (e1 instanceof DNFExpression && ((DNFExpression) e1).type == DNFExpression.DNFExpressionType.AND));
            boolean and2 = (e2 instanceof AndExpression || (e2 instanceof DNFExpression && ((DNFExpression) e2).type == DNFExpression.DNFExpressionType.AND));

            if (e1 instanceof NotExpression && e2 instanceof IsNullExpression) {
                return handleNot((NotExpression) e1, (IsNullExpression) e2);
            }
            else if (e2 instanceof NotExpression && e1 instanceof IsNullExpression) {
                return switchImplicationSides(handleNot((NotExpression) e2,
                                (IsNullExpression) e1));
            }
            else if (and1 && !and2) {
                return handleAndSimple(e1, e2);
            }
            else if (and2 && !and1) {
                return switchImplicationSides(handleAndSimple(e2, e1));
            }
            else if (and1 && and2) {
                return handleAnd(e1, e2);
            }
            else if (e1 instanceof BinaryExpression && e2 instanceof BinaryExpression) {
                return handleBinary(e1, e2);
            }
            else return Unknown;
        }

        private byte switchImplicationSides(byte ori) {
            byte result = ori;
            result &= ~(ImplyLeft | ImplyRight);
            if ((ori & ImplyLeft) != 0) result |= ImplyRight;
            if ((ori & ImplyRight) != 0) result |= ImplyLeft;
            return result;
        }

        private byte handleAnd(LogicalExpression e1, LogicalExpression e2)
                        throws FrontendException {
            // get the inference relation between two AND expressions
            List<Operator> children1 = e1.getPlan().getSuccessors(e1), children2 = e2.getPlan().getSuccessors(
                            e2);
            byte result = 0;
            // knownFlags is used to track which two children from lhs and rhs has inferrable relationships
            // The purpose is to avoid calling handleBinary more than necessary
            // The scenario is that is one side is inferrable from the other, then none of this side's children
            // should have un-inferrable relationship from the other although the other side's children could
            // have un-inferrable relationship with this side's. An example is "C1 > 3" is inferrable from
            // "C1 > 5 AND C2 != null", although "C1 > 3" has no relationship with "C2 != null".
            boolean[][] knownFlags = new boolean[children1.size()][children2.size()];
            boolean[][] equalFlags = new boolean[children1.size()][children2.size()];
            for (int i = 0; i < children1.size(); i++) {
                for (int j = 0; j < children2.size(); j++) {
                    byte inferResult = handleBinary(
                                    (LogicalExpression) children1.get(i),
                                    (LogicalExpression) children2.get(j));
                    if ((inferResult & Unknown) != 0) {
                        knownFlags[i][j] = false;
                        equalFlags[i][j] = false;
                    }
                    else {
                        knownFlags[i][j] = true;
                        result |= inferResult;
                        if ((inferResult & Equal) != 0)
                            equalFlags[i][j] = true;
                    }
                }
            }
            if ((result & Exclusive) != 0) return Exclusive;
            if ((result & ImplyRight) != 0 && (result & ImplyLeft) == 0) {
                boolean allUnknown = true;
                for (int j = 0; j < children2.size(); j++) {
                    allUnknown = true;
                    for (int i = 0; i < children1.size(); i++) {
                        if (knownFlags[i][j]) {
                            allUnknown = false;
                            break;
                        }
                    }
                    if (allUnknown) break;
                }
                if (!allUnknown) result |= ImplyRight;
            }
            if ((result & ImplyLeft) != 0 && (result & ImplyRight) == 0) {
                boolean allUnknown = true;
                for (int i = 0; i < children1.size(); i++) {
                    allUnknown = true;
                    for (int j = 0; j < children2.size(); j++) {
                        if (knownFlags[i][j]) {
                            allUnknown = false;
                            break;
                        }
                    }
                    if (allUnknown) break;
                }
                if (!allUnknown) result |= ImplyRight;
            }
            if ((result & ImplyRight) != 0 && (result & ImplyLeft) != 0) {
                // only if all children of either one AND expr have equal counterpart in the other's children
                // can the two AND exprs be declared as equal 
                boolean allEqual = true;
                for (int i = 0; i < children1.size(); i++) {
                    allEqual = true;
                    for (int j = 0; j < children2.size(); j++) {
                        if (!equalFlags[i][j]) {
                            allEqual = false;
                            break;
                        }
                    }
                    if (!allEqual) break;
                }
                if (allEqual) {
                    for (int j = 0; j < children1.size(); j++) {
                        allEqual = true;
                        for (int i = 0; i < children2.size(); i++) {
                            if (!equalFlags[i][j]) {
                                allEqual = false;
                                break;
                            }
                        }
                        if (!allEqual) break;
                    }
                }
                if (allEqual) result |= Equal;
                else result = Unknown;
            }

            if (result == 0) return Unknown;
            else return result;
        }

        private byte handleAndSimple(LogicalExpression e1, LogicalExpression e2)
                        throws FrontendException {
            if (e2 instanceof ConstantExpression)
                // no relationship between an AND and a constant
                return Unknown;
            // get the inference relation between e1, an AND expression, and e2, a leaf logical expression
            List<Operator> andChildren = e1.getPlan().getSuccessors(e1);
            boolean hasUnknown = false;
            byte result = 0;
            int size = andChildren.size();
            for (int i = 0; i < size; i++) {
                byte inferResult = handleBinary(
                                (LogicalExpression) andChildren.get(i), e2);
                if (!hasUnknown && (inferResult & Unknown) != 0) hasUnknown = true;
                else {
                    result |= inferResult;
                }
            }
            if ((result & Exclusive) != 0) return Exclusive;
            else if (hasUnknown) {
                if ((result & ImplyRight) != 0) return ImplyRight;
                else return Unknown;
            }
            else {
                if ((result & ImplyRight) != 0) return ImplyRight;
                else if (result == ImplyLeft) return ImplyLeft;
                else return Unknown;
            }
        }

        private byte handleNot(NotExpression not, IsNullExpression isnull)
                        throws FrontendException {
            if (not.getExpression().isEqual(isnull)) return Complementary | Exclusive;
            else return Unknown;
        }

        private byte handleBinary(LogicalExpression e1, LogicalExpression e2)
                        throws FrontendException {
            boolean proxy1 = e1 instanceof LogicalExpressionProxy, proxy2 = e2 instanceof LogicalExpressionProxy;
            LogicalExpression le1 = proxy1 ? ((LogicalExpressionProxy) e1).src
                            : e1, le2 = proxy2 ? ((LogicalExpressionProxy) e2).src
                            : e2;
            if (le1 instanceof NotExpression || le1 instanceof IsNullExpression || le2 instanceof NotExpression || le2 instanceof IsNullExpression)
            {
                if (((le1 instanceof NotExpression && ((NotExpression)le1).getExpression() instanceof IsNullExpression) &&
                                le2 instanceof IsNullExpression) ||
                    ((le2 instanceof NotExpression && ((NotExpression)le2).getExpression() instanceof IsNullExpression) &&
                                                le1 instanceof IsNullExpression))
                    return Exclusive;
                else
                    return Unknown;
            }

            if (!(le1 instanceof BinaryExpression)||!(le2 instanceof BinaryExpression))
                return Unknown;
            BinaryExpression b1 = !proxy1 ? (BinaryExpression) e1
                            : (BinaryExpression) ((LogicalExpressionProxy) e1).src, b2 = !proxy2 ? (BinaryExpression) e2
                            : (BinaryExpression) ((LogicalExpressionProxy) e2).src;
            LogicalExpression l1 = b1.getLhs(), r1 = b1.getRhs(), l2 = b2.getLhs(), r2 = b2.getRhs();
            if (l1 instanceof ConstantExpression && l2 instanceof ConstantExpression && r1.isEqual(r2)) {
                return handleComparison(
                                l1,
                                r1,
                                l2,
                                r2,
                                proxy1 ? ((LogicalExpressionProxy) e1).src : e1,
                                proxy2 ? ((LogicalExpressionProxy) e2).src : e2);
            }
            else if (r1 instanceof ConstantExpression && l2 instanceof ConstantExpression && l1.isEqual(r2)) {
                return handleComparison(
                                r1,
                                l1,
                                l2,
                                r2,
                                proxy1 ? ((LogicalExpressionProxy) e1).src : e1,
                                proxy2 ? ((LogicalExpressionProxy) e2).src : e2);
            }
            else if (l1 instanceof ConstantExpression && r2 instanceof ConstantExpression && r1.isEqual(l2)) {
                return handleComparison(
                                l1,
                                r1,
                                r2,
                                l2,
                                proxy1 ? ((LogicalExpressionProxy) e1).src : e1,
                                proxy2 ? ((LogicalExpressionProxy) e2).src : e2);
            }
            else if (r1 instanceof ConstantExpression && r2 instanceof ConstantExpression && l1.isEqual(l2)) {
                return handleComparison(
                                r1,
                                l1,
                                r2,
                                l2,
                                proxy1 ? ((LogicalExpressionProxy) e1).src : e1,
                                proxy2 ? ((LogicalExpressionProxy) e2).src : e2);
            }
            else return Unknown;
        }

        @SuppressWarnings("unchecked")
        private byte handleComparison(LogicalExpression val1,
                        LogicalExpression k1, LogicalExpression val2,
                        LogicalExpression k2, LogicalExpression e1,
                        LogicalExpression e2) {
            Object v1 = ((ConstantExpression) val1).getValue(), v2 = ((ConstantExpression) val2).getValue();
            boolean comparable1 = v1 instanceof Comparable, comparable2 = v2 instanceof Comparable;
            boolean isEqual1 = e1 instanceof EqualExpression, isEqual2 = e2 instanceof EqualExpression, isNotEqual1 = e1 instanceof NotEqualExpression, isNotEqual2 = e2 instanceof NotEqualExpression, isGT1 = e1 instanceof GreaterThanExpression, isGT2 = e2 instanceof GreaterThanExpression, isGE1 = e1 instanceof GreaterThanEqualExpression, isGE2 = e2 instanceof GreaterThanEqualExpression, isLT1 = e1 instanceof LessThanExpression, isLT2 = e2 instanceof LessThanExpression, isLE1 = e1 instanceof LessThanEqualExpression, isLE2 = e2 instanceof LessThanEqualExpression;
            if (isEqual1 && isEqual2) {
                if (v1.equals(v2)) return Equal | ImplyLeft | ImplyRight;
                else return Exclusive;
            }
            else if (isEqual1 && isNotEqual2) {
                if (v1.equals(v2)) return Exclusive;
                else return ImplyRight;
            }
            else if (isNotEqual1 && isEqual2) {
                if (v1.equals(v2)) return Exclusive;
                else return ImplyLeft;
            }
            else if (isNotEqual1 && isNotEqual2) {
                if (v1.equals(v2)) return Equal | ImplyLeft | ImplyRight;
                else return Unknown;
            }
            else if (isEqual1 && isGT2) {
                if (v1.equals(v2)) return Exclusive;

                if (comparable1) {
                    if (((Comparable) v1).compareTo((Comparable) v2) > 0) return ImplyRight;
                    else return Exclusive;
                }
                else return Unknown;
            }
            else if (isEqual1 && isGE2) {
                if (v1.equals(v2)) return ImplyRight;

                if (comparable1) {
                    if (((Comparable) v1).compareTo((Comparable) v2) > 0) return ImplyRight;
                    else return Exclusive;
                }
                else return Unknown;
            }
            else if (isEqual1 && isLT2) {
                if (v1.equals(v2)) return Exclusive;

                if (comparable1) {
                    if (((Comparable) v1).compareTo((Comparable) v2) > 0) return Exclusive;
                    else return ImplyRight;
                }
                else return Unknown;
            }
            else if (isNotEqual1 && isGT2) {
                if (v1.equals(v2)) return ImplyLeft;
                if (comparable1) {
                    if (((Comparable) v1).compareTo((Comparable) v2) < 0) return ImplyLeft;
                    else return Unknown;
                }
                else return Unknown;
            }
            else if (isNotEqual1 && isGE2) {
                if (v1.equals(v2)) return Complementary | Exclusive;
                if (comparable1) {
                    if (((Comparable) v1).compareTo((Comparable) v2) < 0) return ImplyLeft;
                    else return Unknown;
                }
                else return Unknown;
            }
            else if (isNotEqual1 && isLT2) {
                if (v1.equals(v2)) return ImplyLeft;
                if (comparable1) {
                    if (((Comparable) v1).compareTo((Comparable) v2) < 0) return Unknown;
                    else return ImplyLeft;
                }
                else return Unknown;
            }
            else if (isNotEqual1 && isLE2) {
                if (v1.equals(v2)) return Complementary | Exclusive;
                if (comparable1) {
                    if (((Comparable) v1).compareTo((Comparable) v2) < 0) return Unknown;
                    else return ImplyLeft;
                }
                else return Unknown;
            }
            else if (isGT1 && isGT2) {
                if (v1.equals(v2)) return Equal | ImplyLeft | ImplyRight;
                if (((Comparable) v1).compareTo((Comparable) v2) < 0) return ImplyLeft;
                else return ImplyRight;
            }
            else if (isGT1 && isGE2) {
                if (v1.equals(v2)) return ImplyRight;
                if (((Comparable) v1).compareTo((Comparable) v2) < 0) return ImplyLeft;
                else return ImplyRight;
            }
            else if (isGT1 && isLT2) {
                if (v1.equals(v2)) return Unknown;
                if (((Comparable) v1).compareTo((Comparable) v2) < 0) return Unknown;
                else return Exclusive;
            }
            else if (isGT1 && isLE2) {
                if (v1.equals(v2)) return Complementary | Exclusive;
                if (((Comparable) v1).compareTo((Comparable) v2) < 0) return Unknown;
                else return Exclusive;
            }
            else if (isGE1 && isGT2) {
                if (v1.equals(v2)) return ImplyLeft;
                if (((Comparable) v1).compareTo((Comparable) v2) > 0) return ImplyRight;
                else return ImplyLeft;
            }
            else if (isGE1 && isGE2) {
                if (v1.equals(v2)) return Equal | ImplyLeft | ImplyRight;
                if (((Comparable) v1).compareTo((Comparable) v2) > 0) return ImplyRight;
                else return ImplyLeft;
            }
            else if (isGE1 && isLT2) {
                if (v1.equals(v2)) return Complementary | Exclusive;
                if (((Comparable) v1).compareTo((Comparable) v2) > 0) return Exclusive;
                else return Unknown;
            }
            else if (isGE1 && isLE2) {
                if (v1.equals(v2)) return Complementary | Exclusive;
                if (((Comparable) v1).compareTo((Comparable) v2) > 0) return Exclusive;
                else return Unknown;
            }
            else if (isLT1 && isGT2) {
                if (v1.equals(v2)) return Unknown;
                if (((Comparable) v1).compareTo((Comparable) v2) < 0) return Exclusive;
                else return Unknown;
            }
            else if (isLT1 && isGE2) {
                if (v1.equals(v2)) return Complementary | Exclusive;
                if (((Comparable) v1).compareTo((Comparable) v2) < 0) return Exclusive;
                else return Unknown;
            }
            else if (isLT1 && isLT2) {
                if (v1.equals(v2)) return Equal | ImplyLeft | ImplyRight;
                if (((Comparable) v1).compareTo((Comparable) v2) < 0) return ImplyRight;
                else return ImplyLeft;
            }
            else if (isLT1 && isLE2) {
                if (v1.equals(v2)) return ImplyRight;
                if (((Comparable) v1).compareTo((Comparable) v2) < 0) return ImplyRight;
                else return ImplyLeft;
            }
            else if (isLE1 && isGT2) {
                if (v1.equals(v2)) return Complementary | Exclusive;
                if (((Comparable) v1).compareTo((Comparable) v2) > 0) return Unknown;
                else return Exclusive;
            }
            else if (isLE1 && isGE2) {
                if (v1.equals(v2)) return Complementary | Exclusive;
                if (((Comparable) v1).compareTo((Comparable) v2) < 0) return Exclusive;
                else return Unknown;
            }
            else if (isLE1 && isLT2) {
                if (v1.equals(v2)) return ImplyRight;
                if (((Comparable) v1).compareTo((Comparable) v2) > 0) return ImplyLeft;
                else return ImplyRight;
            }
            else if (isLE1 && isLE2) {
                if (v1.equals(v2)) return Equal | ImplyLeft | ImplyRight;
                if (((Comparable) v1).compareTo((Comparable) v2) > 0) return ImplyLeft;
                else return ImplyRight;
            }
            else if (isNotEqual2 && isGT1) {
                if (v1.equals(v2)) return ImplyRight;
                if (comparable2) {
                    if (((Comparable) v2).compareTo((Comparable) v1) < 0) return ImplyRight;
                    else return Unknown;
                }
                else return Unknown;
            }
            else if (isNotEqual2 && isGE1) {
                if (v1.equals(v2)) return Complementary | Exclusive;
                if (comparable2) {
                    if (((Comparable) v2).compareTo((Comparable) v1) < 0) return ImplyRight;
                    else return Unknown;
                }
                else return Unknown;
            }
            else if (isNotEqual2 && isLT1) {
                if (v1.equals(v2)) return ImplyRight;
                if (comparable2) {
                    if (((Comparable) v2).compareTo((Comparable) v1) < 0) return Unknown;
                    else return ImplyRight;
                }
                else return Unknown;
            }
            else if (isNotEqual2 && isLE1) {
                if (v1.equals(v2)) return Complementary | Exclusive;
                if (comparable2) {
                    if (((Comparable) v2).compareTo((Comparable) v1) < 0) return Unknown;
                    else return ImplyRight;
                }
                else return Unknown;
            }
            else if (isGT2 && isGT1) {
                if (v1.equals(v2)) return Equal | ImplyLeft | ImplyRight;
                if (((Comparable) v2).compareTo((Comparable) v1) < 0) return ImplyRight;
                else return Unknown;
            }
            else if (isGT2 && isGE1) {
                if (v1.equals(v2)) return ImplyLeft;
                if (((Comparable) v2).compareTo((Comparable) v1) < 0) return ImplyRight;
                else return ImplyLeft;
            }
            else if (isGT2 && isLT1) {
                if (v1.equals(v2)) return Unknown;
                if (((Comparable) v2).compareTo((Comparable) v1) < 0) return Unknown;
                else return Exclusive;
            }
            else if (isGT2 && isLE1) {
                if (v1.equals(v2)) return Complementary | Exclusive;
                if (((Comparable) v2).compareTo((Comparable) v1) < 0) return Unknown;
                else return Exclusive;
            }
            else if (isGE2 && isGT1) {
                if (v1.equals(v2)) return ImplyRight;
                if (((Comparable) v2).compareTo((Comparable) v1) > 0) return ImplyLeft;
                else return ImplyRight;
            }
            else if (isGE2 && isGE1) {
                if (v1.equals(v2)) return Equal | ImplyLeft | ImplyRight;
                if (((Comparable) v2).compareTo((Comparable) v1) > 0) return ImplyLeft;
                else return ImplyRight;
            }
            else if (isGE2 && isLT1) {
                if (v1.equals(v2)) return Complementary | Exclusive;
                if (((Comparable) v2).compareTo((Comparable) v1) > 0) return Exclusive;
                else return Unknown;
            }
            else if (isGE2 && isLE1) {
                if (v1.equals(v2)) return Complementary | Exclusive;
                if (((Comparable) v2).compareTo((Comparable) v1) > 0) return Exclusive;
                else return Unknown;
            }
            else if (isLT2 && isGT1) {
                if (v1.equals(v2)) return Unknown;
                if (((Comparable) v2).compareTo((Comparable) v1) < 0) return Exclusive;
                else return Unknown;
            }
            else if (isLT2 && isGE1) {
                if (v1.equals(v2)) return Complementary | Exclusive;
                if (((Comparable) v2).compareTo((Comparable) v1) < 0) return Exclusive;
                else return Unknown;
            }
            else if (isLT2 && isLT1) {
                if (v1.equals(v2)) return Equal | ImplyLeft | ImplyRight;
                if (((Comparable) v2).compareTo((Comparable) v1) < 0) return ImplyLeft;
                else return ImplyRight;
            }
            else if (isLT2 && isLE1) {
                if (v1.equals(v2)) return ImplyRight;
                if (((Comparable) v2).compareTo((Comparable) v1) < 0) return ImplyLeft;
                else return ImplyRight;
            }
            else if (isLE2 && isGT1) {
                if (v1.equals(v2)) return Complementary | Exclusive;
                if (((Comparable) v2).compareTo((Comparable) v1) > 0) return Unknown;
                else return Exclusive;
            }
            else if (isLE2 && isGE1) {
                if (v1.equals(v2)) return Complementary | Exclusive;
                if (((Comparable) v2).compareTo((Comparable) v1) < 0) return Exclusive;
                else return Unknown;
            }
            else if (isLE2 && isLT1) {
                if (v1.equals(v2)) return ImplyRight;
                if (((Comparable) v2).compareTo((Comparable) v1) > 0) return ImplyRight;
                else return ImplyLeft;
            }
            else if (isLE2 && isLE1) {
                if (v1.equals(v2)) return Equal | ImplyLeft | ImplyRight;
                if (((Comparable) v2).compareTo((Comparable) v1) > 0) return ImplyRight;
                else return ImplyLeft;
            }
            return Unknown;
        }

        private void trimLogicalExpressionPlan(OperatorPlan ori)
                        throws FrontendException {
            class TrimVisitor extends AllSameExpressionVisitor {
                LogicalExpressionPlan plan;

                TrimVisitor(LogicalExpressionPlan plan)
                                throws FrontendException {
                    super(plan, new ReverseDependencyOrderWalker(plan));
                    // the plan will be trimmed in-place on the original plan: this is
                    // ok because the ReverseDependencyOrderWalker first build the
                    // traversal ordering from the original plan, then starts the
                    // traversal which does not rely upon the topology of the original plan
                    this.plan = plan;
                }

                @Override
                public void execute(LogicalExpression e)
                                throws FrontendException {
                    if (dnfTrimmed(e)) {
                        remove(e);
                    }
                }

                @Override
                public void visit(NotExpression op) throws FrontendException {
                    if (op.getExpression() == null) {
                        remove(op);
                    }
                    else execute(op);
                }

                private void remove(Operator op) throws FrontendException {
                    List<Operator> p = plan.getPredecessors(op);
                    if (p != null) {
                        Operator[] preds = p.toArray(new Operator[0]);
                        for (Operator pred : preds) {
                            plan.disconnect(pred, op);
                        }
                    }
                    removeDescendants(op);
                }

                private void removeDescendants(Operator op)
                                throws FrontendException {
                    List<Operator> p = plan.getSuccessors(op);
                    if (p != null) {
                        Operator[] sucs = p.toArray(new Operator[0]);
                        for (Operator suc : sucs) {
                            plan.disconnect(op, suc);
                            remove(suc);
                        }
                    }
                    plan.remove(op);
                }

                @Override
                public void visit(OrExpression orExpr) throws FrontendException {
                    List<Operator> children = plan.getSuccessors(orExpr);
                    Operator lhs = children != null && children.size() > 0 ? children.get(0)
                                    : null, rhs = children != null && children.size() > 1 ? children.get(1)
                                    : null;
                    if ((lhs == null && rhs == null) || dnfTrimmed(orExpr)) {
                        remove(orExpr);
                    }
                    else if (rhs == null) {
                        trimOneChild(orExpr, lhs);
                        plan.remove(orExpr);
                    }
                }

                @Override
                public void visit(AndExpression andExpr)
                                throws FrontendException {
                    List<Operator> children = plan.getSuccessors(andExpr);
                    Operator lhs = children != null && children.size() > 0 ? children.get(0)
                                    : null, rhs = children != null && children.size() > 1 ? children.get(1)
                                    : null;
                    if ((lhs == null && rhs == null) || dnfTrimmed(andExpr)) {
                        remove(andExpr);
                    }
                    else if (rhs == null) {
                        trimOneChild(andExpr, lhs);
                        plan.remove(andExpr);
                    }
                }

                private void trimOneChild(Operator parent,
                                Operator survivingChild)
                                throws FrontendException {
                    plan.disconnect(parent, survivingChild);
                    if (plan.getPredecessors(parent) == null) return;
                    Operator[] preds = plan.getPredecessors(parent).toArray(
                                    new Operator[0]);
                    for (Operator pred : preds) {
                        // keep the relative position
                        Pair<Integer, Integer> pos = plan.disconnect(pred, parent);
                        plan.connect(pred, pos.first, survivingChild, pos.second);
                    }
                }
            }
            TrimVisitor worker = new TrimVisitor((LogicalExpressionPlan) ori);
            worker.visit();
        }

        @Override
        public OperatorPlan reportChanges() {
            return plan;
        }
    }

    @Override
    protected OperatorPlan buildPattern() {
        LogicalPlan plan = new LogicalPlan();
        LogicalRelationalOperator op = new LOFilter(plan);
        plan.add(op);
        return plan;
    }
}
