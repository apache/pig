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

package org.apache.pig.newplan;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.Expression;
import org.apache.pig.Expression.OpType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.logical.expression.AddExpression;
import org.apache.pig.newplan.logical.expression.AndExpression;
import org.apache.pig.newplan.logical.expression.BinaryExpression;
import org.apache.pig.newplan.logical.expression.ConstantExpression;
import org.apache.pig.newplan.logical.expression.DivideExpression;
import org.apache.pig.newplan.logical.expression.EqualExpression;
import org.apache.pig.newplan.logical.expression.GreaterThanEqualExpression;
import org.apache.pig.newplan.logical.expression.GreaterThanExpression;
import org.apache.pig.newplan.logical.expression.LessThanEqualExpression;
import org.apache.pig.newplan.logical.expression.LessThanExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.ModExpression;
import org.apache.pig.newplan.logical.expression.MultiplyExpression;
import org.apache.pig.newplan.logical.expression.NotEqualExpression;
import org.apache.pig.newplan.logical.expression.OrExpression;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.expression.RegexExpression;
import org.apache.pig.newplan.logical.expression.SubtractExpression;

/**
 * This is a rewrite of {@code PColFilterExtractor}
 *
 * We traverse the expression plan bottom up and separate it into two plans
 * - pushdownExprPlan, plan that can be pushed down to the loader and
 * - filterExprPlan, remaining plan that needs to be evaluated by pig
 *
 */
public class FilterExtractor {

    private static final Log LOG = LogFactory.getLog(FilterExtractor.class);

    /**
     * partition columns associated with the table
     * present in the load on which the filter whose
     * inner plan is being visited is applied
     */
    private List<String> partitionCols;

    /**
     * We visit this plan to create the filteredPlan
     */
    protected LogicalExpressionPlan originalPlan;

    /**
     * Plan that is created after all pushable filters are removed
     */
    protected LogicalExpressionPlan filteredPlan;

    /**
     * Plan that can be pushed down
     */
    protected LogicalExpressionPlan pushdownExprPlan;

    /**
     * Final filterExpr after we are done
     */
    private LogicalExpression filterExpr = null;

    /**
     * @{code Expression} to pushdown
     */
    private Expression pushdownExpr = null;

    /**
     *
     * @param plan logical plan corresponding the filter's comparison condition
     * @param partitionCols list of partition columns of the table which is
     * being loaded in the LOAD statement which is input to the filter
     */
    public FilterExtractor(LogicalExpressionPlan plan,
            List<String> partitionCols) {
        this.originalPlan = plan;
        this.partitionCols = new ArrayList<String>(partitionCols);
        this.filteredPlan = new LogicalExpressionPlan();
        this.pushdownExprPlan = new LogicalExpressionPlan();
    }

    public void visit() throws FrontendException {
        // we will visit the leaf and it will recursively walk the plan
        LogicalExpression leaf = (LogicalExpression)originalPlan.getSources().get( 0 );
        // if the leaf is a unary operator it should be a FilterFunc in
        // which case we don't try to extract partition filter conditions
        if(leaf instanceof BinaryExpression) {
            // recursively traverse the tree bottom up
            // checkPushdown returns KeyState which is pair of LogicalExpression
            BinaryExpression binExpr = (BinaryExpression)leaf;
            KeyState finale = checkPushDown(binExpr);
            this.filterExpr = finale.filterExpr;
            this.pushdownExpr = getExpression(finale.pushdownExpr);
        }
    }

    /**
     * @return new filtered plan after pushdownable filters are removed
     */
    public LogicalExpressionPlan getFilteredPlan() {
        return filteredPlan;
    }

    /**
     * @return true if pushdown is possible
     */
    public boolean canPushDown() {
        return pushdownExpr != null;
    }

    /**
     * @return the filterRemovable
     */
    public boolean isFilterRemovable() {
        return filterExpr == null;
    }

    /**
     * @return the condition on partition columns extracted from filter
     */
    public  Expression getPColCondition(){
        return pushdownExpr;
    }

    private class KeyState {
        LogicalExpression pushdownExpr;
        LogicalExpression filterExpr;
    }

    private KeyState checkPushDown(LogicalExpression op) throws FrontendException {
        // Note: Currently, Expression interface only understands 3 Expression Types
        // (Look at getExpression below) BinaryExpression, ProjectExpression and ConstantExpression
        if(op instanceof ProjectExpression) {
            return checkPushDown((ProjectExpression)op);
        } else if (op instanceof BinaryExpression) {
            return checkPushDown((BinaryExpression)op);
        } else if (op instanceof ConstantExpression) {
            // Constants can be pushdown
            KeyState state = new KeyState();
            state.pushdownExpr = op;
            state.filterExpr = null;
            return state;
        } else {
            KeyState state = new KeyState();
            state.pushdownExpr = null;
            state.filterExpr = addToFilterPlan(op);
            return state;
        }
    }

    private LogicalExpression addToFilterPlan(LogicalExpression op) throws FrontendException {
        // This copies the whole tree underneath op
        LogicalExpression newOp = op.deepCopy(filteredPlan);
        return newOp;
    }

    private LogicalExpression andLogicalExpressions(
            LogicalExpressionPlan plan, LogicalExpression a, LogicalExpression b) throws FrontendException {
        if (a == null) {
            return b;
        }
        if (b == null) {
            return a;
        }
        if (!plan.ops.contains(a)) {
            a = a.deepCopy(plan);
        }
        if (!plan.ops.contains(b)) {
            b = b.deepCopy(plan);
        }
        LogicalExpression andOp = new AndExpression(plan, a, b);
        return andOp;
    }

    private LogicalExpression orLogicalExpressions(
            LogicalExpressionPlan plan, LogicalExpression a, LogicalExpression b) throws FrontendException {
        // Or 2 operators if they are not null
        if (a == null || b == null) {
            return null;
        }
        if (!plan.ops.contains(a)) {
            a = a.deepCopy(plan);
        }
        if (!plan.ops.contains(b)) {
            b = b.deepCopy(plan);
        }
        LogicalExpression orOp = new OrExpression(plan, a, b);
        return orOp;
    }

    private KeyState checkPushDown(BinaryExpression binExpr) throws FrontendException {
        KeyState state = new KeyState();
        KeyState leftState = checkPushDown(binExpr.getLhs());
        KeyState rightState = checkPushDown(binExpr.getRhs());

        if (binExpr instanceof AndExpression) {
            // AND is commutative
            // Expression =
            // (leftState.pushdownExpr AND leftState.filterExpr)
            // AND (rightState.pushdownExpr AND rightState.filterExpr)
            //
            // pushDownExpr = (leftState.pushdownExpr AND rightState.pushdownExpr)
            // filterExpr = (leftState.filterExpr AND rightState.filterExpr)
            state.pushdownExpr = andLogicalExpressions(pushdownExprPlan, leftState.pushdownExpr, rightState.pushdownExpr);
            state.filterExpr = andLogicalExpressions(filteredPlan, leftState.filterExpr, rightState.filterExpr);
        } else if (binExpr instanceof OrExpression) {
            // Expression =
            // (leftState.pushdownExpr AND leftState.filterExpr)
            // OR (rightState.pushdownExpr AND rightState.filterExpr)
            //
            // This could be rewritten with distributive property as
            // (leftState.pushdownExpr OR rightState.pushdownExpr)
            // AND
            // ( (leftState.pushdownExpr OR rightState.filterExpr)
            // AND (leftState.filterExpr OR rightState.pushdownExpr)
            // AND (leftState.filterExpr OR rightState.filterExpr)
            // )
            // In other words,
            // pushdownExpr = leftState.pushdownExpr OR rightState.pushdownExpr
            // filterExpr = (leftState.pushdownExpr OR rightState.filterExpr)
            //              AND (leftState.filterExpr OR rightState.pushdownExpr)
            //              AND (leftState.filterExpr OR rightState.filterExpr)
            state.pushdownExpr = orLogicalExpressions(pushdownExprPlan, leftState.pushdownExpr, rightState.pushdownExpr);
            if (state.pushdownExpr == null) {
                // Whatever we did so far on the right tree is all wasted :(
                // Undo all the mutation (AND OR distributions) until now
                removeFromFilteredPlan(leftState.filterExpr);
                removeFromFilteredPlan(rightState.filterExpr);
                state.filterExpr = addToFilterPlan(binExpr);
            } else {
                LogicalExpression f1 = orLogicalExpressions(filteredPlan, leftState.pushdownExpr, rightState.filterExpr);
                LogicalExpression f2 = orLogicalExpressions(filteredPlan, leftState.filterExpr, rightState.pushdownExpr);
                LogicalExpression f3 = orLogicalExpressions(filteredPlan, leftState.filterExpr, rightState.filterExpr);
                state.filterExpr = andLogicalExpressions(filteredPlan, f1, andLogicalExpressions(filteredPlan, f2, f3));
            }
        } else {
            // leftState OP rightState
            if (leftState.filterExpr == null && rightState.filterExpr == null) {
                state.pushdownExpr = binExpr;
                state.filterExpr = null;
            } else {
                state.pushdownExpr = null;
                removeFromFilteredPlan(leftState.filterExpr);
                removeFromFilteredPlan(rightState.filterExpr);
                state.filterExpr = addToFilterPlan(binExpr);
            }
        }
        return state;
    }

    private KeyState checkPushDown(ProjectExpression project) throws FrontendException {
        String fieldName = project.getFieldSchema().alias;
        KeyState state = new KeyState();
        if(partitionCols.contains(fieldName)) {
            state.filterExpr = null;
            state.pushdownExpr = project;
        } else {
            state.filterExpr = addToFilterPlan(project);
            state.pushdownExpr = null;
        }
        return state;
    }

    /**
     * Assume that the given operator is already disconnected from its predecessors.
     * @param op
     * @throws FrontendException
     */
    private void removeFromFilteredPlan(Operator op) throws FrontendException {
        List<Operator> succs = filteredPlan.getSuccessors( op );
        if( succs == null ) {
            filteredPlan.remove( op );
            return;
        }

        Operator[] children = new Operator[succs.size()];
        for( int i = 0; i < succs.size(); i++ ) {
            children[i] = succs.get(i);
        }

        for( Operator succ : children ) {
            filteredPlan.disconnect( op, succ );
            removeFromFilteredPlan( succ );
        }

        filteredPlan.remove( op );
    }

    public static Expression getExpression(LogicalExpression op) throws FrontendException
    {
        if(op == null) {
            return null;
        }
        if(op instanceof ConstantExpression) {
            ConstantExpression constExpr =(ConstantExpression)op ;
            return new Expression.Const( constExpr.getValue() );
        } else if (op instanceof ProjectExpression) {
            ProjectExpression projExpr = (ProjectExpression)op;
            String fieldName = projExpr.getFieldSchema().alias;
            return new Expression.Column(fieldName);
        } else {
            if( !( op instanceof BinaryExpression ) ) {
                LOG.error("Unsupported conversion of LogicalExpression to Expression: " + op.getName());
                throw new FrontendException("Unsupported conversion of LogicalExpression to Expression: " + op.getName());
            }
            BinaryExpression binOp = (BinaryExpression)op;
            if(binOp instanceof AddExpression) {
                return getExpression( binOp, OpType.OP_PLUS );
            } else if(binOp instanceof SubtractExpression) {
                return getExpression(binOp, OpType.OP_MINUS);
            } else if(binOp instanceof MultiplyExpression) {
                return getExpression(binOp, OpType.OP_TIMES);
            } else if(binOp instanceof DivideExpression) {
                return getExpression(binOp, OpType.OP_DIV);
            } else if(binOp instanceof ModExpression) {
                return getExpression(binOp, OpType.OP_MOD);
            } else if(binOp instanceof AndExpression) {
                return getExpression(binOp, OpType.OP_AND);
            } else if(binOp instanceof OrExpression) {
                return getExpression(binOp, OpType.OP_OR);
            } else if(binOp instanceof EqualExpression) {
                return getExpression(binOp, OpType.OP_EQ);
            } else if(binOp instanceof NotEqualExpression) {
                return getExpression(binOp, OpType.OP_NE);
            } else if(binOp instanceof GreaterThanExpression) {
                return getExpression(binOp, OpType.OP_GT);
            } else if(binOp instanceof GreaterThanEqualExpression) {
                return getExpression(binOp, OpType.OP_GE);
            } else if(binOp instanceof LessThanExpression) {
                return getExpression(binOp, OpType.OP_LT);
            } else if(binOp instanceof LessThanEqualExpression) {
                return getExpression(binOp, OpType.OP_LE);
            } else if(binOp instanceof RegexExpression) {
                return getExpression(binOp, OpType.OP_MATCH);
            } else {
                LOG.error("Unsupported conversion of LogicalExpression to Expression: " + op.getName());
                throw new FrontendException("Unsupported conversion of LogicalExpression to Expression: " + op.getName());
            }
        }
    }

    private static Expression getExpression(BinaryExpression binOp, OpType
            opType) throws FrontendException {
        return new Expression.BinaryExpression(getExpression(binOp.getLhs())
                , getExpression(binOp.getRhs()), opType);
    }
}
