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

import java.util.List;

import org.apache.pig.Expression.OpType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.logical.expression.AddExpression;
import org.apache.pig.newplan.logical.expression.AndExpression;
import org.apache.pig.newplan.logical.expression.BinaryExpression;
import org.apache.pig.newplan.logical.expression.DivideExpression;
import org.apache.pig.newplan.logical.expression.EqualExpression;
import org.apache.pig.newplan.logical.expression.GreaterThanEqualExpression;
import org.apache.pig.newplan.logical.expression.GreaterThanExpression;
import org.apache.pig.newplan.logical.expression.IsNullExpression;
import org.apache.pig.newplan.logical.expression.LessThanEqualExpression;
import org.apache.pig.newplan.logical.expression.LessThanExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.ModExpression;
import org.apache.pig.newplan.logical.expression.MultiplyExpression;
import org.apache.pig.newplan.logical.expression.NotEqualExpression;
import org.apache.pig.newplan.logical.expression.NotExpression;
import org.apache.pig.newplan.logical.expression.OrExpression;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.expression.RegexExpression;
import org.apache.pig.newplan.logical.expression.SubtractExpression;
import org.apache.pig.newplan.logical.expression.UnaryExpression;

/**
 *
 * We traverse the expression plan bottom up and separate it into two plans
 * - pushdownExprPlan, plan that can be pushed down to the loader and
 * - filterExprPlan, remaining plan that needs to be evaluated by pig
 *
 * If the predicate is not removable then filterExprPlan will not have
 * the pushdownExprPlan removed.
 */
public class PredicatePushDownFilterExtractor extends FilterExtractor {

    private List<String> predicateCols;
    private List<OpType> supportedOpTypes;

    public PredicatePushDownFilterExtractor(LogicalExpressionPlan plan, List<String> predicateCols,
            List<OpType> supportedOpTypes) {
        super(plan);
        this.predicateCols = predicateCols;
        this.supportedOpTypes = supportedOpTypes;
    }

    @Override
    public void visit() throws FrontendException {
        super.visit();
        if (supportedOpTypes.contains(OpType.OP_BETWEEN)) {
            // TODO: Collapse multiple ORs into BETWEEN
        } else if (supportedOpTypes.contains(OpType.OP_IN)) {
            // TODO: Collapse multiple ORs into IN
        }
    }

    @Override
    protected KeyState checkPushDown(ProjectExpression project) throws FrontendException {
        String fieldName = project.getFieldSchema().alias;
        KeyState state = new KeyState();
        if(predicateCols.contains(fieldName)) {
            state.filterExpr = null;
            state.pushdownExpr = project;
        } else {
            state.filterExpr = addToFilterPlan(project);
            state.pushdownExpr = null;
        }
        return state;
    }

    @Override
    protected boolean isSupportedOpType(BinaryExpression binOp) {
        if(binOp instanceof AddExpression) {
            return supportedOpTypes.contains(OpType.OP_PLUS );
        } else if(binOp instanceof SubtractExpression) {
            return supportedOpTypes.contains(OpType.OP_MINUS);
        } else if(binOp instanceof MultiplyExpression) {
            return supportedOpTypes.contains(OpType.OP_TIMES);
        } else if(binOp instanceof DivideExpression) {
            return supportedOpTypes.contains(OpType.OP_DIV);
        } else if(binOp instanceof ModExpression) {
            return supportedOpTypes.contains(OpType.OP_MOD);
        } else if(binOp instanceof AndExpression) {
            return supportedOpTypes.contains(OpType.OP_AND);
        } else if(binOp instanceof OrExpression) {
            return supportedOpTypes.contains(OpType.OP_OR);
        } else if(binOp instanceof EqualExpression) {
            return supportedOpTypes.contains(OpType.OP_EQ);
        } else if(binOp instanceof NotEqualExpression) {
            return supportedOpTypes.contains(OpType.OP_NE);
        } else if(binOp instanceof GreaterThanExpression) {
            return supportedOpTypes.contains(OpType.OP_GT);
        } else if(binOp instanceof GreaterThanEqualExpression) {
            return supportedOpTypes.contains(OpType.OP_GE);
        } else if(binOp instanceof LessThanExpression) {
            return supportedOpTypes.contains(OpType.OP_LT);
        } else if(binOp instanceof LessThanEqualExpression) {
            return supportedOpTypes.contains(OpType.OP_LE);
        } else if(binOp instanceof RegexExpression) {
            return supportedOpTypes.contains(OpType.OP_MATCH);
        } else {
            return false;
        }
    }

    @Override
    protected boolean isSupportedOpType(UnaryExpression unaryOp) {
        if(unaryOp instanceof IsNullExpression) {
            return supportedOpTypes.contains(OpType.OP_NULL);
        } else if(unaryOp instanceof NotExpression) {
            return supportedOpTypes.contains(OpType.OP_NOT);
        } else {
            return false;
        }
    }

}
