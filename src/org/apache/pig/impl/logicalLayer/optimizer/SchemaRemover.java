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
package org.apache.pig.impl.logicalLayer.optimizer;

import org.apache.pig.impl.logicalLayer.*;
import org.apache.pig.impl.plan.PlanVisitor;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.VisitorException;

/**
 * A visitor to reset all the schemas in a logical plan.
 */
public class SchemaRemover extends LOVisitor {

    public SchemaRemover(LogicalPlan plan) {
        super(plan,
            new DependencyOrderWalker<LogicalOperator, LogicalPlan>(plan));
    }

    /**
     * @param binOp
     *            the logical binary expression operator that has to be visited
     * @throws VisitorException
     */
    @Override
    protected void visit(BinaryExpressionOperator binOp)
            throws VisitorException {
        binOp.unsetFieldSchema();
        super.visit(binOp);
    }

    /**
     * 
     * @param uniOp
     *            the logical unary operator that has to be visited
     * @throws VisitorException
     */
    @Override
    protected void visit(UnaryExpressionOperator uniOp) throws VisitorException {
        uniOp.unsetFieldSchema();
        super.visit(uniOp);
    }

    /**
     * 
     * @param cg
     *            the logical cogroup operator that has to be visited
     * @throws VisitorException
     */
    @Override
    protected void visit(LOCogroup cg) throws VisitorException {
        cg.unsetSchema();
        super.visit(cg);
    }

    /**
     * 
     * @param s
     *            the logical sort operator that has to be visited
     * @throws VisitorException
     */
    @Override
    protected void visit(LOSort s) throws VisitorException {
        s.unsetSchema();
        super.visit(s);
    }

    /**
     * 
     * @param limit
     *            the logical limit operator that has to be visited
     * @throws VisitorException
     */
    @Override
    protected void visit(LOLimit limit) throws VisitorException {
        limit.unsetSchema();
        super.visit(limit);
    }


    /**
     * 
     * @param filter
     *            the logical filter operator that has to be visited
     * @throws VisitorException
     */
    @Override
    protected void visit(LOFilter filter) throws VisitorException {
        filter.unsetSchema();
        super.visit(filter);
    }

    /**
     * 
     * @param split
     *            the logical split operator that has to be visited
     * @throws VisitorException
     */
    @Override
    protected void visit(LOSplit split) throws VisitorException {
        split.unsetSchema();
        super.visit(split);
    }

    /**
     * 
     * @param forEach
     *            the logical foreach operator that has to be visited
     * @throws VisitorException
     */
    @Override
    protected void visit(LOForEach forEach) throws VisitorException {
        forEach.unsetSchema();
        super.visit(forEach);
    }

    /**
     * Iterate over each expression that is part of the function argument list
     * 
     * @param func
     *            the user defined function
     * @throws VisitorException
     */
    @Override
    protected void visit(LOUserFunc func) throws VisitorException {
        func.unsetFieldSchema();
        super.visit(func);
    }

    /**
     * @param binCond
     *            the logical binCond operator that has to be visited
     * @throws VisitorException
     */
    @Override
    protected void visit(LOBinCond binCond) throws VisitorException {
        binCond.unsetFieldSchema();
        super.visit(binCond);
    }

    /**
     * 
     * @param cast
     *            the logical cast operator that has to be visited
     * @throws VisitorException
     */
    @Override
    protected void visit(LOCast cast) throws VisitorException {
        cast.unsetFieldSchema();
        super.visit(cast);
    }
    
    
    /**
     * 
     * @param regexp
     *            the logical regexp operator that has to be visited
     * @throws ParseException
     */
    @Override
    protected void visit(LORegexp regexp) throws VisitorException {
        regexp.unsetFieldSchema();
        super.visit(regexp);
    }

    @Override
    protected void visit(LOLoad load) throws VisitorException{
        // Don't remove load's schema, it's not like it will change.  And we
        // don't have a way to recover it.
        super.visit(load);
    }
    
    @Override
    protected void visit(LOStore store) throws VisitorException{
        store.unsetSchema();
        super.visit(store);
    }
    
    @Override
    protected void visit(LOConst c) throws VisitorException{
        c.unsetSchema();
        super.visit(c);
    }

    @Override
    protected void visit(LOUnion u) throws VisitorException {
        u.unsetSchema();
        super.visit(u);
    }

    @Override
    protected void visit(LOSplitOutput sop) throws VisitorException {
        sop.unsetSchema();
        super.visit(sop);
    }

    @Override
    protected void visit(LODistinct dt) throws VisitorException {
        dt.unsetSchema();
        super.visit(dt);
    }

    @Override
    protected void visit(LOCross cs) throws VisitorException {
        cs.unsetSchema();
        super.visit(cs);
    }

    @Override
    protected void visit(LOProject project) throws VisitorException {
        project.unsetFieldSchema();
        super.visit(project);
    }

    @Override
    protected void visit(LOJoin join) throws VisitorException {
        join.unsetSchema();
        super.visit(join);
    }

    @Override
    protected void visit(ExpressionOperator op) throws VisitorException {
        op.unsetFieldSchema();
        super.visit(op);
    }

    @Override
    public void visit(LOAdd op) throws VisitorException {
        op.unsetFieldSchema();
        super.visit(op);
    }

    @Override
    public void visit(LOAnd binOp) throws VisitorException {
        binOp.unsetFieldSchema();
        super.visit(binOp);
    }

    @Override
    public void visit(LODivide op) throws VisitorException {
        op.unsetFieldSchema();
        super.visit(op);
    }

    @Override
    public void visit(LOEqual op) throws VisitorException {
        op.unsetFieldSchema();
        super.visit(op);
    }

    @Override
    public void visit(LOGreaterThan op) throws VisitorException {
        op.unsetFieldSchema();
        super.visit(op);
    }

    @Override
    public void visit(LOGreaterThanEqual op) throws VisitorException {
        op.unsetFieldSchema();
        super.visit(op);
    }

    @Override
    public void visit(LOIsNull uniOp) throws VisitorException {
        uniOp.unsetFieldSchema();
        super.visit(uniOp);
    }

    @Override
    public void visit(LOLesserThan op) throws VisitorException {
        op.unsetFieldSchema();
        super.visit(op);
    }

    @Override
    public void visit(LOLesserThanEqual op) throws VisitorException {
        op.unsetFieldSchema();
        super.visit(op);
    }

    @Override
    public void visit(LOMapLookup op) throws VisitorException {
        op.unsetFieldSchema();
        super.visit(op);
    }

    @Override
    public void visit(LOMod op) throws VisitorException {
        op.unsetFieldSchema();
        super.visit(op);
    }

    @Override
    public void visit(LOMultiply op) throws VisitorException {
        op.unsetFieldSchema();
        super.visit(op);
    }

    @Override
    public void visit(LONegative op) throws VisitorException {
        op.unsetFieldSchema();
        super.visit(op);
    }

    @Override
    public void visit(LONot uniOp) throws VisitorException {
        uniOp.unsetFieldSchema();
        super.visit(uniOp);
    }

    @Override
    public void visit(LONotEqual op) throws VisitorException {
        op.unsetFieldSchema();
        super.visit(op);
    }

    @Override
    public void visit(LOOr binOp) throws VisitorException {
        binOp.unsetFieldSchema();
        super.visit(binOp);
    }

    @Override
    public void visit(LOSubtract op) throws VisitorException {
        op.unsetFieldSchema();
        super.visit(op);
    }
}
