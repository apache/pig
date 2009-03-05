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
    protected void visit(LOUserFunc func) throws VisitorException {
        func.unsetFieldSchema();
        super.visit(func);
    }

    /**
     * @param binCond
     *            the logical binCond operator that has to be visited
     * @throws VisitorException
     */
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
    protected void visit(LOCast cast) throws VisitorException {
        super.visit(cast);
    }
    
    /**
     * 
     * @param regexp
     *            the logical regexp operator that has to be visited
     * @throws ParseException
     */
    protected void visit(LORegexp regexp) throws VisitorException {
        regexp.unsetFieldSchema();
        super.visit(regexp);
    }

    protected void visit(LOLoad load) throws VisitorException{
        // Don't remove load's schema, it's not like it will change.  And we
        // don't have a way to recover it.
        super.visit(load);
    }
    
    protected void visit(LOStore store) throws VisitorException{
        store.unsetSchema();
        super.visit(store);
    }
    
    protected void visit(LOConst c) throws VisitorException{
        c.unsetSchema();
        super.visit(c);
    }

    protected void visit(LOUnion u) throws VisitorException {
        u.unsetSchema();
        super.visit(u);
    }

    protected void visit(LOSplitOutput sop) throws VisitorException {
        sop.unsetSchema();
        super.visit(sop);
    }

    protected void visit(LODistinct dt) throws VisitorException {
        dt.unsetSchema();
        super.visit(dt);
    }

    protected void visit(LOCross cs) throws VisitorException {
        cs.unsetSchema();
        super.visit(cs);
    }

    protected void visit(LOProject project) throws VisitorException {
        project.unsetFieldSchema();
        super.visit(project);
    }

    @Override
    protected void visit(LOFRJoin frj) throws VisitorException {
        frj.unsetSchema();
        super.visit(frj);
    }
    
}
