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
public class SchemaCalculator extends LOVisitor {

    public SchemaCalculator(LogicalPlan plan) {
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
        try {
            binOp.getFieldSchema();
            super.visit(binOp);
        } catch (FrontendException fe) {
            throw new VisitorException(fe);
        }
    }

    /**
     * 
     * @param uniOp
     *            the logical unary operator that has to be visited
     * @throws VisitorException
     */
    protected void visit(UnaryExpressionOperator uniOp) throws VisitorException {
        try {
            uniOp.getFieldSchema();
            super.visit(uniOp);
        } catch (FrontendException fe) {
            throw new VisitorException(fe);
        }
    }

    /**
     * 
     * @param cg
     *            the logical cogroup operator that has to be visited
     * @throws VisitorException
     */
    protected void visit(LOCogroup cg) throws VisitorException {
        try {
            cg.getSchema();
            super.visit(cg);
        } catch (FrontendException fe) {
            throw new VisitorException(fe);
        }
    }

    /**
     * 
     * @param s
     *            the logical sort operator that has to be visited
     * @throws VisitorException
     */
    protected void visit(LOSort s) throws VisitorException {
        try {
            s.getSchema();
            super.visit(s);
        } catch (FrontendException fe) {
            throw new VisitorException(fe);
        }
    }

    /**
     * 
     * @param limit
     *            the logical limit operator that has to be visited
     * @throws VisitorException
     */
    protected void visit(LOLimit limit) throws VisitorException {
        try {
            limit.getSchema();
            super.visit(limit);
        } catch (FrontendException fe) {
            throw new VisitorException(fe);
        }
    }

    /**
     * 
     * @param filter
     *            the logical filter operator that has to be visited
     * @throws VisitorException
     */
    protected void visit(LOFilter filter) throws VisitorException {
        try {
            filter.getSchema();
            super.visit(filter);
        } catch (FrontendException fe) {
            throw new VisitorException(fe);
        }
    }

    /**
     * 
     * @param split
     *            the logical split operator that has to be visited
     * @throws VisitorException
     */
    protected void visit(LOSplit split) throws VisitorException {
        try {
            split.getSchema();
            super.visit(split);
        } catch (FrontendException fe) {
            throw new VisitorException(fe);
        }
    }

    /**
     * 
     * @param forEach
     *            the logical foreach operator that has to be visited
     * @throws VisitorException
     */
    protected void visit(LOForEach forEach) throws VisitorException {
        try {
            super.visit(forEach);
            forEach.getSchema();
        } catch (FrontendException fe) {
            throw new VisitorException(fe);
        }
    }

    /**
     * Iterate over each expression that is part of the function argument list
     * 
     * @param func
     *            the user defined function
     * @throws VisitorException
     */
    protected void visit(LOUserFunc func) throws VisitorException {
        try {
            func.getFieldSchema();
            super.visit(func);
        } catch (FrontendException fe) {
            throw new VisitorException(fe);
        }
    }

    /**
     * @param binCond
     *            the logical binCond operator that has to be visited
     * @throws VisitorException
     */
    protected void visit(LOBinCond binCond) throws VisitorException {
        try {
            binCond.getFieldSchema();
            super.visit(binCond);
        } catch (FrontendException fe) {
            throw new VisitorException(fe);
        }
    }

    /**
     * 
     * @param cast
     *            the logical cast operator that has to be visited
     * @throws VisitorException
     */
    protected void visit(LOCast cast) throws VisitorException {
        try {
            cast.getFieldSchema();
            super.visit(cast);
        } catch (FrontendException fe) {
            throw new VisitorException(fe);
        }
    }
    
    /**
     * 
     * @param regexp
     *            the logical regexp operator that has to be visited
     * @throws VisitorException
     */
    protected void visit(LORegexp regexp) throws VisitorException {
        try {
            regexp.getFieldSchema();
            super.visit(regexp);
        } catch (FrontendException fe) {
            throw new VisitorException(fe);
        }
    }

    protected void visit(LOLoad load) throws VisitorException{
        try {
            load.getSchema();
            super.visit(load);
        } catch (FrontendException fe) {
            throw new VisitorException(fe);
        }
    }
    
    protected void visit(LOStore store) throws VisitorException{
        // We don't calculate schema of LOStore
    }
    
    protected void visit(LOConst c) throws VisitorException{
        try {
            c.getFieldSchema();
            super.visit(c);
        } catch (FrontendException fe) {
            throw new VisitorException(fe);
        }
    }

    protected void visit(LOUnion u) throws VisitorException {
        try {
            u.getSchema();
            super.visit(u);
        } catch (FrontendException fe) {
            throw new VisitorException(fe);
        }
    }

    protected void visit(LOSplitOutput sop) throws VisitorException {
        try {
            sop.getSchema();
            super.visit(sop);
        } catch (FrontendException fe) {
            throw new VisitorException(fe);
        }
    }

    protected void visit(LODistinct dt) throws VisitorException {
        try {
            dt.getSchema();
            super.visit(dt);
        } catch (FrontendException fe) {
            throw new VisitorException(fe);
        }
    }

    protected void visit(LOCross cs) throws VisitorException {
        try {
            cs.getSchema();
            super.visit(cs);
        } catch (FrontendException fe) {
            throw new VisitorException(fe);
        }
    }

    protected void visit(LOProject project) throws VisitorException {
        try {
            project.getFieldSchema();
            super.visit(project);
        } catch (FrontendException fe) {
            throw new VisitorException(fe);
        }
    }
    
}
