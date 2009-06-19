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

import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.VisitorException;

/**
 * A visitor to calculate all the projection maps in a logical plan.
 */
public class ProjectionMapCalculator extends
        LOVisitor {
	
    public ProjectionMapCalculator(LogicalPlan plan) {
        super(plan, new DependencyOrderWalker<LogicalOperator, LogicalPlan>(plan));
    }


    /**
     * 
     * @param cg
     *            the logical cogroup operator that has to be visited
     * @throws VisitorException
     */
    protected void visit(LOCogroup cg) throws VisitorException {
        cg.getProjectionMap();
        super.visit(cg);
    }

    /**
     * 
     * @param frjoin
     *            the logical fragment replicate join operator that has to be visited
     * @throws VisitorException
     */
    protected void visit(LOFRJoin frjoin) throws VisitorException {
        frjoin.getProjectionMap();
        super.visit(frjoin);
    }
    
    /**
     * 
     * @param s
     *            the logical sort operator that has to be visited
     * @throws VisitorException
     */
    protected void visit(LOSort s) throws VisitorException {
        s.getProjectionMap();
        super.visit(s);
    }

    /**
     * 
     * @param limit
     *            the logical limit operator that has to be visited
     * @throws VisitorException
     */
    protected void visit(LOLimit limit) throws VisitorException {
        limit.getProjectionMap();
        super.visit(limit);
    }

    /**
     * 
     * @param filter
     *            the logical filter operator that has to be visited
     * @throws VisitorException
     */
    protected void visit(LOFilter filter) throws VisitorException {
        filter.getProjectionMap();
        super.visit(filter);
    }

    /**
     * 
     * @param split
     *            the logical split operator that has to be visited
     * @throws VisitorException
     */
    protected void visit(LOSplit split) throws VisitorException {
        split.getProjectionMap();
        super.visit(split);
    }

    /**
     * 
     * @param forEach
     *            the logical foreach operator that has to be visited
     * @throws VisitorException
     */
    protected void visit(LOForEach forEach) throws VisitorException {
        super.visit(forEach);
        forEach.getProjectionMap();
    }

    protected void visit(LOLoad load) throws VisitorException{
        load.getProjectionMap();
        super.visit(load);
    }
    
    protected void visit(LOStore store) throws VisitorException{
        store.getProjectionMap();
        super.visit(store);
    }
    
    protected void visit(LOUnion u) throws VisitorException {
        u.getProjectionMap();
        super.visit(u);
    }

    protected void visit(LOSplitOutput sop) throws VisitorException {
        sop.getProjectionMap();
        super.visit(sop);
    }

    protected void visit(LODistinct dt) throws VisitorException {
        dt.getProjectionMap();
        super.visit(dt);
    }

    protected void visit(LOCross cs) throws VisitorException {
        cs.getProjectionMap();
        super.visit(cs);
    }

    protected void visit(LOStream stream) throws VisitorException{
        stream.getProjectionMap();
        super.visit(stream);
    }
}
