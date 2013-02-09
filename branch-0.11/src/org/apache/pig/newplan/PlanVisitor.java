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

package org.apache.pig.newplan;

import java.util.Deque;
import java.util.LinkedList;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.plan.VisitorException;

/**
 * A visitor mechanism for navigating and operating on a plan of 
 * Operators.  This class contains the logic to traverse the plan.  It does
 * not visit individual nodes.  That is left to implementing classes
 * (such as LOVisitor).
 */
public abstract class PlanVisitor {

    // TODO Remove this scope value
    final protected static String DEFAULT_SCOPE = "scope";
    
    protected OperatorPlan plan;

    /**
     * Guaranteed to always point to the walker currently being used.
     */
    protected PlanWalker currentWalker;

    private Deque<PlanWalker> walkers;

    /**
     * Entry point for visiting the plan.
     * @throws VisitorException if an error is encountered while visiting.
     */
    public void visit() throws FrontendException {
        currentWalker.walk(this);
    }

    public OperatorPlan getPlan() {
        return plan;
    }

    /**
     * @param plan OperatorPlan this visitor will visit.
     * @param walker PlanWalker this visitor will use to traverse the plan.
     */
    protected PlanVisitor(OperatorPlan plan, PlanWalker walker) {
        this.plan = plan;
        currentWalker = walker;
        walkers = new LinkedList<PlanWalker>();
    }

    /**
     * Push the current walker onto the stack of saved walkers and begin using
     * the newly passed walker as the current walker.
     * @param walker new walker to set as the current walker.
     */
    protected void pushWalker(PlanWalker walker) {
        walkers.push(currentWalker);
        currentWalker = walker;
    }

    /**
     * Pop the next to previous walker off of the stack and set it as the current
     * walker.  This will drop the reference to the current walker.
     * @throws VisitorException if there are no more walkers on the stack.  In
     * this case the current walker is not reset.
     */
    protected void popWalker() throws FrontendException {
        if (walkers.isEmpty()) {
            throw new FrontendException("No more walkers to pop.", 2221);
        }
        currentWalker = walkers.pop();
    }
}
