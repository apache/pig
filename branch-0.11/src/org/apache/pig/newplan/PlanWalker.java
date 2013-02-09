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

import org.apache.pig.impl.logicalLayer.FrontendException;

public abstract class PlanWalker {

    protected OperatorPlan plan;

    /**
     * @param plan Plan for this walker to traverse.
     */
    public PlanWalker(OperatorPlan plan) {
        this.plan = plan;
    }

    /**
     * Begin traversing the graph.
     * @param visitor Visitor this walker is being used by.  This can't be set in
     * the constructor because the visitor is constructing this class, and does
     * not yet have a 'this' pointer to send as an argument.
     * @throws VisitorException if an error is encountered while walking.
     */
    public abstract void walk(PlanVisitor visitor) throws FrontendException;

    /**
     * Return a new instance of this same type of walker for a subplan.
     * When this method is called the same type of walker with the
     * provided plan set as the plan, must be returned.  This can then be
     * used to walk subplans.  This allows abstract visitors to clone
     * walkers without knowning the type of walker their subclasses used.
     * @param plan Plan for the new walker.
     * @return Instance of the same type of walker with plan set to plan.
     */
    public abstract PlanWalker spawnChildWalker(OperatorPlan plan);

    public OperatorPlan getPlan() {
        return plan ;
    }
    
    /**
     * Set the plan for this walker to operate on.
     * @param plan to walk
     */
    public void setPlan(OperatorPlan plan) {
        this.plan = plan;
    }

}
