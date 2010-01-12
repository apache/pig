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

package org.apache.pig.experimental.plan;

public abstract class Operator {
    
    protected String name;
    protected OperatorPlan plan; // plan that contains this operator

    public Operator(String n, OperatorPlan p) {
        name = n;
        plan = p;
    }

    /**
     * Accept a visitor at this node in the graph.
     * @param v Visitor to accept.
     */
    public abstract void accept(PlanVisitor v);

    public String getName() {
        return name;
    }
    
    /**
     * Get the plan associated with this operator.
     * @return plan
     */
    public OperatorPlan getPlan() {
        return plan;
    }

}
