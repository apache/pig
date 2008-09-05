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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.plan.optimizer.*;

/**
 * An optimizer for logical plans.
 */
public class LogicalOptimizer extends PlanOptimizer<LogicalOperator, LogicalPlan> {

    public LogicalOptimizer(LogicalPlan plan) {
        super(plan);

        // List of rules for the logical optimizer
        
        // This one has to be first, as the type cast inserter expects the
        // load to only have one output.
        // Find any places in the plan that have an implicit split and make
        // it explicit.  Since the RuleMatcher doesn't handle trees properly,
        // we cheat and say that we match any node.  Then we'll do the actual
        // test in the transformers check method.
        List<String> nodes = new ArrayList<String>(1);
        Map<Integer, Integer> edges = new HashMap<Integer, Integer>();
        List<Boolean> required = new ArrayList<Boolean>(1);
        nodes.add("any");
        required.add(true);
        mRules.add(new Rule<LogicalOperator, LogicalPlan>(nodes, edges,
            required, new ImplicitSplitInserter(plan)));
        
        // Add type casting to plans where the schema has been declared (by
        // user, data, or data catalog).
        nodes = new ArrayList<String>(1);
        nodes.add("org.apache.pig.impl.logicalLayer.LOLoad");
        edges = new HashMap<Integer, Integer>();
        required = new ArrayList<Boolean>(1);
        required.add(true);
        mRules.add(new Rule<LogicalOperator, LogicalPlan>(nodes, edges, required,
            new TypeCastInserter(plan)));
        
        // Push up limit where ever possible.
        nodes = new ArrayList<String>(1);
        edges = new HashMap<Integer, Integer>();
        required = new ArrayList<Boolean>(1);
        nodes.add("org.apache.pig.impl.logicalLayer.LOLimit");
        required.add(true);
        mRules.add(new Rule<LogicalOperator, LogicalPlan>(nodes, edges, required,
            new OpLimitOptimizer(plan)));
        
        
    }

}


