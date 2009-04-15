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

package org.apache.pig.pen.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.pig.impl.logicalLayer.LOLoad;
import org.apache.pig.impl.logicalLayer.LOStream;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.logicalLayer.optimizer.ImplicitSplitInserter;
import org.apache.pig.impl.logicalLayer.optimizer.OpLimitOptimizer;
import org.apache.pig.impl.logicalLayer.optimizer.StreamOptimizer;
import org.apache.pig.impl.logicalLayer.optimizer.TypeCastInserter;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.optimizer.PlanOptimizer;
import org.apache.pig.impl.plan.optimizer.Rule;
import org.apache.pig.impl.plan.optimizer.RuleOperator;
import org.apache.pig.impl.plan.optimizer.RulePlan;
import org.apache.pig.impl.util.MultiMap;

//This optimiser puts in the bare minimum modifications needed to make sure the plan is functional
public class FunctionalLogicalOptimizer extends
        PlanOptimizer<LogicalOperator, LogicalPlan> {

    private static final String SCOPE = "RULE";
    private static NodeIdGenerator nodeIdGen = NodeIdGenerator.getGenerator();
    
    public FunctionalLogicalOptimizer(LogicalPlan plan) {
        super(plan);

        RulePlan rulePlan;        

        // List of rules for the logical optimizer

        // This one has to be first, as the type cast inserter expects the
        // load to only have one output.
        // Find any places in the plan that have an implicit split and make
        // it explicit. Since the RuleMatcher doesn't handle trees properly,
        // we cheat and say that we match any node. Then we'll do the actual
        // test in the transformers check method.
        
        rulePlan = new RulePlan();
        RuleOperator anyLogicalOperator = new RuleOperator(LogicalOperator.class, RuleOperator.NodeType.ANY_NODE, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        rulePlan.add(anyLogicalOperator);
        mRules.add(new Rule<LogicalOperator, LogicalPlan>(rulePlan,
                new ImplicitSplitInserter(plan), "ImplicitSplitInserter"));


        // Add type casting to plans where the schema has been declared (by
        // user, data, or data catalog).
        rulePlan = new RulePlan();
        RuleOperator loLoad = new RuleOperator(LOLoad.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        rulePlan.add(loLoad);
        mRules.add(new Rule<LogicalOperator, LogicalPlan>(rulePlan,
                new TypeCastInserter(plan, LOLoad.class.getName()), "LoadTypeCastInserter"));

        // Add type casting to plans where the schema has been declared by
        // user in a statement with stream operator.
        rulePlan = new RulePlan();
        RuleOperator loStream= new RuleOperator(LOStream.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        rulePlan.add(loStream);
        mRules.add(new Rule<LogicalOperator, LogicalPlan>(rulePlan, new TypeCastInserter(plan,
                LOStream.class.getName()), "StreamTypeCastInserter"));

    }

}
