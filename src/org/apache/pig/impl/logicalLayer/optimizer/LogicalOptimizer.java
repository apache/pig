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

import java.util.Set;

import org.apache.pig.ExecType;
import org.apache.pig.impl.logicalLayer.LOLimit;
import org.apache.pig.impl.logicalLayer.LOLoad;
import org.apache.pig.impl.logicalLayer.LOPrinter;
import org.apache.pig.impl.logicalLayer.LOStream;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.plan.optimizer.*;

/**
 * An optimizer for logical plans.
 */
public class LogicalOptimizer extends
        PlanOptimizer<LogicalOperator, LogicalPlan> {

    private static final String SCOPE = "RULE";
    private static NodeIdGenerator nodeIdGen = NodeIdGenerator.getGenerator();
    
    private Set<String> mRulesOff = null;

    public LogicalOptimizer(LogicalPlan plan) {
        this(plan, ExecType.MAPREDUCE);
    }

    public LogicalOptimizer(LogicalPlan plan, ExecType mode) {
        super(plan);
        runOptimizations(plan, mode);
    }
    
    public LogicalOptimizer(LogicalPlan plan, ExecType mode, Set<String> turnOffRules) {
        super(plan);
        mRulesOff = turnOffRules;
        runOptimizations(plan, mode);
    }

    private void runOptimizations(LogicalPlan plan, ExecType mode) {
        RulePlan rulePlan;

        // List of rules for the logical optimizer

        // This one has to be first, as the type cast inserter expects the
        // load to only have one output.
        // Find any places in the plan that have an implicit split and make
        // it explicit. Since the RuleMatcher doesn't handle trees properly,
        // we cheat and say that we match any node. Then we'll do the actual
        // test in the transformers check method.
        
        boolean turnAllRulesOff = false;
        if (mRulesOff != null) {
            for (String rule : mRulesOff) {
                if ("all".equalsIgnoreCase(rule)) {
                    turnAllRulesOff = true;
                    break;
                }
            }
        }
        
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

        // Optimize when LOAD precedes STREAM and the loader class
        // is the same as the serializer for the STREAM.
        // Similarly optimize when STREAM is followed by store and the
        // deserializer class is same as the Storage class.
        if(!turnAllRulesOff) {
            Rule rule = new Rule<LogicalOperator, LogicalPlan>(rulePlan, new StreamOptimizer(plan,
                    LOStream.class.getName()), "StreamOptimizer");
            checkAndAddRule(rule);
        }

        // Push up limit where ever possible.
        if(!turnAllRulesOff) {
            rulePlan = new RulePlan();
            RuleOperator loLimit = new RuleOperator(LOLimit.class, 
                    new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
            rulePlan.add(loLimit);
            Rule rule = new Rule<LogicalOperator, LogicalPlan>(rulePlan,
                    new OpLimitOptimizer(plan, mode), "LimitOptimizer");
            checkAndAddRule(rule);
        }
        
    }

    private void checkAndAddRule(Rule rule) {
        if(mRulesOff != null) {
            for(String ruleOff: mRulesOff) {
                String ruleName = rule.getRuleName();
                if(ruleName == null) continue;
                if(ruleName.equalsIgnoreCase(ruleOff)) return;
            }
        }
        mRules.add(rule);
    }

}