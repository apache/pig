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

import java.util.List;
import java.util.Set;

import org.apache.pig.ExecType;
import org.apache.pig.PigException;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.LOFilter;
import org.apache.pig.impl.logicalLayer.LOForEach;
import org.apache.pig.impl.logicalLayer.LOLimit;
import org.apache.pig.impl.logicalLayer.LOLoad;
import org.apache.pig.impl.logicalLayer.LONative;
import org.apache.pig.impl.logicalLayer.LOStream;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.logicalLayer.RelationalOperator;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.optimizer.OptimizerException;
import org.apache.pig.impl.plan.optimizer.PlanOptimizer;
import org.apache.pig.impl.plan.optimizer.Rule;
import org.apache.pig.impl.plan.optimizer.RuleMatcher;
import org.apache.pig.impl.plan.optimizer.RuleOperator;
import org.apache.pig.impl.plan.optimizer.RulePlan;

/**
 * An optimizer for logical plans.
 */
public class LogicalOptimizer extends
        PlanOptimizer<LogicalOperator, LogicalPlan> {

    private static final String SCOPE = "RULE";
    private static NodeIdGenerator nodeIdGen = NodeIdGenerator.getGenerator();
    
    private Set<String> mRulesOff = null;
    private Rule<LogicalOperator, LogicalPlan> pruneRule;

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
        
        boolean turnAllRulesOff = false;
        if (mRulesOff != null) {
            for (String rule : mRulesOff) {
                if ("all".equalsIgnoreCase(rule)) {
                    turnAllRulesOff = true;
                    break;
                }
            }
        }
        
        // This one has to be before the type cast inserter as it expects the
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

        
        // this one is ordered to be before other optimizations since  later 
        // optimizations may move the LOFilter that is looks for just after a 
        // LOLoad
        rulePlan = new RulePlan();
        RuleOperator loLoad = new RuleOperator(LOLoad.class, 
                new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
        rulePlan.add(loLoad);
        mRules.add(new Rule<LogicalOperator, LogicalPlan>(rulePlan,
                new PartitionFilterOptimizer(plan), "LoadPartitionFilterOptimizer"));
        
        // Add type casting to plans where the schema has been declared (by
        // user, data, or data catalog).
        rulePlan = new RulePlan();
        loLoad = new RuleOperator(LOLoad.class, 
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
        
        if(!turnAllRulesOff) {

            // Push up limit wherever possible.
            rulePlan = new RulePlan();
            RuleOperator loLimit = new RuleOperator(LOLimit.class,
					new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
			rulePlan.add(loLimit);
			Rule<LogicalOperator, LogicalPlan> rule = new Rule<LogicalOperator, LogicalPlan>(rulePlan,
					new OpLimitOptimizer(plan, mode), "LimitOptimizer");
            checkAndAddRule(rule);
            
            // Push filters up wherever possible
            rulePlan = new RulePlan();
            RuleOperator loFilter = new RuleOperator(LOFilter.class,
                    new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
            rulePlan.add(loFilter);
            rule = new Rule<LogicalOperator, LogicalPlan>(rulePlan,
                    new PushUpFilter(plan), "PushUpFilter");
            checkAndAddRule(rule);
            
            // Push foreach with flatten down wherever possible
            rulePlan = new RulePlan();
            RuleOperator loForeach = new RuleOperator(LOForEach.class,
                    new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
            rulePlan.add(loForeach);
            rule = new Rule<LogicalOperator, LogicalPlan>(rulePlan,
                    new PushDownForeachFlatten(plan), "PushDownForeachFlatten");
            checkAndAddRule(rule);
            
            // Prune column up wherever possible
            rulePlan = new RulePlan();
            RuleOperator rulePruneColumnsOperator = new RuleOperator(RelationalOperator.class, RuleOperator.NodeType.ANY_NODE,
                    new OperatorKey(SCOPE, nodeIdGen.getNextNodeId(SCOPE)));
            rulePlan.add(rulePruneColumnsOperator);
            pruneRule = new Rule<LogicalOperator, LogicalPlan>(rulePlan,
                    new PruneColumns(plan), "PruneColumns", Rule.WalkerAlgo.ReverseDependencyOrderWalker);
        }
        
    }

    private boolean ruleEnabled(Rule<LogicalOperator, LogicalPlan> rule) {
        if(mRulesOff != null && rule != null) {
            for(String ruleOff: mRulesOff) {
                String ruleName = rule.getRuleName();
                if(ruleName == null) continue;
                if(ruleName.equalsIgnoreCase(ruleOff)) return false;
            }
        }
        mRules.add(rule);
        return true;
    }
    
    private void checkAndAddRule(Rule<LogicalOperator, LogicalPlan> rule) {
        if (ruleEnabled(rule))
            mRules.add(rule);
    }

    @Override
    public final int optimize() throws OptimizerException {
        //the code that follows is a copy of the code in the
        //base class. see the todo note in the base class
        boolean sawMatch = false;
        boolean initialized = false;
        int numIterations = 0;
        do {
            sawMatch = false;
            for (Rule<LogicalOperator, LogicalPlan> rule : mRules) {
                RuleMatcher<LogicalOperator, LogicalPlan> matcher = new RuleMatcher<LogicalOperator, LogicalPlan>();
                if (matcher.match(rule)) {
                    // It matches the pattern.  Now check if the transformer
                    // approves as well.
                    List<List<LogicalOperator>> matches = matcher.getAllMatches();
                    for (List<LogicalOperator> match:matches)
                    {
                        if (rule.getTransformer().check(match)) {
                            try {
                                // The transformer approves.
                                sawMatch = true;
                                if (!initialized)
                                {
                                    ((LogicalTransformer)rule.getTransformer()).rebuildSchemas();
                                    ((LogicalTransformer)rule.getTransformer()).rebuildProjectionMaps();
                                    initialized = true;
                                }
                                rule.getTransformer().transform(match);
                                ((LogicalTransformer)rule.getTransformer()).rebuildSchemas();
                                ((LogicalTransformer)rule.getTransformer()).rebuildProjectionMaps();
                            } catch (FrontendException fee) {
                                int errCode = 2145;
                                String msg = "Problem while rebuilding projection map or schema in logical optimizer.";
                                throw new OptimizerException(msg, errCode, PigException.BUG, fee);
                            }

                        }
                        rule.getTransformer().reset();
                    }
                }
            }
        } while(sawMatch && ++numIterations < mMaxIterations);
        if (pruneRule!=null && ruleEnabled(pruneRule))
        {
            RuleMatcher<LogicalOperator, LogicalPlan> matcher = new RuleMatcher<LogicalOperator, LogicalPlan>();
            if (matcher.match(pruneRule)) {
                List<List<LogicalOperator>> matches = matcher.getAllMatches();
                for (List<LogicalOperator> match:matches)
                {
                    if (pruneRule.getTransformer().check(match)) {
                        pruneRule.getTransformer().transform(match);
                    }
                }
                ((PruneColumns)pruneRule.getTransformer()).prune();
            }
        }
        return numIterations;
    }
}