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
package org.apache.pig.newplan.logical.optimizer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.rules.AddForEach;
import org.apache.pig.newplan.logical.rules.ColumnMapKeyPrune;
import org.apache.pig.newplan.logical.rules.DuplicateForEachColumnRewrite;
import org.apache.pig.newplan.logical.rules.FilterAboveForeach;
import org.apache.pig.newplan.logical.rules.GroupByConstParallelSetter;
import org.apache.pig.newplan.logical.rules.ImplicitSplitInserter;
import org.apache.pig.newplan.logical.rules.InputOutputFileValidator;
import org.apache.pig.newplan.logical.rules.LimitOptimizer;
import org.apache.pig.newplan.logical.rules.LoadTypeCastInserter;
import org.apache.pig.newplan.logical.rules.LogicalExpressionSimplifier;
import org.apache.pig.newplan.logical.rules.MergeFilter;
import org.apache.pig.newplan.logical.rules.MergeForEach;
import org.apache.pig.newplan.logical.rules.PartitionFilterOptimizer;
import org.apache.pig.newplan.logical.rules.PushDownForEachFlatten;
import org.apache.pig.newplan.logical.rules.PushUpFilter;
import org.apache.pig.newplan.logical.rules.SplitFilter;
import org.apache.pig.newplan.logical.rules.StreamTypeCastInserter;
import org.apache.pig.newplan.optimizer.PlanOptimizer;
import org.apache.pig.newplan.optimizer.Rule;

public class LogicalPlanOptimizer extends PlanOptimizer {
    private Set<String> mRulesOff = null;
    
    public LogicalPlanOptimizer(OperatorPlan p, int iterations, Set<String> turnOffRules) {    	
        super(p, null, iterations);
        this.mRulesOff = turnOffRules;
        ruleSets = buildRuleSets();
        addListeners();
    }

    protected List<Set<Rule>> buildRuleSets() {
        List<Set<Rule>> ls = new ArrayList<Set<Rule>>();	    

        
        // ImplicitSplitInserter set
        // This set of rules Insert Foreach dedicated for casting after load
        Set<Rule> s = new HashSet<Rule>();
        Rule r = new ImplicitSplitInserter("ImplicitSplitInserter");
        checkAndAddRule(s, r);
        if (!s.isEmpty())
            ls.add(s);

        // DuplicateForEachColumnRewrite set
        // This insert Identity UDF in the case foreach duplicate field.
        // This is because we need unique uid through out the plan
        s = new HashSet<Rule>();
        r = new DuplicateForEachColumnRewrite("DuplicateForEachColumnRewrite");
        checkAndAddRule(s, r);
        if (!s.isEmpty())
            ls.add(s);
        
        // Logical expression simplifier
        s = new HashSet<Rule>();
        // add logical expression simplification rule
        r = new LogicalExpressionSimplifier("FilterLogicExpressionSimplifier");
        checkAndAddRule(s, r);
        ls.add(s);

        // TypeCastInserter set
        // This set of rules Insert Foreach dedicated for casting after load
        s = new HashSet<Rule>();
        // add split filter rule
        r = new LoadTypeCastInserter("LoadTypeCastInserter");
        checkAndAddRule(s, r);
        r = new StreamTypeCastInserter("StreamTypeCastInserter");
        checkAndAddRule(s, r);
        if (!s.isEmpty())
            ls.add(s);

        // Limit Set
        // This set of rules push up limit
        s = new HashSet<Rule>();
        // Optimize limit
        r = new LimitOptimizer("LimitOptimizer");
        checkAndAddRule(s, r);
        if (!s.isEmpty())
            ls.add(s);
        
        // Split Set
        // This set of rules does splitting of operators only.
        // It does not move operators
        s = new HashSet<Rule>();
        // add split filter rule
        r = new SplitFilter("SplitFilter");
        checkAndAddRule(s, r);
        if (!s.isEmpty())
            ls.add(s);
        
        
        // Push Set,
        // This set does moving of operators only.
        s = new HashSet<Rule>();
        r = new PushUpFilter("PushUpFilter");
        checkAndAddRule(s, r);
        r = new FilterAboveForeach("PushUpFilter");
        checkAndAddRule(s, r);
        if (!s.isEmpty())
            ls.add(s);
        
        // Merge Set
        // This Set merges operators but does not move them.
        s = new HashSet<Rule>();
        checkAndAddRule(s, r);
        // add merge filter rule
        r = new MergeFilter("MergeFilter");        
        checkAndAddRule(s, r);
        if (!s.isEmpty())
            ls.add(s);
        
        // Partition filter set
        // This set of rules push partition filter to LoadFunc
        s = new HashSet<Rule>();
        // Optimize partition filter
        r = new PartitionFilterOptimizer("PartitionFilterOptimizer");
        checkAndAddRule(s, r);
        if (!s.isEmpty())
            ls.add(s);
        
        // PushDownForEachFlatten set
        s = new HashSet<Rule>();
        // Add the PushDownForEachFlatten
        r = new PushDownForEachFlatten("PushDownForEachFlatten");
        checkAndAddRule(s, r);
        if (!s.isEmpty())
            ls.add(s);
        
        // Prune Set
        // This set is used for pruning columns and maps
        s = new HashSet<Rule>();
        // Add the PruneMap Filter
        r = new ColumnMapKeyPrune("ColumnMapKeyPrune");
        checkAndAddRule(s, r);
        if (!s.isEmpty())
            ls.add(s);
        
        // Add LOForEach set
        s = new HashSet<Rule>();
        // Add the AddForEach
        r = new AddForEach("AddForEach");
        checkAndAddRule(s, r);
        if (!s.isEmpty())
            ls.add(s);
        
        // Add MergeForEach set
        s = new HashSet<Rule>();
        // Add the AddForEach
        r = new MergeForEach("MergeForEach");
        checkAndAddRule(s, r);
        if (!s.isEmpty())
            ls.add(s);
        
        //set parallism to 1 for cogroup/group-by on constant
        s = new HashSet<Rule>();
        r = new GroupByConstParallelSetter("GroupByConstParallelSetter");
        checkAndAddRule(s, r);
        if(!s.isEmpty())
            ls.add(s);
        
        return ls;
    }
        
    private void checkAndAddRule(Set<Rule> ruleSet, Rule rule) {
        if (rule.isMandatory()) {
            ruleSet.add(rule);
            return;
        }
        
        boolean turnAllRulesOff = false;
        if (mRulesOff != null) {
            for (String ruleName : mRulesOff) {
                if ("all".equalsIgnoreCase(ruleName)) {
                    turnAllRulesOff = true;
                    break;
                }
            }
        }
        
        if (turnAllRulesOff) return;
        
        if(mRulesOff != null) {
            for(String ruleOff: mRulesOff) {
                String ruleName = rule.getName();
                if(ruleName == null) continue;
                if(ruleName.equalsIgnoreCase(ruleOff)) return;
            }
        }
        
        ruleSet.add(rule);
    }

    private void addListeners() {
        addPlanTransformListener(new SchemaPatcher());
        addPlanTransformListener(new ProjectionPatcher());
    }
}
