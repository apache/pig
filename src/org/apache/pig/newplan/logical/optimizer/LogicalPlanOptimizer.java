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

import com.google.common.base.Preconditions;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.TreeMultimap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.rules.AddForEach;
import org.apache.pig.newplan.logical.rules.ColumnMapKeyPrune;
import org.apache.pig.newplan.logical.rules.FilterAboveForeach;
import org.apache.pig.newplan.logical.rules.FilterConstantCalculator;
import org.apache.pig.newplan.logical.rules.ForEachConstantCalculator;
import org.apache.pig.newplan.logical.rules.GroupByConstParallelSetter;
import org.apache.pig.newplan.logical.rules.LimitOptimizer;
import org.apache.pig.newplan.logical.rules.LoadTypeCastInserter;
import org.apache.pig.newplan.logical.rules.MergeFilter;
import org.apache.pig.newplan.logical.rules.MergeForEach;
import org.apache.pig.newplan.logical.rules.PartitionFilterOptimizer;
import org.apache.pig.newplan.logical.rules.PredicatePushdownOptimizer;
import org.apache.pig.newplan.logical.rules.PushDownForEachFlatten;
import org.apache.pig.newplan.logical.rules.PushUpFilter;
import org.apache.pig.newplan.logical.rules.SplitFilter;
import org.apache.pig.newplan.logical.rules.StreamTypeCastInserter;
import org.apache.pig.newplan.optimizer.PlanOptimizer;
import org.apache.pig.newplan.optimizer.Rule;

public class LogicalPlanOptimizer extends PlanOptimizer {
    private static final Log LOG = LogFactory.getLog(LogicalPlanOptimizer.class);
    private static enum RulesReportKey { RULES_ENABLED, RULES_DISABLED }
    private Set<String> mRulesOff = null;
    private boolean allRulesDisabled = false;
    private SetMultimap<RulesReportKey, String> rulesReport = TreeMultimap.create();
    private PigContext pc = null;

    public LogicalPlanOptimizer(OperatorPlan p, int iterations, Set<String> turnOffRules) {
        this(p, iterations, turnOffRules, null);
    }
    /**
     * Create a new LogicalPlanOptimizer.
     * @param p               Plan to optimize.
     * @param iterations      Maximum number of optimizer iterations.
     * @param turnOffRules    Optimization rules to disable. "all" disables all non-mandatory
     *                        rules. null enables all rules.
     * @param pc              PigContext object
     */
    public LogicalPlanOptimizer(OperatorPlan p, int iterations, Set<String> turnOffRules, PigContext
            pc) {
        super(p, null, iterations);
        this.pc = pc;
        mRulesOff = turnOffRules == null ? new HashSet<String>() : turnOffRules;
        if (mRulesOff.contains("all")) {
            allRulesDisabled = true;
        }

        ruleSets = buildRuleSets();
        LOG.info(rulesReport);
        addListeners();
    }

    protected List<Set<Rule>> buildRuleSets() {
        List<Set<Rule>> ls = new ArrayList<Set<Rule>>();

        // Logical expression simplifier
        Set <Rule> s = new HashSet<Rule>();
        // add constant calculator rule
        Rule r = new FilterConstantCalculator("ConstantCalculator", pc);
        checkAndAddRule(s, r);
        ls.add(s);
        r = new ForEachConstantCalculator("ConstantCalculator", pc);
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

        // Predicate pushdown set
        // This set of rules push filter conditions to LoadFunc
        s = new HashSet<Rule>();
        // Optimize partition filter
        r = new PredicatePushdownOptimizer("PredicatePushdownOptimizer");
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

        // Limit Set
        // This set of rules push up limit
        s = new HashSet<Rule>();
        // Optimize limit
        r = new LimitOptimizer("LimitOptimizer");
        checkAndAddRule(s, r);
        if (!s.isEmpty())
            ls.add(s);

        return ls;
    }

    /**
     * Add rule to ruleSet if its mandatory, or has not been disabled.
     * @param ruleSet    Set rule will be added to if not disabled.
     * @param rule       Rule to potentially add.
     */
    private void checkAndAddRule(Set<Rule> ruleSet, Rule rule) {
        Preconditions.checkArgument(ruleSet != null);
        Preconditions.checkArgument(rule != null && rule.getName() != null);

        if (rule.isMandatory()) {
            ruleSet.add(rule);
            rulesReport.put(RulesReportKey.RULES_ENABLED, rule.getName());
        } else if (!allRulesDisabled && !mRulesOff.contains(rule.getName())) {
            ruleSet.add(rule);
            rulesReport.put(RulesReportKey.RULES_ENABLED, rule.getName());
        } else {
            rulesReport.put(RulesReportKey.RULES_DISABLED, rule.getName());
        }
    }

    private void addListeners() {
        addPlanTransformListener(new SchemaPatcher());
        addPlanTransformListener(new ProjectionPatcher());
    }
}
