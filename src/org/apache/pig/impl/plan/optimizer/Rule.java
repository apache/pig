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
package org.apache.pig.impl.plan.optimizer;

import org.apache.pig.impl.plan.Operator;
import org.apache.pig.impl.plan.OperatorPlan;

/**
 * A rule for optimizing a plan. The rule contains a pattern that must be
 * matched in the plan before the optimizer can consider applying the rule and a
 * transformer to do further checks and possibly transform the plan. The rule
 * pattern is expressed as a list of node names, a map of edges in the plan, and
 * a list of boolean values indicating whether the node is required. For
 * example, a rule pattern could be expressed as: [Filter, Filter] {[0, 1]}
 * [true, true], which would indicate this rule matches two nodes of class name
 * Filter, with an edge between the two, and both are required.
 */
public class Rule<O extends Operator, P extends OperatorPlan<O>> {

    public enum WalkerAlgo {
        DepthFirstWalker, DependencyOrderWalker
    };

    private RulePlan mRulePlan;
    private Transformer<O, P> mTransformer;
    private WalkerAlgo mWalkerAlgo;
    private String mRuleName = null;

    /**
     * @param plan
     *            pattern to look for
     * @param t
     *            Transformer to apply if the rule matches.
     */
    public Rule(RulePlan plan, Transformer<O, P> t, String ruleName) {
        this(plan, t, ruleName, WalkerAlgo.DependencyOrderWalker);
    }

    /**
     * @param plan
     *            pattern to look for
     * @param t
     *            Transformer to apply if the rule matches.
     * @param al
     *            Walker algorithm to find rule match within the plan.
     */
    public Rule(RulePlan plan, Transformer<O, P> t, String ruleName,
            WalkerAlgo al) {
        mRulePlan = plan;
        mTransformer = t;
        mRuleName = ruleName;
        mWalkerAlgo = al;
    }

    public RulePlan getPlan() {
        return mRulePlan;
    }

    public Transformer<O, P> getTransformer() {
        return mTransformer;
    }

    public String getRuleName() {
        return mRuleName;
    }

    public WalkerAlgo getWalkerAlgo() {
        return mWalkerAlgo;
    }

}