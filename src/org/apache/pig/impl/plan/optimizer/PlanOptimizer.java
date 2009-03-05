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

import java.util.ArrayList;
import java.util.List;

import org.apache.pig.impl.plan.Operator;
import org.apache.pig.impl.plan.OperatorPlan;

/******************************************************************************
 * A class to optimize plans.  This class need not be subclassed for a
 * particular type of plan.  It can be instantiated with a set of Rules and
 * then optimize called.
 *
 */

public abstract class PlanOptimizer<O extends Operator, P extends OperatorPlan<O>> {
    
    protected List<Rule> mRules;
    protected P mPlan;

    /**
     * @param plan Plan to optimize
     */
    protected PlanOptimizer(P plan) {
        mRules = new ArrayList<Rule>();
        mPlan = plan;
    }

    /**
     * Run the optimizer.  This method attempts to match each of the Rules
     * against the plan.  If a Rule matches, it then calls the check
     * method of the associated Transformer to give the it a chance to
     * check whether it really wants to do the optimization.  If that
     * returns true as well, then Transformer.transform is called. 
     * @throws OptimizerException
     */
    public final void optimize() throws OptimizerException {
        RuleMatcher matcher = new RuleMatcher();
        for (Rule rule : mRules) {
            if (matcher.match(rule)) {
                // It matches the pattern.  Now check if the transformer
                // approves as well.
                List<List<O>> matches = matcher.getAllMatches();
                for (List<O> match:matches)
                {
	                if (rule.transformer.check(match)) {
	                    // The transformer approves.
	                    rule.transformer.transform(match);
	                }
                }
            }
        }
    }
}
