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

package org.apache.pig.newplan.optimizer;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.OperatorPlan;

/**
 * The core class of the optimizer.  The basic design of this class is that it
 * is provided a list of RuleSets.  RuleSets represent all of the optimizer
 * rules that can be run together.  The rules in the RuleSet will be run
 * repeatedly until either no rule in the RuleSet passes check and calls
 * transform or until maxIter iterations (default 500) has been made over
 * the RuleSet.  Then the next RuleSet will be moved to.  Once finished,
 * a given RuleSet is never returned to.
 * 
 * Each rule is has two parts:  a pattern and and associated transformer.
 * Transformers have two important functions:   check(), and transform().
 * The pattern describes a pattern of node types that the optimizer will
 * look to match.  If that match is found anywhere in the plan, then check()
 * will be called.  check() allows the rule to look more in depth at the 
 * matched pattern and decide whether the rule should be run or not.  For
 * example, one might design a rule to push filters above join that would
 * look for the pattern filter(join) (meaning a filter followed by a join).
 * But only certain types of filters can be pushed.  The check() function 
 * would need to decide whether the filter that it found was pushable or not.
 * If check() returns true, the rule is said to have matched, and transform()
 * is then called.  This function is responsible for making changes in the
 * logical plan.  Once transform is complete PlanPatcher.patchUp will be
 * called to do any necessary cleanup in the plan, such as resetting 
 * schemas, etc.
 */
public abstract class PlanOptimizer {
 
    protected List<Set<Rule>> ruleSets;
    protected OperatorPlan plan;
    protected List<PlanTransformListener> listeners;
    protected int maxIter;
    
    static final int defaultIterations = 500;

    /**
     * @param p Plan to optimize
     * @param rs List of RuleSets to use to optimize
     * @param iterations maximum number of optimization iterations,
     * set to -1 for default
     */
    protected PlanOptimizer(OperatorPlan p,
                            List<Set<Rule>> rs,                            
                            int iterations) {
        plan = p;
        ruleSets = rs;
        listeners = new ArrayList<PlanTransformListener>();
        maxIter = (iterations < 1 ? defaultIterations : iterations);
    }
    
    /**
     * Adds a listener to the optimization.  This listener will be fired 
     * after each rule transforms a plan.  Listeners are guaranteed to
     * be fired in the order they are added.
     * @param listener
     */
    protected void addPlanTransformListener(PlanTransformListener listener) {
        listeners.add(listener);
    }
    
    /**
     * Run the optimizer.  This method attempts to match each of the Rules
     * against the plan.  If a Rule matches, it then calls the check
     * method of the associated Transformer to give the it a chance to
     * check whether it really wants to do the optimization.  If that
     * returns true as well, then Transformer.transform is called. 
     * @throws FrontendException
     */
    public void optimize() throws FrontendException {

        for (Set<Rule> rs : ruleSets) {
            boolean sawMatch = false;
            int numIterations = 0;
            do {
                sawMatch = false;
                for (Rule rule : rs) {
                    List<OperatorPlan> matches = rule.match(plan);
                    if (matches != null) {
                        Transformer transformer = rule.getNewTransformer();
                        for (OperatorPlan m : matches) {
                            try {
                                if (transformer.check(m)) {
                                    sawMatch = true;
                                    transformer.transform(m);
                                    if (!rule.isSkipListener()) {
                                        for(PlanTransformListener l: listeners) {
                                            l.transformed(plan, transformer.reportChanges());
                                        }
                                    }
                                }
                            } catch (Exception e) {
                                StringBuffer message = new StringBuffer("Error processing rule " + rule.name);
                                if (!rule.isMandatory()) {
                                    message.append(". Try -t " + rule.name);
                                }
                                throw new FrontendException(message.toString(), 2000, e);
                            }
                        }
                    }
                }
            } while(sawMatch && ++numIterations < maxIter);
        }
    }
}
