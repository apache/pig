/**
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

package org.apache.pig.newplan.logical.rules;

import java.util.ArrayList;
import java.util.List;

import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.optimizer.Rule;

/**
 * Super class for all rules that operates on the whole plan. It doesn't look for
 * a specific pattern. An example of such kind rule is ColumnPrune.
 *
 */
public abstract class WholePlanRule extends Rule {

    public WholePlanRule(String n, boolean mandatory) {
        super(n, mandatory);
    }

    public List<OperatorPlan> match(OperatorPlan plan) {
        currentPlan = plan;
        List<OperatorPlan> ll = new ArrayList<OperatorPlan>();
        ll.add(plan);
        return ll;
    }
    
    @Override
    protected OperatorPlan buildPattern() {
        return null;
    }
}
