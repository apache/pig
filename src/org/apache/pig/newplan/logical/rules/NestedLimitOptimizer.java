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
package org.apache.pig.newplan.logical.rules;

import java.util.Iterator;
import java.util.List;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOLimit;
import org.apache.pig.newplan.logical.relational.LOSort;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.optimizer.Rule;
import org.apache.pig.newplan.optimizer.Transformer;

public class NestedLimitOptimizer extends Rule {

    public NestedLimitOptimizer(String name) {
        super(name, false);
    }

    @Override
    protected OperatorPlan buildPattern() {
        LogicalPlan plan = new LogicalPlan();
        LogicalRelationalOperator forEach = new LOForEach(plan);
        plan.add(forEach);
        return plan;
    }

    @Override
    public Transformer getNewTransformer() {
        return new OptimizeNestedLimitTransformer();
    }

    public class OptimizeNestedLimitTransformer extends Transformer {

        @Override
        public boolean check(OperatorPlan matched) {

            LOForEach forEach = (LOForEach) matched.getSources().get(0);
            LogicalPlan innerPlan = forEach.getInnerPlan();

            // check if there is a LOSort immediately followed by LOLimit in innerPlan
            Iterator<Operator> it = innerPlan.getOperators();

            while(it.hasNext()) {
                Operator op = it.next();
                if (op instanceof LOLimit) {
                    List<Operator> preds = innerPlan.getPredecessors(op);
                    // Limit should always have exactly 1 predecessor
                    if (preds.get(0) instanceof LOSort) {
                        return true;
                    }
                }
            }
            return false;
        }

        @Override
        public OperatorPlan reportChanges() {
            return currentPlan;
        }

        @Override
        public void transform(OperatorPlan matched) throws FrontendException {

            LOForEach forEach = (LOForEach) matched.getSources().get(0);
            LogicalPlan innerPlan = forEach.getInnerPlan();

            // Get LOSort and LOLimit from innerPlan
            Iterator<Operator> it = innerPlan.getOperators();

            LOLimit limit = null;
            LOSort sort = null;
            while(it.hasNext()) {
                Operator op = it.next();
                if (op instanceof LOLimit) {
                    List<Operator> preds = innerPlan.getPredecessors(op);
                    // Limit should always have exactly 1 predecessor
                    if (preds.get(0) instanceof LOSort) {
                        limit = (LOLimit) op;
                        sort = (LOSort) (preds.get(0));
                        break;
                    }
                }
            }

            // set limit in LOSort, and remove LOLimit
            sort.setLimit(limit.getLimit());
            innerPlan.removeAndReconnect(limit);
        }
    }
}
