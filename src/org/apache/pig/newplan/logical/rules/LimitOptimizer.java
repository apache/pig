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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.relational.LOCogroup;
import org.apache.pig.newplan.logical.relational.LOCross;
import org.apache.pig.newplan.logical.relational.LODistinct;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOGenerate;
import org.apache.pig.newplan.logical.relational.LOJoin;
import org.apache.pig.newplan.logical.relational.LOLimit;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LOSort;
import org.apache.pig.newplan.logical.relational.LOSplit;
import org.apache.pig.newplan.logical.relational.LOSplitOutput;
import org.apache.pig.newplan.logical.relational.LOUnion;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.optimizer.Rule;
import org.apache.pig.newplan.optimizer.Transformer;

public class LimitOptimizer extends Rule {

    public LimitOptimizer(String name) {
        super(name, false);
    }

    @Override
    protected OperatorPlan buildPattern() {
        LogicalPlan plan = new LogicalPlan();
        LogicalRelationalOperator limit = new LOLimit(plan, 0);
        plan.add(limit);
        return plan;
    }

    @Override
    public Transformer getNewTransformer() {
        return new OptimizeLimitTransformer();
    }

    public class OptimizeLimitTransformer extends Transformer {

        @Override
        public boolean check(OperatorPlan matched) {
            LOLimit limit = (LOLimit) matched.getSources().get(0);
            // Match each foreach.
            List<Operator> preds = currentPlan.getPredecessors(limit);
            if (preds == null || preds.size() == 0)
                return false;

            Operator pred = preds.get(0);

            // Limit cannot be pushed up
            if (pred instanceof LOCogroup || pred instanceof LOFilter
                    || pred instanceof LOSplit || pred instanceof LODistinct || pred instanceof LOJoin) {
                return false;
            }

            // Limit cannot be pushed in front of ForEach if it has a flatten
            if (pred instanceof LOForEach) {
                LOForEach foreach = (LOForEach) pred;
                LogicalPlan innerPlan = foreach.getInnerPlan();
                Iterator<Operator> it = innerPlan.getOperators();
                while (it.hasNext()) {
                    Operator op = it.next();
                    if (op instanceof LOGenerate) {
                        LOGenerate gen = (LOGenerate) op;
                        boolean[] flattenFlags = gen.getFlattenFlags();
                        if (flattenFlags != null) {
                            for (boolean flatten : flattenFlags) {
                                if (flatten)
                                    return false;
                            }
                        }
                    }
                }
            }
            return true;
        }

        @Override
        public OperatorPlan reportChanges() {
            return currentPlan;
        }

        @Override
        public void transform(OperatorPlan matched) throws FrontendException {

            LOLimit limit = (LOLimit) matched.getSources().get(0);

            // Find the next foreach operator.
            List<Operator> preds = currentPlan.getPredecessors(limit);
            Operator pred = preds.get(0);

            if (pred instanceof LOForEach) {
                // We can safely move LOLimit up
                // Get operator before LOForEach
                Operator prepredecessor = currentPlan.getPredecessors(pred)
                    .get(0);
                
                List<Operator> softPrepredecessors=null;
                // get a clone of softPrepredecessors to avoid ConcurrentModificationException
                if (currentPlan.getSoftLinkPredecessors(limit)!=null) {
                    softPrepredecessors=new ArrayList<Operator>(
                            currentPlan.getSoftLinkPredecessors(limit));
                }
                if (softPrepredecessors!=null) {
                    for (Operator op : softPrepredecessors) {
                        currentPlan.removeSoftLink(op, limit);
                    }
                }
                currentPlan.removeAndReconnect(limit);
                currentPlan.insertBetween(prepredecessor, limit, pred);
                if (softPrepredecessors!=null) {
                    for (Operator op : softPrepredecessors) {
                        currentPlan.createSoftLink(op, limit);
                    }
                }
            } else if (limit.getLimitPlan() == null) {
                // TODO selectively enable optimizations for variable limit
                if (pred instanceof LOCross || pred instanceof LOUnion) {
                // Limit can be duplicated, and the new instance pushed in front
                // of an operator for the following operators
                // (that is, if you have X->limit, you can transform that to
                // limit->X->limit):
                LOLimit newLimit = null;
                List<Operator> nodesToProcess = new ArrayList<Operator>();
                for (Operator prepredecessor : currentPlan
                        .getPredecessors(pred))
                    nodesToProcess.add(prepredecessor);
                for (Operator prepredecessor : nodesToProcess) {
                    if (prepredecessor instanceof LOLimit) {
                        LOLimit l = (LOLimit) prepredecessor;
                        l.setLimit(l.getLimit() < limit.getLimit() ? l
                                .getLimit() : limit.getLimit());
                    } else {
                        newLimit = new LOLimit((LogicalPlan) currentPlan, limit
                                .getLimit());
                        currentPlan.insertBetween(prepredecessor, newLimit, pred);
                    }
                }
            } else if (pred instanceof LOSort) {
                LOSort sort = (LOSort) pred;
                if (sort.getLimit() == -1)
                    sort.setLimit(limit.getLimit());
                else
                    sort.setLimit(sort.getLimit() < limit.getLimit() ? sort
                            .getLimit() : limit.getLimit());

                // remove the limit
                currentPlan.removeAndReconnect(limit);
            } else if (pred instanceof LOLoad) {
                // Push limit to load
                LOLoad load = (LOLoad) pred;
                if (load.getLimit() == -1)
                    load.setLimit(limit.getLimit());
                else
                    load.setLimit(load.getLimit() < limit.getLimit() ? load
                            .getLimit() : limit.getLimit());
            } else if (pred instanceof LOLimit) {
                // Limit is merged into another LOLimit
                LOLimit beforeLimit = (LOLimit) pred;
                beforeLimit
                        .setLimit(beforeLimit.getLimit() < limit.getLimit() ? beforeLimit
                                .getLimit()
                                : limit.getLimit());
                // remove the limit
                currentPlan.removeAndReconnect(limit);
            } else if (pred instanceof LOSplitOutput) {
                // Limit and OrderBy (LOSort) can be separated by split
                List<Operator> grandparants = currentPlan.getPredecessors(pred);
                // After insertion of splitters, any node in the plan can
                // have at most one predecessor
                if (grandparants != null && grandparants.size() != 0
                        && grandparants.get(0) instanceof LOSplit) {
                    List<Operator> greatGrandparants = currentPlan
                            .getPredecessors(grandparants.get(0));
                    if (greatGrandparants != null
                            && greatGrandparants.size() != 0
                            && greatGrandparants.get(0) instanceof LOSort) {
                        LOSort sort = (LOSort) greatGrandparants.get(0);
                        LOSort newSort = LOSort.createCopy(sort);
                        newSort.setLimit(limit.getLimit());

                        currentPlan.replace(limit, newSort);
                    }
                }
            }
        }
    }
}
}
