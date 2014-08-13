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
package org.apache.pig.backend.hadoop.executionengine.tez.optimizers;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.ConstantExpression;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFilter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.backend.hadoop.executionengine.tez.TezOpPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.tez.TezOperPlan;
import org.apache.pig.backend.hadoop.executionengine.tez.TezOperator;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;

/**
 * For historical reasons splits will always produce filters that pass
 * everything through unchanged. This optimizer removes these.
 *
 * The condition we look for is POFilters with a constant boolean
 * (true) expression as it's plan.
 */
public class NoopFilterRemover extends TezOpPlanVisitor {

    private static Log LOG = LogFactory.getLog(NoopFilterRemover.class);

    public NoopFilterRemover(TezOperPlan plan) {
        super(plan, new DependencyOrderWalker<TezOperator, TezOperPlan>(plan));
    }

    @Override
    public void visitTezOp(TezOperator tezOp) throws VisitorException {
        if (tezOp.getSplitParent() == null) {
            return;
        }
        try {
            List<POFilter> filters = PlanHelper.getPhysicalOperators(tezOp.plan, POFilter.class);
            for (POFilter filter : filters) {
                PhysicalPlan filterPlan = filter.getPlan();
                if (filterPlan.size() == 1) {
                    PhysicalOperator fp = filterPlan.getRoots().get(0);
                    if (fp instanceof ConstantExpression) {
                        ConstantExpression exp = (ConstantExpression)fp;
                        Object value = exp.getValue();
                        if (value instanceof Boolean) {
                            Boolean filterValue = (Boolean)value;
                            if (filterValue) {
                                tezOp.plan.removeAndReconnect(filter);
                            }
                        }
                    }
                }
            }
        } catch (PlanException e) {
            throw new VisitorException(e);
        }
    }
}
