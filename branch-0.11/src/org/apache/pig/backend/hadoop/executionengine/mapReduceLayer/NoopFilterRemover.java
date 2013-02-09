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
package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer;

import java.util.List;
import java.util.LinkedList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROpPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFilter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.ConstantExpression;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;

/**
 * For historical reasons splits will always produce filters that pass
 * everything through unchanged. This optimizer removes these. This
 * optimizer is also a pre-req for the NoopStoreRemover.
 *
 * The condition we look for is POFilters with a constant boolean
 * (true) expression as it's plan. 
 */
class NoopFilterRemover extends MROpPlanVisitor {
    
    private Log log = LogFactory.getLog(getClass());
    
    NoopFilterRemover(MROperPlan plan) {
        super(plan, new DependencyOrderWalker<MapReduceOper, MROperPlan>(plan));
    }

    @Override
    public void visitMROp(MapReduceOper mr) throws VisitorException {
        new PhysicalRemover(mr.mapPlan).visit();
        new PhysicalRemover(mr.combinePlan).visit();
        new PhysicalRemover(mr.reducePlan).visit();
    }   

    private class PhysicalRemover extends PhyPlanVisitor {
        private List< Pair<POFilter, PhysicalPlan> > removalQ;
        
        PhysicalRemover(PhysicalPlan plan) {
            super(plan, new DependencyOrderWalker<PhysicalOperator, PhysicalPlan>(plan));
            removalQ = new LinkedList< Pair<POFilter, PhysicalPlan> >();
        }

        @Override
        public void visit() throws VisitorException {
            super.visit();
            for (Pair<POFilter, PhysicalPlan> pair: removalQ) {
                removeFilter(pair.first, pair.second);
            }
            removalQ.clear();
        }
        
        @Override
        public void visitFilter(POFilter fl) throws VisitorException {
            PhysicalPlan filterPlan = fl.getPlan();
            if (filterPlan.size() == 1) {
                PhysicalOperator op = filterPlan.getRoots().get(0);
                if (op instanceof ConstantExpression) {
                    ConstantExpression exp = (ConstantExpression)op;
                    Object value = exp.getValue();
                    if (value instanceof Boolean) {
                        Boolean filterValue = (Boolean)value;
                        if (filterValue) {
                            removalQ.add(new Pair<POFilter, PhysicalPlan>(fl, mCurrentWalker.getPlan()));
                        }
                    }
                }
            }
        }
        
        private void removeFilter(POFilter filter, PhysicalPlan plan) {
            if (plan.size() > 1) {
                try {
                    List<PhysicalOperator> fInputs = filter.getInputs();
                    List<PhysicalOperator> sucs = plan.getSuccessors(filter);

                    plan.removeAndReconnect(filter);
                    if(sucs!=null && sucs.size()!=0){
                        for (PhysicalOperator suc : sucs) {
                            suc.setInputs(fInputs);
                        }
                    }
                } catch (PlanException pe) {
                    log.info("Couldn't remove a filter in optimizer: "+pe.getMessage());
                }
            }
        }
    }
}
