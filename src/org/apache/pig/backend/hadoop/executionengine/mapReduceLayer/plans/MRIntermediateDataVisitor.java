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
package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans;

import java.util.HashSet;
import java.util.Set;

import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.VisitorException;

/**
 * An {@link MROpPlanVisitor} that gathers the paths for all
 * intermediate data from a {@link MROperPlan}
 */
public class MRIntermediateDataVisitor extends MROpPlanVisitor {
    private final Set<String> intermediate;

    public MRIntermediateDataVisitor(MROperPlan plan) {
        super(plan, new DepthFirstWalker<MapReduceOper, MROperPlan>(plan));
        this.intermediate = new HashSet<String>();
    }

    /**
     * Get all paths for intermediate data. visit() must be called before this.
     * 
     * @return All intermediate data ElementDescriptors
     */
    public Set<String> getIntermediate() {
        return intermediate;
    }

    class StoreFinder extends PhyPlanVisitor {

        protected StoreFinder(PhysicalPlan plan) {
            super(plan, new DepthFirstWalker<PhysicalOperator, PhysicalPlan>(
                    plan));
        }

        @Override
        public void visitStore(POStore store) {
            if (store.isTmpStore()) {
                intermediate.add(store.getSFile().getFileName());
            }
        }
    }

    @Override
    public void visitMROp(MapReduceOper mr) throws VisitorException {
        if (mr.mapPlan != null && mr.mapPlan.size() > 0) {
            StoreFinder finder = new StoreFinder(mr.mapPlan);
            finder.visit();
        }
        if (mr.combinePlan != null && mr.combinePlan.size() > 0) {
            StoreFinder finder = new StoreFinder(mr.combinePlan);
            finder.visit();
        }
        if (mr.reducePlan != null && mr.reducePlan.size() > 0) {
            StoreFinder finder = new StoreFinder(mr.reducePlan);
            finder.visit();
        }
    }
}
