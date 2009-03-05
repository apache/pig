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

import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStream;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.VisitorException;

/**
 * This visitor visits the MRPlan and does the following
 * for each MROper
 *  - If the map plan or the reduce plan of the MROper has
 *  a POStream in it, this marks in the MROper whether the map 
 * has a POStream or if the reduce has a POStream.
 *  
 */
public class MRStreamHandler extends MROpPlanVisitor {

    /**
     * @param plan MR plan to visit
     */
    public MRStreamHandler(MROperPlan plan) {
        super(plan, new DepthFirstWalker<MapReduceOper, MROperPlan>(plan));
    }

    @Override
    public void visitMROp(MapReduceOper mr) throws VisitorException {
        
        StreamChecker checker = new StreamChecker(mr.mapPlan);
        checker.visit();
        if(checker.isStreamPresent()) {
            mr.setStreamInMap(true);            
        }
        
        checker = new StreamChecker(mr.reducePlan);
        checker.visit();
        if(checker.isStreamPresent()) {
            mr.setStreamInReduce(true);            
        }      
        
    }

    class StreamChecker extends PhyPlanVisitor {
        
        private boolean streamPresent = false;
        public StreamChecker(PhysicalPlan plan) {
            super(plan, new DepthFirstWalker<PhysicalOperator, PhysicalPlan>(plan));
        }
        
        /* (non-Javadoc)
         * @see org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor#visitStream(org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStream)
         */
        @Override
        public void visitStream(POStream stream) throws VisitorException {
            // stream present
            streamPresent = true;
        }

        /**
         * @return if stream is present
         */
        public boolean isStreamPresent() {
            return streamPresent;
        }
    }
}

