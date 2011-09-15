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
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POMergeCogroup;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POMergeJoin;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPartialAgg;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStream;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POCollectedGroup;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.VisitorException;

/**
 * This visitor visits the MRPlan and does the following
 * for each MROper: If the map plan or the reduce plan of the MROper has
 *  an end of all input flag present in it, this marks in the MROper whether the map 
 * has an end of all input flag set or if the reduce has an end of all input flag set.
 *  
 */
public class EndOfAllInputSetter extends MROpPlanVisitor {

    /**
     * @param plan MR plan to visit
     */
    public EndOfAllInputSetter(MROperPlan plan) {
        super(plan, new DepthFirstWalker<MapReduceOper, MROperPlan>(plan));
    }

    @Override
    public void visitMROp(MapReduceOper mr) throws VisitorException {
        
        EndOfAllInputChecker checker = new EndOfAllInputChecker(mr.mapPlan);
        checker.visit();
        if(checker.isEndOfAllInputPresent()) {
            mr.setEndOfAllInputInMap(true);            
        }
        
        checker = new EndOfAllInputChecker(mr.reducePlan);
        checker.visit();
        if(checker.isEndOfAllInputPresent()) {
            mr.setEndOfAllInputInReduce(true);            
        }      
        
    }

    static class EndOfAllInputChecker extends PhyPlanVisitor {
        
        private boolean endOfAllInputFlag = false;
        public EndOfAllInputChecker(PhysicalPlan plan) {
            super(plan, new DepthFirstWalker<PhysicalOperator, PhysicalPlan>(plan));
        }
        
        /* (non-Javadoc)
         * @see org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor#visitStream(org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStream)
         */
        @Override
        public void visitStream(POStream stream) throws VisitorException {
            // stream present
            endOfAllInputFlag = true;
        }
        
        @Override
        public void visitMergeJoin(POMergeJoin join) throws VisitorException {
            // merge join present
            endOfAllInputFlag = true;
        }
       
        @Override
        public void visitCollectedGroup(POCollectedGroup mg) throws VisitorException {
            // map side group present
            endOfAllInputFlag = true;
        }

        @Override
        public void visitMergeCoGroup(POMergeCogroup mergeCoGrp)
                throws VisitorException {
            endOfAllInputFlag = true;
        }

        @Override
        public void visitPartialAgg(POPartialAgg partAgg){
            endOfAllInputFlag = true;
        }

        /**
         * @return if end of all input is present
         */
        public boolean isEndOfAllInputPresent() {
            return endOfAllInputFlag;
        }
    }
}

