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
/**
 * 
 */
package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.pig.PigException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROpPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.VisitorException;

/**
 * A visitor to figure out the type of the key for the map plan
 * this is needed when the key is null to create
 * an appropriate NullableXXXWritable object
 */
public class KeyTypeDiscoveryVisitor extends MROpPlanVisitor {

    
    /* (non-Javadoc)
     * @see org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor#visitLocalRearrange(org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange)
     */
    /**
     * @param plan The MROperPlan to visit to discover keyType
     */
    public KeyTypeDiscoveryVisitor(MROperPlan plan) {
        super(plan, new DepthFirstWalker<MapReduceOper, MROperPlan>(plan));
    }
    
    @Override
    public void visitMROp(MapReduceOper mr) throws VisitorException {
        if(mr instanceof NativeMapReduceOper) {
            return;
        }
        if (mr.useSecondaryKey) {
            mr.mapKeyType = DataType.TUPLE;
            return;
        }
        boolean foundKeyType = false;
        PhyPlanKeyTypeVisitor kvisitor = new PhyPlanKeyTypeVisitor(mr.mapPlan, mr);
        kvisitor.visit();
        if(!kvisitor.foundKeyType) {
            // look for the key type from a POLocalRearrange in the previous reduce
            List<MapReduceOper> preds = mPlan.getPredecessors(mr);
            // if there are no predecessors, then we probably are in a 
            // simple load-store script - there is no way to figure out
            // the key type in this case which probably means we don't need
            // to figure it out
            if(preds != null) {
                Map<Byte, Integer> seen = new HashMap<Byte, Integer>();
                for (MapReduceOper pred : preds) {
                    PhyPlanKeyTypeVisitor visitor = new PhyPlanKeyTypeVisitor(pred.reducePlan, mr);
                    visitor.visit();
                    foundKeyType |= visitor.foundKeyType;
                    byte type = mr.mapKeyType;
                    seen.put(type, 1);
                }
                if(seen.size() > 1) {
                    // throw exception since we should get the same key type from all predecessors
                    int errorCode = 2119;
                    String message = "Internal Error: Found multiple data types for map key";
                    throw new VisitorException(message, errorCode, PigException.BUG);
                }
                // if we were not able to find the key and 
                // if there is a map and reduce phase, then the
                // map would need to send a valid key object and this
                // can be an issue when the key is null - so error out here!
                // if the reduce phase is empty, then this is a map only job
                // which will not need a key object -
                // IMPORTANT NOTE: THIS RELIES ON THE FACT THAT CURRENTLY
                // IN PigMapOnly.collect(), null IS SENT IN THE collect() CALL
                // FOR THE KEY - IF THAT CHANGES, THEN THIS CODE HERE MAY NEED TO
                // CHANGE!
                if(!foundKeyType && !mr.reducePlan.isEmpty()) {
                    // throw exception since we were not able to determine key type!
                    int errorCode = 2120;
                    String message = "Internal Error: Unable to determine data type for map key";
                    throw new VisitorException(message, errorCode, PigException.BUG);
                }
            }
        }
    }
    
    static class PhyPlanKeyTypeVisitor extends PhyPlanVisitor {
        
        private MapReduceOper mro;
        private boolean foundKeyType = false;
        
        public PhyPlanKeyTypeVisitor(PhysicalPlan plan, MapReduceOper mro) {
            super(plan, new DepthFirstWalker<PhysicalOperator, PhysicalPlan>(plan));
            this.mro = mro;
        }
        
        @Override
        public void visitLocalRearrange(POLocalRearrange lr)
                throws VisitorException {
            this.mro.mapKeyType = lr.getKeyType();
            foundKeyType = true;
        }
    }

}
