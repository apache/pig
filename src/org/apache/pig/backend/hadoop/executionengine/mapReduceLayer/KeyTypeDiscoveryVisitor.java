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

import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROpPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.PlanWalker;
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
        PhyPlanKeyTypeVisitor kvisitor = new PhyPlanKeyTypeVisitor(mr.mapPlan, mr);
        kvisitor.visit();
    }
    
    class PhyPlanKeyTypeVisitor extends PhyPlanVisitor {
        
        private MapReduceOper mro;
        
        public PhyPlanKeyTypeVisitor(PhysicalPlan plan, MapReduceOper mro) {
            super(plan, new DepthFirstWalker<PhysicalOperator, PhysicalPlan>(plan));
            this.mro = mro;
        }
        
        /* (non-Javadoc)
         * @see org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor#visitPackage(org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage)
         */
        @Override
        public void visitPackage(POPackage pkg) throws VisitorException {
            this.mro.mapKeyType = pkg.getKeyType();        
        }
    
    
        @Override
        public void visitLocalRearrange(POLocalRearrange lr)
                throws VisitorException {
            this.mro.mapKeyType = lr.getKeyType();        
        }
    }

}
