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

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.pig.PigException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POJoinPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackageLite;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POCombinerPackage;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.plan.optimizer.OptimizerException;
import org.apache.pig.impl.util.Pair;

/**
 * This visitor visits the MRPlan and does the following
 * for each MROper
 *  - visits the POPackage in the reduce plan and finds the corresponding
 *  POLocalRearrange(s) (either in the map plan of the same oper OR
 *  reduce plan of predecessor MROper). It then annotates the POPackage
 *  with information about which columns in the "value" are present in the
 *  "key" and will need to stitched in to the "value"
 */
public class POPackageAnnotator extends MROpPlanVisitor {

    /**
     * @param plan MR plan to visit
     */
    public POPackageAnnotator(MROperPlan plan) {
        super(plan, new DepthFirstWalker<MapReduceOper, MROperPlan>(plan));
    }

    @Override
    public void visitMROp(MapReduceOper mr) throws VisitorException {
        
        // POPackage OR POJoinPackage could be present in the combine plan
        // OR in the reduce plan. POPostCombinerPackage could
        // be present only in the reduce plan. Search in these two
        // plans accordingly
        if(!mr.combinePlan.isEmpty()) {
            PackageDiscoverer pkgDiscoverer = new PackageDiscoverer(mr.combinePlan);
            pkgDiscoverer.visit();
            POPackage pkg = pkgDiscoverer.getPkg();
            if(pkg != null) {
                handlePackage(mr, pkg);
            }   
        }
        
        if(!mr.reducePlan.isEmpty()) {
            PackageDiscoverer pkgDiscoverer = new PackageDiscoverer(mr.reducePlan);
            pkgDiscoverer.visit();
            POPackage pkg = pkgDiscoverer.getPkg();
            if(pkg != null) {
                // if the POPackage is actually a POPostCombinerPackage, then we should
                // just look for the corresponding LocalRearrange(s) in the combine plan
                if(pkg instanceof POCombinerPackage) {
                    if(patchPackage(mr.combinePlan, pkg) != pkg.getNumInps()) {
                        int errCode = 2085;
                        String msg = "Unexpected problem during optimization." +
                        " Could not find LocalRearrange in combine plan.";
                        throw new OptimizerException(msg, errCode, PigException.BUG);
                    }
                } else {
                    handlePackage(mr, pkg);
                }
            }
        }
        
    }
    
    private void handlePackage(MapReduceOper mr, POPackage pkg) throws VisitorException {
        // the LocalRearrange(s) could either be in the map of this MapReduceOper
        // OR in the reduce of predecessor MapReduceOpers
        int lrFound = 0;
        
        lrFound = patchPackage(mr.mapPlan, pkg);
        if(lrFound != pkg.getNumInps()) {
            // we did not find the LocalRearrange(s) in the map plan
            // let's look in the predecessors
            List<MapReduceOper> preds = this.mPlan.getPredecessors(mr);
            for (Iterator<MapReduceOper> it = preds.iterator(); it.hasNext();) {
                MapReduceOper mrOper = it.next();
                if (mrOper.isLimitOnly() && !mPlan.getPredecessors(mrOper).get(0).isGlobalSort())
                    mrOper = this.mPlan.getPredecessors(mrOper).get(0);
                lrFound += patchPackage(mrOper.reducePlan, pkg);
                if(lrFound == pkg.getNumInps()) {
                    break;
                }     
            }
        }
        if(lrFound != pkg.getNumInps()) {
            int errCode = 2086;
            String msg = "Unexpected problem during optimization. Could not find all LocalRearrange operators.";
            throw new OptimizerException(msg, errCode, PigException.BUG);
        }
    }

    private int patchPackage(PhysicalPlan plan, POPackage pkg) throws VisitorException {
        LoRearrangeDiscoverer lrDiscoverer = new LoRearrangeDiscoverer(plan, pkg);
        lrDiscoverer.visit();
        // let our caller know if we managed to patch
        // the package
        return lrDiscoverer.getLoRearrangeFound();
    }
    
    /**
     * Simple visitor of the "Reduce" physical plan
     * which will get a reference to the POPacakge
     * present in the plan
     */
    static class PackageDiscoverer extends PhyPlanVisitor {

        private POPackage pkg;
        
        public PackageDiscoverer(PhysicalPlan plan) {
            super(plan, new DepthFirstWalker<PhysicalOperator, PhysicalPlan>(plan));
        }
        
        /* (non-Javadoc)
         * @see org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor#visitStream(org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStream)
         */
        @Override
        public void visitPackage(POPackage pkg) throws VisitorException {
            this.pkg = pkg;
        };
        
        /* (non-Javadoc)
         * @see org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor#visitJoinPackage(org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POJoinPackage)
         */
        @Override
        public void visitJoinPackage(POJoinPackage joinPackage)
                throws VisitorException {
            this.pkg = joinPackage;
        }
        
        /* (non-Javadoc)
         * @see org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor#visitCombinerPackage(org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPostCombinerPackage)
         */
        @Override
        public void visitCombinerPackage(POCombinerPackage pkg)
                throws VisitorException {
            this.pkg = pkg;
        }

        /**
         * @return the pkg
         */
        public POPackage getPkg() {
            return pkg;
        }
        
    }
    
    /**
     * Physical Plan visitor which tries to get the
     * LocalRearrange(s) present in the plan (if any) and
     * annotate the POPackage given to it with the information
     * in the LocalRearrange (regarding columns in the "value"
     * present in the "key")
     */
    static class LoRearrangeDiscoverer extends PhyPlanVisitor {
        
        private int loRearrangeFound = 0;
        private POPackage pkg;
        
        public LoRearrangeDiscoverer(PhysicalPlan plan, POPackage pkg) {
            super(plan, new DepthFirstWalker<PhysicalOperator, PhysicalPlan>(plan));
            this.pkg = pkg;
        }
        
        /* (non-Javadoc)
         * @see org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor#visitStream(org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStream)
         */
        @Override
        public void visitLocalRearrange(POLocalRearrange lrearrange) throws VisitorException {
            loRearrangeFound++;
            Map<Integer,Pair<Boolean, Map<Integer, Integer>>> keyInfo;

            if (pkg instanceof POPackageLite) {
                if(lrearrange.getIndex() != 0) {
                    // Throw some exception here
                    throw new RuntimeException("POLocalRearrange for POPackageLite cannot have index other than 0, but has index - "+lrearrange.getIndex());
                }
            }

            // annotate the package with information from the LORearrange
            // update the keyInfo information if already present in the POPackage
            keyInfo = pkg.getKeyInfo();
            if(keyInfo == null)
                keyInfo = new HashMap<Integer, Pair<Boolean, Map<Integer, Integer>>>();
            
            if(keyInfo.get(Integer.valueOf(lrearrange.getIndex())) != null) {
                // something is wrong - we should not be getting key info 
                // for the same index from two different Local Rearranges
                int errCode = 2087;
                String msg = "Unexpected problem during optimization." +
                " Found index:" + lrearrange.getIndex() + 
                " in multiple LocalRearrange operators.";
                throw new OptimizerException(msg, errCode, PigException.BUG);
                
            }
            keyInfo.put(Integer.valueOf(lrearrange.getIndex()), 
                new Pair<Boolean, Map<Integer, Integer>>(
                        lrearrange.isProjectStar(), lrearrange.getProjectedColsMap()));
            pkg.setKeyInfo(keyInfo);
            pkg.setKeyTuple(lrearrange.isKeyTuple());
            pkg.setKeyCompound(lrearrange.isKeyCompound());
        }

        /**
         * @return the loRearrangeFound
         */
        public int getLoRearrangeFound() {
            return loRearrangeFound;
        }

    }
}

