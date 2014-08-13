/**
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

package org.apache.pig.backend.hadoop.executionengine.tez;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.pig.PigException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.LitePackager;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.plan.optimizer.OptimizerException;
import org.apache.pig.impl.util.Pair;

/**
 * A port of the POPackageAnnotator from MR to Tez.
 *
 */
public class TezPOPackageAnnotator extends TezOpPlanVisitor {

    /**
     * @param plan Tez plan to visit
     */
    public TezPOPackageAnnotator(TezOperPlan plan) {
        super(plan, new DepthFirstWalker<TezOperator, TezOperPlan>(plan));
    }

    @Override
    public void visitTezOp(TezOperator tezOp) throws VisitorException {
        if(!tezOp.plan.isEmpty()) {
            PackageDiscoverer pkgDiscoverer = new PackageDiscoverer(tezOp.plan);
            pkgDiscoverer.visit();
            POPackage pkg = pkgDiscoverer.getPkg();
            if(pkg != null) {
                handlePackage(tezOp, pkg);
            }
        }
    }

    private void handlePackage(TezOperator pkgTezOp, POPackage pkg) throws VisitorException {
        // the LocalRearrange(s) must be in the plan of a predecessor tez op
        int lrFound = 0;
        List<TezOperator> preds = this.mPlan.getPredecessors(pkgTezOp);
        for (Iterator<TezOperator> it = preds.iterator(); it.hasNext();) {
            TezOperator predTezOp = it.next();
            if (predTezOp.isVertexGroup()) {
                // Just get one of the inputs to vertex group
                predTezOp = getPlan().getOperator(predTezOp.getVertexGroupMembers().get(0));
            }
            lrFound += patchPackage(predTezOp, pkgTezOp, pkg);
            if(lrFound == pkg.getNumInps()) {
                break;
            }
        }

        if(lrFound != pkg.getNumInps()) {
            int errCode = 2086;
            String msg = "Unexpected problem during optimization. Could not find all LocalRearrange operators.";
            throw new OptimizerException(msg, errCode, PigException.BUG);
        }
    }

    private int patchPackage(TezOperator predTezOp, TezOperator pkgTezOp, POPackage pkg) throws VisitorException {
        LoRearrangeDiscoverer lrDiscoverer = new LoRearrangeDiscoverer(predTezOp.plan, pkgTezOp, pkg);
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

        @Override
        public void visitPackage(POPackage pkg) throws VisitorException {
            this.pkg = pkg;
        };

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
        private TezOperator pkgTezOp;
        private POPackage pkg;

        public LoRearrangeDiscoverer(PhysicalPlan plan, TezOperator pkgTezOp, POPackage pkg) {
            super(plan, new DepthFirstWalker<PhysicalOperator, PhysicalPlan>(plan));
            this.pkgTezOp = pkgTezOp;
            this.pkg = pkg;
        }

        @Override
        public void visitLocalRearrange(POLocalRearrange lrearrange) throws VisitorException {
            POLocalRearrangeTez lr = (POLocalRearrangeTez) lrearrange;
            if (!(lr.isConnectedToPackage() && lr.getOutputKey().equals(pkgTezOp.getOperatorKey().toString()))) {
                return;
            }
            loRearrangeFound++;
            Map<Integer,Pair<Boolean, Map<Integer, Integer>>> keyInfo;

            if (pkg.getPkgr() instanceof LitePackager) {
                if(lrearrange.getIndex() != 0) {
                    // Throw some exception here
                    throw new RuntimeException("POLocalRearrange for POPackageLite cannot have index other than 0, but has index - "+lrearrange.getIndex());
                }
            }

            // annotate the package with information from the LORearrange
            // update the keyInfo information if already present in the POPackage
            keyInfo = pkg.getPkgr().getKeyInfo();
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
            pkg.getPkgr().setKeyInfo(keyInfo);
            pkg.getPkgr().setKeyTuple(lrearrange.isKeyTuple());
            pkg.getPkgr().setKeyCompound(lrearrange.isKeyCompound());
        }

        /**
         * @return the loRearrangeFound
         */
        public int getLoRearrangeFound() {
            return loRearrangeFound;
        }

    }
}
