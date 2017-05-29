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
package org.apache.pig.backend.hadoop.executionengine.spark.plan;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.PigException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.LitePackager;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POGlobalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.plan.optimizer.OptimizerException;
import org.apache.pig.impl.util.Pair;

/**
 * This visitor visits the SparkPlan and does the following for each
 * SparkOperator - visits the POPackage in the plan and finds the corresponding
 * POLocalRearrange(s). It then annotates the POPackage with information about
 * which columns in the "value" are present in the "key" and will need to
 * stitched in to the "value"
 */
public class SparkPOPackageAnnotator extends SparkOpPlanVisitor {
    private static final Log LOG = LogFactory.getLog(SparkPOPackageAnnotator.class);

    public SparkPOPackageAnnotator(SparkOperPlan plan) {
        super(plan, new DepthFirstWalker<SparkOperator, SparkOperPlan>(plan));
    }

    @Override
    public void visitSparkOp(SparkOperator sparkOp) throws VisitorException {
        if (!sparkOp.physicalPlan.isEmpty()) {
            PackageDiscoverer pkgDiscoverer = new PackageDiscoverer(
                    sparkOp.physicalPlan);
            pkgDiscoverer.visit();
        }
    }

    static class PackageDiscoverer extends PhyPlanVisitor {
        private POPackage pkg;
        private PhysicalPlan plan;

        public PackageDiscoverer(PhysicalPlan plan) {
            super(plan, new DepthFirstWalker<PhysicalOperator, PhysicalPlan>(
                    plan));
            this.plan = plan;
        }

        @Override
        public void visitPackage(POPackage pkg) throws VisitorException {
            this.pkg = pkg;

            // Find POLocalRearrange(s) corresponding to this POPackage
            PhysicalOperator graOp = plan.getPredecessors(pkg).get(0);
            if (! (graOp instanceof POGlobalRearrange)) {
                  throw new OptimizerException("Package operator is not preceded by " +
                        "GlobalRearrange operator in Spark Plan", 2087, PigException.BUG);
            }

            List<PhysicalOperator> lraOps = plan.getPredecessors(graOp);
            if (pkg.getNumInps() != lraOps.size()) {
          throw new OptimizerException("Unexpected problem during optimization. " +
                            "Could not find all LocalRearrange operators. Expected " + pkg.getNumInps() +
                        ". Got " + lraOps.size() + ".", 2086, PigException.BUG);
            }
            Collections.sort(lraOps);
            for (PhysicalOperator op : lraOps) {
                if (! (op instanceof POLocalRearrange)) {
                    throw new OptimizerException("GlobalRearrange operator can only be preceded by " +
                            "LocalRearrange operator(s) in Spark Plan", 2087, PigException.BUG);
                }
                annotatePkgWithLRA((POLocalRearrange)op);
            }
        };

        private void annotatePkgWithLRA(POLocalRearrange lrearrange)
                throws VisitorException {

            Map<Integer, Pair<Boolean, Map<Integer, Integer>>> keyInfo;
            if (LOG.isDebugEnabled())
                 LOG.debug("Annotating package " + pkg + " with localrearrange operator "
               + lrearrange + " with index " + lrearrange.getIndex());

            if (pkg.getPkgr() instanceof LitePackager) {
                if (lrearrange.getIndex() != 0) {
                    throw new VisitorException(
                            "POLocalRearrange for POPackageLite cannot have index other than 0, but has index - "
                                    + lrearrange.getIndex());
                }
            }

            // annotate the package with information from the LORearrange
            // update the keyInfo information if already present in the
            // POPackage
            keyInfo = pkg.getPkgr().getKeyInfo();
            if (keyInfo == null)
                keyInfo = new HashMap<Integer, Pair<Boolean, Map<Integer, Integer>>>();

            if (keyInfo.get(Integer.valueOf(lrearrange.getIndex())) != null) {
                // something is wrong - we should not be getting key info
                // for the same index from two different Local Rearranges
                int errCode = 2087;
                String msg = "Unexpected problem during optimization."
                        + " Found index:" + lrearrange.getIndex()
                        + " in multiple LocalRearrange operators.";
                throw new OptimizerException(msg, errCode, PigException.BUG);

            }
            keyInfo.put(
                    Integer.valueOf(lrearrange.getIndex()),
                    new Pair<Boolean, Map<Integer, Integer>>(lrearrange
                            .isProjectStar(), lrearrange.getProjectedColsMap()));
            if (LOG.isDebugEnabled())
          LOG.debug("KeyInfo for packager for package operator " + pkg + " is "
              + keyInfo );
            pkg.getPkgr().setKeyInfo(keyInfo);
            pkg.getPkgr().setKeyTuple(lrearrange.isKeyTuple());
            pkg.getPkgr().setKeyCompound(lrearrange.isKeyCompound());
        }
    }
}