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

import java.util.HashMap;
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
 * This visitor visits the SparkPlan and does the following for each
 * SparkOperator - visits the POPackage in the plan and finds the corresponding
 * POLocalRearrange(s). It then annotates the POPackage with information about
 * which columns in the "value" are present in the "key" and will need to
 * stitched in to the "value"
 */
public class SparkPOPackageAnnotator extends SparkOpPlanVisitor {
	public SparkPOPackageAnnotator(SparkOperPlan plan) {
		super(plan, new DepthFirstWalker<SparkOperator, SparkOperPlan>(plan));
	}

	@Override
	public void visitSparkOp(SparkOperator sparkOp) throws VisitorException {
		if (!sparkOp.physicalPlan.isEmpty()) {
			PackageDiscoverer pkgDiscoverer = new PackageDiscoverer(
					sparkOp.physicalPlan);
			pkgDiscoverer.visit();
			POPackage pkg = pkgDiscoverer.getPkg();
			if (pkg != null) {
				handlePackage(sparkOp, pkg);
			}
		}
	}

	private void handlePackage(SparkOperator pkgSparkOp, POPackage pkg)
			throws VisitorException {
		int lrFound = 0;
		List<SparkOperator> predecessors = this.mPlan
				.getPredecessors(pkgSparkOp);
		if (predecessors != null && predecessors.size() > 0) {
			for (SparkOperator pred : predecessors) {
				lrFound += patchPackage(pred, pkgSparkOp, pkg);
				if (lrFound == pkg.getNumInps()) {
					break;
				}
			}
		}
		if (lrFound != pkg.getNumInps()) {
			int errCode = 2086;
			String msg = "Unexpected problem during optimization. Could not find all LocalRearrange operators.";
			throw new OptimizerException(msg, errCode, PigException.BUG);
		}
	}

	private int patchPackage(SparkOperator pred, SparkOperator pkgSparkOp,
			POPackage pkg) throws VisitorException {
		LoRearrangeDiscoverer lrDiscoverer = new LoRearrangeDiscoverer(
				pred.physicalPlan, pkg);
		lrDiscoverer.visit();
		// let our caller know if we managed to patch
		// the package
		return lrDiscoverer.getLoRearrangeFound();
	}

	static class PackageDiscoverer extends PhyPlanVisitor {

		private POPackage pkg;

		public PackageDiscoverer(PhysicalPlan plan) {
			super(plan, new DepthFirstWalker<PhysicalOperator, PhysicalPlan>(
					plan));
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

	static class LoRearrangeDiscoverer extends PhyPlanVisitor {

		private int loRearrangeFound = 0;
		private POPackage pkg;

		public LoRearrangeDiscoverer(PhysicalPlan plan, POPackage pkg) {
			super(plan, new DepthFirstWalker<PhysicalOperator, PhysicalPlan>(
					plan));
			this.pkg = pkg;
		}

		@Override
		public void visitLocalRearrange(POLocalRearrange lrearrange)
				throws VisitorException {
			loRearrangeFound++;
			Map<Integer, Pair<Boolean, Map<Integer, Integer>>> keyInfo;

			if (pkg.getPkgr() instanceof LitePackager) {
				if (lrearrange.getIndex() != 0) {
					// Throw some exception here
					throw new RuntimeException(
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
