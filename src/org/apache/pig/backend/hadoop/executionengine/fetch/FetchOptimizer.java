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
package org.apache.pig.backend.hadoop.executionengine.fetch;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.PigConfiguration;
import org.apache.pig.backend.datastorage.DataStorageException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PhyPlanSetter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POCollectedGroup;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POCounter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POCross;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.PODemux;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.PODistinct;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFRJoin;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POGlobalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POMergeCogroup;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POMergeJoin;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.PONative;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POOptimizedForEach;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPartialAgg;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPartitionRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPreCombinerLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.PORank;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSkewedJoin;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSort;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSplit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStream;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.PigImplConstants;
import org.apache.pig.impl.builtin.SampleLoader;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.Utils;

/**
 * FetchOptimizer determines whether the entire physical plan is fetchable, meaning
 * that the task's result can be directly read (fetched) from the underlying storage
 * rather than creating MR jobs. During the check {@link FetchablePlanVisitor} is used
 * to walk through the plan.
 *
 */
public class FetchOptimizer {
    private static final Log LOG = LogFactory.getLog(FetchOptimizer.class);

    /**
     * Checks whether the fetch is enabled
     *
     * @param pc
     * @return true if fetching is enabled
     */
    public static boolean isFetchEnabled(PigContext pc) {
        return "true".equalsIgnoreCase(
                pc.getProperties().getProperty(PigConfiguration.PIG_OPT_FETCH, "true"));
    }

    /**
     * Visits the plan with {@link FetchablePlanVisitor} and checks whether the
     * plan is fetchable.
     *
     * @param pc PigContext
     * @param pp the physical plan to be examined
     * @return true if the plan is fetchable
     * @throws VisitorException
     */
    public static boolean isPlanFetchable(PigContext pc, PhysicalPlan pp) throws VisitorException {
        if (isEligible(pc, pp)) {
            FetchablePlanVisitor fpv = new FetchablePlanVisitor(pc, pp);
            fpv.visit();
            // Plan is fetchable only if FetchablePlanVisitor returns true AND
            // limit is present in the plan, i.e: limit is pushed up to the loader.
            // Limit is a safeguard. If the input is large, and there is no limit, 
            // fetch optimizer will fetch the entire input to the client. That can be dangerous.
            if (!fpv.isPlanFetchable()) {
                return false;
            }
            for (POLoad load : PlanHelper.getPhysicalOperators(pp, POLoad.class)) {
                if (load.getLimit() == -1) {
                    return false;
                }
            }
            pc.getProperties().setProperty(PigImplConstants.CONVERTED_TO_FETCH, "true");
            init(pp);
            return true;
        }
        return false;
    }

    private static void init(PhysicalPlan pp) throws VisitorException {
        //mark POStream ops 'fetchable'
        LinkedList<POStream> posList = PlanHelper.getPhysicalOperators(pp, POStream.class);
        for (POStream pos : posList) {
            pos.setFetchable(true);
        }
    }

    /**
     * Checks whether the plan fulfills the prerequisites needed for fetching.
     *
     * @param pc PigContext
     * @param pp the physical plan to be examined
     * @return
     */
    private static boolean isEligible(PigContext pc, PhysicalPlan pp) {
        if (!isFetchEnabled(pc)) {
            return false;
        }

        List<PhysicalOperator> roots = pp.getRoots();
        for (PhysicalOperator po : roots) {
            if (!(po instanceof POLoad)) {
                String msg = "Expected physical operator at root is POLoad. Found : "
                        + po.getClass().getCanonicalName() + ". Fetch optimizer will be disabled.";
                LOG.debug(msg);
                return false;
            }
        }

        //consider single leaf jobs only
        int leafSize = pp.getLeaves().size();
        if (pp.getLeaves().size() != 1) {
            LOG.debug("Expected physical plan should have one leaf. Found " + leafSize);
            return false;
        }

        return true;
    }

    /**
     * A plan is considered 'fetchable' if:
     * <pre>
     * - it contains only: LIMIT, FILTER, FOREACH, STREAM, UNION(no implicit SPLIT is allowed)
     * - no STORE
     * - no scalar aliases ({@link org.apache.pig.impl.builtin.ReadScalars ReadScalars})
     * - {@link org.apache.pig.LoadFunc LoadFunc} is not a {@link org.apache.pig.impl.builtin.SampleLoader SampleLoader}
     * </pre>
     */
    private static class FetchablePlanVisitor extends PhyPlanVisitor {

        private boolean planFetchable = true;
        private PigContext pc;

        public FetchablePlanVisitor(PigContext pc, PhysicalPlan plan) {
            super(plan, new DepthFirstWalker<PhysicalOperator, PhysicalPlan>(plan));
            this.pc = pc;
        }

        @Override
        public void visit() throws VisitorException {
            super.visit();
        }

        @Override
        public void visitLoad(POLoad ld) throws VisitorException{
            if (ld.getLoadFunc() instanceof SampleLoader) {
                planFetchable = false;
            }
        }

        @Override
        public void visitStore(POStore st) throws VisitorException{
            String basePathName = st.getSFile().getFileName();

            //plan is fetchable if POStore belongs to EXPLAIN
            if ("fakefile".equals(basePathName)) {
                return;
            }

            //Otherwise check if target storage format equals to the intermediate storage format
            //and its path points to a temporary storage path
            boolean hasTmpStorageClass = st.getStoreFunc().getClass()
                .equals(Utils.getTmpFileStorageClass(pc.getProperties()));

            try {
                boolean hasTmpTargetPath = isTempPath(basePathName);
                if (!(hasTmpStorageClass && hasTmpTargetPath)) {
                    planFetchable = false;
                }
            }
            catch (IOException e) {
                String msg = "Internal error. Could not retrieve temporary store location.";
                throw new VisitorException(msg, e);
            }
        }

        @Override
        public void visitNative(PONative nat) throws VisitorException {
            planFetchable = false;
        }

        @Override
        public void visitCollectedGroup(POCollectedGroup mg) throws VisitorException {
            planFetchable = false;
        }

        @Override
        public void visitLocalRearrange(POLocalRearrange lr) throws VisitorException {
            planFetchable = false;
        }

        @Override
        public void visitGlobalRearrange(POGlobalRearrange gr) throws VisitorException {
            planFetchable = false;
        }

        @Override
        public void visitPackage(POPackage pkg) throws VisitorException {
            planFetchable = false;
        }

        @Override
        public void visitSplit(POSplit spl) throws VisitorException {
            planFetchable = false;
        }

        @Override
        public void visitDemux(PODemux demux) throws VisitorException {
            planFetchable = false;
        }

        @Override
        public void visitCounter(POCounter poCounter) throws VisitorException {
            planFetchable = false;
        }

        @Override
        public void visitRank(PORank rank) throws VisitorException {
            planFetchable = false;
        }

        @Override
        public void visitDistinct(PODistinct distinct) throws VisitorException {
            planFetchable = false;
        }

        @Override
        public void visitSort(POSort sort) throws VisitorException {
            planFetchable = false;
        }

        @Override
        public void visitCross(POCross cross) throws VisitorException {
            planFetchable = false;
        }

        @Override
        public void visitFRJoin(POFRJoin join) throws VisitorException {
            planFetchable = false;
        }

        @Override
        public void visitMergeJoin(POMergeJoin join) throws VisitorException {
            planFetchable = false;
        }

        @Override
        public void visitMergeCoGroup(POMergeCogroup mergeCoGrp) throws VisitorException {
            planFetchable = false;
        }

        @Override
        public void visitSkewedJoin(POSkewedJoin sk) throws VisitorException {
            planFetchable = false;
        }

        @Override
        public void visitPartitionRearrange(POPartitionRearrange pr) throws VisitorException {
            planFetchable = false;
        }

        @Override
        public void visitPOOptimizedForEach(POOptimizedForEach optimizedForEach)
                throws VisitorException {
            planFetchable = false;
        }

        @Override
        public void visitPreCombinerLocalRearrange(
                POPreCombinerLocalRearrange preCombinerLocalRearrange) {
            planFetchable = false;
        }

        @Override
        public void visitPartialAgg(POPartialAgg poPartialAgg) {
            planFetchable = false;
        }

        private boolean isPlanFetchable() {
            return planFetchable;
        }

        private boolean isTempPath(String basePathName) throws DataStorageException {
            String tdir = pc.getProperties().getProperty("pig.temp.dir", "/tmp");
            String tempStore = pc.getDfs().asContainer(tdir + "/temp").toString();
            Matcher matcher = Pattern.compile(tempStore + "-?[0-9]+").matcher(basePathName);
            return matcher.lookingAt();
        }
    }

}
