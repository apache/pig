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
package org.apache.pig.backend.hadoop.executionengine.spark;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.hadoop.executionengine.JobCreationException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PhyPlanSetter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSplit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.RDDConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.operator.NativeSparkOperator;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkOpPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkOperPlan;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkOperator;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.tools.pigstats.spark.SparkPigStats;
import org.apache.pig.tools.pigstats.spark.SparkStatsUtil;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;

import com.google.common.collect.Lists;

public class JobGraphBuilder extends SparkOpPlanVisitor {

    private static final Log LOG = LogFactory.getLog(JobGraphBuilder.class);

    private Map<Class<? extends PhysicalOperator>, RDDConverter> convertMap = null;
    private SparkPigStats sparkStats = null;
    private JavaSparkContext sparkContext = null;
    private JobMetricsListener jobMetricsListener = null;
    private String jobGroupID = null;
    private Set<Integer> seenJobIDs = new HashSet<Integer>();
    private SparkOperPlan sparkPlan = null;
    private Map<OperatorKey, RDD<Tuple>> sparkOpRdds = new HashMap<OperatorKey, RDD<Tuple>>();
    private Map<OperatorKey, RDD<Tuple>> physicalOpRdds = new HashMap<OperatorKey, RDD<Tuple>>();

    public JobGraphBuilder(SparkOperPlan plan, Map<Class<? extends PhysicalOperator>, RDDConverter> convertMap, SparkPigStats sparkStats, JavaSparkContext sparkContext, JobMetricsListener jobMetricsListener, String jobGroupID) {
        super(plan, new DependencyOrderWalker<SparkOperator, SparkOperPlan>(plan, true));
        this.sparkPlan = plan;
        this.convertMap = convertMap;
        this.sparkStats = sparkStats;
        this.sparkContext = sparkContext;
        this.jobMetricsListener = jobMetricsListener;
        this.jobGroupID = jobGroupID;
    }

    @Override
    public void visitSparkOp(SparkOperator sparkOp) throws VisitorException {
        new PhyPlanSetter(sparkOp.physicalPlan).visit();
        try {
            sparkOperToRDD(sparkOp);
        } catch (InterruptedException e) {
            throw new RuntimeException("fail to get the rdds of this spark operator: ", e);
        } catch (JobCreationException e){
            throw new RuntimeException("fail to get the rdds of this spark operator: ", e);
        }
    }

    private void sparkOperToRDD(SparkOperator sparkOperator) throws InterruptedException, VisitorException, JobCreationException {
        List<SparkOperator> predecessors = sparkPlan
                .getPredecessors(sparkOperator);
        Set<OperatorKey> predecessorOfPreviousSparkOp = new HashSet<OperatorKey>();
        if (predecessors != null) {
            for (SparkOperator pred : predecessors) {
                predecessorOfPreviousSparkOp.add(pred.getOperatorKey());
            }
        }

        boolean isFail = false;
        Exception exception = null;
        if (sparkOperator instanceof NativeSparkOperator) {
            ((NativeSparkOperator) sparkOperator).runJob();
        } else {
            List<PhysicalOperator> leafPOs = sparkOperator.physicalPlan.getLeaves();

            //One SparkOperator may have multiple leaves(POStores) after multiquery feature is enabled
            if (LOG.isDebugEnabled()) {
                LOG.debug("sparkOperator.physicalPlan have " + sparkOperator.physicalPlan.getLeaves().size() + " leaves");
            }
            for (PhysicalOperator leafPO : leafPOs) {
                try {
                    physicalToRDD(sparkOperator, sparkOperator.physicalPlan, leafPO,
                            predecessorOfPreviousSparkOp);
                    sparkOpRdds.put(sparkOperator.getOperatorKey(),
                            physicalOpRdds.get(leafPO.getOperatorKey()));
                } catch (Exception e) {
                    LOG.error("throw exception in sparkOperToRDD: ", e);
                    exception = e;
                    isFail = true;
                }
            }


            List<POStore> poStores = PlanHelper.getPhysicalOperators(
                    sparkOperator.physicalPlan, POStore.class);
            Collections.sort(poStores);
            if (poStores.size() > 0) {
                int i = 0;
                if (!isFail) {
                    List<Integer> jobIDs = getJobIDs(seenJobIDs);
                    for (POStore poStore : poStores) {
                        SparkStatsUtil.waitForJobAddStats(jobIDs.get(i++), poStore, sparkOperator,
                                jobMetricsListener, sparkContext, sparkStats);
                    }
                } else {
                    for (POStore poStore : poStores) {
                        String failJobID = sparkOperator.name().concat("_fail");
                        SparkStatsUtil.addFailJobStats(failJobID, poStore, sparkOperator, sparkStats, exception);
                    }
                }
            }
        }
    }

    private void physicalToRDD(SparkOperator sparkOperator, PhysicalPlan plan,
                               PhysicalOperator physicalOperator,
                               Set<OperatorKey> predsFromPreviousSparkOper)
            throws IOException {
        RDD<Tuple> nextRDD = null;
        List<PhysicalOperator> predecessorsOfCurrentPhysicalOp = plan
                .getPredecessors(physicalOperator);
        if (predecessorsOfCurrentPhysicalOp != null && predecessorsOfCurrentPhysicalOp.size() > 1) {
            Collections.sort(predecessorsOfCurrentPhysicalOp);
        }

        Set<OperatorKey> operatorKeysOfAllPreds = new HashSet<OperatorKey>();
        addPredsFromPrevoiousSparkOp(sparkOperator, physicalOperator, operatorKeysOfAllPreds);
        if (predecessorsOfCurrentPhysicalOp != null) {
            for (PhysicalOperator predecessor : predecessorsOfCurrentPhysicalOp) {
                physicalToRDD(sparkOperator, plan, predecessor, predsFromPreviousSparkOper);
                operatorKeysOfAllPreds.add(predecessor.getOperatorKey());
            }

        } else {
            if (predsFromPreviousSparkOper != null
                    && predsFromPreviousSparkOper.size() > 0) {
                for (OperatorKey predFromPreviousSparkOper : predsFromPreviousSparkOper) {
                    operatorKeysOfAllPreds.add(predFromPreviousSparkOper);
                }
            }
        }


        if (physicalOperator instanceof POSplit) {
            List<PhysicalPlan> successorPlans = ((POSplit) physicalOperator).getPlans();
            for (PhysicalPlan successPlan : successorPlans) {
                List<PhysicalOperator> leavesOfSuccessPlan = successPlan.getLeaves();
                if (leavesOfSuccessPlan.size() != 1) {
                    LOG.error("the size of leaves of SuccessPlan should be 1");
                    break;
                }
                PhysicalOperator leafOfSuccessPlan = leavesOfSuccessPlan.get(0);
                physicalToRDD(sparkOperator, successPlan, leafOfSuccessPlan, operatorKeysOfAllPreds);
            }
        } else {
            RDDConverter converter = convertMap.get(physicalOperator.getClass());
            if (converter == null) {
                throw new IllegalArgumentException(
                        "Pig on Spark does not support Physical Operator: " + physicalOperator);
            }

            LOG.info("Converting operator "
                    + physicalOperator.getClass().getSimpleName() + " "
                    + physicalOperator);
            List<RDD<Tuple>> allPredRDDs = sortPredecessorRDDs(operatorKeysOfAllPreds);
            nextRDD = converter.convert(allPredRDDs, physicalOperator);

            if (nextRDD == null) {
                throw new IllegalArgumentException(
                        "RDD should not be null after PhysicalOperator: "
                                + physicalOperator);
            }

            physicalOpRdds.put(physicalOperator.getOperatorKey(), nextRDD);
        }
    }

    //get all rdds of predecessors sorted by the OperatorKey
    private List<RDD<Tuple>> sortPredecessorRDDs(Set<OperatorKey> operatorKeysOfAllPreds) {
        List<RDD<Tuple>> predecessorRDDs = Lists.newArrayList();
        List<OperatorKey> operatorKeyOfAllPreds = Lists.newArrayList(operatorKeysOfAllPreds);
        Collections.sort(operatorKeyOfAllPreds);
        for (OperatorKey operatorKeyOfAllPred : operatorKeyOfAllPreds) {
            predecessorRDDs.add(physicalOpRdds.get(operatorKeyOfAllPred));
        }
        return predecessorRDDs;
    }

    //deal special cases containing operators with multiple predecessors when multiquery is enabled to get the predecessors of specified
    // physicalOp in previous SparkOp(see PIG-4675)
    private void addPredsFromPrevoiousSparkOp(SparkOperator sparkOperator, PhysicalOperator physicalOperator, Set<OperatorKey> operatorKeysOfPredecessors) {
        // the relationship is stored in sparkOperator.getMultiQueryOptimizeConnectionItem()
        List<OperatorKey> predOperatorKeys = sparkOperator.getMultiQueryOptimizeConnectionItem().get(physicalOperator.getOperatorKey());
        if (predOperatorKeys != null) {
            for (OperatorKey predOperator : predOperatorKeys) {
                LOG.debug(String.format("add predecessor(OperatorKey:%s) for OperatorKey:%s", predOperator, physicalOperator.getOperatorKey()));
                operatorKeysOfPredecessors.add(predOperator);
            }
        }
    }

    /**
     * In Spark, currently only async actions return job id. There is no async
     * equivalent of actions like saveAsNewAPIHadoopFile()
     * <p/>
     * The only other way to get a job id is to register a "job group ID" with
     * the spark context and request all job ids corresponding to that job group
     * via getJobIdsForGroup.
     * <p/>
     * However getJobIdsForGroup does not guarantee the order of the elements in
     * it's result.
     * <p/>
     * This method simply returns the previously unseen job ids.
     *
     * @param seenJobIDs job ids in the job group that are already seen
     * @return Spark job ids not seen before
     */
    private List<Integer> getJobIDs(Set<Integer> seenJobIDs) {
        Set<Integer> groupjobIDs = new HashSet<Integer>(
                Arrays.asList(ArrayUtils.toObject(sparkContext.statusTracker()
                        .getJobIdsForGroup(jobGroupID))));
        groupjobIDs.removeAll(seenJobIDs);
        List<Integer> unseenJobIDs = new ArrayList<Integer>(groupjobIDs);
        if (unseenJobIDs.size() == 0) {
            throw new RuntimeException("Expected at least one unseen jobID "
                    + " in this call to getJobIdsForGroup, but got "
                    + unseenJobIDs.size());
        }
        seenJobIDs.addAll(unseenJobIDs);
        return unseenJobIDs;
    }
}