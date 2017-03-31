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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.JobCreationException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MRConfiguration;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PhyPlanSetter;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.UDFFinishVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.ConstantExpression;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POBroadcastSpark;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POMergeJoin;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSplit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.FRJoinConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.RDDConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.SkewedJoinConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.operator.NativeSparkOperator;
import org.apache.pig.backend.hadoop.executionengine.spark.operator.POJoinGroupSpark;
import org.apache.pig.backend.hadoop.executionengine.spark.operator.POPoissonSampleSpark;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkOpPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkOperPlan;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkOperator;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.newplan.logical.relational.LOJoin;
import org.apache.pig.tools.pigstats.spark.SparkPigStats;
import org.apache.pig.tools.pigstats.spark.SparkStatsUtil;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;

import com.google.common.collect.Lists;

public class JobGraphBuilder extends SparkOpPlanVisitor {

    private static final Log LOG = LogFactory.getLog(JobGraphBuilder.class);
    public static final int NULLPART_JOB_ID = -1;

    private Map<Class<? extends PhysicalOperator>, RDDConverter> convertMap = null;
    private SparkPigStats sparkStats = null;
    private JavaSparkContext sparkContext = null;
    private JobMetricsListener jobMetricsListener = null;
    private String jobGroupID = null;
    private Set<Integer> seenJobIDs = new HashSet<Integer>();
    private SparkOperPlan sparkPlan = null;
    private Map<OperatorKey, RDD<Tuple>> sparkOpRdds = new HashMap<OperatorKey, RDD<Tuple>>();
    private Map<OperatorKey, RDD<Tuple>> physicalOpRdds = new HashMap<OperatorKey, RDD<Tuple>>();
    private JobConf jobConf = null;
    private PigContext pc;

    public JobGraphBuilder(SparkOperPlan plan, Map<Class<? extends PhysicalOperator>, RDDConverter> convertMap,
                           SparkPigStats sparkStats, JavaSparkContext sparkContext, JobMetricsListener
            jobMetricsListener, String jobGroupID, JobConf jobConf, PigContext pc) {
        super(plan, new DependencyOrderWalker<SparkOperator, SparkOperPlan>(plan, true));
        this.sparkPlan = plan;
        this.convertMap = convertMap;
        this.sparkStats = sparkStats;
        this.sparkContext = sparkContext;
        this.jobMetricsListener = jobMetricsListener;
        this.jobGroupID = jobGroupID;
        this.jobConf = jobConf;
        this.pc = pc;
    }

    @Override
    public void visitSparkOp(SparkOperator sparkOp) throws VisitorException {
        new PhyPlanSetter(sparkOp.physicalPlan).visit();
        try {
            setReplicationForMergeJoin(sparkOp.physicalPlan);
            sparkOperToRDD(sparkOp);
            finishUDFs(sparkOp.physicalPlan);
        } catch (InterruptedException e) {
            throw new RuntimeException("fail to get the rdds of this spark operator: ", e);
        } catch (JobCreationException e) {
            throw new RuntimeException("fail to get the rdds of this spark operator: ", e);
        } catch (ExecException e) {
            throw new RuntimeException("fail to get the rdds of this spark operator: ", e);
        } catch (IOException e) {
            throw new RuntimeException("fail to get the rdds of this spark operator: ", e);
        }
    }

    private void setReplicationForMergeJoin(PhysicalPlan plan) throws IOException {
        List<Path> filesForMoreReplication = new ArrayList<>();
        List<POMergeJoin> poMergeJoins = PlanHelper.getPhysicalOperators(plan, POMergeJoin.class);
        if (poMergeJoins.size() > 0) {
            for (POMergeJoin poMergeJoin : poMergeJoins) {
                String idxFileName = poMergeJoin.getIndexFile();
                if (idxFileName != null) {
                    filesForMoreReplication.add(new Path(idxFileName));
                }
                // in spark mode, set as null so that PoMergeJoin won't use hadoop distributed cache
                // see POMergeJoin.seekInRightStream()
                poMergeJoin.setIndexFile(null);
            }
        }

        setReplicationForFiles(filesForMoreReplication);
    }

    private void setReplicationForFiles(List<Path> files) throws IOException {
        FileSystem fs = FileSystem.get(this.jobConf);
        short replication = (short) jobConf.getInt(MRConfiguration.SUMIT_REPLICATION, 10);
        for (int i = 0; i < files.size(); i++) {
            fs.setReplication(files.get(i), replication);
        }
    }

    // Calling EvalFunc.finish()
    private void finishUDFs(PhysicalPlan physicalPlan) throws VisitorException {
        UDFFinishVisitor finisher = new UDFFinishVisitor(physicalPlan,
                new DependencyOrderWalker<PhysicalOperator, PhysicalPlan>(
                        physicalPlan));
        try {
            finisher.visit();
        } catch (VisitorException e) {
            int errCode = 2121;
            String msg = "Error while calling finish method on UDFs.";
            throw new VisitorException(msg, errCode, PigException.BUG, e);
        }
    }

    private void sparkOperToRDD(SparkOperator sparkOperator) throws InterruptedException, VisitorException, JobCreationException, ExecException {
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
                    boolean stopOnFailure = Boolean.valueOf(pc
                            .getProperties().getProperty("stop.on.failure",
                                    "false"));
                    if (stopOnFailure) {
                        int errCode = 6017;
                        throw new ExecException(e.getMessage(), errCode,
                                PigException.REMOTE_ENVIRONMENT);
                    }
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
                        if (jobIDs.size() == 0) {
                            /**
                             * Spark internally misses information about its jobs that mapped 0 partitions.
                             * Although these have valid jobIds, Spark itself is unable to tell anything about them.
                             * If the store rdd had 0 partitions we return a dummy success stat with jobId =
                             * NULLPART_JOB_ID, in any other cases we throw exception if no new jobId was seen.
                             */
                            if (physicalOpRdds.get(poStore.getOperatorKey()).partitions().length == 0) {
                                sparkStats.addJobStats(poStore, sparkOperator, NULLPART_JOB_ID, null, sparkContext);
                                return;
                            } else {
                                throw new RuntimeException("Expected at least one unseen jobID "
                                        + " in this call to getJobIdsForGroup, but got 0");
                            }
                        }
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
        List<PhysicalOperator> predecessorsOfCurrentPhysicalOp = getPredecessors(plan, physicalOperator);
        Set<OperatorKey> operatorKeysOfAllPreds = new LinkedHashSet<OperatorKey>();
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

            if (converter instanceof FRJoinConverter) {
                setReplicatedInputs(physicalOperator, (FRJoinConverter) converter);
            }

            if (sparkOperator.isSkewedJoin() && converter instanceof SkewedJoinConverter) {
                SkewedJoinConverter skewedJoinConverter = (SkewedJoinConverter) converter;
                skewedJoinConverter.setSkewedJoinPartitionFile(sparkOperator.getSkewedJoinPartitionFile());
            }
            adjustRuntimeParallelismForSkewedJoin(physicalOperator, sparkOperator, allPredRDDs);
            nextRDD = converter.convert(allPredRDDs, physicalOperator);

            if (nextRDD == null) {
                throw new IllegalArgumentException(
                        "RDD should not be null after PhysicalOperator: "
                                + physicalOperator);
            }

            physicalOpRdds.put(physicalOperator.getOperatorKey(), nextRDD);
        }
    }

    private void setReplicatedInputs(PhysicalOperator physicalOperator, FRJoinConverter converter) {
        Set<String> replicatedInputs = new HashSet<>();
        for (PhysicalOperator operator : physicalOperator.getInputs()) {
            if (operator instanceof POBroadcastSpark) {
                replicatedInputs.add(((POBroadcastSpark) operator).getBroadcastedVariableName());
            }
        }
        converter.setReplicatedInputs(replicatedInputs);
    }

    private List<PhysicalOperator> getPredecessors(PhysicalPlan plan, PhysicalOperator op) {
        List preds = null;
        if (!(op instanceof POJoinGroupSpark)) {
            preds = plan.getPredecessors(op);
            if (preds != null && preds.size() > 1) {
                Collections.sort(preds);
            }
        } else {
            //For POJoinGroupSpark, we could not use plan.getPredecessors(op)+ sort to get
            //the predecessors with correct order, more detail see JoinOptimizerSpark#restructSparkOp
            preds = ((POJoinGroupSpark) op).getPredecessors();
        }
        return preds;
    }

    //get all rdds of predecessors sorted by the OperatorKey
    private List<RDD<Tuple>> sortPredecessorRDDs(Set<OperatorKey> operatorKeysOfAllPreds) {
        List<RDD<Tuple>> predecessorRDDs = Lists.newArrayList();
//        List<OperatorKey> operatorKeyOfAllPreds = Lists.newArrayList(operatorKeysOfAllPreds);
//        Collections.sort(operatorKeyOfAllPreds);
        //We need not sort operatorKeyOfAllPreds any more because operatorKeyOfAllPreds is LinkedHashSet
        //which provides the order of insertion, before we insert element which is sorted by OperatorKey
        for (OperatorKey operatorKeyOfAllPred : operatorKeysOfAllPreds) {
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
        seenJobIDs.addAll(unseenJobIDs);
        return unseenJobIDs;
    }


    /**
     * if the parallelism of skewed join is NOT specified by user in the script when sampling,
     * set a default parallelism for sampling
     *
     * @param physicalOperator
     * @param sparkOperator
     * @param allPredRDDs
     * @throws VisitorException
     */
    private void adjustRuntimeParallelismForSkewedJoin(PhysicalOperator physicalOperator,
                                                       SparkOperator sparkOperator,
                                                       List<RDD<Tuple>> allPredRDDs) throws VisitorException {
        // We need to calculate the final number of reducers of the next job (skew-join)
        // adjust parallelism of ConstantExpression
        if (sparkOperator.isSampler() && sparkPlan.getSuccessors(sparkOperator) != null
                && physicalOperator instanceof POPoissonSampleSpark) {
            // set the runtime #reducer of the next job as the #partition

            int defaultParallelism = SparkUtil.getParallelism(allPredRDDs, physicalOperator);

            ParallelConstantVisitor visitor =
                    new ParallelConstantVisitor(sparkOperator.physicalPlan, defaultParallelism);
            visitor.visit();
        }
    }

    /**
     * here, we don't reuse MR/Tez's ParallelConstantVisitor
     * To automatic adjust reducer parallelism for skewed join, we only adjust the
     * ConstantExpression operator after POPoissionSampleSpark operator
     */
    private static class ParallelConstantVisitor extends PhyPlanVisitor {

        private int rp;
        private boolean replaced = false;
        private boolean isAfterSampleOperator = false;

        public ParallelConstantVisitor(PhysicalPlan plan, int rp) {
            super(plan, new DepthFirstWalker<PhysicalOperator, PhysicalPlan>(
                    plan));
            this.rp = rp;
        }

        @Override
        public void visitConstant(ConstantExpression cnst) throws VisitorException {
            if (isAfterSampleOperator && cnst.getRequestedParallelism() == -1) {
                Object obj = cnst.getValue();
                if (obj instanceof Integer) {
                    if (replaced) {
                        // sample job should have only one ConstantExpression
                        throw new VisitorException("Invalid reduce plan: more " +
                                "than one ConstantExpression found in sampling job");
                    }
                    cnst.setValue(rp);
                    cnst.setRequestedParallelism(rp);
                    replaced = true;
                }
            }
        }

        @Override
        public void visitPoissonSampleSpark(POPoissonSampleSpark po) {
            isAfterSampleOperator = true;
        }
    }
}
