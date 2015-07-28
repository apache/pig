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
package org.apache.pig.backend.hadoop.executionengine.spark;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.pig.PigConfiguration;
import org.apache.pig.PigConstants;
import org.apache.pig.PigException;
import org.apache.pig.backend.BackendException;
import org.apache.pig.backend.hadoop.executionengine.Launcher;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PhyPlanSetter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POCollectedGroup;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POCounter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.PODistinct;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFRJoin;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFilter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLimit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POMergeJoin;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.PORank;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSkewedJoin;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSort;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSplit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStream;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POUnion;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.CollectedGroupConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.CounterConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.DistinctConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.FRJoinConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.FilterConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.ForEachConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.GlobalRearrangeConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.LimitConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.LoadConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.LocalRearrangeConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.MergeJoinConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.PackageConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.RDDConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.RankConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.SkewedJoinConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.SortConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.SplitConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.StoreConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.StreamConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.UnionConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.operator.POGlobalRearrangeSpark;
import org.apache.pig.backend.hadoop.executionengine.spark.optimizer.AccumulatorOptimizer;
import org.apache.pig.backend.hadoop.executionengine.spark.optimizer.MultiQueryOptimizerSpark;
import org.apache.pig.backend.hadoop.executionengine.spark.optimizer.NoopFilterRemover;
import org.apache.pig.backend.hadoop.executionengine.spark.optimizer.ParallelismSetter;
import org.apache.pig.backend.hadoop.executionengine.spark.optimizer.SecondaryKeyOptimizerSpark;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkCompiler;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkOperPlan;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkOperator;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkPOPackageAnnotator;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkPrinter;
import org.apache.pig.backend.hadoop.executionengine.util.MapRedUtil;
import org.apache.pig.data.SchemaTupleBackend;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.JarManager;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.spark.SparkPigStats;
import org.apache.pig.tools.pigstats.spark.SparkStatsUtil;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.scheduler.JobLogger;
import org.apache.spark.scheduler.StatsReportListener;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

/**
 * Main class that launches pig for Spark
 */
public class SparkLauncher extends Launcher {

    private static final Log LOG = LogFactory.getLog(SparkLauncher.class);

    // Our connection to Spark. It needs to be static so that it can be reused
    // across jobs, because a
    // new SparkLauncher gets created for each job.
    private static JavaSparkContext sparkContext = null;
    private static JobMetricsListener jobMetricsListener = new JobMetricsListener();
    private String jobGroupID;
    private PigContext pigContext = null;
    private JobConf jobConf = null;
    private String currentDirectoryPath = null;

    @Override
    public PigStats launchPig(PhysicalPlan physicalPlan, String grpName,
                              PigContext pigContext) throws Exception {
        if (LOG.isDebugEnabled())
            LOG.debug(physicalPlan);
        this.pigContext = pigContext;
        initialize();
        SparkOperPlan sparkplan = compile(physicalPlan, pigContext);
        if (LOG.isDebugEnabled())
            explain(sparkplan, System.out, "text", true);
        SparkPigStats sparkStats = (SparkPigStats) pigContext
                .getExecutionEngine().instantiatePigStats();
        sparkStats.initialize(sparkplan);
        PigStats.start(sparkStats);

        startSparkIfNeeded(pigContext);

        // Set a unique group id for this query, so we can lookup all Spark job
        // ids
        // related to this query.
        jobGroupID = UUID.randomUUID().toString();
        sparkContext.setJobGroup(jobGroupID, "Pig query to Spark cluster",
                false);
        jobMetricsListener.reset();

        this.currentDirectoryPath = Paths.get(".").toAbsolutePath()
                .normalize().toString()
                + "/";
        addFilesToSparkJob();
        LinkedList<POStore> stores = PlanHelper.getPhysicalOperators(
                physicalPlan, POStore.class);
        POStore firstStore = stores.getFirst();
        if (firstStore != null) {
            MapRedUtil.setupStreamingDirsConfSingle(firstStore, pigContext,
                    jobConf);
        }

        new ParallelismSetter(sparkplan, jobConf).visit();

        byte[] confBytes = KryoSerializer.serializeJobConf(jobConf);

        // Create conversion map, mapping between pig operator and spark convertor
        Map<Class<? extends PhysicalOperator>, RDDConverter> convertMap
                = new HashMap<Class<? extends PhysicalOperator>, RDDConverter>();
        convertMap.put(POLoad.class, new LoadConverter(pigContext,
                physicalPlan, sparkContext.sc()));
        convertMap.put(POStore.class, new StoreConverter(pigContext));
        convertMap.put(POForEach.class, new ForEachConverter(confBytes));
        convertMap.put(POFilter.class, new FilterConverter());
        convertMap.put(POPackage.class, new PackageConverter(confBytes));
        convertMap.put(POLocalRearrange.class, new LocalRearrangeConverter());
        convertMap.put(POGlobalRearrangeSpark.class, new GlobalRearrangeConverter());
        convertMap.put(POLimit.class, new LimitConverter());
        convertMap.put(PODistinct.class, new DistinctConverter());
        convertMap.put(POUnion.class, new UnionConverter(sparkContext.sc()));
        convertMap.put(POSort.class, new SortConverter());
        convertMap.put(POSplit.class, new SplitConverter());
        convertMap.put(POSkewedJoin.class, new SkewedJoinConverter());
        convertMap.put(POMergeJoin.class, new MergeJoinConverter());
        convertMap.put(POCollectedGroup.class, new CollectedGroupConverter());
        convertMap.put(POCounter.class, new CounterConverter());
        convertMap.put(PORank.class, new RankConverter());
        convertMap.put(POStream.class, new StreamConverter(confBytes));
        convertMap.put(POFRJoin.class, new FRJoinConverter());

        sparkPlanToRDD(sparkplan, convertMap, sparkStats, jobConf);
        cleanUpSparkJob();
        sparkStats.finish();

        return sparkStats;
    }

    private void optimize(PigContext pc, SparkOperPlan plan) throws IOException {
        String prop = pc.getProperties().getProperty(PigConfiguration.PIG_EXEC_NO_SECONDARY_KEY);
        if (!pc.inIllustrator && !("true".equals(prop))) {
            SecondaryKeyOptimizerSpark skOptimizer = new SecondaryKeyOptimizerSpark(plan);
            skOptimizer.visit();
        }

        boolean isAccum =
                Boolean.valueOf(pc.getProperties().getProperty("opt.accumulator", "true"));
        if (isAccum) {
            AccumulatorOptimizer accum = new AccumulatorOptimizer(plan);
            accum.visit();
        }

        // removes the filter(constant(true)) operators introduced by
        // splits.
        NoopFilterRemover fRem = new NoopFilterRemover(plan);
        fRem.visit();

        boolean isMultiQuery =
                Boolean.valueOf(pc.getProperties().getProperty(PigConfiguration.PIG_OPT_MULTIQUERY, "true"));

        if (LOG.isDebugEnabled()) {
            System.out.println("before multiquery optimization:");
            explain(plan, System.out, "text", true);
        }

        if (isMultiQuery) {
            // reduces the number of SparkOpers in the Spark plan generated
            // by multi-query (multi-store) script.
            MultiQueryOptimizerSpark mqOptimizer = new MultiQueryOptimizerSpark(plan);
            mqOptimizer.visit();
        }

        if (LOG.isDebugEnabled()) {
            System.out.println("after multiquery optimization:");
            explain(plan, System.out, "text", true);
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

    private void cleanUpSparkJob() {
        LOG.info("clean up Spark Job");
        boolean isLocal = System.getenv("SPARK_MASTER") != null ? System
                .getenv("SPARK_MASTER").equalsIgnoreCase("LOCAL") : true;
        if (isLocal) {
            String shipFiles = pigContext.getProperties().getProperty(
                    "pig.streaming.ship.files");
            if (shipFiles != null) {
                for (String file : shipFiles.split(",")) {
                    File shipFile = new File(file);
                    File deleteFile = new File(currentDirectoryPath + "/"
                            + shipFile.getName());
                    if (deleteFile.exists()) {
                        LOG.info(String.format("delete ship file result: %b",
                                deleteFile.delete()));
                    }
                }
            }
            String cacheFiles = pigContext.getProperties().getProperty(
                    "pig.streaming.cache.files");
            if (cacheFiles != null) {
                for (String file : cacheFiles.split(",")) {
                    String fileName = extractFileName(file.trim());
                    File deleteFile = new File(currentDirectoryPath + "/"
                            + fileName);
                    if (deleteFile.exists()) {
                        LOG.info(String.format("delete cache file result: %b",
                                deleteFile.delete()));
                    }
                }
            }
        }
    }

    private void addFilesToSparkJob() throws IOException {
        LOG.info("add Files Spark Job");
        String shipFiles = pigContext.getProperties().getProperty(
                "pig.streaming.ship.files");
        shipFiles(shipFiles);
        String cacheFiles = pigContext.getProperties().getProperty(
                "pig.streaming.cache.files");
        cacheFiles(cacheFiles);
    }


    private void shipFiles(String shipFiles)
            throws IOException {
        if (shipFiles != null) {
            for (String file : shipFiles.split(",")) {
                File shipFile = new File(file.trim());
                if (shipFile.exists()) {
                    LOG.info(String.format("shipFile:%s", shipFile));
                    addJarToSparkJobWorkingDirectory(shipFile, shipFile.getName());
                }
            }
        }
    }

    private void cacheFiles(String cacheFiles) throws IOException {
        if (cacheFiles != null) {
            Configuration conf = SparkUtil.newJobConf(pigContext);
            for (String file : cacheFiles.split(",")) {
                String fileName = extractFileName(file.trim());
                Path src = new Path(extractFileUrl(file.trim()));
                File tmpFile = File.createTempFile(fileName, ".tmp");
                Path tmpFilePath = new Path(tmpFile.getAbsolutePath());
                FileSystem fs = tmpFilePath.getFileSystem(conf);
                fs.copyToLocalFile(src, tmpFilePath);
                tmpFile.deleteOnExit();
                LOG.info(String.format("cacheFile:%s", fileName));
                addJarToSparkJobWorkingDirectory(tmpFile, fileName);
            }
        }
    }

    private void addJarToSparkJobWorkingDirectory(File jarFile, String jarName) throws IOException {
        LOG.info("Added jar " + jarName);
        boolean isLocal = System.getenv("SPARK_MASTER") != null ? System
                .getenv("SPARK_MASTER").equalsIgnoreCase("LOCAL") : true;
        if (isLocal) {
            File localFile = new File(currentDirectoryPath + "/"
                    + jarName);
            if (jarFile.getAbsolutePath().equals(localFile.getAbsolutePath())
                    && jarFile.exists()) {
                return;
            }
            if (localFile.exists()) {
                LOG.info(String.format(
                        "jar file %s exists, ready to delete",
                        localFile.getAbsolutePath()));
                localFile.delete();
            } else {
                LOG.info(String.format("jar file %s not exists,",
                        localFile.getAbsolutePath()));
            }
            Files.copy(Paths.get(new Path(jarFile.getAbsolutePath()).toString()),
                    Paths.get(localFile.getAbsolutePath()));
        } else {
            sparkContext.addFile(jarFile.toURI().toURL()
                    .toExternalForm());
        }
    }

    private String extractFileName(String cacheFileUrl) {
        String[] tmpAry = cacheFileUrl.split("#");
        String fileName = tmpAry != null && tmpAry.length == 2 ? tmpAry[1]
                : null;
        if (fileName == null) {
            throw new RuntimeException("cache file is invalid format, file:"
                    + cacheFileUrl);
        } else {
            LOG.debug("cache file name is valid:" + cacheFileUrl);
            return fileName;
        }
    }

    private String extractFileUrl(String cacheFileUrl) {
        String[] tmpAry = cacheFileUrl.split("#");
        String fileName = tmpAry != null && tmpAry.length == 2 ? tmpAry[0]
                : null;
        if (fileName == null) {
            throw new RuntimeException("cache file is invalid format, file:"
                    + cacheFileUrl);
        } else {
            LOG.debug("cache file name is valid:" + cacheFileUrl);
            return fileName;
        }
    }

    private SparkOperPlan compile(PhysicalPlan physicalPlan,
                                  PigContext pigContext) throws PlanException, IOException,
            VisitorException {
        SparkCompiler sparkCompiler = new SparkCompiler(physicalPlan,
                pigContext);
        sparkCompiler.compile();
        SparkOperPlan sparkPlan = sparkCompiler.getSparkPlan();

        // optimize key - value handling in package
        SparkPOPackageAnnotator pkgAnnotator = new SparkPOPackageAnnotator(
                sparkPlan);
        pkgAnnotator.visit();

        optimize(pigContext, sparkPlan);
        return sparkPlan;
    }

    private static void startSparkIfNeeded(PigContext pc) throws PigException {
        if (sparkContext == null) {
            String master = null;
            if (pc.getExecType().isLocal()) {
                master = "local";
            } else {
                master = System.getenv("SPARK_MASTER");
                if (master == null) {
                    LOG.info("SPARK_MASTER not specified, using \"local\"");
                    master = "local";
                }
            }

            String sparkHome = System.getenv("SPARK_HOME");
            String sparkJarsSetting = System.getenv("SPARK_JARS");
            String pigJar = System.getenv("SPARK_PIG_JAR");
            String[] sparkJars = sparkJarsSetting == null ? new String[]{}
                    : sparkJarsSetting.split(",");
            List<String> jars = Lists.asList(pigJar, sparkJars);

            if (!master.startsWith("local") && !master.equals("yarn-client")) {
                // Check that we have the Mesos native library and Spark home
                // are set
                if (sparkHome == null) {
                    System.err
                            .println("You need to set SPARK_HOME to run on a Mesos cluster!");
                    throw new PigException("SPARK_HOME is not set");
                }
            }

            sparkContext = new JavaSparkContext(master, "PigOnSpark", sparkHome,
                    jars.toArray(new String[jars.size()]));
            sparkContext.sc().addSparkListener(new StatsReportListener());
            sparkContext.sc().addSparkListener(new JobLogger());
            sparkContext.sc().addSparkListener(jobMetricsListener);
        }
    }

    // You can use this in unit tests to stop the SparkContext between tests.
    static void stopSpark() {
        if (sparkContext != null) {
            sparkContext.stop();
            sparkContext = null;
        }
    }

    private void sparkPlanToRDD(SparkOperPlan sparkPlan,
                                Map<Class<? extends PhysicalOperator>, RDDConverter> convertMap,
                                SparkPigStats sparkStats, JobConf jobConf)
            throws IOException, InterruptedException {
        Set<Integer> seenJobIDs = new HashSet<Integer>();
        if (sparkPlan == null) {
            throw new RuntimeException("SparkPlan is null.");
        }

        List<SparkOperator> leaves = sparkPlan.getLeaves();
        Collections.sort(leaves);
        Map<OperatorKey, RDD<Tuple>> sparkOpToRdds = new HashMap();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Converting " + leaves.size() + " Spark Operators to RDDs");
        }

        for (SparkOperator leaf : leaves) {
            new PhyPlanSetter(leaf.physicalPlan).visit();
            Map<OperatorKey, RDD<Tuple>> physicalOpToRdds = new HashMap();
            sparkOperToRDD(sparkPlan, leaf, sparkOpToRdds,
                    physicalOpToRdds, convertMap, seenJobIDs, sparkStats,
                    jobConf);
        }
    }

    private void addUDFJarsToSparkJobWorkingDirectory(SparkOperator leaf) throws IOException {

        for (String udf : leaf.UDFs) {
            Class clazz = pigContext.getClassForAlias(udf);
            if (clazz != null) {
                String jar = JarManager.findContainingJar(clazz);
                if (jar != null) {
                    File jarFile = new File(jar);
                    addJarToSparkJobWorkingDirectory(jarFile, jarFile.getName());
                }
            }
        }
    }

    private void sparkOperToRDD(SparkOperPlan sparkPlan,
                                SparkOperator sparkOperator,
                                Map<OperatorKey, RDD<Tuple>> sparkOpRdds,
                                Map<OperatorKey, RDD<Tuple>> physicalOpRdds,
                                Map<Class<? extends PhysicalOperator>, RDDConverter> convertMap,
                                Set<Integer> seenJobIDs, SparkPigStats sparkStats, JobConf conf)
            throws IOException, InterruptedException {
        addUDFJarsToSparkJobWorkingDirectory(sparkOperator);
        List<SparkOperator> predecessors = sparkPlan
                .getPredecessors(sparkOperator);
        List<RDD<Tuple>> predecessorRDDs = Lists.newArrayList();
        if (predecessors != null) {
            for (SparkOperator pred : predecessors) {
                if (sparkOpRdds.get(pred.getOperatorKey()) == null) {
                    sparkOperToRDD(sparkPlan, pred, sparkOpRdds,
                            physicalOpRdds, convertMap, seenJobIDs, sparkStats,
                            conf);
                }
                predecessorRDDs.add(sparkOpRdds.get(pred.getOperatorKey()));
            }
        }

        List<PhysicalOperator> leafPOs = sparkOperator.physicalPlan.getLeaves();
        boolean isFail = false;
        Exception exception = null;
        //One SparkOperator may have multiple leaves(POStores) after multiquery feature is enabled
        if (LOG.isDebugEnabled()) {
            LOG.debug("sparkOperator.physicalPlan have " + sparkOperator.physicalPlan.getLeaves().size() + " leaves");
        }
        for (PhysicalOperator leafPO : leafPOs) {
            try {
                physicalToRDD(sparkOperator.physicalPlan, leafPO, physicalOpRdds,
                        predecessorRDDs, convertMap);
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
                            jobMetricsListener, sparkContext, sparkStats, conf);
                }
            } else {
                for (POStore poStore : poStores) {
                    String failJobID = sparkOperator.name().concat("_fail");
                    SparkStatsUtil.addFailJobStats(failJobID, poStore, sparkOperator, sparkStats,
                            conf, exception);
                }
            }
        }
    }

    private void physicalToRDD(PhysicalPlan plan,
                               PhysicalOperator physicalOperator,
                               Map<OperatorKey, RDD<Tuple>> rdds,
                               List<RDD<Tuple>> rddsFromPredeSparkOper,
                               Map<Class<? extends PhysicalOperator>, RDDConverter> convertMap)
            throws IOException {
        RDD<Tuple> nextRDD = null;
        List<PhysicalOperator> predecessors = plan
                .getPredecessors(physicalOperator);
        if (predecessors != null && predecessors.size() > 1) {
            Collections.sort(predecessors);
        }

        List<RDD<Tuple>> predecessorRdds = Lists.newArrayList();
        if (predecessors != null) {
            for (PhysicalOperator predecessor : predecessors) {
                physicalToRDD(plan, predecessor, rdds, rddsFromPredeSparkOper,
                        convertMap);
                predecessorRdds.add(rdds.get(predecessor.getOperatorKey()));
            }

        } else {
            if (rddsFromPredeSparkOper != null
                    && rddsFromPredeSparkOper.size() > 0) {
                predecessorRdds.addAll(rddsFromPredeSparkOper);
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
                physicalToRDD(successPlan, leafOfSuccessPlan, rdds, predecessorRdds, convertMap);
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
            nextRDD = converter.convert(predecessorRdds, physicalOperator);

            if (nextRDD == null) {
                throw new IllegalArgumentException(
                        "RDD should not be null after PhysicalOperator: "
                                + physicalOperator);
            }

            rdds.put(physicalOperator.getOperatorKey(), nextRDD);
        }
    }

    @Override
    public void explain(PhysicalPlan pp, PigContext pc, PrintStream ps,
                        String format, boolean verbose) throws IOException {
        SparkOperPlan sparkPlan = compile(pp, pc);
        explain(sparkPlan, ps, format, verbose);
    }

    private void explain(SparkOperPlan sparkPlan, PrintStream ps,
                         String format, boolean verbose)
            throws IOException {
        Map<OperatorKey, SparkOperator> allOperKeys = sparkPlan.getKeys();
        List<OperatorKey> operKeyList = new ArrayList(allOperKeys.keySet());
        Collections.sort(operKeyList);
        for (OperatorKey operatorKey : operKeyList) {
            SparkOperator op = sparkPlan.getOperator(operatorKey);
            ps.print(op.getOperatorKey());
            List<SparkOperator> successors = sparkPlan.getSuccessors(op);
            if (successors != null) {
                ps.print("->");
                for (SparkOperator suc : successors) {
                    ps.print(suc.getOperatorKey() + " ");
                }
            }
            ps.println();
        }

        if (format.equals("text")) {
            SparkPrinter printer = new SparkPrinter(ps, sparkPlan);
            printer.setVerbose(verbose);
            printer.visit();
        } else { // TODO: add support for other file format
            throw new IOException(
                    "Non-text output of explain is not supported.");
        }
    }

    @Override
    public void kill() throws BackendException {
        // TODO Auto-generated method stub

    }

    @Override
    public void killJob(String jobID, Configuration conf)
            throws BackendException {
        // TODO Auto-generated method stub

    }

    /**
     * We store the value of udf.import.list in PigContext#properties.getProperty("spark.udf.import.list") in spark mode.
     * Later we will use PigContext#properties.getProperty("spark.udf.import.list")in PigContext#writeObject.
     * we don't save this value in PigContext#properties.getProperty("udf.import.list")
     * because this will cause OOM problem(detailed see PIG-4295).
     */
    private void saveUdfImportList() {
        String udfImportList = Joiner.on(",").join(PigContext.getPackageImportList());
        pigContext.getProperties().setProperty("spark.udf.import.list", udfImportList);
    }

    private void initialize() throws IOException {
        saveUdfImportList();
        jobConf = SparkUtil.newJobConf(pigContext);
        jobConf.set(PigConstants.LOCAL_CODE_DIR,
                System.getProperty("java.io.tmpdir"));

        SchemaTupleBackend.initialize(jobConf, pigContext);
        Utils.setDefaultTimeZone(jobConf);
    }
}
