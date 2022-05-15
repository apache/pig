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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.PigConfiguration;
import org.apache.pig.PigException;
import org.apache.pig.PigWarning;
import org.apache.pig.backend.BackendException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.executionengine.Launcher;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MRConfiguration;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POBroadcastSpark;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POCollectedGroup;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POCounter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.PODistinct;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFRJoinSpark;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFilter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLimit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POMergeCogroup;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POMergeJoin;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPreCombinerLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.PORank;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSkewedJoin;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSort;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSplit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStream;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POUnion;
import org.apache.pig.backend.hadoop.executionengine.shims.HadoopShims;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.BroadcastConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.CollectedGroupConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.CounterConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.DistinctConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.FRJoinConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.FilterConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.ForEachConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.GlobalRearrangeConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.JoinGroupSparkConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.LimitConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.LoadConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.LocalRearrangeConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.MergeCogroupConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.MergeJoinConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.PackageConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.PoissonSampleConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.RDDConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.RankConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.ReduceByConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.SkewedJoinConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.SortConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.SparkSampleSortConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.SplitConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.StoreConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.StreamConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.UnionConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.operator.POGlobalRearrangeSpark;
import org.apache.pig.backend.hadoop.executionengine.spark.operator.POJoinGroupSpark;
import org.apache.pig.backend.hadoop.executionengine.spark.operator.POPoissonSampleSpark;
import org.apache.pig.backend.hadoop.executionengine.spark.operator.POReduceBySpark;
import org.apache.pig.backend.hadoop.executionengine.spark.operator.POSampleSortSpark;
import org.apache.pig.backend.hadoop.executionengine.spark.optimizer.AccumulatorOptimizer;
import org.apache.pig.backend.hadoop.executionengine.spark.optimizer.CombinerOptimizer;
import org.apache.pig.backend.hadoop.executionengine.spark.optimizer.JoinGroupOptimizerSpark;
import org.apache.pig.backend.hadoop.executionengine.spark.optimizer.MultiQueryOptimizerSpark;
import org.apache.pig.backend.hadoop.executionengine.spark.optimizer.NoopFilterRemover;
import org.apache.pig.backend.hadoop.executionengine.spark.optimizer.ParallelismSetter;
import org.apache.pig.backend.hadoop.executionengine.spark.optimizer.SecondaryKeyOptimizerSpark;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.DotSparkPrinter;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkCompiler;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkOperPlan;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkOperator;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkPOPackageAnnotator;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkPrinter;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.XMLSparkPrinter;
import org.apache.pig.data.SchemaTupleBackend;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.JarManager;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.tools.pigstats.OutputStats;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.spark.SparkCounterGroup;
import org.apache.pig.tools.pigstats.spark.SparkCounters;
import org.apache.pig.tools.pigstats.spark.SparkPigStats;
import org.apache.pig.tools.pigstats.spark.SparkPigStatusReporter;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.scheduler.StatsReportListener;

import com.google.common.base.Joiner;

import static org.apache.pig.backend.hadoop.executionengine.spark.SparkShims.SPARK_VERSION;

/**
 * Main class that launches pig for Spark
 */
public class SparkLauncher extends Launcher {

    private static final Log LOG = LogFactory.getLog(SparkLauncher.class);

    // Our connection to Spark. It needs to be static so that it can be reused
    // across jobs, because a
    // new SparkLauncher gets created for each job.
    private static JavaSparkContext sparkContext = null;
    private static JobStatisticCollector jobStatisticCollector = new JobStatisticCollector();
    private String jobGroupID;
    private PigContext pigContext = null;
    private JobConf jobConf = null;
    private String currentDirectoryPath = null;
    private SparkEngineConf sparkEngineConf = new SparkEngineConf();
    private static final String PIG_WARNING_FQCN = PigWarning.class.getCanonicalName();

    // this set is unnecessary once PIG-5241 is fixed
    private static Set<String> allCachedFiles = null;

    @Override
    public PigStats launchPig(PhysicalPlan physicalPlan, String grpName,
                              PigContext pigContext) throws Exception {
        if (LOG.isDebugEnabled())
            LOG.debug(physicalPlan);
        this.pigContext = pigContext;
        initialize(physicalPlan);
        SparkOperPlan sparkplan = compile(physicalPlan, pigContext);
        if (LOG.isDebugEnabled()) {
            LOG.debug(sparkplan);
        }
        SparkPigStats sparkStats = (SparkPigStats) pigContext
                .getExecutionEngine().instantiatePigStats();
        sparkStats.initialize(pigContext, sparkplan, jobConf);
        PigStats.start(sparkStats);

        startSparkIfNeeded(jobConf, pigContext);

        jobGroupID = String.format("%s-%s",sparkContext.getConf().getAppId(),
                UUID.randomUUID().toString());
        jobConf.set(MRConfiguration.JOB_ID,jobGroupID);
        jobConf.set(MRConfiguration.TASK_ID, HadoopShims.getNewTaskAttemptID().toString());

        sparkContext.setJobGroup(jobGroupID, "Pig query to Spark cluster",
                false);
        jobStatisticCollector.reset();

        this.currentDirectoryPath = Paths.get(".").toAbsolutePath()
                .normalize().toString()
                + "/";

        new ParallelismSetter(sparkplan, jobConf).visit();

        prepareSparkCounters(jobConf);

        // Create conversion map, mapping between pig operator and spark convertor
        Map<Class<? extends PhysicalOperator>, RDDConverter> convertMap
                = new HashMap<Class<? extends PhysicalOperator>, RDDConverter>();
        convertMap.put(POLoad.class, new LoadConverter(pigContext,
                physicalPlan, sparkContext.sc(), jobConf, sparkEngineConf));
        convertMap.put(POStore.class, new StoreConverter(jobConf));
        convertMap.put(POForEach.class, new ForEachConverter(jobConf));
        convertMap.put(POFilter.class, new FilterConverter());
        convertMap.put(POPackage.class, new PackageConverter());
        convertMap.put(POLocalRearrange.class, new LocalRearrangeConverter());
        convertMap.put(POGlobalRearrangeSpark.class, new GlobalRearrangeConverter());
        convertMap.put(POJoinGroupSpark.class, new JoinGroupSparkConverter());
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
        convertMap.put(POStream.class, new StreamConverter());
        convertMap.put(POFRJoinSpark.class, new FRJoinConverter());
        convertMap.put(POMergeCogroup.class, new MergeCogroupConverter());
        convertMap.put(POReduceBySpark.class, new ReduceByConverter());
        convertMap.put(POPreCombinerLocalRearrange.class, new LocalRearrangeConverter());
        convertMap.put(POBroadcastSpark.class, new BroadcastConverter(sparkContext));
        convertMap.put(POSampleSortSpark.class, new SparkSampleSortConverter());
        convertMap.put(POPoissonSampleSpark.class, new PoissonSampleConverter());
        //Print SPARK plan before launching if needed
        Configuration conf = ConfigurationUtil.toConfiguration(pigContext.getProperties());
        if (conf.getBoolean(PigConfiguration.PIG_PRINT_EXEC_PLAN, false)) {
            LOG.info(sparkplan);
        }
        uploadResources(sparkplan);

        new JobGraphBuilder(sparkplan, convertMap, sparkStats, sparkContext, jobStatisticCollector, jobGroupID, jobConf, pigContext).visit();
        cleanUpSparkJob(sparkStats);
        sparkStats.finish();
        resetUDFContext();
        return sparkStats;
    }

    private void resetUDFContext() {
        UDFContext.getUDFContext().addJobConf(null);
    }

    private void uploadResources(SparkOperPlan sparkPlan) throws IOException {
        addFilesToSparkJob(sparkPlan);
        addJarsToSparkJob(sparkPlan);
    }

    private void optimize(SparkOperPlan plan, PigContext pigContext) throws IOException {

        Configuration conf = ConfigurationUtil.toConfiguration(pigContext.getProperties());

        // Should be the first optimizer as it introduces new operators to the plan.
        boolean noCombiner = conf.getBoolean(PigConfiguration.PIG_EXEC_NO_COMBINER, false);
        if (!pigContext.inIllustrator && !noCombiner)  {
            CombinerOptimizer combinerOptimizer = new CombinerOptimizer(plan);
            combinerOptimizer.visit();
            if (LOG.isDebugEnabled()) {
                LOG.debug("After combiner optimization:");
                LOG.debug(plan);
            }
        }

        boolean noSecondaryKey = conf.getBoolean(PigConfiguration.PIG_EXEC_NO_SECONDARY_KEY, false);
        if (!pigContext.inIllustrator && !noSecondaryKey) {
            SecondaryKeyOptimizerSpark skOptimizer = new SecondaryKeyOptimizerSpark(plan);
            skOptimizer.visit();
        }

        boolean isAccum = conf.getBoolean(PigConfiguration.PIG_OPT_ACCUMULATOR, true);
        if (isAccum) {
            AccumulatorOptimizer accum = new AccumulatorOptimizer(plan);
            accum.visit();
        }

        // removes the filter(constant(true)) operators introduced by
        // splits.
        NoopFilterRemover fRem = new NoopFilterRemover(plan);
        fRem.visit();

        boolean isMultiQuery = conf.getBoolean(PigConfiguration.PIG_OPT_MULTIQUERY, true);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Before multiquery optimization:");
            LOG.debug(plan);
        }

        if (isMultiQuery) {
            // reduces the number of SparkOpers in the Spark plan generated
            // by multi-query (multi-store) script.
            MultiQueryOptimizerSpark mqOptimizer = new MultiQueryOptimizerSpark(plan);
            mqOptimizer.visit();
        }

        //since JoinGroupOptimizerSpark modifies the plan and collapses LRA+GLA+PKG into POJoinGroupSpark while
        //CombinerOptimizer collapses GLA+PKG into ReduceBy, so if JoinGroupOptimizerSpark first, the spark plan will be
        //changed and not suitable for CombinerOptimizer.More detail see PIG-4797
        JoinGroupOptimizerSpark joinOptimizer = new JoinGroupOptimizerSpark(plan);
        joinOptimizer.visit();

        if (LOG.isDebugEnabled()) {
            LOG.debug("After multiquery optimization:");
            LOG.debug(plan);
        }
    }

    private void cleanUpSparkJob(SparkPigStats sparkStats) throws ExecException {
        LOG.info("Clean up Spark Job");
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
                        LOG.info(String.format("Delete ship file result: %b",
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
                        LOG.info(String.format("Delete cache file result: %b",
                                deleteFile.delete()));
                    }
                }
            }
        }

        // run cleanup for all of the stores
        for (OutputStats output : sparkStats.getOutputStats()) {
            POStore store = output.getPOStore();
            try {
                if (!output.isSuccessful()) {
                    store.getStoreFunc().cleanupOnFailure(
                            store.getSFile().getFileName(),
                            Job.getInstance(output.getConf()));
                } else {
                    store.getStoreFunc().cleanupOnSuccess(
                            store.getSFile().getFileName(),
                            Job.getInstance(output.getConf()));
                }
            } catch (IOException e) {
                throw new ExecException(e);
            } catch (AbstractMethodError nsme) {
                // Just swallow it.  This means we're running against an
                // older instance of a StoreFunc that doesn't implement
                // this method.
            }
        }
    }

    private void addFilesToSparkJob(SparkOperPlan sparkPlan) throws IOException {
        LOG.info("Add files Spark Job");
        String shipFiles = pigContext.getProperties().getProperty(
                "pig.streaming.ship.files");
        shipFiles(shipFiles);
        String cacheFiles = pigContext.getProperties().getProperty(
                "pig.streaming.cache.files");
        cacheFiles(cacheFiles);
        addUdfResourcesToSparkJob(sparkPlan);
    }

    private void addUdfResourcesToSparkJob(SparkOperPlan sparkPlan) throws IOException {
        SparkPOUserFuncVisitor sparkPOUserFuncVisitor = new SparkPOUserFuncVisitor(sparkPlan);
        sparkPOUserFuncVisitor.visit();
        Joiner joiner = Joiner.on(",");
        String shipFiles = joiner.join(sparkPOUserFuncVisitor.getShipFiles());
        shipFiles(shipFiles);
        String cacheFiles = joiner.join(sparkPOUserFuncVisitor.getCacheFiles());
        cacheFiles(cacheFiles);
    }

    private void shipFiles(String shipFiles)
            throws IOException {
        if (shipFiles != null && !shipFiles.isEmpty()) {
            for (String file : shipFiles.split(",")) {
                File shipFile = new File(file.trim());
                if (shipFile.exists()) {
                    addResourceToSparkJobWorkingDirectory(shipFile, shipFile.getName(),
                            shipFile.getName().endsWith(".jar") ? ResourceType.JAR : ResourceType.FILE );
                }
            }
        }
    }

    private void cacheFiles(String cacheFiles) throws IOException {
        if (cacheFiles != null && !cacheFiles.isEmpty()) {
            File tmpFolder = Files.createTempDirectory("cache").toFile();
            tmpFolder.deleteOnExit();
            for (String file : cacheFiles.split(",")) {
                String fileName = extractFileName(file.trim());
                if( fileName != null) {
                    String fileUrl = extractFileUrl(file.trim());
                    if( fileUrl != null) {
                        Path src = new Path(fileUrl);
                        File tmpFile = new File(tmpFolder, fileName);
                        Path tmpFilePath = new Path(tmpFile.getAbsolutePath());
                        FileSystem fs = tmpFilePath.getFileSystem(jobConf);
                        //TODO:PIG-5241 Specify the hdfs path directly to spark and avoid the unnecessary download and upload in SparkLauncher.java
                        fs.copyToLocalFile(src, tmpFilePath);
                        tmpFile.deleteOnExit();
                        LOG.info(String.format("CacheFile:%s", fileName));
                        if(!allCachedFiles.contains(file.trim())) {
                            allCachedFiles.add(file.trim());
                            addResourceToSparkJobWorkingDirectory(tmpFile, fileName,
                                     ResourceType.FILE);
                        }
                    }
                }
            }
        }
    }

    public static enum ResourceType {
        JAR,
        FILE
    }


    private void addJarsToSparkJob(SparkOperPlan sparkPlan) throws IOException {
        Set<String> allJars = new HashSet<String>();
        LOG.info("Add default jars to Spark Job");
        allJars.addAll(JarManager.getDefaultJars());
        JarManager.addPigTestJarIfPresent(allJars);
        LOG.info("Add script jars to Spark Job");
        for (String scriptJar : pigContext.scriptJars) {
            allJars.add(scriptJar);
        }

        LOG.info("Add udf jars to Spark Job");
        UDFJarsFinder udfJarsFinder = new UDFJarsFinder(sparkPlan, pigContext);
        udfJarsFinder.visit();
        Set<String> udfJars = udfJarsFinder.getUdfJars();
        for (String udfJar : udfJars) {
            allJars.add(udfJar);
        }

        File scriptUDFJarFile = JarManager.createPigScriptUDFJar(pigContext);
        if (scriptUDFJarFile != null) {
            LOG.info("Add script udf jar to Spark job");
            allJars.add(scriptUDFJarFile.getAbsolutePath().toString());
        }

        LOG.info("Add extra jars to Spark job");
        for (URL extraJarUrl : pigContext.extraJars) {
            allJars.add(extraJarUrl.getFile());
        }

        //Upload all jars to spark working directory
        for (String jar : allJars) {
            File jarFile = new File(jar);
            addResourceToSparkJobWorkingDirectory(jarFile, jarFile.getName(),
                    ResourceType.JAR);
        }
    }

    private void addResourceToSparkJobWorkingDirectory(File resourcePath,
                                                       String resourceName, ResourceType resourceType) throws IOException {
        if (resourceType == ResourceType.JAR) {
            LOG.info("Added jar " + resourceName);
        } else {
            LOG.info("Added file " + resourceName);
        }
        boolean isLocal = System.getenv("SPARK_MASTER") != null ? System
                .getenv("SPARK_MASTER").equalsIgnoreCase("LOCAL") : true;
        if (isLocal) {
            File localFile = new File(currentDirectoryPath + "/"
                    + resourceName);
            if (resourcePath.getAbsolutePath().equals(localFile.getAbsolutePath())
                    && resourcePath.exists()) {
                return;
            }
            // When multiple threads start SparkLauncher, delete/copy actions should be in a critical section
            synchronized(SparkLauncher.class) {
                if (localFile.exists()) {
                    LOG.info(String.format(
                            "Jar file %s exists, ready to delete",
                            localFile.getAbsolutePath()));
                    localFile.delete();
                } else {
                    LOG.info(String.format("Jar file %s not exists,",
                            localFile.getAbsolutePath()));
                }
                Files.copy(Paths.get(new Path(resourcePath.getAbsolutePath()).toString()),
                    Paths.get(localFile.getAbsolutePath()));
            }
        } else {
            if(resourceType == ResourceType.JAR){
                sparkContext.addJar(resourcePath.toURI().toURL()
                        .toExternalForm());
            }else if( resourceType == ResourceType.FILE){
                sparkContext.addFile(resourcePath.toURI().toURL()
                        .toExternalForm());
            }
        }
    }

    private String extractFileName(String cacheFileUrl) {
        String[] tmpAry = cacheFileUrl.split("#");
        String fileName = tmpAry != null && tmpAry.length == 2 ? tmpAry[1]
                : null;
        return fileName;
    }

    private String extractFileUrl(String cacheFileUrl) {
        String[] tmpAry = cacheFileUrl.split("#");
        String fileName = tmpAry != null && tmpAry.length == 2 ? tmpAry[0]
                : null;
        return fileName;
    }

    public SparkOperPlan compile(PhysicalPlan physicalPlan,
                                  PigContext pigContext) throws PlanException, IOException,
            VisitorException {
        SparkCompiler sparkCompiler = new SparkCompiler(physicalPlan,
                pigContext);
        sparkCompiler.compile();
        sparkCompiler.connectSoftLink();
        SparkOperPlan sparkPlan = sparkCompiler.getSparkPlan();

        // optimize key - value handling in package
        SparkPOPackageAnnotator pkgAnnotator = new SparkPOPackageAnnotator(
                sparkPlan);
        pkgAnnotator.visit();

        optimize(sparkPlan, pigContext);
        return sparkPlan;
    }


    private static String getMaster(PigContext pc){
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
        return master;
    }

    /**
     * Only one SparkContext may be active per JVM (SPARK-2243). When multiple threads start SparkLaucher,
     * the static member sparkContext should be initialized only by either local or cluster mode at a time.
     *
     * In case it was already initialized with a different mode than what the new pigContext instance wants, it will
     * close down the existing SparkContext and re-initalize it with the new mode.
     */
    private static synchronized void startSparkIfNeeded(JobConf jobConf, PigContext pc) throws PigException {
        String master = getMaster(pc);
        if (sparkContext != null && !master.equals(sparkContext.master())){
            sparkContext.close();
            sparkContext = null;
        }
        if (sparkContext == null) {
            String sparkHome = System.getenv("SPARK_HOME");
            if (!master.startsWith("local") && !master.equals("yarn")) {
                // Check that we have the Mesos native library and Spark home
                // are set
                if (sparkHome == null) {
                    System.err
                            .println("You need to set SPARK_HOME to run on a Mesos cluster!");
                    throw new PigException("SPARK_HOME is not set");
                }
            }

            SparkConf sparkConf = new SparkConf();
            Properties pigCtxtProperties = pc.getProperties();

            sparkConf.setMaster(master);
            sparkConf.setAppName(pigCtxtProperties.getProperty(PigContext.JOB_NAME,"pig"));
            // On Spark 1.6, Netty file server doesn't allow adding the same file with the same name twice
            // This is a problem for streaming using a script + explicit ship the same script combination (PIG-5134)
            // HTTP file server doesn't have this restriction, it overwrites the file if added twice
            String useNettyFileServer = pigCtxtProperties.getProperty(PigConfiguration.PIG_SPARK_USE_NETTY_FILESERVER, "false");
            sparkConf.set("spark.rpc.useNettyFileServer", useNettyFileServer);

            if (sparkHome != null && !sparkHome.isEmpty()) {
                sparkConf.setSparkHome(sparkHome);
            } else {
                LOG.warn("SPARK_HOME is not set");
            }

            String sparkConfEnv = System.getenv("SPARK_CONF_DIR");
            if( sparkConfEnv == null && sparkHome != null) {
                sparkConfEnv = sparkHome + "/conf";
            }
            if( sparkConfEnv != null ) {
                try {
                    Properties props = new Properties();
                    File propsFile = new File (sparkConfEnv,"spark-defaults.conf");
                    if (propsFile.isFile()) {
                        try (InputStreamReader isr = new InputStreamReader( 
                            new FileInputStream(propsFile), StandardCharsets.UTF_8)) {
                            props.load(isr);
                            for (Map.Entry<Object, Object> e : props.entrySet()) {
                                pigCtxtProperties.setProperty(e.getKey().toString(), 
                                                     e.getValue().toString().trim());
                            }
                        }
                    }
                } catch (IOException ex) {
                  LOG.warn("Reading $SPARK_CONF_DIR/spark-defaults.conf failed");
                }
            }


            //Copy all spark.* properties to SparkConf
            for (String key : pigCtxtProperties.stringPropertyNames()) {
                if (key.startsWith("spark.")) {
                    LOG.debug("Copying key " + key + " with value " +
                            pigCtxtProperties.getProperty(key) + " to SparkConf");
                    sparkConf.set(key, pigCtxtProperties.getProperty(key));
                }
            }

            //see PIG-5200 why need to set spark.executor.userClassPathFirst as true on cluster modes
            if (! "local".equals(master)) {
                sparkConf.set("spark.executor.userClassPathFirst", "true");
            }
            checkAndConfigureDynamicAllocation(master, sparkConf);

            sparkContext = new JavaSparkContext(sparkConf);
            SparkShims.getInstance().addSparkListener(sparkContext.sc(), jobStatisticCollector.getSparkListener());
            SparkShims.getInstance().addSparkListener(sparkContext.sc(), new StatsReportListener());
            allCachedFiles = new HashSet<String>();
        }
        jobConf.set(SPARK_VERSION, sparkContext.version());
    }

    private static void checkAndConfigureDynamicAllocation(String master, SparkConf sparkConf) {
        if (sparkConf.getBoolean("spark.dynamicAllocation.enabled", false)) {
            if (!master.startsWith("yarn")) {
                LOG.warn("Dynamic allocation is enabled, but " +
                        "script isn't running on yarn. Ignoring ...");
            }
            if (!sparkConf.getBoolean("spark.shuffle.service.enabled", false)) {
                LOG.info("Spark shuffle service is being enabled as dynamic " +
                        "allocation is enabled");
                sparkConf.set("spark.shuffle.service.enabled", "true");
            }
        }
    }

    // You can use this in unit tests to stop the SparkContext between tests.
    static void stopSpark() {
        if (sparkContext != null) {
            sparkContext.stop();
            sparkContext = null;
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
        List<OperatorKey> operKeyList = new ArrayList<>(allOperKeys.keySet());
        Collections.sort(operKeyList);

        if (format.equals("text")) {
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
            SparkPrinter printer = new SparkPrinter(ps, sparkPlan);
            printer.setVerbose(verbose);
            printer.visit();
        } else if (format.equals("dot")) {
            ps.println("#--------------------------------------------------");
            ps.println("# Spark Plan");
            ps.println("#--------------------------------------------------");

            DotSparkPrinter printer = new DotSparkPrinter(sparkPlan, ps);
            printer.setVerbose(verbose);
            printer.dump();
            ps.println("");
        } else if (format.equals("xml")) {
            try {
                XMLSparkPrinter printer = new XMLSparkPrinter(ps, sparkPlan);
                printer.visit();
                printer.closePlan();
            } catch (ParserConfigurationException e) {
                e.printStackTrace();
            } catch (TransformerException e) {
                e.printStackTrace();
            }
        }
        else {
            throw new IOException(
                    "Unsupported explain format. Supported formats are: text, dot, xml");
        }
    }

    @Override
    public void kill() throws BackendException {
        if (sparkContext != null) {
            sparkContext.stop();
            sparkContext = null;
        }
    }

    @Override
    public void killJob(String jobID, Configuration conf)
            throws BackendException {
        if (sparkContext != null) {
            sparkContext.stop();
            sparkContext = null;
        }
    }

    /**
     * We store the value of udf.import.list in SparkEngineConf#properties
     * Later we will serialize it in SparkEngineConf#writeObject and deserialize in SparkEngineConf#readObject. More
     * detail see PIG-4920
     */
    private void saveUdfImportList() {
        String udfImportList = Joiner.on(",").join(PigContext.getPackageImportList());
        sparkEngineConf.setSparkUdfImportListStr(udfImportList);
    }

    private void initialize(PhysicalPlan physicalPlan) throws IOException {
        saveUdfImportList();
        jobConf = SparkUtil.newJobConf(pigContext, physicalPlan, sparkEngineConf);
        SchemaTupleBackend.initialize(jobConf, pigContext);
        Utils.setDefaultTimeZone(jobConf);
        PigMapReduce.sJobConfInternal.set(jobConf);
        String parallelism = pigContext.getProperties().getProperty("spark.default.parallelism");
        if (parallelism != null) {
            SparkPigContext.get().setDefaultParallelism(Integer.parseInt(parallelism));
        }
    }

    /**
     * Creates new SparkCounters instance for the job, initializes aggregate warning counters if required
     * @param jobConf
     * @throws IOException
     */
    private static void prepareSparkCounters(JobConf jobConf) throws IOException {
        SparkPigStatusReporter statusReporter = SparkPigStatusReporter.getInstance();
        SparkCounters counters = new SparkCounters(sparkContext);

        if ("true".equalsIgnoreCase(jobConf.get("aggregate.warning"))) {
            SparkCounterGroup pigWarningGroup = new SparkCounterGroup.MapSparkCounterGroup(
                    PIG_WARNING_FQCN, PIG_WARNING_FQCN,sparkContext
            );
            pigWarningGroup.createCounter(PigWarning.SPARK_WARN.name(), new HashMap<String,Long>());
            pigWarningGroup.createCounter(PigWarning.SPARK_CUSTOM_WARN.name(), new HashMap<String,Long>());
            counters.getSparkCounterGroups().put(PIG_WARNING_FQCN, pigWarningGroup);
        }
        statusReporter.setCounters(counters);
        jobConf.set("pig.spark.counters", ObjectSerializer.serialize(counters));
    }
}
