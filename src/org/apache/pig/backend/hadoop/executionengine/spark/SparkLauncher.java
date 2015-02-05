package org.apache.pig.backend.hadoop.executionengine.spark;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.Lists;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.pig.PigConstants;
import org.apache.pig.PigException;
import org.apache.pig.backend.BackendException;
import org.apache.pig.backend.hadoop.executionengine.Launcher;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MRCompiler;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.POPackageAnnotator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POCollectedGroup;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POCounter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.PODistinct;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFilter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POGlobalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLimit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.PORank;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSkewedJoin;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSort;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSplit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POUnion;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStream;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.CollectedGroupConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.CounterConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.DistinctConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.FilterConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.ForEachConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.GlobalRearrangeConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.LimitConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.LoadConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.LocalRearrangeConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.POConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.PackageConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.RankConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.SkewedJoinConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.SortConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.SplitConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.StoreConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.UnionConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.StreamConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.operator.POStreamSpark;
import org.apache.pig.backend.hadoop.executionengine.util.MapRedUtil;
import org.apache.pig.data.SchemaTupleBackend;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.spark.SparkPigStats;
import org.apache.pig.tools.pigstats.spark.SparkStatsUtil;
import org.apache.spark.rdd.RDD;
import org.apache.spark.scheduler.JobLogger;
import org.apache.spark.scheduler.StatsReportListener;
import org.apache.spark.api.java.JavaSparkContext;

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

    public static BroadCastServer bcaster;
    private static final Matcher DISTRIBUTED_CACHE_ARCHIVE_MATCHER = Pattern
            .compile("\\.(zip|tgz|tar\\.gz|tar)$").matcher("");
    // An object that handle cache calls in the operator graph. This is again
    // static because we want
    // it to be shared across SparkLaunchers. It gets cleared whenever we close
    // the SparkContext.
    // private static CacheConverter cacheConverter = null;
    private String jobGroupID;

    @Override
    public PigStats launchPig(PhysicalPlan physicalPlan, String grpName,
            PigContext pigContext) throws Exception {
        LOG.info("!!!!!!!!!!  Launching Spark (woot) !!!!!!!!!!!!");
        LOG.debug(physicalPlan);
        JobConf c = SparkUtil.newJobConf(pigContext);
        c.set(PigConstants.LOCAL_CODE_DIR, System.getProperty("java.io.tmpdir"));

        SchemaTupleBackend.initialize(c, pigContext);

        // Code pulled from MapReduceLauncher
        MRCompiler mrCompiler = new MRCompiler(physicalPlan, pigContext);
        mrCompiler.compile();
        MROperPlan plan = mrCompiler.getMRPlan();
        POPackageAnnotator pkgAnnotator = new POPackageAnnotator(plan);
        pkgAnnotator.visit();

        if (System.getenv("BROADCAST_PORT") == null
                && System.getenv("BROADCAST_MASTER_IP") == null) {
            LOG.warn("Missing BROADCAST_POST/BROADCAST_HOST in the environment.");
        } else {
            if (bcaster == null) {
                bcaster = new BroadCastServer();
                bcaster.startBroadcastServer(Integer.parseInt(System
                        .getenv("BROADCAST_PORT")));
            }
        }

        SparkPigStats sparkStats = (SparkPigStats)
            pigContext.getExecutionEngine().instantiatePigStats();
        PigStats.start(sparkStats);

        startSparkIfNeeded();
        // Set a unique group id for this query, so we can lookup all Spark job ids
        // related to this query.
        jobGroupID = UUID.randomUUID().toString();
        sparkContext.setJobGroup(jobGroupID, "Pig query to Spark cluster", false);
        jobMetricsListener.reset();

        String currentDirectoryPath = Paths.get(".").toAbsolutePath().normalize().toString() + "/";
        startSparkJob(pigContext, currentDirectoryPath);
        LinkedList<POStore> stores = PlanHelper.getPhysicalOperators(
                physicalPlan, POStore.class);
        POStore firstStore = stores.getFirst();
        if( firstStore != null ){
            MapRedUtil.setupStreamingDirsConfSingle(firstStore, pigContext, c);
        }

        //        ObjectSerializer.serialize(c);
        byte[] confBytes = KryoSerializer.serializeJobConf(c);
        // initialize the supported converters
        Map<Class<? extends PhysicalOperator>, POConverter> convertMap = new HashMap<Class<? extends PhysicalOperator>, POConverter>();

        convertMap.put(POLoad.class, new LoadConverter(pigContext,
                physicalPlan, sparkContext.sc()));
        convertMap.put(POStore.class, new StoreConverter(pigContext));
        convertMap.put(POForEach.class, new ForEachConverter(confBytes));
        convertMap.put(POFilter.class, new FilterConverter());
        convertMap.put(POPackage.class, new PackageConverter(confBytes
        ));
        // convertMap.put(POCache.class, cacheConverter);
        convertMap.put(POLocalRearrange.class, new LocalRearrangeConverter());
        convertMap.put(POGlobalRearrange.class, new GlobalRearrangeConverter());
        convertMap.put(POLimit.class, new LimitConverter());
        convertMap.put(PODistinct.class, new DistinctConverter());
        convertMap.put(POUnion.class, new UnionConverter(sparkContext.sc()));
        convertMap.put(POSort.class, new SortConverter());
        convertMap.put(POSplit.class, new SplitConverter());
        convertMap.put(POSkewedJoin.class, new SkewedJoinConverter());
        convertMap.put(POCollectedGroup.class, new CollectedGroupConverter());
        convertMap.put(POCounter.class, new CounterConverter());
        convertMap.put(PORank.class, new RankConverter());
        convertMap.put(POStreamSpark.class,new StreamConverter(confBytes));

        Map<OperatorKey, RDD<Tuple>> rdds = new HashMap<OperatorKey, RDD<Tuple>>();

        Set<Integer> seenJobIDs = new HashSet<Integer>();
        for (POStore poStore : stores) {
            physicalToRDD(physicalPlan, poStore, rdds, convertMap);
            for (int jobID : getJobIDs(seenJobIDs)) {
                SparkStatsUtil.waitForJobAddStats(jobID, poStore,
                    jobMetricsListener, sparkContext, sparkStats, c);
            }
        }

        cleanUpSparkJob(pigContext,currentDirectoryPath);
        sparkStats.finish();
        return sparkStats;
    }


    /**
     * In Spark, currently only async actions return job id.
     * There is no async equivalent of actions like saveAsNewAPIHadoopFile()
     *
     * The only other way to get a job id is to register a "job group ID" with the
     * spark context and request all job ids corresponding to that job group via
     * getJobIdsForGroup.
     *
     * However getJobIdsForGroup does not guarantee the order of the elements in
     * it's result.
     *
     * This method simply returns the previously unseen job ids.
     *
     * @param seenJobIDs job ids in the job group that are already seen
     * @return Spark job ids not seen before
     */
    private List<Integer> getJobIDs(Set<Integer> seenJobIDs) {
        Set<Integer> groupjobIDs = new HashSet<Integer>(Arrays.asList(
            ArrayUtils.toObject(sparkContext.statusTracker()
                .getJobIdsForGroup(jobGroupID))));
        groupjobIDs.removeAll(seenJobIDs);
        List<Integer> unseenJobIDs = new ArrayList<Integer>(groupjobIDs);
        if (unseenJobIDs.size() == 0) {
          throw new RuntimeException("Expected at least one unseen jobID " +
              " in this call to getJobIdsForGroup, but got " + unseenJobIDs.size());
        }

        seenJobIDs.addAll(unseenJobIDs);
        return unseenJobIDs;
    }

    private void cleanUpSparkJob(PigContext pigContext, String currentDirectoryPath) {
        LOG.info("clean up Spark Job");
        boolean isLocal = System.getenv("SPARK_MASTER")!= null?System.getenv("SPARK_MASTER").equalsIgnoreCase("LOCAL"): true;
        if (isLocal) {
            String shipFiles = pigContext.getProperties().getProperty("pig.streaming.ship.files");
            if (shipFiles != null) {
                for (String file : shipFiles.split(",")) {
                    File shipFile = new File(file);
                    File deleteFile = new File(currentDirectoryPath + "/" + shipFile.getName());
                    if (deleteFile.exists()) {
                        LOG.info(String.format("delete ship file result: %b", deleteFile.delete()));
                    }
                }
            }
            String cacheFiles = pigContext.getProperties().getProperty("pig.streaming.cache.files");
            if (cacheFiles != null) {
                for (String file : cacheFiles.split(",")) {
                    String fileName = extractFileName(file.trim());
                    File deleteFile = new File(currentDirectoryPath + "/" + fileName);
                    if (deleteFile.exists()) {
                        LOG.info(String.format("delete cache file result: %b", deleteFile.delete()));
                    }
                }
            }
        }
    }

    private void startSparkJob(PigContext pigContext, String currentDirectoryPath) throws IOException {
        LOG.info("start Spark Job");
        String shipFiles = pigContext.getProperties().getProperty("pig.streaming.ship.files");
        shipFiles(shipFiles, currentDirectoryPath);
        String cacheFiles = pigContext.getProperties().getProperty("pig.streaming.cache.files");
        cacheFiles(cacheFiles, currentDirectoryPath, pigContext);
    }

    private void shipFiles(String shipFiles, String currentDirectoryPath) throws IOException {
        if (shipFiles != null) {
            for (String file : shipFiles.split(",")) {
                File shipFile = new File(file.trim());
                if (shipFile.exists()) {
                    LOG.info(String.format("shipFile:%s",shipFile));
                    boolean isLocal = System.getenv("SPARK_MASTER")!= null?System.getenv("SPARK_MASTER").equalsIgnoreCase("LOCAL"): true;
                    if (isLocal) {
                        File localFile = new File(currentDirectoryPath+"/" + shipFile.getName());
                        if( localFile.exists()){
                            LOG.info(String.format("ship file %s exists, ready to delete",localFile.getAbsolutePath()));
                            localFile.delete();
                        } else{
                            LOG.info(String.format("ship file %s  not exists,",localFile.getAbsolutePath()));
                        }
                        Files.copy(shipFile.toPath(), Paths.get(localFile.getAbsolutePath()));
                    } else {
                        sparkContext.addFile(shipFile.toURI().toURL().toExternalForm());
                    }
                }
            }
        }
    }

    private void cacheFiles(String cacheFiles, String currentDirectoryPath, PigContext pigContext) throws IOException {
        if (cacheFiles != null) {
            Configuration conf = SparkUtil.newJobConf(pigContext);
            boolean isLocal = System.getenv("SPARK_MASTER")!= null?System.getenv("SPARK_MASTER").equalsIgnoreCase("LOCAL"): true;
            for (String file : cacheFiles.split(",")) {
                String fileName = extractFileName(file.trim());
                Path src = new Path(extractFileUrl(file.trim()));
                File tmpFile = File.createTempFile(fileName,".tmp");
                Path tmpFilePath = new Path(tmpFile.getAbsolutePath());
                FileSystem fs = tmpFilePath.getFileSystem(conf);
                fs.copyToLocalFile(src, tmpFilePath);
                tmpFile.deleteOnExit();
                if (isLocal) {
                    File localFile = new File(currentDirectoryPath + "/" + fileName);
                    if( localFile.exists()){
                        LOG.info(String.format("cache file %s exists, ready to delete",localFile.getAbsolutePath()));
                        localFile.delete();
                    } else{
                        LOG.info(String.format("cache file %s not exists,",localFile.getAbsolutePath()));
                    }
                    Files.copy( Paths.get(tmpFilePath.toString()), Paths.get(localFile.getAbsolutePath()));
                } else {
                    sparkContext.addFile(tmpFile.toURI().toURL().toExternalForm());
                }
            }
        }
    }

    private String extractFileName(String cacheFileUrl) {
        String[] tmpAry = cacheFileUrl.split("#");
        String fileName = tmpAry != null && tmpAry.length == 2 ? tmpAry[1] : null;
        if (fileName == null) {
            throw new RuntimeException("cache file is invalid format, file:" + cacheFileUrl);
        } else {
            LOG.debug("cache file name is valid:" + cacheFileUrl);
            return fileName;
        }
    }

    private String extractFileUrl(String cacheFileUrl) {
        String[] tmpAry = cacheFileUrl.split("#");
        String fileName = tmpAry != null && tmpAry.length == 2 ? tmpAry[0] : null;
        if (fileName == null) {
            throw new RuntimeException("cache file is invalid format, file:" + cacheFileUrl);
        } else {
            LOG.debug("cache file name is valid:" + cacheFileUrl);
            return fileName;
        }
    }

    private static void startSparkIfNeeded() throws PigException {
        if (sparkContext == null) {
            String master = System.getenv("SPARK_MASTER");
            if (master == null) {
                LOG.info("SPARK_MASTER not specified, using \"local\"");
                master = "local";
            }

            String sparkHome = System.getenv("SPARK_HOME"); // It's okay if this
            // is null for local
            // mode
            String sparkJarsSetting = System.getenv("SPARK_JARS");
            String pigJar = System.getenv("SPARK_PIG_JAR");
            String[] sparkJars = sparkJarsSetting == null ? new String[] {}
                    : sparkJarsSetting.split(",");

            // TODO: Don't hardcode this JAR
            List<String> jars = Lists.asList(pigJar, sparkJars);

            if (!master.startsWith("local") && !master.equals("yarn-client")) {
                // Check that we have the Mesos native library and Spark home
                // are set
                if (sparkHome == null) {
                    System.err
                            .println("You need to set SPARK_HOME to run on a Mesos cluster!");
                    throw new PigException("SPARK_HOME is not set");
                }
                /*
                 * if (System.getenv("MESOS_NATIVE_LIBRARY") == null) {
                 *
                 * System.err.println(
                 * "You need to set MESOS_NATIVE_LIBRARY to run on a Mesos cluster!"
                 * ); throw new PigException("MESOS_NATIVE_LIBRARY is not set");
                 * }
                 *
                 * // Tell Spark to use Mesos in coarse-grained mode (only
                 * affects Spark 0.6+; no impact on others)
                 * System.setProperty("spark.mesos.coarse", "true");
                 */
            }

//            // For coarse-grained Mesos mode, tell it an upper bound on how many
//            // cores to grab in total;
//            // we conservatively set this to 32 unless the user set the
//            // SPARK_MAX_CPUS environment variable.
//            if (System.getenv("SPARK_MAX_CPUS") != null) {
//                int maxCores = 32;
//                maxCores = Integer.parseInt(System.getenv("SPARK_MAX_CPUS"));
//                System.setProperty("spark.cores.max", "" + maxCores);
//            }
//            System.setProperty("spark.cores.max", "1");
//            System.setProperty("spark.executor.memory", "" + "512m");
//            System.setProperty("spark.shuffle.memoryFraction", "0.0");
//            System.setProperty("spark.storage.memoryFraction", "0.0");

            sparkContext = new JavaSparkContext(master,
                    "Spork", sparkHome, jars.toArray(new String[jars.size()]));
            sparkContext.sc().addSparkListener(new StatsReportListener());
            sparkContext.sc().addSparkListener(new JobLogger());
            sparkContext.sc().addSparkListener(jobMetricsListener);
            // cacheConverter = new CacheConverter();
        }
    }

    // You can use this in unit tests to stop the SparkContext between tests.
    static void stopSpark() {
        if (sparkContext != null) {
            sparkContext.stop();
            sparkContext = null;
            // cacheConverter = null;
        }
    }

    private void physicalToRDD(PhysicalPlan plan,
            PhysicalOperator physicalOperator,
            Map<OperatorKey, RDD<Tuple>> rdds,
            Map<Class<? extends PhysicalOperator>, POConverter> convertMap)
            throws IOException {

        RDD<Tuple> nextRDD = null;
        List<PhysicalOperator> predecessors = plan
                .getPredecessors(physicalOperator);
        List<RDD<Tuple>> predecessorRdds = Lists.newArrayList();
        if (predecessors != null) {
            for (PhysicalOperator predecessor : predecessors) {
                physicalToRDD(plan, predecessor, rdds, convertMap);
                predecessorRdds.add(rdds.get(predecessor.getOperatorKey()));
            }
        }

        if( physicalOperator instanceof  POStream ){
            POStream poStream = (POStream)physicalOperator;
            physicalOperator = new POStreamSpark(poStream);
        }

        POConverter converter = convertMap.get(physicalOperator.getClass());
        if (converter == null) {
            throw new IllegalArgumentException(
                    "Spork unsupported PhysicalOperator: " + physicalOperator);
        }

        LOG.info("Converting operator "
                + physicalOperator.getClass().getSimpleName() + " "
                + physicalOperator);
        nextRDD = converter.convert(predecessorRdds, physicalOperator);

        if (POStore.class.equals(physicalOperator.getClass())) {
            return;
        }

        if (nextRDD == null) {
            throw new IllegalArgumentException(
                    "RDD should not be null after PhysicalOperator: "
                            + physicalOperator);
        }

        rdds.put(physicalOperator.getOperatorKey(), nextRDD);
    }

    @Override
    public void explain(PhysicalPlan pp, PigContext pc, PrintStream ps,
            String format, boolean verbose) throws IOException {
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
}