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

import com.google.common.collect.Lists;

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
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStream;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POUnion;
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
import org.apache.pig.backend.hadoop.executionengine.spark.converter.StreamConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.UnionConverter;
import org.apache.pig.backend.hadoop.executionengine.spark.optimizer.AccumulatorOptimizer;
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
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.spark.SparkPigStats;
import org.apache.pig.tools.pigstats.spark.SparkStatsUtil;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.scheduler.JobLogger;
import org.apache.spark.scheduler.StatsReportListener;
import org.apache.spark.SparkException;

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

	@Override
	public PigStats launchPig(PhysicalPlan physicalPlan, String grpName,
			PigContext pigContext) throws Exception {
		if (LOG.isDebugEnabled())
		    LOG.debug(physicalPlan);
		JobConf jobConf = SparkUtil.newJobConf(pigContext);
		jobConf.set(PigConstants.LOCAL_CODE_DIR,
				System.getProperty("java.io.tmpdir"));

		SchemaTupleBackend.initialize(jobConf, pigContext);
		SparkOperPlan sparkplan = compile(physicalPlan, pigContext);
		if (LOG.isDebugEnabled())
			  explain(sparkplan, System.out, "text", true);
		SparkPigStats sparkStats = (SparkPigStats) pigContext
				.getExecutionEngine().instantiatePigStats();
		PigStats.start(sparkStats);

		startSparkIfNeeded(pigContext);

		// Set a unique group id for this query, so we can lookup all Spark job
		// ids
		// related to this query.
		jobGroupID = UUID.randomUUID().toString();
		sparkContext.setJobGroup(jobGroupID, "Pig query to Spark cluster",
				false);
		jobMetricsListener.reset();

		String currentDirectoryPath = Paths.get(".").toAbsolutePath()
				.normalize().toString()
				+ "/";
		startSparkJob(pigContext, currentDirectoryPath);
		LinkedList<POStore> stores = PlanHelper.getPhysicalOperators(
				physicalPlan, POStore.class);
		POStore firstStore = stores.getFirst();
		if (firstStore != null) {
			MapRedUtil.setupStreamingDirsConfSingle(firstStore, pigContext,
					jobConf);
		}

		byte[] confBytes = KryoSerializer.serializeJobConf(jobConf);

		// Create conversion map, mapping between pig operator and spark convertor
		Map<Class<? extends PhysicalOperator>, POConverter> convertMap = new HashMap<Class<? extends PhysicalOperator>, POConverter>();
		convertMap.put(POLoad.class, new LoadConverter(pigContext,
				physicalPlan, sparkContext.sc()));
		convertMap.put(POStore.class, new StoreConverter(pigContext));
		convertMap.put(POForEach.class, new ForEachConverter(confBytes));
		convertMap.put(POFilter.class, new FilterConverter());
		convertMap.put(POPackage.class, new PackageConverter(confBytes));
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
		convertMap.put(POStream.class, new StreamConverter(confBytes));

		sparkPlanToRDD(sparkplan, convertMap, sparkStats, jobConf);
		cleanUpSparkJob(pigContext, currentDirectoryPath);
		sparkStats.finish();

		return sparkStats;
	}

	private void optimize(PigContext pc, SparkOperPlan plan) throws VisitorException {
		boolean isAccumulator =
				Boolean.valueOf(pc.getProperties().getProperty("opt.accumulator","true"));
		if (isAccumulator) {
			AccumulatorOptimizer accumulatorOptimizer = new AccumulatorOptimizer(plan);
			accumulatorOptimizer.visit();
		}
	}

	/**
	 * In Spark, currently only async actions return job id. There is no async
	 * equivalent of actions like saveAsNewAPIHadoopFile()
	 * 
	 * The only other way to get a job id is to register a "job group ID" with
	 * the spark context and request all job ids corresponding to that job group
	 * via getJobIdsForGroup.
	 * 
	 * However getJobIdsForGroup does not guarantee the order of the elements in
	 * it's result.
	 * 
	 * This method simply returns the previously unseen job ids.
	 * 
	 * @param seenJobIDs
	 *            job ids in the job group that are already seen
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

	private void cleanUpSparkJob(PigContext pigContext,
			String currentDirectoryPath) {
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

	private void startSparkJob(PigContext pigContext,
			String currentDirectoryPath) throws IOException {
		LOG.info("start Spark Job");
		String shipFiles = pigContext.getProperties().getProperty(
				"pig.streaming.ship.files");
		shipFiles(shipFiles, currentDirectoryPath);
		String cacheFiles = pigContext.getProperties().getProperty(
				"pig.streaming.cache.files");
		cacheFiles(cacheFiles, currentDirectoryPath, pigContext);
	}

	private void shipFiles(String shipFiles, String currentDirectoryPath)
			throws IOException {
		if (shipFiles != null) {
			for (String file : shipFiles.split(",")) {
				File shipFile = new File(file.trim());
				if (shipFile.exists()) {
					LOG.info(String.format("shipFile:%s", shipFile));
					boolean isLocal = System.getenv("SPARK_MASTER") != null ? System
							.getenv("SPARK_MASTER").equalsIgnoreCase("LOCAL")
							: true;
					if (isLocal) {
						File localFile = new File(currentDirectoryPath + "/"
								+ shipFile.getName());
						if (localFile.exists()) {
							LOG.info(String.format(
									"ship file %s exists, ready to delete",
									localFile.getAbsolutePath()));
							localFile.delete();
						} else {
							LOG.info(String.format("ship file %s  not exists,",
									localFile.getAbsolutePath()));
						}
						Files.copy(shipFile.toPath(),
								Paths.get(localFile.getAbsolutePath()));
					} else {
						sparkContext.addFile(shipFile.toURI().toURL()
								.toExternalForm());
					}
				}
			}
		}
	}

	private void cacheFiles(String cacheFiles, String currentDirectoryPath,
			PigContext pigContext) throws IOException {
		if (cacheFiles != null) {
			Configuration conf = SparkUtil.newJobConf(pigContext);
			boolean isLocal = System.getenv("SPARK_MASTER") != null ? System
					.getenv("SPARK_MASTER").equalsIgnoreCase("LOCAL") : true;
			for (String file : cacheFiles.split(",")) {
				String fileName = extractFileName(file.trim());
				Path src = new Path(extractFileUrl(file.trim()));
				File tmpFile = File.createTempFile(fileName, ".tmp");
				Path tmpFilePath = new Path(tmpFile.getAbsolutePath());
				FileSystem fs = tmpFilePath.getFileSystem(conf);
				fs.copyToLocalFile(src, tmpFilePath);
				tmpFile.deleteOnExit();
				if (isLocal) {
					File localFile = new File(currentDirectoryPath + "/"
							+ fileName);
					if (localFile.exists()) {
						LOG.info(String.format(
								"cache file %s exists, ready to delete",
								localFile.getAbsolutePath()));
						localFile.delete();
					} else {
						LOG.info(String.format("cache file %s not exists,",
								localFile.getAbsolutePath()));
					}
					Files.copy(Paths.get(tmpFilePath.toString()),
							Paths.get(localFile.getAbsolutePath()));
				} else {
					sparkContext.addFile(tmpFile.toURI().toURL()
							.toExternalForm());
				}
			}
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
			String[] sparkJars = sparkJarsSetting == null ? new String[] {}
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
			Map<Class<? extends PhysicalOperator>, POConverter> convertMap,
			SparkPigStats sparkStats, JobConf jobConf) throws IOException,
			InterruptedException {
		Set<Integer> seenJobIDs = new HashSet<Integer>();
		if (sparkPlan != null) {
			List<SparkOperator> leaves = sparkPlan.getLeaves();
			Collections.sort(leaves);
			Map<OperatorKey, RDD<Tuple>> sparkOpToRdds = new HashMap();
			if (LOG.isDebugEnabled())
			    LOG.debug("Converting " + leaves.size() + " Spark Operators");
			for (SparkOperator leaf : leaves) {
				new PhyPlanSetter(leaf.physicalPlan).visit();
				Map<OperatorKey, RDD<Tuple>> physicalOpToRdds = new HashMap();
				sparkOperToRDD(sparkPlan, leaf, sparkOpToRdds,
						physicalOpToRdds, convertMap, seenJobIDs, sparkStats,
						jobConf);
			}
		} else {
			throw new RuntimeException("sparkPlan is null");
		}
	}

	private void sparkOperToRDD(SparkOperPlan sparkPlan,
			SparkOperator sparkOperator,
			Map<OperatorKey, RDD<Tuple>> sparkOpRdds,
			Map<OperatorKey, RDD<Tuple>> physicalOpRdds,
			Map<Class<? extends PhysicalOperator>, POConverter> convertMap,
			Set<Integer> seenJobIDs, SparkPigStats sparkStats, JobConf conf)
			throws IOException, InterruptedException {
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
		if (leafPOs != null && leafPOs.size() != 1) {
			throw new IllegalArgumentException(
					String.format(
							"sparkOperator "
									+ ".physicalPlan should have 1 leaf, but  sparkOperator"
									+ ".physicalPlan.getLeaves():{} not equals 1, sparkOperator"
									+ "sparkOperator:{}",
							sparkOperator.physicalPlan.getLeaves().size(),
							sparkOperator.name()));
		} else {
			PhysicalOperator leafPO = leafPOs.get(0);
			try {
				physicalToRDD(sparkOperator.physicalPlan, leafPO, physicalOpRdds,
						predecessorRDDs, convertMap);
				sparkOpRdds.put(sparkOperator.getOperatorKey(),
						physicalOpRdds.get(leafPO.getOperatorKey()));
			} catch(Exception e) {
				if( e instanceof  SparkException) {
					LOG.info("throw SparkException, error founds when running " +
							"rdds in spark");
				}
				exception = e;
				isFail = true;
			}
		}

		List<POStore> poStores = PlanHelper.getPhysicalOperators(
				sparkOperator.physicalPlan, POStore.class);
		if (poStores != null && poStores.size() == 1) {
			  POStore poStore = poStores.get(0);
            if (!isFail) {
                for (int jobID : getJobIDs(seenJobIDs)) {
                    SparkStatsUtil.waitForJobAddStats(jobID, poStore,
                            jobMetricsListener, sparkContext, sparkStats, conf);
                }
            } else {
                String failJobID = sparkOperator.name().concat("_fail");
                SparkStatsUtil.addFailJobStats(failJobID, poStore, sparkStats,
                        conf, exception);
            }
        } else {
			      LOG.info(String
					      .format(String.format("sparkOperator:{} does not have POStore or" +
                    " sparkOperator has more than 1 POStore. {} is the size of POStore."),
                    sparkOperator.name(), poStores.size()));
		}
	}

	private void physicalToRDD(PhysicalPlan plan,
			PhysicalOperator physicalOperator,
			Map<OperatorKey, RDD<Tuple>> rdds,
			List<RDD<Tuple>> rddsFromPredeSparkOper,
			Map<Class<? extends PhysicalOperator>, POConverter> convertMap)
			throws IOException {
        RDD<Tuple> nextRDD = null;
        List<PhysicalOperator> predecessors = plan
                .getPredecessors(physicalOperator);
        if (predecessors != null) {
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

		POConverter converter = convertMap.get(physicalOperator.getClass());
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
}
