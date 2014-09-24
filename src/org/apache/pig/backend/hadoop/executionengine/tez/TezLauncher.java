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

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.PigConfiguration;
import org.apache.pig.PigWarning;
import org.apache.pig.backend.BackendException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.executionengine.Launcher;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezCompiler;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezOperPlan;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezPOPackageAnnotator;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezPlanContainer;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezPlanContainerNode;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezPlanContainerPrinter;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.operator.NativeTezOper;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.optimizer.AccumulatorOptimizer;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.optimizer.CombinerOptimizer;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.optimizer.LoaderProcessor;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.optimizer.MultiQueryOptimizerTez;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.optimizer.NoopFilterRemover;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.optimizer.ParallelismSetter;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.optimizer.SecondaryKeyOptimizerTez;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.optimizer.UnionOptimizer;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.plan.CompilationMessageCollector;
import org.apache.pig.impl.plan.CompilationMessageCollector.MessageType;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.tools.pigstats.OutputStats;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.tez.TezScriptState;
import org.apache.pig.tools.pigstats.tez.TezStats;
import org.apache.pig.tools.pigstats.tez.TezTaskStats;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;

/**
 * Main class that launches pig for Tez
 */
public class TezLauncher extends Launcher {
    private static final Log log = LogFactory.getLog(TezLauncher.class);
    private ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(1);
    private boolean aggregateWarning = false;
    private TezScriptState tezScriptState;
    private TezStats tezStats;
    private TezJob runningJob;

    @Override
    public PigStats launchPig(PhysicalPlan php, String grpName, PigContext pc) throws Exception {
        if (pc.getExecType().isLocal()) {
            pc.getProperties().setProperty(TezConfiguration.TEZ_LOCAL_MODE, "true");
            pc.getProperties().setProperty(TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH, "true");
            pc.getProperties().setProperty("tez.ignore.lib.uris", "true");
        }
        Configuration conf = ConfigurationUtil.toConfiguration(pc.getProperties(), true);
        if (pc.defaultParallel == -1 && !conf.getBoolean(PigConfiguration.TEZ_AUTO_PARALLELISM, true)) {
            pc.defaultParallel = 1;
        }
        aggregateWarning = conf.getBoolean("aggregate.warning", false);

        TezResourceManager tezResourceManager = TezResourceManager.getInstance();
        tezResourceManager.init(pc, conf);

        Path stagingDir = tezResourceManager.getStagingDir();
        log.info("Tez staging directory is " + stagingDir.toString());
        conf.set(TezConfiguration.TEZ_AM_STAGING_DIR, stagingDir.toString());

        List<TezOperPlan> processedPlans = new ArrayList<TezOperPlan>();

        tezScriptState = TezScriptState.get();
        tezStats = new TezStats(pc);
        PigStats.start(tezStats);

        conf.set(TezConfiguration.TEZ_USE_CLUSTER_HADOOP_LIBS, "true");
        TezJobCompiler jc = new TezJobCompiler(pc, conf);
        TezPlanContainer tezPlanContainer = compile(php, pc);

        int defaultTimeToSleep = pc.getExecType().isLocal() ? 100 : 1000;
        int timeToSleep = conf.getInt("pig.jobcontrol.sleep", defaultTimeToSleep);
        if (timeToSleep != defaultTimeToSleep) {
            log.info("overriding default JobControl sleep (" +
                    defaultTimeToSleep + ") to " + timeToSleep);
        }

        TezOperPlan tezPlan;
        while ((tezPlan=tezPlanContainer.getNextPlan(processedPlans)) != null) {
            optimize(tezPlan, pc);
            processedPlans.add(tezPlan);
            ProgressReporter reporter = new ProgressReporter();
            tezStats.initialize(tezPlan);
            if (tezPlan.size()==1 && tezPlan.getRoots().get(0) instanceof NativeTezOper) {
                // Native Tez Plan
                NativeTezOper nativeOper = (NativeTezOper)tezPlan.getRoots().get(0);
                tezScriptState.emitInitialPlanNotification(tezPlan);
                tezScriptState.emitLaunchStartedNotification(tezPlan.size());
                tezScriptState.emitJobsSubmittedNotification(1);
                nativeOper.runJob();
            } else {
                TezPOPackageAnnotator pkgAnnotator = new TezPOPackageAnnotator(tezPlan);
                pkgAnnotator.visit();

                runningJob = jc.compile(tezPlan, grpName, tezPlanContainer);

                tezScriptState.emitInitialPlanNotification(tezPlan);
                tezScriptState.emitLaunchStartedNotification(tezPlan.size());

                // Set the thread UDFContext so registered classes are available.
                final UDFContext udfContext = UDFContext.getUDFContext();
                Thread task = new Thread(runningJob) {
                    @Override
                    public void run() {
                        UDFContext.setUdfContext(udfContext.clone());
                        super.run();
                    }
                };

                JobControlThreadExceptionHandler jctExceptionHandler = new JobControlThreadExceptionHandler();
                task.setUncaughtExceptionHandler(jctExceptionHandler);
                task.setContextClassLoader(PigContext.getClassLoader());

                tezStats.setTezJob(runningJob);

                // Mark the times that the jobs were submitted so it's reflected in job
                // history props
                long scriptSubmittedTimestamp = System.currentTimeMillis();
                // Job.getConfiguration returns the shared configuration object
                Configuration jobConf = runningJob.getConfiguration();
                jobConf.set("pig.script.submitted.timestamp",
                        Long.toString(scriptSubmittedTimestamp));
                jobConf.set("pig.job.submitted.timestamp",
                        Long.toString(System.currentTimeMillis()));

                Future<?> future = executor.schedule(task, timeToSleep, TimeUnit.MILLISECONDS);

                tezScriptState.emitJobsSubmittedNotification(1);
                reporter.notifyStarted();

                while (!future.isDone()) {
                    reporter.notifyUpdate();
                    Thread.sleep(1000);
                }

                tezStats.accumulateStats(runningJob);
            }
            tezScriptState.emitProgressUpdatedNotification(100);
            tezPlanContainer.updatePlan(tezPlan, reporter.notifyFinishedOrFailed());
        }

        tezStats.finish();
        tezScriptState.emitLaunchCompletedNotification(tezStats.getNumberSuccessfulJobs());

        for (OutputStats output : tezStats.getOutputStats()) {
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

        return tezStats;
    }

    private void computeWarningAggregate(Map<String, Map<String, Long>> counterGroups, Map<Enum, Long> aggMap) {
        for (Map<String, Long> counters : counterGroups.values()) {
            for (Enum e : PigWarning.values()) {
                if (counters.containsKey(e.toString())) {
                    if (aggMap.containsKey(e.toString())) {
                        Long currentCount = aggMap.get(e.toString());
                        currentCount = (currentCount == null ? 0 : currentCount);
                        if (counters != null) {
                            currentCount += counters.get(e.toString());
                        }
                        aggMap.put(e, currentCount);
                    } else {
                        aggMap.put(e, counters.get(e.toString()));
                    }
                }
            }
        }
    }

    private class ProgressReporter {
        private int count = 0;
        private int prevProgress = 0;

        public void notifyStarted() throws IOException {
            for (Vertex v : runningJob.getDAG().getVertices()) {
                TezTaskStats tts = tezStats.getVertexStats(v.getName());
                UserPayload payload = v.getProcessorDescriptor().getUserPayload();
                Configuration conf = TezUtils.createConfFromUserPayload(payload);
                tts.setConf(conf);
                tts.setId(v.getName());
                tezScriptState.emitJobStartedNotification(v.getName());
            }
        }

        public void notifyUpdate() {
            DAGStatus dagStatus = runningJob.getDAGStatus();
            if (dagStatus != null && dagStatus.getState() == DAGStatus.State.RUNNING) {
                // Emit notification when the job has progressed more than 1%,
                // or every 10 second
                int currProgress = Math.round(runningJob.getDAGProgress() * 100f);
                if (currProgress - prevProgress >= 1 || count % 100 == 0) {
                    tezScriptState.emitProgressUpdatedNotification(currProgress);
                    prevProgress = currProgress;
                }
                count++;
            }
        }

        public boolean notifyFinishedOrFailed() {
            DAGStatus dagStatus = runningJob.getDAGStatus();
            if (dagStatus.getState() == DAGStatus.State.SUCCEEDED) {
                Map<Enum, Long> warningAggMap = new HashMap<Enum, Long>();
                for (Vertex v : runningJob.getDAG().getVertices()) {
                    TezTaskStats tts = tezStats.getVertexStats(v.getName());
                    tezScriptState.emitjobFinishedNotification(tts);
                    Map<String, Map<String, Long>> counterGroups = runningJob.getVertexCounters(v.getName());
                    if (counterGroups == null) {
                        log.warn("Counters are not available for vertex " + v.getName() + ". Not computing warning aggregates.");
                    } else {
                        computeWarningAggregate(counterGroups, warningAggMap);
                    }
                }
                if (aggregateWarning) {
                    CompilationMessageCollector.logAggregate(warningAggMap, MessageType.Warning, log);
                }
                return true;
            } else if (dagStatus.getState() == DAGStatus.State.FAILED) {
                for (Vertex v : ((TezJob)runningJob).getDAG().getVertices()) {
                    TezTaskStats tts = tezStats.getVertexStats(v.getName());
                    tezScriptState.emitJobFailedNotification(tts);
                }
            }
            return false;
        }
    }

    @Override
    public void explain(PhysicalPlan php, PigContext pc, PrintStream ps,
            String format, boolean verbose) throws PlanException,
            VisitorException, IOException {
        log.debug("Entering TezLauncher.explain");
        TezPlanContainer tezPlanContainer = compile(php, pc);
        for (Map.Entry<OperatorKey,TezPlanContainerNode> entry : tezPlanContainer.getKeys().entrySet()) {
            TezOperPlan tezPlan = entry.getValue().getNode();
            optimize(tezPlan, pc);
        }

        if (format.equals("text")) {
            TezPlanContainerPrinter printer = new TezPlanContainerPrinter(ps, tezPlanContainer);
            printer.setVerbose(verbose);
            printer.visit();
        } else {
            // TODO: add support for other file format
            throw new IOException("Non-text output of explain is not supported.");
        }
    }

    public static void optimize(TezOperPlan tezPlan, PigContext pc) throws VisitorException {
        Configuration conf = ConfigurationUtil.toConfiguration(pc.getProperties());
        boolean aggregateWarning = conf.getBoolean("aggregate.warning", false);

        NoopFilterRemover filter = new NoopFilterRemover(tezPlan);
        filter.visit();

        // Run CombinerOptimizer on Tez plan
        boolean nocombiner = conf.getBoolean(PigConfiguration.PROP_NO_COMBINER, false);
        if (!pc.inIllustrator && !nocombiner)  {
            boolean doMapAgg = Boolean.parseBoolean(pc.getProperties().getProperty(
                    PigConfiguration.PROP_EXEC_MAP_PARTAGG, "false"));
            CombinerOptimizer co = new CombinerOptimizer(tezPlan, doMapAgg);
            co.visit();
            co.getMessageCollector().logMessages(MessageType.Warning, aggregateWarning, log);
        }

        // Run optimizer to make use of secondary sort key when possible for nested foreach
        // order by and distinct. Should be done before AccumulatorOptimizer
        boolean noSecKeySort = conf.getBoolean(PigConfiguration.PIG_EXEC_NO_SECONDARY_KEY, false);
        if (!pc.inIllustrator && !noSecKeySort)  {
            SecondaryKeyOptimizerTez skOptimizer = new SecondaryKeyOptimizerTez(tezPlan);
            skOptimizer.visit();
        }

        boolean isMultiQuery = conf.getBoolean(PigConfiguration.OPT_MULTIQUERY, true);
        if (isMultiQuery) {
            // reduces the number of TezOpers in the Tez plan generated
            // by multi-query (multi-store) script.
            MultiQueryOptimizerTez mqOptimizer = new MultiQueryOptimizerTez(tezPlan);
            mqOptimizer.visit();
        }

        // Run AccumulatorOptimizer on Tez plan
        boolean isAccum = conf.getBoolean(PigConfiguration.OPT_ACCUMULATOR, true);
        if (isAccum) {
            AccumulatorOptimizer accum = new AccumulatorOptimizer(tezPlan);
            accum.visit();
        }

        // Use VertexGroup in Tez
        boolean isUnionOpt = conf.getBoolean(PigConfiguration.TEZ_OPT_UNION, true);
        if (isUnionOpt) {
            UnionOptimizer uo = new UnionOptimizer(tezPlan);
            uo.visit();
        }

        if (!pc.inExplain && !pc.inDumpSchema) {
            LoaderProcessor loaderStorer = new LoaderProcessor(tezPlan, pc);
            loaderStorer.visit();

            ParallelismSetter parallelismSetter = new ParallelismSetter(tezPlan, pc);
            parallelismSetter.visit();
        }
    }

    public TezPlanContainer compile(PhysicalPlan php, PigContext pc)
            throws PlanException, IOException, VisitorException {
        TezCompiler comp = new TezCompiler(php, pc);
        comp.compile();
        return comp.getPlanContainer();
    }

    @Override
    public void kill() throws BackendException {
        if (runningJob != null) {
            try {
                runningJob.killJob();
            } catch (Exception e) {
                throw new BackendException(e);
            }
        }
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Override
    public void killJob(String jobID, Configuration conf) throws BackendException {
        if (runningJob != null && runningJob.getApplicationId().toString() == jobID) {
            try {
                runningJob.killJob();
            } catch (Exception e) {
                throw new BackendException(e);
            }
        } else {
            log.info("Cannot find job: " + jobID);
        }
    }
}
