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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.pig.PigConfiguration;
import org.apache.pig.PigWarning;
import org.apache.pig.backend.BackendException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.executionengine.Launcher;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.tez.optimizers.NoopFilterRemover;
import org.apache.pig.backend.hadoop.executionengine.tez.optimizers.UnionOptimizer;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.plan.CompilationMessageCollector;
import org.apache.pig.impl.plan.CompilationMessageCollector.MessageType;
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
import org.apache.tez.dag.api.Vertex;

/**
 * Main class that launches pig for Tez
 */
public class TezLauncher extends Launcher {

    private static final Log log = LogFactory.getLog(TezLauncher.class);
    private boolean aggregateWarning = false;
    private TezScriptState tezScriptState;
    private TezStats tezStats;

    @Override
    public PigStats launchPig(PhysicalPlan php, String grpName, PigContext pc) throws Exception {
        if (pc.defaultParallel == -1 &&
                !Boolean.parseBoolean(pc.getProperties().getProperty(PigConfiguration.TEZ_AUTO_PARALLELISM, "true"))) {
            pc.defaultParallel = 1;
        }
        aggregateWarning = Boolean.parseBoolean(pc.getProperties().getProperty("aggregate.warning", "false"));
        Configuration conf = ConfigurationUtil.toConfiguration(pc.getProperties(), true);

        Path stagingDir = FileLocalizer.getTemporaryPath(pc, "-tez");

        TezResourceManager tezResourceManager = TezResourceManager.getInstance();
        tezResourceManager.init(pc, conf);

        stagingDir.getFileSystem(conf).mkdirs(stagingDir);
        log.info("Tez staging directory is " + stagingDir.toString());
        conf.set(TezConfiguration.TEZ_AM_STAGING_DIR, stagingDir.toString());

        List<TezOperPlan> processedPlans = new ArrayList<TezOperPlan>();

        tezScriptState = TezScriptState.get();
        tezStats = new TezStats(pc);
        PigStats.start(tezStats);

        TezJobControlCompiler jcc = new TezJobControlCompiler(pc, conf);
        TezPlanContainer tezPlanContainer = compile(php, pc);

        TezOperPlan tezPlan;

        while ((tezPlan=tezPlanContainer.getNextPlan(processedPlans))!=null) {
            processedPlans.add(tezPlan);

            TezPOPackageAnnotator pkgAnnotator = new TezPOPackageAnnotator(tezPlan);
            pkgAnnotator.visit();

            tezStats.initialize(tezPlan);

            jc = jcc.compile(tezPlan, grpName, tezPlanContainer);
            TezJobNotifier notifier = new TezJobNotifier(tezPlanContainer, tezPlan);
            ((TezJobControl)jc).setJobNotifier(notifier);

            // Initially, all jobs are in wait state.
            List<ControlledJob> jobsWithoutIds = jc.getWaitingJobList();
            log.info(jobsWithoutIds.size() + " tez job(s) waiting for submission.");

            tezScriptState.emitInitialPlanNotification(tezPlan);
            tezScriptState.emitLaunchStartedNotification(tezPlan.size());

            // Set the thread UDFContext so registered classes are available.
            final UDFContext udfContext = UDFContext.getUDFContext();
            Thread jcThread = new Thread(jc, "JobControl") {
                @Override
                public void run() {
                    UDFContext.setUdfContext(udfContext.clone());
                    super.run();
                }
            };

            JobControlThreadExceptionHandler jctExceptionHandler = new JobControlThreadExceptionHandler();
            jcThread.setUncaughtExceptionHandler(jctExceptionHandler);
            jcThread.setContextClassLoader(PigContext.getClassLoader());

            // TezJobControl always holds a single TezJob. We use JobControl
            // only because it is convenient to launch the job via
            // ControlledJob.submit().
            TezJob job = (TezJob)jobsWithoutIds.get(0);
            tezStats.setTezJob(job);

            // Mark the times that the jobs were submitted so it's reflected in job
            // history props
            long scriptSubmittedTimestamp = System.currentTimeMillis();
            // Job.getConfiguration returns the shared configuration object
            Configuration jobConf = job.getJob().getConfiguration();
            jobConf.set("pig.script.submitted.timestamp",
                    Long.toString(scriptSubmittedTimestamp));
            jobConf.set("pig.job.submitted.timestamp",
                    Long.toString(System.currentTimeMillis()));

            // Inform ppnl of jobs submission
            tezScriptState.emitJobsSubmittedNotification(jobsWithoutIds.size());

            // All the setup done, now lets launch the jobs. DAG is submitted to
            // YARN cluster by TezJob.submit().
            jcThread.start();

            Double prevProgress = 0.0;
            while(!jc.allFinished()) {
                List<ControlledJob> jobsAssignedIdInThisRun = new ArrayList<ControlledJob>();
                if (job.getApplicationId() != null) {
                    jobsAssignedIdInThisRun.add(job);
                }
                notifyStarted(job);
                jobsWithoutIds.removeAll(jobsAssignedIdInThisRun);
                prevProgress = notifyProgress(job, prevProgress);
            }

            notifyFinishedOrFailed(job);
            tezStats.accumulateStats(job);
            Map<Enum, Long> warningAggMap = new HashMap<Enum, Long>();

            if (aggregateWarning && job.getJobState() == ControlledJob.State.SUCCESS) {
                for (Vertex vertex : job.getDAG().getVertices()) {
                    String vertexName = vertex.getName();
                    Map<String, Map<String, Long>> counterGroups = job.getVertexCounters(vertexName);
                    computeWarningAggregate(counterGroups, warningAggMap);
                }
            }

            if(aggregateWarning) {
                CompilationMessageCollector.logAggregate(warningAggMap, MessageType.Warning, log) ;
            }
            tezScriptState.emitProgressUpdatedNotification(100);
        }

        tezStats.finish();
        for (OutputStats output : tezStats.getOutputStats()) {
            POStore store = output.getPOStore();
            try {
                if (!output.isSuccessful()) {
                    store.getStoreFunc().cleanupOnFailure(
                            store.getSFile().getFileName(),
                            new org.apache.hadoop.mapreduce.Job(output.getConf()));
                } else {
                    store.getStoreFunc().cleanupOnSuccess(
                            store.getSFile().getFileName(),
                            new org.apache.hadoop.mapreduce.Job(output.getConf()));
                }
            } catch (IOException e) {
                throw new ExecException(e);
            } catch (AbstractMethodError nsme) {
                // Just swallow it.  This means we're running against an
                // older instance of a StoreFunc that doesn't implement
                // this method.
            }
        }
        tezScriptState.emitLaunchCompletedNotification(tezStats.getNumberSuccessfulJobs());

        return tezStats;
    }

    void computeWarningAggregate(Map<String, Map<String, Long>> counterGroups, Map<Enum, Long> aggMap) {
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

    private void notifyStarted(TezJob job) throws IOException {
        for (Vertex v : job.getDAG().getVertices()) {
            TezTaskStats tts = tezStats.getVertexStats(v.getName());
            byte[] bb = v.getProcessorDescriptor().getUserPayload();
            Configuration conf = TezUtils.createConfFromUserPayload(bb);
            tts.setConf(conf);
            tts.setId(v.getName());
            tezScriptState.emitJobStartedNotification(v.getName());
        }
    }

    private Double notifyProgress(TezJob job, Double prevProgress) {
        int numberOfJobs = tezStats.getNumberJobs();
        Double perCom = 0.0;
        if (job.getJobState() == ControlledJob.State.RUNNING) {
            Double fractionComplete = job.getDAGProgress();
            perCom += fractionComplete;
        }
        perCom = (perCom/(double)numberOfJobs)*100;
        if (perCom >= (prevProgress + 4.0)) {
            tezScriptState.emitProgressUpdatedNotification( perCom.intValue() );
            return perCom;
        } else {
            return prevProgress;
        }
    }

    private void notifyFinishedOrFailed(TezJob job) {
        if (job.getJobState() == ControlledJob.State.SUCCESS) {
            for (Vertex v : job.getDAG().getVertices()) {
                TezTaskStats tts = tezStats.getVertexStats(v.getName());
                tezScriptState.emitjobFinishedNotification(tts);
            }
        } else if (job.getJobState() == ControlledJob.State.FAILED) {
            for (Vertex v : ((TezJob)job).getDAG().getVertices()) {
                TezTaskStats tts = tezStats.getVertexStats(v.getName());
                tezScriptState.emitJobFailedNotification(tts);
            }
        }
    }

    @Override
    public void explain(PhysicalPlan php, PigContext pc, PrintStream ps,
            String format, boolean verbose) throws PlanException,
            VisitorException, IOException {
        log.debug("Entering TezLauncher.explain");
        TezPlanContainer tezPlanContainer = compile(php, pc);

        if (format.equals("text")) {
            TezPlanContainerPrinter printer = new TezPlanContainerPrinter(ps, tezPlanContainer);
            printer.setVerbose(verbose);
            printer.visit();
        } else {
            // TODO: add support for other file format
            throw new IOException("Non-text output of explain is not supported.");
        }
    }

    public TezPlanContainer compile(PhysicalPlan php, PigContext pc)
            throws PlanException, IOException, VisitorException {
        TezCompiler comp = new TezCompiler(php, pc);
        TezOperPlan tezPlan = comp.compile();

        NoopFilterRemover filter = new NoopFilterRemover(tezPlan);
        filter.visit();

        boolean nocombiner = Boolean.parseBoolean(pc.getProperties().getProperty(
                PigConfiguration.PROP_NO_COMBINER, "false"));

        // Run CombinerOptimizer on Tez plan
        if (!pc.inIllustrator && !nocombiner)  {
            boolean doMapAgg = Boolean.parseBoolean(pc.getProperties().getProperty(
                    PigConfiguration.PROP_EXEC_MAP_PARTAGG, "false"));
            CombinerOptimizer co = new CombinerOptimizer(tezPlan, doMapAgg);
            co.visit();
            co.getMessageCollector().logMessages(MessageType.Warning, aggregateWarning, log);
        }

        // Run optimizer to make use of secondary sort key when possible for nested foreach
        // order by and distinct. Should be done before AccumulatorOptimizer
        boolean noSecKeySort = Boolean.parseBoolean(pc.getProperties().getProperty(
                PigConfiguration.PIG_EXEC_NO_SECONDARY_KEY, "false"));
        if (!pc.inIllustrator && !noSecKeySort)  {
            SecondaryKeyOptimizerTez skOptimizer = new SecondaryKeyOptimizerTez(tezPlan);
            skOptimizer.visit();
        }

        boolean isMultiQuery =
                "true".equalsIgnoreCase(pc.getProperties().getProperty(PigConfiguration.OPT_MULTIQUERY, "true"));

        if (isMultiQuery) {
            // reduces the number of TezOpers in the Tez plan generated
            // by multi-query (multi-store) script.
            MultiQueryOptimizerTez mqOptimizer = new MultiQueryOptimizerTez(tezPlan);
            mqOptimizer.visit();
        }

        // Run AccumulatorOptimizer on Tez plan
        boolean isAccum = Boolean.parseBoolean(pc.getProperties().getProperty(
                    PigConfiguration.OPT_ACCUMULATOR, "true"));
        if (isAccum) {
            AccumulatorOptimizer accum = new AccumulatorOptimizer(tezPlan);
            accum.visit();
        }

        boolean isUnionOpt = "true".equalsIgnoreCase(pc.getProperties()
                .getProperty(PigConfiguration.TEZ_OPT_UNION, "true"));
        // Use VertexGroup in Tez
        if (isUnionOpt) {
            UnionOptimizer uo = new UnionOptimizer(tezPlan);
            uo.visit();
        }

        return comp.getPlanContainer();
    }

    @Override
    public void kill() throws BackendException {
        if (jc == null) return;
        for (ControlledJob job : jc.getRunningJobs()) {
            try {
                job.killJob();
                break;
            } catch (Exception e) {
                throw new BackendException(e);
            }
        }
    }

    @Override
    public void killJob(String jobID, Configuration conf) throws BackendException {
        for (ControlledJob job : jc.getRunningJobs()) {
            if (job.getJobID().equals(jobID)) {
                try {
                    job.killJob();
                } catch (Exception e) {
                    throw new BackendException(e);
                }
                break;
            }
        }
        log.info("Cannot find job: " + jobID);
    }
}
