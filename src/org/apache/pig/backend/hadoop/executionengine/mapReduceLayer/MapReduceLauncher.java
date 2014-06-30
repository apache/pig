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
package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskReport;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.pig.PigConfiguration;
import org.apache.pig.PigException;
import org.apache.pig.PigRunner.ReturnCode;
import org.apache.pig.PigWarning;
import org.apache.pig.backend.BackendException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.executionengine.HExecutionEngine;
import org.apache.pig.backend.hadoop.executionengine.JobCreationException;
import org.apache.pig.backend.hadoop.executionengine.Launcher;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MRCompiler.LastInputStreamingOptimizer;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.DotMRPrinter;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.EndOfAllInputSetter;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MRIntermediateDataVisitor;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MRPrinter;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.POPackageAnnotator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.XMLMRPrinter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.JoinPackager;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.shims.HadoopShims;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.plan.CompilationMessageCollector;
import org.apache.pig.impl.plan.CompilationMessageCollector.MessageType;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.ConfigurationValidator;
import org.apache.pig.impl.util.LogUtils;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.tools.pigstats.OutputStats;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.PigStatsUtil;
import org.apache.pig.tools.pigstats.mapreduce.MRPigStatsUtil;
import org.apache.pig.tools.pigstats.mapreduce.MRScriptState;


/**
 * Main class that launches pig for Map Reduce
 *
 */
public class MapReduceLauncher extends Launcher{

    public static final String SUCCEEDED_FILE_NAME = "_SUCCESS";

    public static final String SUCCESSFUL_JOB_OUTPUT_DIR_MARKER =
            "mapreduce.fileoutputcommitter.marksuccessfuljobs";

    private static final Log log = LogFactory.getLog(MapReduceLauncher.class);

    private boolean aggregateWarning = false;

    @Override
    public void kill() {
        try {
            log.debug("Receive kill signal");
            if (jc!=null) {
                for (Job job : jc.getRunningJobs()) {
                    HadoopShims.killJob(job);
                    log.info("Job " + job.getAssignedJobID() + " killed");
                }
            }
        } catch (Exception e) {
            log.warn("Encounter exception on cleanup:" + e);
        }
    }

    @Override
    public void killJob(String jobID, Configuration conf) throws BackendException {
        try {
            if (conf != null) {
                JobConf jobConf = new JobConf(conf);
                JobClient jc = new JobClient(jobConf);
                JobID id = JobID.forName(jobID);
                RunningJob job = jc.getJob(id);
                if (job == null)
                    System.out.println("Job with id " + jobID + " is not active");
                else
                {
                    job.killJob();
                    log.info("Kill " + id + " submitted.");
                }
            }
        } catch (IOException e) {
            throw new BackendException(e);
        }
    }

    /**
     * Get the exception that caused a failure on the backend for a
     * store location (if any).
     */
    public Exception getError(FileSpec spec) {
        return failureMap.get(spec);
    }

    @Override
    public PigStats launchPig(PhysicalPlan php,
            String grpName,
            PigContext pc) throws PlanException,
            VisitorException,
            IOException,
            ExecException,
            JobCreationException,
            Exception {
        long sleepTime = 500;
        aggregateWarning = Boolean.valueOf(pc.getProperties().getProperty("aggregate.warning"));
        MROperPlan mrp = compile(php, pc);

        ConfigurationValidator.validatePigProperties(pc.getProperties());
        Configuration conf = ConfigurationUtil.toConfiguration(pc.getProperties());

        MRExecutionEngine exe = (MRExecutionEngine) pc.getExecutionEngine();
        Properties defaultProperties = new Properties();
        JobConf defaultJobConf = exe.getLocalConf(defaultProperties);
        Utils.recomputeProperties(defaultJobConf, defaultProperties);

        // This is a generic JobClient for checking progress of the jobs
        JobClient statsJobClient = new JobClient(exe.getJobConf());

        JobControlCompiler jcc = new JobControlCompiler(pc, conf, ConfigurationUtil.toConfiguration(defaultProperties));

        MRScriptState.get().addWorkflowAdjacenciesToConf(mrp, conf);

        // start collecting statistics
        PigStats.start(pc.getExecutionEngine().instantiatePigStats());
        MRPigStatsUtil.startCollection(pc, statsJobClient, jcc, mrp);

        // Find all the intermediate data stores. The plan will be destroyed during compile/execution
        // so this needs to be done before.
        MRIntermediateDataVisitor intermediateVisitor = new MRIntermediateDataVisitor(mrp);
        intermediateVisitor.visit();

        List<Job> failedJobs = new LinkedList<Job>();
        List<NativeMapReduceOper> failedNativeMR = new LinkedList<NativeMapReduceOper>();
        List<Job> completeFailedJobsInThisRun = new LinkedList<Job>();
        List<Job> succJobs = new LinkedList<Job>();
        int totalMRJobs = mrp.size();
        int numMRJobsCompl = 0;
        double lastProg = -1;
        long scriptSubmittedTimestamp = System.currentTimeMillis();

        //create the exception handler for the job control thread
        //and register the handler with the job control thread
        JobControlThreadExceptionHandler jctExceptionHandler = new JobControlThreadExceptionHandler();

        boolean stop_on_failure =
            Boolean.valueOf(pc.getProperties().getProperty("stop.on.failure", "false"));

        // jc is null only when mrp.size == 0
        while(mrp.size() != 0) {
            jc = jcc.compile(mrp, grpName);
            if(jc == null) {
                List<MapReduceOper> roots = new LinkedList<MapReduceOper>();
                roots.addAll(mrp.getRoots());

                // run the native mapreduce roots first then run the rest of the roots
                for(MapReduceOper mro: roots) {
                    if(mro instanceof NativeMapReduceOper) {
                        NativeMapReduceOper natOp = (NativeMapReduceOper)mro;
                        try {
                            MRScriptState.get().emitJobsSubmittedNotification(1);
                            natOp.runJob();
                            numMRJobsCompl++;
                        } catch (IOException e) {

                            mrp.trimBelow(natOp);
                            failedNativeMR.add(natOp);

                            String msg = "Error running native mapreduce" +
                                    " operator job :" + natOp.getJobId() + e.getMessage();

                            String stackTrace = Utils.getStackStraceStr(e);
                            LogUtils.writeLog(msg,
                                    stackTrace,
                                    pc.getProperties().getProperty("pig.logfile"),
                                    log
                                    );
                            log.info(msg);

                            if (stop_on_failure) {
                                int errCode = 6017;

                                throw new ExecException(msg, errCode,
                                        PigException.REMOTE_ENVIRONMENT);
                            }

                        }
                        double prog = ((double)numMRJobsCompl)/totalMRJobs;
                        notifyProgress(prog, lastProg);
                        lastProg = prog;
                        mrp.remove(natOp);
                    }
                }
                continue;
            }
            // Initially, all jobs are in wait state.
            List<Job> jobsWithoutIds = jc.getWaitingJobs();
            log.info(jobsWithoutIds.size() +" map-reduce job(s) waiting for submission.");
            //notify listeners about jobs submitted
            MRScriptState.get().emitJobsSubmittedNotification(jobsWithoutIds.size());

            // update Pig stats' job DAG with just compiled jobs
            MRPigStatsUtil.updateJobMroMap(jcc.getJobMroMap());

            // determine job tracker url
            String jobTrackerLoc;
            JobConf jobConf = jobsWithoutIds.get(0).getJobConf();
            try {
                String port = jobConf.get("mapred.job.tracker.http.address");
                String jobTrackerAdd = jobConf.get(HExecutionEngine.JOB_TRACKER_LOCATION);

                jobTrackerLoc = jobTrackerAdd.substring(0,jobTrackerAdd.indexOf(":"))
                        + port.substring(port.indexOf(":"));
            }
            catch(Exception e){
                // Could not get the job tracker location, most probably we are running in local mode.
                // If it is the case, we don't print out job tracker location,
                // because it is meaningless for local mode.
                jobTrackerLoc = null;
                log.debug("Failed to get job tracker location.");
            }

            completeFailedJobsInThisRun.clear();

            // Set the thread UDFContext so registered classes are available.
            final UDFContext udfContext = UDFContext.getUDFContext();
            Thread jcThread = new Thread(jc, "JobControl") {
                @Override
                public void run() {
                    UDFContext.setUdfContext(udfContext.clone()); //PIG-2576
                    super.run();
                }
            };

            jcThread.setUncaughtExceptionHandler(jctExceptionHandler);

            jcThread.setContextClassLoader(PigContext.getClassLoader());

            // mark the times that the jobs were submitted so it's reflected in job history props
            for (Job job : jc.getWaitingJobs()) {
                JobConf jobConfCopy = job.getJobConf();
                jobConfCopy.set("pig.script.submitted.timestamp",
                        Long.toString(scriptSubmittedTimestamp));
                jobConfCopy.set("pig.job.submitted.timestamp",
                        Long.toString(System.currentTimeMillis()));
                job.setJobConf(jobConfCopy);
            }

            //All the setup done, now lets launch the jobs.
            jcThread.start();

            try {
                // a flag whether to warn failure during the loop below, so users can notice failure earlier.
                boolean warn_failure = true;

                // Now wait, till we are finished.
                while(!jc.allFinished()){

                    try { jcThread.join(sleepTime); }
                    catch (InterruptedException e) {}

                    List<Job> jobsAssignedIdInThisRun = new ArrayList<Job>();

                    for(Job job : jobsWithoutIds){
                        if (job.getAssignedJobID() != null){

                            jobsAssignedIdInThisRun.add(job);
                            log.info("HadoopJobId: "+job.getAssignedJobID());

                            // display the aliases being processed
                            MapReduceOper mro = jcc.getJobMroMap().get(job);
                            if (mro != null) {
                                String alias = MRScriptState.get().getAlias(mro);
                                log.info("Processing aliases " + alias);
                                String aliasLocation = MRScriptState.get().getAliasLocation(mro);
                                log.info("detailed locations: " + aliasLocation);
                            }

                            if (!HadoopShims.isHadoopYARN() && jobTrackerLoc != null) {
                                log.info("More information at: http://" + jobTrackerLoc
                                        + "/jobdetails.jsp?jobid=" + job.getAssignedJobID());
                            }

                            // update statistics for this job so jobId is set
                            MRPigStatsUtil.addJobStats(job);
                            MRScriptState.get().emitJobStartedNotification(
                                    job.getAssignedJobID().toString());
                        }
                        else{
                            // This job is not assigned an id yet.
                        }
                    }
                    jobsWithoutIds.removeAll(jobsAssignedIdInThisRun);

                    double prog = (numMRJobsCompl+calculateProgress(jc))/totalMRJobs;
                    if (notifyProgress(prog, lastProg)) {
                        List<Job> runnJobs = jc.getRunningJobs();
                        if (runnJobs != null) {
                            StringBuilder msg = new StringBuilder();
                            for (Object object : runnJobs) {
                                Job j = (Job) object;
                                if (j != null) {
                                    msg.append(j.getAssignedJobID()).append(",");
                                }
                            }
                            if (msg.length() > 0) {
                                msg.setCharAt(msg.length() - 1, ']');
                                log.info("Running jobs are [" + msg);
                            }
                        }
                        lastProg = prog;
                    }

                    // collect job stats by frequently polling of completed jobs (PIG-1829)
                    MRPigStatsUtil.accumulateStats(jc);

                    // if stop_on_failure is enabled, we need to stop immediately when any job has failed
                    checkStopOnFailure(stop_on_failure);
                    // otherwise, we just display a warning message if there's any failure
                    if (warn_failure && !jc.getFailedJobs().isEmpty()) {
                        // we don't warn again for this group of jobs
                        warn_failure = false;
                        log.warn("Ooops! Some job has failed! Specify -stop_on_failure if you "
                                + "want Pig to stop immediately on failure.");
                    }
                }

                //check for the jobControlException first
                //if the job controller fails before launching the jobs then there are
                //no jobs to check for failure
                if (jobControlException != null) {
                    if (jobControlException instanceof PigException) {
                        if (jobControlExceptionStackTrace != null) {
                            LogUtils.writeLog("Error message from job controller",
                                    jobControlExceptionStackTrace, pc
                                    .getProperties().getProperty(
                                            "pig.logfile"), log);
                        }
                        throw jobControlException;
                    } else {
                        int errCode = 2117;
                        String msg = "Unexpected error when launching map reduce job.";
                        throw new ExecException(msg, errCode, PigException.BUG,
                                jobControlException);
                    }
                }

                if (!jc.getFailedJobs().isEmpty() ) {
                    // stop if stop_on_failure is enabled
                    checkStopOnFailure(stop_on_failure);

                    // If we only have one store and that job fail, then we sure
                    // that the job completely fail, and we shall stop dependent jobs
                    for (Job job : jc.getFailedJobs()) {
                        completeFailedJobsInThisRun.add(job);
                        log.info("job " + job.getAssignedJobID() + " has failed! Stop running all dependent jobs");
                    }
                    failedJobs.addAll(jc.getFailedJobs());
                }

                int removedMROp = jcc.updateMROpPlan(completeFailedJobsInThisRun);

                numMRJobsCompl += removedMROp;

                List<Job> jobs = jc.getSuccessfulJobs();
                jcc.moveResults(jobs);
                succJobs.addAll(jobs);

                // collecting final statistics
                MRPigStatsUtil.accumulateStats(jc);

            }
            catch (Exception e) {
                throw e;
            }
            finally {
                jc.stop();
            }
        }

        MRScriptState.get().emitProgressUpdatedNotification(100);

        log.info( "100% complete");

        boolean failed = false;

        if(failedNativeMR.size() > 0){
            failed = true;
        }

        if (Boolean.valueOf(pc.getProperties().getProperty(PigConfiguration.PIG_DELETE_TEMP_FILE, "true"))) {
            // Clean up all the intermediate data
            for (String path : intermediateVisitor.getIntermediate()) {
                // Skip non-file system paths such as hbase, see PIG-3617
                if (HadoopShims.hasFileSystemImpl(new Path(path), conf)) {
                    FileLocalizer.delete(path, pc);
                }
            }
        }

        // Look to see if any jobs failed.  If so, we need to report that.
        if (failedJobs != null && failedJobs.size() > 0) {

            Exception backendException = null;
            for (Job fj : failedJobs) {
                try {
                    getStats(fj, true, pc);
                } catch (Exception e) {
                    backendException = e;
                }
                List<POStore> sts = jcc.getStores(fj);
                for (POStore st: sts) {
                    failureMap.put(st.getSFile(), backendException);
                }
                MRPigStatsUtil.setBackendException(fj, backendException);
            }
            failed = true;
        }

        // stats collection is done, log the results
        MRPigStatsUtil.stopCollection(true);

        // PigStatsUtil.stopCollection also computes the return code based on
        // total jobs to run, jobs successful and jobs failed
        failed = failed || !PigStats.get().isSuccessful();

        Map<Enum, Long> warningAggMap = new HashMap<Enum, Long>();

        if (succJobs != null) {
            for (Job job : succJobs) {
                List<POStore> sts = jcc.getStores(job);
                for (POStore st : sts) {
                    if (Utils.isLocal(pc, job.getJobConf())) {
                        HadoopShims.storeSchemaForLocal(job, st);
                    }

                    if (!st.isTmpStore()) {
                        // create an "_SUCCESS" file in output location if
                        // output location is a filesystem dir
                        createSuccessFile(job, st);
                    } else {
                        log.debug("Successfully stored result in: \""
                                + st.getSFile().getFileName() + "\"");
                    }
                }

                getStats(job, false, pc);
                if (aggregateWarning) {
                    computeWarningAggregate(job, warningAggMap);
                }
            }

        }

        if(aggregateWarning) {
            CompilationMessageCollector.logAggregate(warningAggMap, MessageType.Warning, log) ;
        }

        if (!failed) {
            log.info("Success!");
        } else {
            if (succJobs != null && succJobs.size() > 0) {
                log.info("Some jobs have failed! Stop running all dependent jobs");
            } else {
                log.info("Failed!");
            }
        }
        jcc.reset();

        int ret = failed ? ((succJobs != null && succJobs.size() > 0)
                ? ReturnCode.PARTIAL_FAILURE
                        : ReturnCode.FAILURE)
                        : ReturnCode.SUCCESS;

        PigStats pigStats = PigStatsUtil.getPigStats(ret);
        // run cleanup for all of the stores
        for (OutputStats output : pigStats.getOutputStats()) {
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
        return pigStats;
    }

    /**
     * If stop_on_failure is enabled and any job has failed, an ExecException is thrown.
     * @param stop_on_failure whether it's enabled.
     * @throws ExecException If stop_on_failure is enabled and any job is failed
     */
    private void checkStopOnFailure(boolean stop_on_failure) throws ExecException{
        if (jc.getFailedJobs().isEmpty())
            return;

        if (stop_on_failure){
            int errCode = 6017;
            StringBuilder msg = new StringBuilder();

            for (int i=0; i<jc.getFailedJobs().size(); i++) {
                Job j = jc.getFailedJobs().get(i);
                msg.append("JobID: " + j.getAssignedJobID() + " Reason: " + j.getMessage());
                if (i!=jc.getFailedJobs().size()-1) {
                    msg.append("\n");
                }
            }

            throw new ExecException(msg.toString(), errCode,
                    PigException.REMOTE_ENVIRONMENT);
        }
    }

    /**
     * Log the progress and notify listeners if there is sufficient progress
     * @param prog current progress
     * @param lastProg progress last time
     */
    private boolean notifyProgress(double prog, double lastProg) {
        if (prog >= (lastProg + 0.04)) {
            int perCom = (int)(prog * 100);
            if(perCom!=100) {
                log.info( perCom + "% complete");
                MRScriptState.get().emitProgressUpdatedNotification(perCom);
            }
            return true;
        }
        return false;
    }

    @Override
    public void explain(
            PhysicalPlan php,
            PigContext pc,
            PrintStream ps,
            String format,
            boolean verbose) throws PlanException, VisitorException,
            IOException {
        log.trace("Entering MapReduceLauncher.explain");
        MROperPlan mrp = compile(php, pc);

        if (format.equals("text")) {
            MRPrinter printer = new MRPrinter(ps, mrp);
            printer.setVerbose(verbose);
            printer.visit();
        } else if (format.equals("xml")) {
            try {
                XMLMRPrinter printer = new XMLMRPrinter(ps, mrp);
                printer.visit();
                printer.closePlan();
            } catch (ParserConfigurationException e) {
                e.printStackTrace();
            } catch (TransformerException e) {
                e.printStackTrace();
            }
        } else {
            ps.println("#--------------------------------------------------");
            ps.println("# Map Reduce Plan                                  ");
            ps.println("#--------------------------------------------------");

            DotMRPrinter printer =new DotMRPrinter(mrp, ps);
            printer.setVerbose(verbose);
            printer.dump();
            ps.println("");
        }
    }

    public MROperPlan compile(
            PhysicalPlan php,
            PigContext pc) throws PlanException, IOException, VisitorException {
        MRCompiler comp = new MRCompiler(php, pc);
        comp.compile();
        comp.aggregateScalarsFiles();
        comp.connectSoftLink();
        MROperPlan plan = comp.getMRPlan();

        //display the warning message(s) from the MRCompiler
        comp.getMessageCollector().logMessages(MessageType.Warning, aggregateWarning, log);

        String lastInputChunkSize =
                pc.getProperties().getProperty(
                        "last.input.chunksize", JoinPackager.DEFAULT_CHUNK_SIZE);

        String prop = pc.getProperties().getProperty(PigConfiguration.PROP_NO_COMBINER);
        if (!pc.inIllustrator && !("true".equals(prop)))  {
            boolean doMapAgg =
                    Boolean.valueOf(pc.getProperties().getProperty(PigConfiguration.PROP_EXEC_MAP_PARTAGG,"false"));
            CombinerOptimizer co = new CombinerOptimizer(plan, doMapAgg);
            co.visit();
            //display the warning message(s) from the CombinerOptimizer
            co.getMessageCollector().logMessages(MessageType.Warning, aggregateWarning, log);
        }

        // Optimize the jobs that have a load/store only first MR job followed
        // by a sample job.
        SampleOptimizer so = new SampleOptimizer(plan, pc);
        so.visit();

        // We must ensure that there is only 1 reducer for a limit. Add a single-reducer job.
        if (!pc.inIllustrator) {
            LimitAdjuster la = new LimitAdjuster(plan, pc);
            la.visit();
            la.adjust();
        }
        // Optimize to use secondary sort key if possible
        prop = pc.getProperties().getProperty(PigConfiguration.PIG_EXEC_NO_SECONDARY_KEY);
        if (!pc.inIllustrator && !("true".equals(prop)))  {
            SecondaryKeyOptimizerMR skOptimizer = new SecondaryKeyOptimizerMR(plan);
            skOptimizer.visit();
        }

        // optimize key - value handling in package
        POPackageAnnotator pkgAnnotator = new POPackageAnnotator(plan);
        pkgAnnotator.visit();

        // optimize joins
        LastInputStreamingOptimizer liso =
                new MRCompiler.LastInputStreamingOptimizer(plan, lastInputChunkSize);
        liso.visit();

        // figure out the type of the key for the map plan
        // this is needed when the key is null to create
        // an appropriate NullableXXXWritable object
        KeyTypeDiscoveryVisitor kdv = new KeyTypeDiscoveryVisitor(plan);
        kdv.visit();

        // removes the filter(constant(true)) operators introduced by
        // splits.
        NoopFilterRemover fRem = new NoopFilterRemover(plan);
        fRem.visit();

        boolean isMultiQuery =
            Boolean.valueOf(pc.getProperties().getProperty(PigConfiguration.OPT_MULTIQUERY, "true"));

        if (isMultiQuery) {
            // reduces the number of MROpers in the MR plan generated
            // by multi-query (multi-store) script.
            MultiQueryOptimizer mqOptimizer = new MultiQueryOptimizer(plan, pc.inIllustrator);
            mqOptimizer.visit();
        }

        // removes unnecessary stores (as can happen with splits in
        // some cases.). This has to run after the MultiQuery and
        // NoopFilterRemover.
        NoopStoreRemover sRem = new NoopStoreRemover(plan);
        sRem.visit();

        // check whether stream operator is present
        // after MultiQueryOptimizer because it can shift streams from
        // map to reduce, etc.
        EndOfAllInputSetter checker = new EndOfAllInputSetter(plan);
        checker.visit();

        boolean isAccum =
            Boolean.valueOf(pc.getProperties().getProperty("opt.accumulator","true"));
        if (isAccum) {
            AccumulatorOptimizer accum = new AccumulatorOptimizer(plan);
            accum.visit();
        }
        return plan;
    }

    private boolean shouldMarkOutputDir(Job job) {
        return job.getJobConf().getBoolean(SUCCESSFUL_JOB_OUTPUT_DIR_MARKER,
                false);
    }

    private void createSuccessFile(Job job, POStore store) throws IOException {
        if(shouldMarkOutputDir(job)) {
            Path outputPath = new Path(store.getSFile().getFileName());
            String scheme = outputPath.toUri().getScheme();
            if (HadoopShims.hasFileSystemImpl(outputPath, job.getJobConf())) {
                FileSystem fs = outputPath.getFileSystem(job.getJobConf());
                if (fs.exists(outputPath)) {
                    // create a file in the folder to mark it
                    Path filePath = new Path(outputPath, SUCCEEDED_FILE_NAME);
                    if (!fs.exists(filePath)) {
                        fs.create(filePath).close();
                    }
                }
            } else {
                log.warn("No FileSystem for scheme: " + scheme + ". Not creating success file");
            }
        }
    }

    @SuppressWarnings("deprecation")
    void computeWarningAggregate(Job job, Map<Enum, Long> aggMap) {
        try {
            Counters counters = HadoopShims.getCounters(job);
            if (counters==null)
            {
                long nullCounterCount =
                        (aggMap.get(PigWarning.NULL_COUNTER_COUNT) == null)
                          ? 0
                          : aggMap.get(PigWarning.NULL_COUNTER_COUNT);
                nullCounterCount++;
                aggMap.put(PigWarning.NULL_COUNTER_COUNT, nullCounterCount);
            }
            for (Enum e : PigWarning.values()) {
                if (e != PigWarning.NULL_COUNTER_COUNT) {
                    Long currentCount = aggMap.get(e);
                    currentCount = (currentCount == null ? 0 : currentCount);
                    // This code checks if the counters is null, if it is,
                    // we need to report to the user that the number
                    // of warning aggregations may not be correct. In fact,
                    // Counters should not be null, it is
                    // a hadoop bug, once this bug is fixed in hadoop, the
                    // null handling code should never be hit.
                    // See Pig-943
                    if (counters != null)
                        currentCount += counters.getCounter(e);
                    aggMap.put(e, currentCount);
                }
            }
        } catch (Exception e) {
            String msg = "Unable to retrieve job to compute warning aggregation.";
            log.warn(msg);
        }
    }

    private void getStats(Job job, boolean errNotDbg,
            PigContext pigContext) throws ExecException {
        JobID MRJobID = job.getAssignedJobID();
        String jobMessage = job.getMessage();
        Exception backendException = null;
        if (MRJobID == null) {
            try {
                LogUtils.writeLog(
                        "Backend error message during job submission",
                        jobMessage,
                        pigContext.getProperties().getProperty("pig.logfile"),
                        log);
                backendException = getExceptionFromString(jobMessage);
            } catch (Exception e) {
                int errCode = 2997;
                String msg = "Unable to recreate exception from backend error: "
                        + jobMessage;
                throw new ExecException(msg, errCode, PigException.BUG);
            }
            throw new ExecException(backendException);
        }
        try {
            TaskReport[] mapRep = HadoopShims.getTaskReports(job, TaskType.MAP);
            if (mapRep != null) {
                getErrorMessages(mapRep, "map", errNotDbg, pigContext);
                totalHadoopTimeSpent += computeTimeSpent(mapRep);
                mapRep = null;
            }
            TaskReport[] redRep = HadoopShims.getTaskReports(job, TaskType.REDUCE);
            if (redRep != null) {
                getErrorMessages(redRep, "reduce", errNotDbg, pigContext);
                totalHadoopTimeSpent += computeTimeSpent(redRep);
                redRep = null;
            }
        } catch (IOException e) {
            if (job.getState() == Job.SUCCESS) {
                // if the job succeeded, let the user know that
                // we were unable to get statistics
                log.warn("Unable to get job related diagnostics");
            } else {
                throw new ExecException(e);
            }
        } catch (Exception e) {
            throw new ExecException(e);
        }
    }
}

