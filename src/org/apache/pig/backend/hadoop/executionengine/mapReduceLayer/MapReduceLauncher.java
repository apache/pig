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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.pig.ExecType;
import org.apache.pig.PigException;
import org.apache.pig.PigWarning;
import org.apache.pig.PigRunner.ReturnCode;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.executionengine.HExecutionEngine;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MRCompiler.LastInputStreamingOptimizer;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.DotMRPrinter;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.EndOfAllInputSetter;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MRPrinter;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.POPackageAnnotator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POJoinPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.shims.HadoopShims;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.plan.CompilationMessageCollector;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.plan.CompilationMessageCollector.MessageType;
import org.apache.pig.impl.util.ConfigurationValidator;
import org.apache.pig.impl.util.LogUtils;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.PigStatsUtil;
import org.apache.pig.tools.pigstats.ScriptState;


/**
 * Main class that launches pig for Map Reduce
 *
 */
public class MapReduceLauncher extends Launcher{

    public static final String SUCCEEDED_FILE_NAME = "_SUCCESS";
    
    public static final String SUCCESSFUL_JOB_OUTPUT_DIR_MARKER =
        "mapreduce.fileoutputcommitter.marksuccessfuljobs";

    public static final String PROP_EXEC_MAP_PARTAGG = "pig.exec.mapPartAgg";

    
    private static final Log log = LogFactory.getLog(MapReduceLauncher.class);
 
    //used to track the exception thrown by the job control which is run in a separate thread
    private Exception jobControlException = null;
    private String jobControlExceptionStackTrace = null;
    private boolean aggregateWarning = false;

    private Map<FileSpec, Exception> failureMap;
    
    private JobControl jc=null;
    
    private class HangingJobKiller extends Thread {
        public HangingJobKiller() {
        }
        @Override
        public void run() {
            try {
                log.debug("Receive kill signal");
                if (jc!=null) {
                    for (Job job : jc.getRunningJobs()) {
                        RunningJob runningJob = job.getJobClient().getJob(job.getAssignedJobID());
                        if (runningJob!=null)
                            runningJob.killJob();
                        log.info("Job " + job.getJobID() + " killed");
                    }
                }
            } catch (Exception e) {
                log.warn("Encounter exception on cleanup:" + e);
            }
        }
    }

    public MapReduceLauncher() {
        Runtime.getRuntime().addShutdownHook(new HangingJobKiller());
    }
    
    /**
     * Get the exception that caused a failure on the backend for a
     * store location (if any).
     */
    public Exception getError(FileSpec spec) {
        return failureMap.get(spec);
    }

    @Override
    public void reset() {  
        failureMap = new HashMap<FileSpec, Exception>();
        super.reset();
    }
   
    @SuppressWarnings("deprecation")
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
        aggregateWarning = "true".equalsIgnoreCase(pc.getProperties().getProperty("aggregate.warning"));
        MROperPlan mrp = compile(php, pc);
                
        ConfigurationValidator.validatePigProperties(pc.getProperties());
        Configuration conf = ConfigurationUtil.toConfiguration(pc.getProperties());
        
        HExecutionEngine exe = pc.getExecutionEngine();
        JobClient jobClient = new JobClient(exe.getJobConf());
        
        JobControlCompiler jcc = new JobControlCompiler(pc, conf);
        
        // start collecting statistics
        PigStatsUtil.startCollection(pc, jobClient, jcc, mrp); 
        
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
            pc.getProperties().getProperty("stop.on.failure", "false").equals("true");
        
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
                            ScriptState.get().emitJobsSubmittedNotification(1);
                            natOp.runJob();
                            numMRJobsCompl++;
                        } catch (IOException e) {
                            
                            mrp.trimBelow(natOp);
                            failedNativeMR.add(natOp);
                            
                            String msg = "Error running native mapreduce" +
                            " operator job :" + natOp.getJobId() + e.getMessage();
                            
                            String stackTrace = getStackStraceStr(e);
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
            ScriptState.get().emitJobsSubmittedNotification(jobsWithoutIds.size());
            
            // update Pig stats' job DAG with just compiled jobs
            PigStatsUtil.updateJobMroMap(jcc.getJobMroMap());
            
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
                job.getJobConf().set("pig.script.submitted.timestamp",
                                Long.toString(scriptSubmittedTimestamp));
                job.getJobConf().set("pig.job.submitted.timestamp",
                                Long.toString(System.currentTimeMillis()));
            }

            //All the setup done, now lets launch the jobs.
            jcThread.start();
            
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
                            String alias = ScriptState.get().getAlias(mro);
                            log.info("Processing aliases " + alias);
                            String aliasLocation = ScriptState.get().getAliasLocation(mro);
                            log.info("detailed locations: " + aliasLocation);
                        }

                        
            			if(jobTrackerLoc != null){
            				log.info("More information at: http://"+ jobTrackerLoc+
            						"/jobdetails.jsp?jobid="+job.getAssignedJobID());
            			}  

                        // update statistics for this job so jobId is set
                        PigStatsUtil.addJobStats(job);
            			ScriptState.get().emitJobStartedNotification(
                                job.getAssignedJobID().toString());                        
            		}
            		else{
            			// This job is not assigned an id yet.
            		}
            	}
            	jobsWithoutIds.removeAll(jobsAssignedIdInThisRun);

            	double prog = (numMRJobsCompl+calculateProgress(jc, jobClient))/totalMRJobs;
            	notifyProgress(prog, lastProg);
            	lastProg = prog;
            	
            	// collect job stats by frequently polling of completed jobs (PIG-1829)
            	PigStatsUtil.accumulateStats(jc);
            	
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
            PigStatsUtil.accumulateStats(jc);

            jc.stop(); 
        }

        ScriptState.get().emitProgressUpdatedNotification(100);
        
        log.info( "100% complete");
             
        boolean failed = false;
        
        if(failedNativeMR.size() > 0){
            failed = true;
        }
        
        // Look to see if any jobs failed.  If so, we need to report that.
        if (failedJobs != null && failedJobs.size() > 0) {
            
            Exception backendException = null;
            for (Job fj : failedJobs) {                
                try {
                    getStats(fj, jobClient, true, pc);
                } catch (Exception e) {
                    backendException = e;
                }
                List<POStore> sts = jcc.getStores(fj);
                for (POStore st: sts) {
                    failureMap.put(st.getSFile(), backendException);
                }
                PigStatsUtil.setBackendException(fj, backendException);
            }
            failed = true;
        }
        
        // stats collection is done, log the results
        PigStatsUtil.stopCollection(true); 
        
        // PigStatsUtil.stopCollection also computes the return code based on
        // total jobs to run, jobs successful and jobs failed
        failed = failed || !PigStats.get().isSuccessful();
        
        Map<Enum, Long> warningAggMap = new HashMap<Enum, Long>();
                
        if (succJobs != null) {
            for (Job job : succJobs) {
                List<POStore> sts = jcc.getStores(job);                
                for (POStore st : sts) {
                    if (pc.getExecType() == ExecType.LOCAL) {
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
                                
                getStats(job, jobClient, false, pc);
                if (aggregateWarning) {
                    computeWarningAggregate(job, jobClient, warningAggMap);
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
        return PigStatsUtil.getPigStats(ret);
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
                msg.append(j.getMessage());
                if (i!=jc.getFailedJobs().size()-1) {
                    msg.append("\n");
                }
            }
            
            throw new ExecException(msg.toString(), errCode,
                    PigException.REMOTE_ENVIRONMENT);
        }
    }
    
    private String getStackStraceStr(Throwable e) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        e.printStackTrace(ps);
        return baos.toString();
    }

    /**
     * Log the progress and notify listeners if there is sufficient progress 
     * @param prog current progress
     * @param lastProg progress last time
     */
    private void notifyProgress(double prog, double lastProg) {
        if(prog>=(lastProg+0.01)){
            int perCom = (int)(prog * 100);
            if(perCom!=100) {
                log.info( perCom + "% complete");
                
                ScriptState.get().emitProgressUpdatedNotification(perCom);
            }
        }      
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
        comp.randomizeFileLocalizer();
        comp.compile();
        comp.aggregateScalarsFiles();
        MROperPlan plan = comp.getMRPlan();
        
        //display the warning message(s) from the MRCompiler
        comp.getMessageCollector().logMessages(MessageType.Warning, aggregateWarning, log);
        
        String lastInputChunkSize = 
            pc.getProperties().getProperty(
                    "last.input.chunksize", POJoinPackage.DEFAULT_CHUNK_SIZE);
        
        String prop = pc.getProperties().getProperty("pig.exec.nocombiner");
        if (!pc.inIllustrator && !("true".equals(prop)))  {
            boolean doMapAgg = 
                    Boolean.valueOf(pc.getProperties().getProperty(PROP_EXEC_MAP_PARTAGG,"false"));
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
        prop = pc.getProperties().getProperty("pig.exec.nosecondarykey");
        if (!pc.inIllustrator && !("true".equals(prop)))  {
            SecondaryKeyOptimizer skOptimizer = new SecondaryKeyOptimizer(plan);
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
            "true".equalsIgnoreCase(pc.getProperties().getProperty("opt.multiquery","true"));
        
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
            "true".equalsIgnoreCase(pc.getProperties().getProperty("opt.accumulator","true"));
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
            FileSystem fs = outputPath.getFileSystem(job.getJobConf());
            if(fs.exists(outputPath)){
                // create a file in the folder to mark it
                Path filePath = new Path(outputPath, SUCCEEDED_FILE_NAME);
                if(!fs.exists(filePath)) {
                    fs.create(filePath).close();
                }
            }    
        } 
    }
    
    /**
     * An exception handler class to handle exceptions thrown by the job controller thread
     * Its a local class. This is the only mechanism to catch unhandled thread exceptions
     * Unhandled exceptions in threads are handled by the VM if the handler is not registered
     * explicitly or if the default handler is null
     */
    class JobControlThreadExceptionHandler implements Thread.UncaughtExceptionHandler {
        
        @Override
        public void uncaughtException(Thread thread, Throwable throwable) {
            jobControlExceptionStackTrace = getStackStraceStr(throwable);
            try {	
                jobControlException = getExceptionFromString(jobControlExceptionStackTrace);
            } catch (Exception e) {
                String errMsg = "Could not resolve error that occured when launching map reduce job: "
                        + jobControlExceptionStackTrace;
                jobControlException = new RuntimeException(errMsg, throwable);
            }
        }
    }
    
    @SuppressWarnings("deprecation")
    void computeWarningAggregate(Job job, JobClient jobClient, Map<Enum, Long> aggMap) {
        JobID mapRedJobID = job.getAssignedJobID();
        RunningJob runningJob = null;
        try {
            runningJob = jobClient.getJob(mapRedJobID);
            if(runningJob != null) {
                Counters counters = runningJob.getCounters();
                if (counters==null)
                {
                    long nullCounterCount = aggMap.get(PigWarning.NULL_COUNTER_COUNT)==null?0 : aggMap.get(PigWarning.NULL_COUNTER_COUNT);
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
            }
        } catch (IOException ioe) {
            String msg = "Unable to retrieve job to compute warning aggregation.";
            log.warn(msg);
        }    	
    }

}
