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
import java.util.List;
import java.util.LinkedList;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.pig.ExecType;
import org.apache.pig.PigException;
import org.apache.pig.PigWarning;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecutionEngine;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.executionengine.HExecutionEngine;
import org.apache.pig.impl.PigContext;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MRCompiler.LastInputStreamingOptimizer;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.EndOfAllInputSetter;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MRPrinter;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.DotMRPrinter;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.POPackageAnnotator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POJoinPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.impl.plan.CompilationMessageCollector;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.plan.CompilationMessageCollector.MessageType;
import org.apache.pig.impl.util.ConfigurationValidator;
import org.apache.pig.impl.util.LogUtils;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.tools.pigstats.PigStats;

/**
 * Main class that launches pig for Map Reduce
 *
 */
public class MapReduceLauncher extends Launcher{
    private static final Log log = LogFactory.getLog(MapReduceLauncher.class);
 
    //used to track the exception thrown by the job control which is run in a separate thread
    private Exception jobControlException = null;
    private String jobControlExceptionStackTrace = null;
    private boolean aggregateWarning = false;
    private Map<FileSpec, Exception> failureMap;

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
        PigStats stats = new PigStats();
        stats.setMROperatorPlan(mrp);
        stats.setExecType(pc.getExecType());
        stats.setPhysicalPlan(php);
        
        ExecutionEngine exe = pc.getExecutionEngine();
        ConfigurationValidator.validatePigProperties(exe.getConfiguration());
        Configuration conf = ConfigurationUtil.toConfiguration(exe.getConfiguration());
        JobClient jobClient = new JobClient(((HExecutionEngine)exe).getJobConf());

        JobControlCompiler jcc = new JobControlCompiler(pc, conf);
        
        List<Job> failedJobs = new LinkedList<Job>();
        List<Job> completeFailedJobsInThisRun = new LinkedList<Job>();
        List<Job> succJobs = new LinkedList<Job>();
        JobControl jc;
        int totalMRJobs = mrp.size();
        int numMRJobsCompl = 0;
        double lastProg = -1;
        
        //create the exception handler for the job control thread
        //and register the handler with the job control thread
        JobControlThreadExceptionHandler jctExceptionHandler = new JobControlThreadExceptionHandler();

        while((jc = jcc.compile(mrp, grpName)) != null) {
            
        	// Initially, all jobs are in wait state.
            List<Job> jobsWithoutIds = jc.getWaitingJobs();
            log.info(jobsWithoutIds.size() +" map-reduce job(s) waiting for submission.");
            
            String jobTrackerAdd;
            String port;
            String jobTrackerLoc;
            JobConf jobConf = jobsWithoutIds.get(0).getJobConf();
            try {
                port = jobConf.get("mapred.job.tracker.http.address");
                jobTrackerAdd = jobConf.get(HExecutionEngine.JOB_TRACKER_LOCATION);
                jobTrackerLoc = jobTrackerAdd.substring(0,jobTrackerAdd.indexOf(":")) + port.substring(port.indexOf(":"));
            }
            catch(Exception e){
                // Could not get the job tracker location, most probably we are running in local mode.
                // If it is the case, we don't print out job tracker location,
                // because it is meaningless for local mode.
            	jobTrackerLoc = null;
                log.debug("Failed to get job tracker location.");
            }
            
            completeFailedJobsInThisRun.clear();
            
            Thread jcThread = new Thread(jc);
            jcThread.setUncaughtExceptionHandler(jctExceptionHandler);
            
            //All the setup done, now lets launch the jobs.
            jcThread.start();
            
            // Now wait, till we are finished.
            while(!jc.allFinished()){

            	try { Thread.sleep(sleepTime); } 
            	catch (InterruptedException e) {}
            	
            	List<Job> jobsAssignedIdInThisRun = new ArrayList<Job>();

            	for(Job job : jobsWithoutIds){
            		if (job.getAssignedJobID() != null){

            			jobsAssignedIdInThisRun.add(job);
            			log.info("HadoopJobId: "+job.getAssignedJobID());
            			if(jobTrackerLoc != null){
            				log.info("More information at: http://"+ jobTrackerLoc+
            						"/jobdetails.jsp?jobid="+job.getAssignedJobID());
            			}  
            		}
            		else{
            			// This job is not assigned an id yet.
            		}
            	}
            	jobsWithoutIds.removeAll(jobsAssignedIdInThisRun);

            	double prog = (numMRJobsCompl+calculateProgress(jc, jobClient))/totalMRJobs;
            	if(prog>=(lastProg+0.01)){
            		int perCom = (int)(prog * 100);
            		if(perCom!=100)
            			log.info( perCom + "% complete");
            	}
            	lastProg = prog;
            }

            //check for the jobControlException first
            //if the job controller fails before launching the jobs then there are
            //no jobs to check for failure
            if(jobControlException != null) {
                if(jobControlException instanceof PigException) {
                        if(jobControlExceptionStackTrace != null) {
                            LogUtils.writeLog("Error message from job controller", jobControlExceptionStackTrace, 
                                    pc.getProperties().getProperty("pig.logfile"), 
                                    log);
                        }
                        throw jobControlException;
                } else {
                        int errCode = 2117;
                        String msg = "Unexpected error when launching map reduce job.";        	
                        throw new ExecException(msg, errCode, PigException.BUG, jobControlException);
                }
            }

            if (!jc.getFailedJobs().isEmpty() )
            {
                if ("true".equalsIgnoreCase(
                  pc.getProperties().getProperty("stop.on.failure","false"))) {
                    int errCode = 6017;
                    StringBuilder msg = new StringBuilder();
                    
                    for (int i=0;i<jc.getFailedJobs().size();i++) {
                        Job j = jc.getFailedJobs().get(i);
                        msg.append(getFirstLineFromMessage(j.getMessage()));
                        if (i!=jc.getFailedJobs().size()-1)
                            msg.append("\n");
                    }
                    
                    throw new ExecException(msg.toString(), 
                                            errCode, PigException.REMOTE_ENVIRONMENT);
                }
                // If we only have one store and that job fail, then we sure that the job completely fail, and we shall stop dependent jobs
                for (Job job : jc.getFailedJobs())
                {
                    List<POStore> sts = jcc.getStores(job);
                    if (sts.size()==1)
                        completeFailedJobsInThisRun.add(job);
                }
                failedJobs.addAll(jc.getFailedJobs());
            }
            
            int removedMROp = jcc.updateMROpPlan(completeFailedJobsInThisRun);
            
            numMRJobsCompl += removedMROp;

            List<Job> jobs = jc.getSuccessfulJobs();
            jcc.moveResults(jobs);
            succJobs.addAll(jobs);
            
            
            stats.setJobClient(jobClient);
            stats.setJobControl(jc);
            stats.accumulateStats();
            
            jc.stop(); 
        }

        log.info( "100% complete");

        boolean failed = false;
        int finalStores = 0;
        // Look to see if any jobs failed.  If so, we need to report that.
        if (failedJobs != null && failedJobs.size() > 0) {
            log.error(failedJobs.size()+" map reduce job(s) failed!");
            Exception backendException = null;

            for (Job fj : failedJobs) {
                
                try {
                    getStats(fj, jobClient, true, pc);
                } catch (Exception e) {
                    backendException = e;
                }

                List<POStore> sts = jcc.getStores(fj);
                for (POStore st: sts) {
                    if (!st.isTmpStore()) {
                        finalStores++;
                        log.error("Failed to produce result in: \""+st.getSFile().getFileName()+"\"");
                    }
                    failedStores.add(st.getSFile());
                    failureMap.put(st.getSFile(), backendException);
                    FileLocalizer.registerDeleteOnFail(st.getSFile().getFileName(), pc);
                    //log.error("Failed to produce result in: \""+st.getSFile().getFileName()+"\"");
                }
            }
            failed = true;
        }

        Map<Enum, Long> warningAggMap = new HashMap<Enum, Long>();
                
        if(succJobs!=null) {
            for(Job job : succJobs){
                List<POStore> sts = jcc.getStores(job);
                for (POStore st: sts) {
                    // Currently (as of Feb 3 2010), hadoop's local mode does not
                    // call cleanupJob on OutputCommitter (see https://issues.apache.org/jira/browse/MAPREDUCE-1447)
                    // So to workaround that bug, we are calling setStoreSchema on
                    // StoreFunc's which implement StoreMetadata here
                    /**********************************************************/
                    // NOTE: THE FOLLOWING IF SHOULD BE REMOVED ONCE MAPREDUCE-1447
                    // IS FIXED - TestStore.testSetStoreSchema() should fail at
                    // that time and removing this code should fix it.
                    /**********************************************************/
                    if(pc.getExecType() == ExecType.LOCAL) {
                        storeSchema(job, st);
                    }
                    if (!st.isTmpStore()) {
                        succeededStores.add(st.getSFile());
                        finalStores++;
                        log.info("Successfully stored result in: \""+st.getSFile().getFileName()+"\"");
                    }
                    else
                        log.debug("Successfully stored result in: \""+st.getSFile().getFileName()+"\"");
                }
                getStats(job,jobClient, false, pc);
                if(aggregateWarning) {
                    computeWarningAggregate(job, jobClient, warningAggMap);
                }
            }
        }
        
        if(aggregateWarning) {
            CompilationMessageCollector.logAggregate(warningAggMap, MessageType.Warning, log) ;
        }
        
        // Report records and bytes written.  Only do this in the single store case.  Multi-store
        // scripts mess up the stats reporting from hadoop.
        List<String> rji = stats.getRootJobIDs();
        if ( (rji != null && rji.size() == 1 && finalStores == 1) || pc.getExecType() == ExecType.LOCAL ) {
            if(stats.getRecordsWritten()==-1) {
                log.info("Records written : Unable to determine number of records written");
            } else {
                log.info("Records written : " + stats.getRecordsWritten());
            }
            if(stats.getBytesWritten()==-1) {
                log.info("Bytes written : Unable to determine number of bytes written");
            } else {
                log.info("Bytes written : " + stats.getBytesWritten());
            }
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

        return stats;
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

    private MROperPlan compile(
            PhysicalPlan php,
            PigContext pc) throws PlanException, IOException, VisitorException {
        MRCompiler comp = new MRCompiler(php, pc);
        comp.randomizeFileLocalizer();
        comp.compile();
        MROperPlan plan = comp.getMRPlan();
        
        //display the warning message(s) from the MRCompiler
        comp.getMessageCollector().logMessages(MessageType.Warning, aggregateWarning, log);
        
        String lastInputChunkSize = 
            pc.getProperties().getProperty(
                    "last.input.chunksize", POJoinPackage.DEFAULT_CHUNK_SIZE);
        
        //String prop = System.getProperty("pig.exec.nocombiner");
        String prop = pc.getProperties().getProperty("pig.exec.nocombiner");
        if (!("true".equals(prop)))  {
            CombinerOptimizer co = new CombinerOptimizer(plan, lastInputChunkSize);
            co.visit();
            //display the warning message(s) from the CombinerOptimizer
            co.getMessageCollector().logMessages(MessageType.Warning, aggregateWarning, log);
        }
        
        // Optimize the jobs that have a load/store only first MR job followed
        // by a sample job.
        SampleOptimizer so = new SampleOptimizer(plan);
        so.visit();
        
        // Optimize to use secondary sort key if possible
        prop = System.getProperty("pig.exec.nosecondarykey");
        if (!("true".equals(prop)))  {
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
            MultiQueryOptimizer mqOptimizer = new MultiQueryOptimizer(plan);
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
    
    /**
     * @param job
     * @param st
     * @throws IOException 
     */
    private void storeSchema(Job job, POStore st) throws IOException {
        JobContext jc = new JobContext(job.getJobConf(), 
                new org.apache.hadoop.mapreduce.JobID());
        JobContext updatedJc = PigOutputCommitter.setUpContext(jc, st);
        PigOutputCommitter.storeCleanup(st, updatedJc.getConfiguration());
    }

    
    /**
     * An exception handler class to handle exceptions thrown by the job controller thread
     * Its a local class. This is the only mechanism to catch unhandled thread exceptions
     * Unhandled exceptions in threads are handled by the VM if the handler is not registered
     * explicitly or if the default handler is null
     */
    class JobControlThreadExceptionHandler implements Thread.UncaughtExceptionHandler {
        
        public void uncaughtException(Thread thread, Throwable throwable) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            PrintStream ps = new PrintStream(baos);
            throwable.printStackTrace(ps);
            jobControlExceptionStackTrace = baos.toString();    		
            try {	
                jobControlException = getExceptionFromString(jobControlExceptionStackTrace);
            } catch (Exception e) {
                String errMsg = "Could not resolve error that occured when launching map reduce job: "
                        + getFirstLineFromMessage(jobControlExceptionStackTrace);
                jobControlException = new RuntimeException(errMsg);
            }
        }
    }
    
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
