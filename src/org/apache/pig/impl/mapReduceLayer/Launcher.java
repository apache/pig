package org.apache.pig.impl.mapReduceLayer;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskReport;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecutionEngine;
import org.apache.pig.backend.hadoop.datastorage.HConfiguration;
import org.apache.pig.backend.hadoop.executionengine.HExecutionEngine;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.impl.physicalLayer.PhysicalOperator;
import org.apache.pig.impl.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;

public abstract class Launcher {
    private static final Log log = LogFactory.getLog(Launcher.class);
    
    long totalHadoopTimeSpent;
    
    protected Launcher(){
        totalHadoopTimeSpent = 0;
    }
    /**
     * Method to launch pig for hadoop either for a cluster's
     * job tracker or for a local job runner. THe only difference
     * between the two is the job client. Depending on the pig context
     * the job client will be initialize to one of the two.
     * Launchers for other frameworks can overide these methods.
     * Given an input PhysicalPlan, it compiles it
     * to get a MapReduce Plan. The MapReduce plan which
     * has multiple MapReduce operators each one of which
     * has to be run as a map reduce job with dependency
     * information stored in the plan. It compiles the
     * MROperPlan into a JobControl object. Each Map Reduce
     * operator is converted into a Job and added to the JobControl
     * object. Each Job also has a set of dependent Jobs that
     * are created using the MROperPlan.
     * The JobControl object is obtained from the JobControlCompiler
     * Then a new thread is spawned that submits these jobs
     * while respecting the dependency information.
     * The parent thread monitors the submitted jobs' progress and
     * after it is complete, stops the JobControl thread.
     * @param php
     * @param grpName
     * @param pc
     * @throws PlanException
     * @throws VisitorException
     * @throws IOException
     * @throws ExecException
     * @throws JobCreationException
     */
    public abstract boolean launchPig(PhysicalPlan<PhysicalOperator> php, String grpName, PigContext pc)
            throws PlanException, VisitorException, IOException, ExecException,
            JobCreationException;
    
    protected boolean isComplete(double prog){
        return (int)(Math.ceil(prog)) == (int)1;
    }
    
    protected void getStats(Job job, JobClient jobClient) throws IOException{
        String MRJobID = job.getMapredJobID();
        TaskReport[] mapRep = jobClient.getMapTaskReports(MRJobID);
        getErrorMessages(mapRep, "map");
        totalHadoopTimeSpent += computeTimeSpent(mapRep);
        TaskReport[] redRep = jobClient.getReduceTaskReports(MRJobID);
        getErrorMessages(redRep, "reduce");
        totalHadoopTimeSpent += computeTimeSpent(mapRep);
    }
    
    protected long computeTimeSpent(TaskReport[] mapReports) {
        long timeSpent = 0;
        for (TaskReport r : mapReports) {
            timeSpent += (r.getFinishTime() - r.getStartTime());
        }
        return timeSpent;
    }
    
    protected void getErrorMessages(TaskReport reports[], String type)
    {
        for (int i = 0; i < reports.length; i++) {
            String msgs[] = reports[i].getDiagnostics();
            StringBuilder sb = new StringBuilder("Error message from task (" + type + ") " +
                reports[i].getTaskId());
            for (int j = 0; j < msgs.length; j++) {
                sb.append(" " + msgs[j]);
            }
            log.error(sb.toString());
        }
    }
    
    /**
     * Compute the progress of the current job submitted 
     * through the JobControl object jc to the JobClient jobClient
     * @param jc - The JobControl object that has been submitted
     * @param jobClient - The JobClient to which it has been submitted
     * @return The progress as a precentage in double format
     * @throws IOException
     */
    protected double calculateProgress(JobControl jc, JobClient jobClient) throws IOException{
        double prog = 0.0;
        prog += jc.getSuccessfulJobs().size();
        
        List runnJobs = jc.getRunningJobs();
        for (Object object : runnJobs) {
            Job j = (Job)object;
            prog += progressOfRunningJob(j, jobClient);
        }
        return prog;
    }
    
    /**
     * Returns the progress of a Job j which is part of a submitted
     * JobControl object. The progress is for this Job. So it has to
     * be scaled down by the num of jobs that are present in the 
     * JobControl.
     * @param j - The Job for which progress is required
     * @param jobClient - the JobClient to which it has been submitted
     * @return Returns the percentage progress of this Job
     * @throws IOException
     */
    protected double progressOfRunningJob(Job j, JobClient jobClient) throws IOException{
        String mrJobID = j.getMapredJobID();
        RunningJob rj = jobClient.getJob(mrJobID);
        if(rj==null && j.getState()==Job.SUCCESS)
            return 1;
        else if(rj==null)
            return 0;
        else{
            double mapProg = rj.mapProgress();
            double redProg = rj.reduceProgress();
            return (mapProg + redProg)/2;
        }
    }
    public long getTotalHadoopTimeSpent() {
        return totalHadoopTimeSpent;
    }
}
