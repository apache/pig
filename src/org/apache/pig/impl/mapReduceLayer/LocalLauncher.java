package org.apache.pig.impl.mapReduceLayer;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.impl.physicalLayer.PhysicalOperator;
import org.apache.pig.impl.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;


public class LocalLauncher extends Launcher{
    private static final Log log = LogFactory.getLog(Launcher.class);
    
    @Override
    public boolean launchPig(PhysicalPlan<PhysicalOperator> php,
                             String grpName,
                             PigContext pc) throws PlanException,
                                                   VisitorException,
                                                   IOException,
                                                   ExecException,
                                                   JobCreationException {
        long sleepTime = 500;
        MRCompiler comp = new MRCompiler(php, pc);
        comp.compile();
        
        Configuration conf = new Configuration();
        conf.set("mapred.job.tracker", "local");
        JobClient jobClient = new JobClient(new JobConf(conf));

        MROperPlan mrp = comp.getMRPlan();
        JobControlCompiler jcc = new JobControlCompiler();
        
        JobControl jc = jcc.compile(mrp, grpName, conf, pc);
        
        
        int numMRJobs = jc.getWaitingJobs().size();
        
        new Thread(jc).start();

        double lastProg = -1;
        while(!jc.allFinished()){
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {}
            double prog = calculateProgress(jc, jobClient)/numMRJobs;
            if(prog>lastProg)
                log.info(prog * 100 + "% complete");
            lastProg = prog;
        }
        lastProg = calculateProgress(jc, jobClient)/numMRJobs;
        if(isComplete(lastProg))
            log.info("Completed Successfully");
        else{
            log.info("Unsuccessful attempt. Completed " + lastProg * 100 + "% of the job");
            List<Job> failedJobs = jc.getFailedJobs();
            if(failedJobs==null)
                throw new ExecException("Something terribly wrong with Job Control.");
            for (Job job : failedJobs) {
                getStats(job,jobClient);
            }
        }
        List<Job> succJobs = jc.getSuccessfulJobs();
        if(succJobs!=null)
            for(Job job : succJobs){
                getStats(job,jobClient);
            }

        jc.stop(); 
        
        return isComplete(lastProg);
    }
    
    //A purely testing method. Not to be used elsewhere
    public boolean launchPigWithCombinePlan(PhysicalPlan<PhysicalOperator> php,
            String grpName, PigContext pc, PhysicalPlan combinePlan) throws PlanException,
            VisitorException, IOException, ExecException, JobCreationException {
        long sleepTime = 500;
        MRCompiler comp = new MRCompiler(php, pc);
        comp.compile();

        Configuration conf = new Configuration();
        conf.set("mapred.job.tracker", "local");
        JobClient jobClient = new JobClient(new JobConf(conf));

        MROperPlan mrp = comp.getMRPlan();
        if(mrp.getLeaves().get(0)!=mrp.getRoots().get(0))
            throw new PlanException("Unsupported configuration to test combine plan");
        
        MapReduceOper mro = mrp.getLeaves().get(0);
        mro.combinePlan = combinePlan;
        
        JobControlCompiler jcc = new JobControlCompiler();

        JobControl jc = jcc.compile(mrp, grpName, conf, pc);

        int numMRJobs = jc.getWaitingJobs().size();

        new Thread(jc).start();

        double lastProg = -1;
        while (!jc.allFinished()) {
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
            }
            double prog = calculateProgress(jc, jobClient) / numMRJobs;
            if (prog > lastProg)
                log.info(prog * 100 + "% complete");
            lastProg = prog;
        }
        lastProg = calculateProgress(jc, jobClient) / numMRJobs;
        if (isComplete(lastProg))
            log.info("Completed Successfully");
        else {
            log.info("Unsuccessful attempt. Completed " + lastProg * 100
                    + "% of the job");
            List<Job> failedJobs = jc.getFailedJobs();
            if (failedJobs == null)
                throw new ExecException(
                        "Something terribly wrong with Job Control.");
            for (Job job : failedJobs) {
                getStats(job, jobClient);
            }
        }
        List<Job> succJobs = jc.getSuccessfulJobs();
        if (succJobs != null)
            for (Job job : succJobs) {
                getStats(job, jobClient);
            }

        jc.stop();

        return isComplete(lastProg);
    }
}
