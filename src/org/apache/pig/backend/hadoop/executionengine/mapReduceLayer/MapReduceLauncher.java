package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer;

import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecutionEngine;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.datastorage.HConfiguration;
import org.apache.pig.backend.hadoop.executionengine.HExecutionEngine;
import org.apache.pig.impl.PigContext;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MRPrinter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;

/**
 * Main class that launches pig for Map Reduce
 *
 */
public class MapReduceLauncher extends Launcher{
    private static final Log log = LogFactory.getLog(Launcher.class);
    @Override
    public boolean launchPig(PhysicalPlan php,
                             String grpName,
                             PigContext pc) throws PlanException,
                                                   VisitorException,
                                                   IOException,
                                                   ExecException,
                                                   JobCreationException {
        long sleepTime = 500;
        MRCompiler comp = new MRCompiler(php, pc);
        comp.compile();
        
        ExecutionEngine exe = pc.getExecutionEngine();
        Configuration conf = ConfigurationUtil.toConfiguration(exe.getConfiguration());
        JobClient jobClient = ((HExecutionEngine)exe).getJobClient();

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

    @Override
    public void explain(PhysicalPlan php,
                        PigContext pc,
                        PrintStream ps) throws PlanException,
                                               VisitorException,
                                               IOException {
        MRCompiler comp = new MRCompiler(php, pc);
        comp.compile();
        MROperPlan mrp = comp.getMRPlan();

        MRPrinter printer = new MRPrinter(ps, mrp);
        printer.visit();
    }
 
}
