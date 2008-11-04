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
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MRCompiler.CoGroupStreamingOptimizerVisitor;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MRPrinter;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MRStreamHandler;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.POPackageAnnotator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POJoinPackage;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.ConfigurationValidator;

/**
 * Main class that launches pig for Map Reduce
 *
 */
public class MapReduceLauncher extends Launcher{
    private static final Log log = LogFactory.getLog(MapReduceLauncher.class);
    @Override
    public boolean launchPig(PhysicalPlan php,
                             String grpName,
                             PigContext pc) throws PlanException,
                                                   VisitorException,
                                                   IOException,
                                                   ExecException,
                                                   JobCreationException {
        long sleepTime = 5000;
        MROperPlan mrp = compile(php, pc);
        
        ExecutionEngine exe = pc.getExecutionEngine();
        ConfigurationValidator.validatePigProperties(exe.getConfiguration());
        Configuration conf = ConfigurationUtil.toConfiguration(exe.getConfiguration());
        JobClient jobClient = ((HExecutionEngine)exe).getJobClient();

        JobControlCompiler jcc = new JobControlCompiler();
        
        JobControl jc = jcc.compile(mrp, grpName, conf, pc);
        
        int numMRJobs = jc.getWaitingJobs().size();
        
        new Thread(jc).start();

        double lastProg = -1;
        int perCom = 0;
        while(!jc.allFinished()){
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {}
            double prog = calculateProgress(jc, jobClient)/numMRJobs;
            if(prog>=(lastProg+0.01)){
                perCom = (int)(prog * 100);
                if(perCom!=100)
                    log.info( perCom + "% complete");
            }
            lastProg = prog;
        }
        // Look to see if any jobs failed.  If so, we need to report that.
        List<Job> failedJobs = jc.getFailedJobs();
        if (failedJobs != null && failedJobs.size() > 0) {
            log.error("Map reduce job failed");
            for (Job fj : failedJobs) {
                log.error(fj.getMessage());
                getStats(fj, jobClient, true);
            }
            jc.stop(); 
            return false;
        }

        List<Job> succJobs = jc.getSuccessfulJobs();
        if(succJobs!=null)
            for(Job job : succJobs){
                getStats(job,jobClient, false);
            }

        jc.stop(); 
        log.info( "100% complete");
        log.info("Success!");
        return true;
    }

    @Override
    public void explain(
            PhysicalPlan php,
            PigContext pc,
            PrintStream ps) throws PlanException, VisitorException,
                                   IOException {
        log.trace("Entering MapReduceLauncher.explain");
        MROperPlan mrp = compile(php, pc);

        MRPrinter printer = new MRPrinter(ps, mrp);
        printer.visit();
    }

    private MROperPlan compile(
            PhysicalPlan php,
            PigContext pc) throws PlanException, IOException, VisitorException {
        MRCompiler comp = new MRCompiler(php, pc);
        comp.randomizeFileLocalizer();
        comp.compile();
        MROperPlan plan = comp.getMRPlan();
        String prop = System.getProperty("pig.exec.nocombiner");
        if (!("true".equals(prop)))  {
            CombinerOptimizer co = new CombinerOptimizer(plan);
            co.visit();
        }
        
        // optimize key - value handling in package
        POPackageAnnotator pkgAnnotator = new POPackageAnnotator(plan);
        pkgAnnotator.visit();
        
        // check whether stream operator is present
        MRStreamHandler checker = new MRStreamHandler(plan);
        checker.visit();
        
        // optimize joins
        CoGroupStreamingOptimizerVisitor cgso = new MRCompiler.CoGroupStreamingOptimizerVisitor(plan, 
                pc.getProperties().getProperty("join.biggest.input.chunksize", POJoinPackage.DEFAULT_CHUNK_SIZE));
        cgso.visit();
        
        // figure out the type of the key for the map plan
        // this is needed when the key is null to create
        // an appropriate NullableXXXWritable object
        KeyTypeDiscoveryVisitor kdv = new KeyTypeDiscoveryVisitor(plan);
        kdv.visit();
        return plan;
    }
 
}
