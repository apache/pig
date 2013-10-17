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
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.pig.backend.BackendException;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.executionengine.Launcher;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.tools.pigstats.PigStats;

/**
 * Main class that launches pig for Tez
 */
public class TezLauncher extends Launcher {
    private static final Log log = LogFactory.getLog(TezLauncher.class);

    @Override
    public PigStats launchPig(PhysicalPlan php, String grpName, PigContext pc)
            throws Exception {
        // TODO: TezStats is a skeleton class for now.
        PigStats ret = new TezStats();

        Configuration conf = ConfigurationUtil.toConfiguration(pc.getProperties(), true);
        TezJobControlCompiler jcc = new TezJobControlCompiler(pc, conf);
        TezOperPlan tezPlan = compile(php, pc);

        TezPOPackageAnnotator pkgAnnotator = new TezPOPackageAnnotator(tezPlan);
        pkgAnnotator.visit();

        jc = jcc.compile(tezPlan, grpName);

        // Initially, all jobs are in wait state.
        List<ControlledJob> jobsWithoutIds = jc.getWaitingJobList();
        log.info(jobsWithoutIds.size() + " tez job(s) waiting for submission.");

        // TODO: MapReduceLauncher does a couple of things here. For example,
        // notify PPNL of job submission, update PigStas, etc. We will worry
        // about them later.

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

        // Mark the times that the jobs were submitted so it's reflected in job
        // history props
        long scriptSubmittedTimestamp = System.currentTimeMillis();
        for (ControlledJob job : jobsWithoutIds) {
            // Job.getConfiguration returns the shared configuration object
            Configuration jobConf = job.getJob().getConfiguration();
            jobConf.set("pig.script.submitted.timestamp",
                    Long.toString(scriptSubmittedTimestamp));
            jobConf.set("pig.job.submitted.timestamp",
                    Long.toString(System.currentTimeMillis()));
        }

        // All the setup done, now lets launch the jobs. DAG is submitted to
        // YARN cluster by TezJob.submit().
        jcThread.start();

        // Wait for all the jobs are finished.
        while (!jc.allFinished()) {
            try {
                jcThread.join(500);
            } catch (InterruptedException e) {
                // Do nothing
            }
        }

        return ret;
    }

    @Override
    public void explain(PhysicalPlan php, PigContext pc, PrintStream ps,
            String format, boolean verbose) throws PlanException,
            VisitorException, IOException {
        log.debug("Entering TezLauncher.explain");
        TezOperPlan tezp = compile(php, pc);

        if (format.equals("text")) {
            TezPrinter printer = new TezPrinter(ps, tezp);
            printer.setVerbose(verbose);
            printer.visit();
        } else {
            // TODO: add support for other file format
            throw new IOException("Non-text output of explain is not supported.");
        }
    }

    public TezOperPlan compile(PhysicalPlan php, PigContext pc)
            throws PlanException, IOException, VisitorException {
        TezCompiler comp = new TezCompiler(php, pc);
        comp.compile();
        // TODO: Run optimizations here
        return comp.getTezPlan();
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

