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
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.pig.PigException;
import org.apache.pig.backend.hadoop.executionengine.JobCreationException;
import org.apache.pig.impl.PigContext;
import org.apache.tez.dag.api.TezConfiguration;

/**
 * This is compiler class that takes a TezOperPlan and converts it into a
 * JobControl object with the relevant dependency info maintained. The
 * JobControl object is made up of TezJobs each of which has a JobConf.
 */
public class TezJobControlCompiler {
    private static final Log log = LogFactory.getLog(TezJobControlCompiler.class);

    private PigContext pigContext;
    private TezConfiguration tezConf;

    public TezJobControlCompiler(PigContext pigContext, Configuration conf) throws IOException {
        this.pigContext = pigContext;
        this.tezConf = new TezConfiguration(conf);
    }

    public TezDAG buildDAG(TezOperPlan tezPlan, Map<String, LocalResource> localResources)
            throws IOException, YarnException {
        String jobName = pigContext.getProperties().getProperty(PigContext.JOB_NAME, "pig");
        TezDAG tezDag = new TezDAG(jobName);
        TezDagBuilder dagBuilder = new TezDagBuilder(pigContext, tezPlan, tezDag, localResources);
        dagBuilder.visit();
        return tezDag;
    }

    public TezJobControl compile(TezOperPlan tezPlan, String grpName, TezPlanContainer planContainer)
            throws JobCreationException {
        int timeToSleep;
        String defaultPigJobControlSleep = pigContext.getExecType().isLocal() ? "100" : "1000";
        String pigJobControlSleep = tezConf.get("pig.jobcontrol.sleep", defaultPigJobControlSleep);
        if (!pigJobControlSleep.equals(defaultPigJobControlSleep)) {
            log.info("overriding default JobControl sleep (" +
                    defaultPigJobControlSleep + ") to " + pigJobControlSleep);
        }

        try {
            timeToSleep = Integer.parseInt(pigJobControlSleep);
        } catch (NumberFormatException e) {
            throw new RuntimeException("Invalid configuration " +
                    "pig.jobcontrol.sleep=" + pigJobControlSleep +
                    " should be a time in ms. default=" + defaultPigJobControlSleep, e);
        }

        TezJobControl jobCtrl = new TezJobControl(grpName, timeToSleep);

        try {
            // A single Tez job always pack only 1 Tez plan. We will track
            // Tez job asynchronously to exploit parallel execution opportunities.
            TezJob job = getJob(tezPlan, planContainer);
            jobCtrl.addJob(job);
        } catch (JobCreationException jce) {
            throw jce;
        } catch(Exception e) {
            int errCode = 2017;
            String msg = "Internal error creating job configuration.";
            throw new JobCreationException(msg, errCode, PigException.BUG, e);
        }

        return jobCtrl;
    }

    private TezJob getJob(TezOperPlan tezPlan, TezPlanContainer planContainer)
            throws JobCreationException {
        try {
            Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
            localResources.putAll(planContainer.getLocalResources());
            localResources.putAll(tezPlan.getLocalExtraResources());
            TezDAG tezDag = buildDAG(tezPlan, localResources);
            return new TezJob(tezConf, tezDag, localResources);
        } catch (Exception e) {
            int errCode = 2017;
            String msg = "Internal error creating job configuration.";
            throw new JobCreationException(msg, errCode, PigException.BUG, e);
        }
    }
}

