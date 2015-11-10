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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.pig.PigException;
import org.apache.pig.backend.hadoop.executionengine.JobCreationException;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezOperPlan;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezPlanContainer;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezPlanContainerNode;
import org.apache.pig.impl.PigContext;
import org.apache.pig.tools.pigstats.tez.TezScriptState;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.TezConfiguration;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/**
 * This is compiler class that takes a TezOperPlan and converts it into a
 * JobControl object with the relevant dependency info maintained. The
 * JobControl object is made up of TezJobs each of which has a JobConf.
 */
public class TezJobCompiler {
    private static final Log log = LogFactory.getLog(TezJobCompiler.class);

    private PigContext pigContext;
    private TezConfiguration tezConf;

    public TezJobCompiler(PigContext pigContext, Configuration conf) throws IOException {
        this.pigContext = pigContext;
        this.tezConf = new TezConfiguration(conf);
    }

    public DAG buildDAG(TezPlanContainerNode tezPlanNode, Map<String, LocalResource> localResources)
            throws IOException, YarnException {
        DAG tezDag = DAG.create(tezPlanNode.getOperatorKey().toString());
        tezDag.setCredentials(tezPlanNode.getTezOperPlan().getCredentials());
        TezDagBuilder dagBuilder = new TezDagBuilder(pigContext, tezPlanNode.getTezOperPlan(), tezDag, localResources);
        dagBuilder.visit();
        dagBuilder.avoidContainerReuseIfInputSplitInDisk();
        return tezDag;
    }

    public TezJob compile(TezPlanContainerNode tezPlanNode, TezPlanContainer planContainer)
            throws JobCreationException {
        TezJob job = null;
        try {
            // A single Tez job always pack only 1 Tez plan. We will track
            // Tez job asynchronously to exploit parallel execution opportunities.
            job = getJob(tezPlanNode, planContainer);
        } catch (JobCreationException jce) {
            throw jce;
        } catch(Exception e) {
            int errCode = 2017;
            String msg = "Internal error creating job configuration.";
            throw new JobCreationException(msg, errCode, PigException.BUG, e);
        }

        return job;
    }

    private TezJob getJob(TezPlanContainerNode tezPlanNode, TezPlanContainer planContainer)
            throws JobCreationException {
        try {
            Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
            localResources.putAll(planContainer.getLocalResources());
            TezOperPlan tezPlan = tezPlanNode.getTezOperPlan();
            localResources.putAll(tezPlan.getExtraResources());
            String shipFiles = pigContext.getProperties().getProperty("pig.streaming.ship.files");
            if (shipFiles != null) {
                for (String file : shipFiles.split(",")) {
                    TezResourceManager.getInstance().addTezResource(new File(file.trim()).toURI());
                }
            }
            String cacheFiles = pigContext.getProperties().getProperty("pig.streaming.cache.files");
            if (cacheFiles != null) {
                for (String file : cacheFiles.split(",")) {
                    // Do new URI() before passing to Path constructor else it encodes # when there is symlink
                    TezResourceManager.getInstance().addTezResource(new Path(new URI(file.trim())).toUri());
                }
            }
            for (Map.Entry<String, LocalResource> entry : localResources.entrySet()) {
                log.info("Local resource: " + entry.getKey());
            }
            DAG tezDag = buildDAG(tezPlanNode, localResources);
            tezDag.setDAGInfo(createDagInfo(TezScriptState.get().getScript()));
            log.info("Total estimated parallelism is " + tezPlan.getEstimatedTotalParallelism());
            return new TezJob(tezConf, tezDag, localResources, tezPlan.getEstimatedTotalParallelism());
        } catch (Exception e) {
            int errCode = 2017;
            String msg = "Internal error creating job configuration.";
            throw new JobCreationException(msg, errCode, PigException.BUG, e);
        }
    }

    private String createDagInfo(String script) throws IOException {
        String dagInfo;
        try {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("context", "Pig");
            jsonObject.put("description", script);
            dagInfo = jsonObject.toString();
        } catch (JSONException e) {
            throw new IOException("Error when trying to convert Pig script to JSON", e);
        }
        log.debug("DagInfo: " + dagInfo);
        return dagInfo;
    }
}

