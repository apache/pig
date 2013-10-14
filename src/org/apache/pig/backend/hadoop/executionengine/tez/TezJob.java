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
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.client.AMConfiguration;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;

/**
 * Wrapper class that encapsulates Tez DAG. This class mediates between Tez DAGs
 * and JobControl.
 */
public class TezJob extends ControlledJob {
    private static final Log log = LogFactory.getLog(TezJob.class);
    private AMConfiguration amConfig;
    private ApplicationId appId;
    private TezClient tezClient;
    private DAGClient dagClient;
    private DAG dag;

    public TezJob(TezConfiguration conf, ApplicationId appId, DAG dag,
            Map<String, LocalResource> localResources) throws IOException {
        super(conf);
        this.amConfig = new AMConfiguration(null, null, localResources, conf, null);
        this.tezClient = new TezClient(conf);
        this.appId = appId;
        this.dag = dag;
    }

    public ApplicationId getAppId() {
        return appId;
    }

    public DAG getDag() {
        return dag;
    }

    @Override
    public void submit() {
        try {
            log.info("Submitting DAG - Application id: " + appId);
            dagClient = tezClient.submitDAGApplication(appId, dag, amConfig);
        } catch (Exception e) {
            log.info("Cannot submit DAG - Application id: " + appId, e);
            setJobState(ControlledJob.State.FAILED);
            return;
        }

        while (true) {
            DAGStatus status = null;
            try {
                status = dagClient.getDAGStatus();
            } catch (Exception e) {
                log.info("Cannot retrieve DAG status", e);
                setJobState(ControlledJob.State.FAILED);
                break;
            }

            log.info("DAG Status: " + status);
            setJobState(dagState2JobState(status.getState()));
            if (status.isCompleted()) {
                break;
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // Do nothing
            }
        }
    }

    @Override
    public void killJob() throws IOException {
        try {
            dagClient.tryKillDAG();
        } catch (TezException e) {
            throw new IOException("Cannot kill DAG - Application Id: " + appId, e);
        }
    }

    private static ControlledJob.State dagState2JobState(DAGStatus.State dagState) {
        ControlledJob.State jobState = null;
        switch (dagState) {
            case SUBMITTED:
            case INITING:
            case RUNNING:
                jobState = ControlledJob.State.RUNNING;
                break;
            case SUCCEEDED:
                jobState = ControlledJob.State.SUCCESS;
                break;
            case KILLED:
            case FAILED:
            case ERROR:
                jobState = ControlledJob.State.FAILED;
                break;
        }
        return jobState;
    }
}

