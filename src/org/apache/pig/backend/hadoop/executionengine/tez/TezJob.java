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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.client.TezSession;
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
    private DAGStatus dagStatus;
    private Configuration conf;
    private DAG dag;
    private DAGClient dagClient;
    private Map<String, LocalResource> requestAMResources;
    private TezSession tezSession;

    public TezJob(TezConfiguration conf, DAG dag, Map<String, LocalResource> requestAMResources)
            throws IOException {
        super(conf);
        this.conf = conf;
        this.dag = dag;
        this.requestAMResources = requestAMResources;
    }

    public DAG getDag() {
        return dag;
    }

    public DAGStatus getDagStatus() {
        return dagStatus;
    }

    @Override
    public void submit() {
        try {
            tezSession = TezSessionManager.getSession(conf, requestAMResources);
            log.info("Submitting DAG - Application id: " + tezSession.getApplicationId());
            dagClient = tezSession.submitDAG(dag);
        } catch (Exception e) {
            if (tezSession!=null) {
                log.info("Cannot submit DAG - Application id: " + tezSession.getApplicationId(), e);
            }
            setJobState(ControlledJob.State.FAILED);
            return;
        }

        while (true) {
            try {
                dagStatus = dagClient.getDAGStatus(null);
            } catch (Exception e) {
                log.info("Cannot retrieve DAG status", e);
                setJobState(ControlledJob.State.FAILED);
                break;
            }

            log.info("DAG Status: " + dagStatus);
            setJobState(dagState2JobState(dagStatus.getState()));
            if (dagStatus.isCompleted()) {
                StringBuilder sb = new StringBuilder();
                for (String msg : dagStatus.getDiagnostics()) {
                    sb.append(msg);
                    sb.append("\n");
                }
                setMessage(sb.toString());
                TezSessionManager.freeSession(tezSession);
                tezSession = null;
                dagClient = null;
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
            tezSession.stop();
        } catch (TezException e) {
            throw new IOException("Cannot kill DAG - Application Id: " + tezSession.getApplicationId(), e);
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

