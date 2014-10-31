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
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.pig.PigConfiguration;
import org.apache.tez.client.TezClient;
import org.apache.tez.common.counters.CounterGroup;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.Progress;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.dag.api.client.VertexStatus;

import com.google.common.collect.Maps;

/**
 * Wrapper class that encapsulates Tez DAG. This class mediates between Tez DAGs
 * and JobControl.
 */
public class TezJob implements Runnable {
    private static final Log log = LogFactory.getLog(TezJob.class);
    private Configuration conf;
    private EnumSet<StatusGetOpts> statusGetOpts;
    private Map<String, LocalResource> requestAMResources;
    private ApplicationId appId;
    private DAG dag;
    private DAGClient dagClient;
    private DAGStatus dagStatus;
    private TezClient tezClient;
    private boolean reuseSession;
    private TezCounters dagCounters;
    // Vertex, CounterGroup, Counter, Value
    private Map<String, Map<String, Map<String, Long>>> vertexCounters;
    // Timer for DAG status reporter
    private Timer timer;
    private TezJobConfig tezJobConf;

    public TezJob(TezConfiguration conf, DAG dag,
            Map<String, LocalResource> requestAMResources,
            int estimatedTotalParallelism) throws IOException {
        this.conf = conf;
        this.dag = dag;
        this.requestAMResources = requestAMResources;
        this.reuseSession = conf.getBoolean(PigConfiguration.TEZ_SESSION_REUSE, true);
        this.statusGetOpts = EnumSet.of(StatusGetOpts.GET_COUNTERS);
        this.vertexCounters = Maps.newHashMap();
        tezJobConf = new TezJobConfig(estimatedTotalParallelism);
    }

    static class TezJobConfig {

        private int estimatedTotalParallelism = -1;

        public TezJobConfig(int estimatedTotalParallelism) {
            this.estimatedTotalParallelism = estimatedTotalParallelism;
        }

        public int getEstimatedTotalParallelism() {
            return estimatedTotalParallelism;
        }

        public void setEstimatedTotalParallelism(int estimatedTotalParallelism) {
            this.estimatedTotalParallelism = estimatedTotalParallelism;
        }

    }

    public DAG getDAG() {
        return dag;
    }

    public String getName() {
        return dag == null ? "" : dag.getName();
    }

    public Configuration getConfiguration() {
        return conf;
    }

    public ApplicationId getApplicationId() {
        return appId;
    }

    public DAGStatus getDAGStatus() {
        return dagStatus;
    }

    public TezCounters getDAGCounters() {
        return dagCounters;
    }

    public Map<String, Map<String, Long>> getVertexCounters(String group) {
        return vertexCounters.get(group);
    }

    public Map<String, Long> getVertexCounters(String group, String name) {
        return vertexCounters.get(group).get(name);
    }

    public float getDAGProgress() {
        Progress p = dagStatus.getDAGProgress();
        return p == null ? 0 : (float)p.getSucceededTaskCount() / (float)p.getTotalTaskCount();
    }

    public Map<String, Float> getVertexProgress() {
        Map<String, Float> vertexProgress = Maps.newHashMap();
        for (Map.Entry<String, Progress> entry : dagStatus.getVertexProgress().entrySet()) {
            Progress p = entry.getValue();
            float progress = (float)p.getSucceededTaskCount() / (float)p.getTotalTaskCount();
            vertexProgress.put(entry.getKey(), progress);
        }
        return vertexProgress;
    }

    @Override
    public void run() {
        try {
            tezClient = TezSessionManager.getClient(conf, requestAMResources,
                    dag.getCredentials(), tezJobConf);
            log.info("Submitting DAG " + dag.getName());
            dagClient = tezClient.submitDAG(dag);
            appId = tezClient.getAppMasterApplicationId();
            log.info("Submitted DAG " + dag.getName() + ". Application id: " + appId);
        } catch (Exception e) {
            if (tezClient != null) {
                log.error("Cannot submit DAG - Application id: " + tezClient.getAppMasterApplicationId(), e);
            } else {
                log.error("Cannot submit DAG", e);
            }
            return;
        }

        timer = new Timer();
        timer.schedule(new DAGStatusReporter(), 1000, conf.getLong(
                PigConfiguration.TEZ_DAG_STATUS_REPORT_INTERVAL, 20) * 1000);

        while (true) {
            try {
                dagStatus = dagClient.getDAGStatus(statusGetOpts);
            } catch (Exception e) {
                log.info("Cannot retrieve DAG status", e);
                break;
            }

            if (dagStatus.isCompleted()) {
                log.info("DAG Status: " + dagStatus);
                dagCounters = dagStatus.getDAGCounters();
                collectVertexCounters();
                TezSessionManager.freeSession(tezClient);
                try {
                    if (!reuseSession) {
                        TezSessionManager.stopSession(tezClient);
                    }
                    tezClient = null;
                    dagClient = null;
                } catch (Exception e) {
                    log.info("Cannot stop Tez session", e);
                }
                break;
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // Do nothing
            }
        }

        timer.cancel();
    }

    private class DAGStatusReporter extends TimerTask {

        private final String LINE_SEPARATOR = System.getProperty("line.separator");

        @Override
        public void run() {
            String msg = "status=" + dagStatus.getState()
              + ", progress=" + dagStatus.getDAGProgress()
              + ", diagnostics="
              + StringUtils.join(dagStatus.getDiagnostics(), LINE_SEPARATOR);
            log.info("DAG Status: " + msg);
        }
    }

    private void collectVertexCounters() {
        for (Vertex v : dag.getVertices()) {
            String name = v.getName();
            try {
                VertexStatus s = dagClient.getVertexStatus(name, statusGetOpts);
                if (s == null) {
                    log.info("Cannot retrieve counters for vertex " + name);
                    continue;
                }
                TezCounters counters = s.getVertexCounters();
                Map<String, Map<String, Long>> grpCounters = Maps.newHashMap();
                Iterator<CounterGroup> grpIt = counters.iterator();
                while (grpIt.hasNext()) {
                    CounterGroup grp = grpIt.next();
                    Iterator<TezCounter> cntIt = grp.iterator();
                    Map<String, Long> cntMap = Maps.newHashMap();
                    while (cntIt.hasNext()) {
                        TezCounter cnt = cntIt.next();
                        cntMap.put(cnt.getName(), cnt.getValue());
                    }
                    grpCounters.put(grp.getName(), cntMap);
                }
                vertexCounters.put(name, grpCounters);
            } catch (Exception e) {
                // Don't fail the job even if vertex counters couldn't
                // be retrieved.
                log.info("Cannot retrieve counters for vertex " + name, e);
            }
        }
    }

    public void killJob() throws IOException {
        try {
            if (dagClient != null) {
                dagClient.tryKillDAG();
            }
            if (tezClient != null) {
                tezClient.stop();
            }
        } catch (TezException e) {
            throw new IOException("Cannot kill DAG - Application Id: " + appId, e);
        }
    }

    public String getDiagnostics() {
        try {
            if (dagClient != null && dagStatus == null) {
                dagStatus = dagClient.getDAGStatus(new HashSet<StatusGetOpts>());
            }
            if (dagStatus != null) {
                return StringUtils.join(dagStatus.getDiagnostics(), "\n");
            }
        } catch (Exception e) {
            //Ignore
        }
        return "";
    }
}
