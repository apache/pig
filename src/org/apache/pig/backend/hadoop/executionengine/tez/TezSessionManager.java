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
package org.apache.pig.backend.hadoop.executionengine.tez;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.pig.backend.hadoop.executionengine.tez.util.MRToTezHelper;
import org.apache.pig.impl.PigContext;
import org.apache.tez.client.TezAppMasterStatus;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.mapreduce.hadoop.MRHelpers;

import com.google.common.annotations.VisibleForTesting;

public class TezSessionManager {

    private static final Log log = LogFactory.getLog(TezSessionManager.class);

    static {
        Runtime.getRuntime().addShutdownHook(new Thread() {

            @Override
            public void run() {
                TezSessionManager.shutdown();
            }
        });
    }

    private TezSessionManager() {
    }

    public static class SessionInfo {
        SessionInfo(TezClient session, Map<String, LocalResource> resources) {
            this.session = session;
            this.resources = resources;
        }
        public Map<String, LocalResource> getResources() {
            return resources;
        }
        public TezClient getTezSession() {
            return session;
        }
        public void setInUse(boolean inUse) {
            this.inUse = inUse;
        }
        private TezClient session;
        private Map<String, LocalResource> resources;
        private boolean inUse = false;
    }

    private static List<SessionInfo> sessionPool = new ArrayList<SessionInfo>();

    private static SessionInfo createSession(Configuration conf, Map<String, LocalResource> requestedAMResources, Credentials creds) throws TezException, IOException {
        TezConfiguration amConf = MRToTezHelper.getDAGAMConfFromMRConf(conf);
        Map<String, String> amEnv = new HashMap<String, String>();
        MRHelpers.updateEnvironmentForMRAM(amConf, amEnv);
        String jobName = conf.get(PigContext.JOB_NAME, "pig");
        TezClient tezClient = new TezClient(jobName, amConf, true, requestedAMResources, creds);
        tezClient.start();
        TezAppMasterStatus appMasterStatus = tezClient.getAppMasterStatus();
        if (appMasterStatus.equals(TezAppMasterStatus.SHUTDOWN)) {
            throw new RuntimeException("TezSession has already shutdown");
        }
        tezClient.waitTillReady();
        return new SessionInfo(tezClient, requestedAMResources);
    }

    private static boolean validateSessionResources(SessionInfo currentSession, Map<String, LocalResource> requestedAMResources) throws TezException, IOException {
        for (Map.Entry<String, LocalResource> entry : requestedAMResources.entrySet()) {
            if (!currentSession.resources.entrySet().contains(entry)) {
                return false;
            }
        }
        return true;
    }

    static TezClient getSession(Configuration conf, Map<String, LocalResource> requestedAMResources, Credentials creds) throws TezException, IOException {
        synchronized (sessionPool) {
            List<SessionInfo> sessionsToRemove = new ArrayList<SessionInfo>();
            for (SessionInfo sessionInfo : sessionPool) {
                TezAppMasterStatus appMasterStatus = sessionInfo.session.getAppMasterStatus();
                if (appMasterStatus.equals(TezAppMasterStatus.SHUTDOWN)) {
                    sessionsToRemove.add(sessionInfo);
                } else if (!sessionInfo.inUse && appMasterStatus.equals(TezAppMasterStatus.READY) &&
                        validateSessionResources(sessionInfo, requestedAMResources)) {
                    sessionInfo.inUse = true;
                    return sessionInfo.session;
                }
            }

            for (SessionInfo sessionToRemove : sessionsToRemove) {
                sessionPool.remove(sessionToRemove);
            }

            // We cannot find available AM, create new one
            SessionInfo sessionInfo = createSession(conf, requestedAMResources, creds);
            sessionInfo.inUse = true;
            sessionPool.add(sessionInfo);
            return sessionInfo.session;
        }
    }

    static void freeSession(TezClient session) {
        synchronized (sessionPool) {
            for (SessionInfo sessionInfo : sessionPool) {
                if (sessionInfo.session == session) {
                    sessionInfo.inUse = false;
                    break;
                }
            }
        }
    }

    static void stopSession(TezClient session) throws TezException, IOException {
        synchronized (sessionPool) {
            Iterator<SessionInfo> iter = sessionPool.iterator();
            while (iter.hasNext()) {
                SessionInfo sessionInfo = iter.next();
                if (sessionInfo.session == session) {
                    log.info("Shutting down Tez session " + session);
                    session.stop();
                    iter.remove();
                    break;
                }
            }
        }
    }

    @VisibleForTesting
    public static void shutdown() {
        synchronized (sessionPool) {
            for (SessionInfo sessionInfo : sessionPool) {
                try {
                    log.info("Shutting down Tez session " + sessionInfo.session);
                    sessionInfo.session.stop();
                } catch (Exception e) {
                    log.error("Error shutting down Tez session "  + sessionInfo.session, e);
                }
            }
            sessionPool.clear();
        }
    }
}

