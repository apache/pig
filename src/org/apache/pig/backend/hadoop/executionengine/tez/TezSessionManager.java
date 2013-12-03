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
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.pig.impl.PigContext;
import org.apache.tez.client.AMConfiguration;
import org.apache.tez.client.TezClient;
import org.apache.tez.client.TezSession;
import org.apache.tez.client.TezSessionConfiguration;
import org.apache.tez.client.TezSessionStatus;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;

public class TezSessionManager {
    public static class SessionInfo {
        SessionInfo(TezSession session, Map<String, LocalResource> resources) {
            this.session = session;
            this.resources = resources;
        }
        public Map<String, LocalResource> getResources() {
            return resources;
        }
        public TezSession getTezSession() {
            return session;
        }
        public void setInUse(boolean inUse) {
            this.inUse = inUse;
        }
        private TezSession session;
        private Map<String, LocalResource> resources;
        private boolean inUse = false;
    }

    private static List<SessionInfo> sessionPool = new ArrayList<SessionInfo>();

    private static void waitForTezSessionReady(TezSession tezSession)
        throws IOException, TezException {
        while (true) {
            TezSessionStatus status = tezSession.getSessionStatus();
            if (status.equals(TezSessionStatus.SHUTDOWN)) {
                throw new RuntimeException("TezSession has already shutdown");
            }
            if (status.equals(TezSessionStatus.READY)) {
                return;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new IOException("Interrupted while trying to check session status", e);
            }
        }
    }

    private static SessionInfo createSession(Configuration conf, Map<String, LocalResource> requestedAMResources, Credentials creds) throws TezException, IOException {
        TezConfiguration tezConf = new TezConfiguration(conf);
        TezClient tezClient = new TezClient(tezConf);
        ApplicationId appId = tezClient.createApplication();

        Map<String, LocalResource> resources = new HashMap<String, LocalResource>();
        resources.putAll(requestedAMResources);

        String jobName = conf.get(PigContext.JOB_NAME, "pig");
        AMConfiguration amConfig = new AMConfiguration(null, resources, tezConf, creds);
        TezSessionConfiguration sessionConfig = new TezSessionConfiguration(amConfig, tezConf);
        TezSession tezSession = new TezSession(jobName, appId, sessionConfig);
        tezSession.start();
        waitForTezSessionReady(tezSession);
        return new SessionInfo(tezSession, resources);
    }

    private static boolean validateSessionResources(SessionInfo currentSession, Map<String, LocalResource> requestedAMResources) throws TezException, IOException {
        for (Map.Entry<String, LocalResource> entry : requestedAMResources.entrySet()) {
            if (!currentSession.resources.entrySet().contains(entry)) {
                return false;
            }
        }
        return true;
    }

    static TezSession getSession(Configuration conf, Map<String, LocalResource> requestedAMResources, Credentials creds) throws TezException, IOException {
        synchronized (sessionPool) {
            List<SessionInfo> sessionsToRemove = new ArrayList<SessionInfo>();
            for (SessionInfo sessionInfo : sessionPool) {
                if (sessionInfo.session.getSessionStatus()==TezSessionStatus.SHUTDOWN) {
                    sessionsToRemove.add(sessionInfo);
                }
                else if (!sessionInfo.inUse && sessionInfo.session.getSessionStatus()==TezSessionStatus.READY &&
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

    static void freeSession(TezSession session) {
        synchronized (sessionPool) {
            for (SessionInfo sessionInfo : sessionPool) {
                if (sessionInfo.session==session) {
                    sessionInfo.inUse = false;
                }
            }
        }
    }
}

