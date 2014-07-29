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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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

    private static ReentrantReadWriteLock sessionPoolLock = new ReentrantReadWriteLock();
    private static boolean shutdown = false;

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

    static TezClient getClient(Configuration conf, Map<String, LocalResource> requestedAMResources, Credentials creds) throws TezException, IOException {
        List<SessionInfo> sessionsToRemove = new ArrayList<SessionInfo>();
        SessionInfo newSession = null;
        sessionPoolLock.readLock().lock();
        try {
            if (shutdown == true) {
                throw new IOException("TezSessionManager is shut down");
            }

            for (SessionInfo sessionInfo : sessionPool) {
                synchronized (sessionInfo) {
                    TezAppMasterStatus appMasterStatus = sessionInfo.session
                            .getAppMasterStatus();
                    if (appMasterStatus.equals(TezAppMasterStatus.SHUTDOWN)) {
                        sessionsToRemove.add(sessionInfo);
                    } else if (!sessionInfo.inUse
                            && appMasterStatus.equals(TezAppMasterStatus.READY)
                            && validateSessionResources(sessionInfo,requestedAMResources)) {
                        sessionInfo.inUse = true;
                        return sessionInfo.session;
                    }
                }
            }
        } finally {
            sessionPoolLock.readLock().unlock();
        }
        // We cannot find available AM, create new one
        // Create session outside of locks so that getClient/freeSession is not
        // blocked for parallel embedded pig runs
        newSession = createSession(conf, requestedAMResources, creds);
        newSession.inUse = true;
        sessionPoolLock.writeLock().lock();
        try {
            if (shutdown == true) {
                newSession.session.stop();
                throw new IOException("TezSessionManager is shut down");
            }
            sessionPool.add(newSession);
            for (SessionInfo sessionToRemove : sessionsToRemove) {
                sessionPool.remove(sessionToRemove);
            }
            return newSession.session;
        } finally {
            sessionPoolLock.writeLock().unlock();
        }
    }

    static void freeSession(TezClient session) {
        sessionPoolLock.readLock().lock();
        try {
            for (SessionInfo sessionInfo : sessionPool) {
                synchronized (sessionInfo) {
                    if (sessionInfo.session == session) {
                        sessionInfo.inUse = false;
                        break;
                    }
                }
            }
        } finally {
            sessionPoolLock.readLock().unlock();
        }
    }

    static void stopSession(TezClient session) throws TezException, IOException {
        Iterator<SessionInfo> iter = sessionPool.iterator();
        SessionInfo sessionToRemove = null;
        sessionPoolLock.readLock().lock();
        try {
            while (iter.hasNext()) {
                SessionInfo sessionInfo = iter.next();
                synchronized (sessionInfo) {
                    if (sessionInfo.session == session) {
                        log.info("Stopping Tez session " + session);
                        session.stop();
                        sessionToRemove = sessionInfo;
                        break;
                    }
                }
            }
        } finally {
            sessionPoolLock.readLock().unlock();
        }
        if (sessionToRemove != null) {
            sessionPoolLock.writeLock().lock();
            try {
                sessionPool.remove(sessionToRemove);
            } finally {
                sessionPoolLock.writeLock().unlock();
            }
        }
    }

    @VisibleForTesting
    public static void shutdown() {
        try {
            sessionPoolLock.writeLock().lock();
            shutdown = true;
            for (SessionInfo sessionInfo : sessionPool) {
                synchronized (sessionInfo) {
                    try {
                        if (sessionInfo.session.getAppMasterStatus().equals(
                                TezAppMasterStatus.SHUTDOWN)) {
                            log.info("Tez session is already shutdown "
                                    + sessionInfo.session);
                            continue;
                        }
                        log.info("Shutting down Tez session "
                                + sessionInfo.session);
                        sessionInfo.session.stop();
                    } catch (Exception e) {
                        log.error("Error shutting down Tez session "
                                + sessionInfo.session, e);
                    }
                }
            }
            sessionPool.clear();
        } finally {
            sessionPoolLock.writeLock().unlock();
        }
    }
}

