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
package org.apache.pig.backend.hadoop;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.PigImplConstants;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.tools.pigstats.ScriptState;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class PigATSClient {
    public static class ATSEvent {
        public ATSEvent(String pigAuditId, String callerId) {
            this.pigScriptId = pigAuditId;
            this.callerId = callerId;
        }
        String callerId;
        String pigScriptId;
    }
    public static final String ENTITY_TYPE = "PIG_SCRIPT_ID";
    public static final String ENTITY_CALLERID = "callerId";
    public static final String CALLER_CONTEXT = "PIG";
    public static final int AUDIT_ID_MAX_LENGTH = 128;

    private static final Log log = LogFactory.getLog(PigATSClient.class.getName());
    private static PigATSClient instance;
    private static ExecutorService executor;
    private TimelineClient timelineClient;

    public static synchronized PigATSClient getInstance() {
        if (instance==null) {
            instance = new PigATSClient();
        }
        return instance;
    }

    private PigATSClient() {
        if (executor == null) {
            executor = Executors.newSingleThreadExecutor(
                    new ThreadFactoryBuilder().setDaemon(true).setNameFormat("ATS Logger %d").build());
            YarnConfiguration yarnConf = new YarnConfiguration();
            timelineClient = TimelineClient.createTimelineClient();
            timelineClient.init(yarnConf);
            timelineClient.start();
        }
        Utils.addShutdownHookWithPriority(new Runnable() {
            @Override
            public void run() {
                timelineClient.stop();
                executor.shutdownNow();
                executor = null;
            }
        }, PigImplConstants.SHUTDOWN_HOOK_ATS_CLIENT_PRIORITY);
        log.info("Created ATS Hook");
    }

    public static String getPigAuditId(PigContext context) {
        String auditId;
        if (context.getProperties().get(PigImplConstants.PIG_AUDIT_ID) != null) {
            auditId = (String)context.getProperties().get(PigImplConstants.PIG_AUDIT_ID);
        } else {
            ScriptState ss = ScriptState.get();
            String filename = ss.getFileName().isEmpty()?"default" : new File(ss.getFileName()).getName();
            auditId = CALLER_CONTEXT + "-" + filename + "-" + ss.getId();
        }
        return auditId.substring(0, Math.min(auditId.length(), AUDIT_ID_MAX_LENGTH));
    }

    synchronized public void logEvent(final ATSEvent event) {
        executor.submit(new Runnable() {
            @Override
            public void run() {
                TimelineEntity entity = new TimelineEntity();
                entity.setEntityId(event.pigScriptId);
                entity.setEntityType(ENTITY_TYPE);
                entity.addPrimaryFilter(ENTITY_CALLERID, event.callerId!=null?event.callerId : "default");
                try {
                    timelineClient.putEntities(entity);
                } catch (Exception e) {
                    log.info("Failed to submit plan to ATS: " + e.getMessage());
                }
            }
        });
    }
}
