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

import org.apache.pig.impl.PigContext;

public class PigATSClient {
    public static class ATSEvent {
        public ATSEvent(String pigAuditId, String callerId) {
            this.pigScriptId = pigAuditId;
            this.callerId = callerId;
        }
        String callerId;
        String pigScriptId;
    }
    private static PigATSClient instance;

    public static synchronized PigATSClient getInstance() {
        if (instance==null) {
            instance = new PigATSClient();
        }
        return instance;
    }

    private PigATSClient() {
    }

    public static String getPigAuditId(PigContext context) {
        return "";
    }

    synchronized public void logEvent(final ATSEvent event) {
    }
}
