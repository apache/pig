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

import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.ExecType;
import org.apache.pig.backend.executionengine.ExecutionEngine;
import org.apache.pig.impl.PigContext;
import org.apache.tez.client.TezClient;

/**
 * TezExecType is the ExecType for distributed mode in Tez.
 */
public class TezExecType implements ExecType {
    private static final Log LOG = LogFactory.getLog(TezExecType.class);
    private static final long serialVersionUID = 1L;
    private static final String[] modes = { "TEZ" };
    private static final String POM_PROPERTIES = "/META-INF/maven/org.apache.tez/tez-api/pom.properties";
    private static String tezVersion = null;

    @Override
    public boolean accepts(Properties properties) {
        String execTypeSpecified =
                properties.getProperty("exectype", "").toUpperCase();
        for (String mode : modes) {
            if (execTypeSpecified.equals(mode)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public ExecutionEngine getExecutionEngine(PigContext pigContext) {
        return new TezExecutionEngine(pigContext);
    }

    @Override
    public Class<? extends ExecutionEngine> getExecutionEngineClass() {
        return TezExecutionEngine.class;
    }

    @Override
    public boolean isLocal() {
        return false;
    }

    @Override
    public String name() {
        return "TEZ";
    }

    public String toString() {
        return name();
    }

    public static String getTezVersion() {
        if (tezVersion == null) {
            try {
                Properties prop = new Properties();
                prop.load(TezClient.class.getResourceAsStream(POM_PROPERTIES));
                tezVersion = prop.getProperty("version", "");
            } catch (Exception e) {
                LOG.info("Failed to load Tez pom.properties");
            }
        }
        return tezVersion == null ? "" : tezVersion;
    }
}

