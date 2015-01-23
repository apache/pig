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
import java.util.UUID;

import org.apache.hadoop.mapred.JobConf;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.HExecutionEngine;
import org.apache.pig.impl.PigContext;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.ScriptState;
import org.apache.pig.tools.pigstats.tez.TezPigScriptStats;
import org.apache.pig.tools.pigstats.tez.TezScriptState;
import org.apache.tez.dag.api.TezConfiguration;

public class TezExecutionEngine extends HExecutionEngine {

    public TezExecutionEngine(PigContext pigContext) {
        super(pigContext);
        this.launcher = new TezLauncher();
    }

    @Override
    public ScriptState instantiateScriptState() {
        TezScriptState ss = new TezScriptState(UUID.randomUUID().toString());
        ss.setPigContext(pigContext);
        return ss;
    }

    @Override
    public PigStats instantiatePigStats() {
        return new TezPigScriptStats(pigContext);
    }

    @Override
    public JobConf getExecConf(Properties properties) throws ExecException {
        JobConf jc = super.getExecConf(properties);
        jc.addResource(TezConfiguration.TEZ_SITE_XML);
        return jc;
    }
}
