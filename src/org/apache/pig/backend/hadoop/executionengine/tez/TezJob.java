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

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.TezConfiguration;

/**
 * Wrapper class that encapsulates Tez DAG. This class mediates between Tez DAGs
 * and JobControl.
 */
public class TezJob extends Job {
    private ApplicationId appId;
    private DAG dag;

    public TezJob(TezConfiguration conf, ApplicationId appId, DAG dag) throws IOException {
        super(new JobConf(conf));
        this.appId = appId;
        this.dag = dag;
    }

    public ApplicationId getAppId() {
        return appId;
    }

    public DAG getDag() {
        return dag;
    }
}

