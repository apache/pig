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
package org.apache.pig.backend.hadoop.executionengine.tez.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.impl.util.Pair;
import org.apache.tez.common.TezJobConfig;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.mapreduce.hadoop.DeprecatedKeys;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;

@InterfaceAudience.Private
public class MRToTezHelper {

    private static final Log LOG = LogFactory.getLog(MRToTezHelper.class);

    /**
     * Keys which are used across an edge. i.e. by an Output-Input pair.
     */
    private static Map<String, Pair<String, String>> mrToTezIOParamMap = new HashMap<String, Pair<String, String>>();

    private MRToTezHelper() {
    }

    static {
        populateMRToTezIOParamMap();
    }

    private static void populateMRToTezIOParamMap() {
        mrToTezIOParamMap.put(MRJobConfig.MAP_OUTPUT_COMPRESS,
                new Pair<String, String> (
                        TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_OUTPUT_SHOULD_COMPRESS,
                        TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_INPUT_IS_COMPRESSED));

        mrToTezIOParamMap.put(MRJobConfig.MAP_OUTPUT_COMPRESS_CODEC,
                new Pair<String, String> (
                        TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_OUTPUT_COMPRESS_CODEC,
                        TezJobConfig.TEZ_RUNTIME_INTERMEDIATE_INPUT_COMPRESS_CODEC));

    }

    public static TezConfiguration getDAGAMConfFromMRConf(
            TezConfiguration tezConf) {

        // Set Tez parameters based on MR parameters.
        TezConfiguration dagAMConf = new TezConfiguration(tezConf);
        Map<String, String> mrParamToDAGParamMap = DeprecatedKeys
                .getMRToDAGParamMap();

        for (Entry<String, String> entry : mrParamToDAGParamMap.entrySet()) {
            if (dagAMConf.get(entry.getKey()) != null) {
                dagAMConf.set(entry.getValue(), dagAMConf.get(entry.getKey()));
                dagAMConf.unset(entry.getKey());
                if (LOG.isDebugEnabled()) {
                    LOG.debug("MR->DAG Translating MR key: " + entry.getKey()
                            + " to Tez key: " + entry.getValue()
                            + " with value " + dagAMConf.get(entry.getValue()));
                }
            }
        }

        dagAMConf.setIfUnset(TezConfiguration.TEZ_AM_JAVA_OPTS,
                org.apache.tez.mapreduce.hadoop.MRHelpers
                        .getMRAMJavaOpts(tezConf));

        String queueName = tezConf.get(JobContext.QUEUE_NAME,
                YarnConfiguration.DEFAULT_QUEUE_NAME);
        dagAMConf.setIfUnset(TezConfiguration.TEZ_QUEUE_NAME, queueName);

        int amMemMB = tezConf.getInt(MRJobConfig.MR_AM_VMEM_MB,
                MRJobConfig.DEFAULT_MR_AM_VMEM_MB);
        int amCores = tezConf.getInt(MRJobConfig.MR_AM_CPU_VCORES,
                MRJobConfig.DEFAULT_MR_AM_CPU_VCORES);
        dagAMConf.setIfUnset(TezConfiguration.TEZ_AM_RESOURCE_MEMORY_MB, ""
                + amMemMB);
        dagAMConf.setIfUnset(TezConfiguration.TEZ_AM_RESOURCE_CPU_VCORES, ""
                + amCores);

        dagAMConf.setIfUnset(TezConfiguration.TEZ_AM_MAX_APP_ATTEMPTS, ""
                + dagAMConf.getInt(MRJobConfig.MR_AM_MAX_ATTEMPTS,
                        MRJobConfig.DEFAULT_MR_AM_MAX_ATTEMPTS));

        return dagAMConf;
    }

    public static void convertMRToTezRuntimeConf(Configuration conf, Configuration baseConf) {
        for (Entry<String, String> dep : DeprecatedKeys
                .getMRToTezRuntimeParamMap().entrySet()) {
            if (baseConf.get(dep.getKey()) != null) {
                conf.unset(dep.getKey());
                LOG.info("Setting " + dep.getValue() + " to "
                        + baseConf.get(dep.getKey()) + " from MR setting "
                        + dep.getKey());
                conf.setIfUnset(dep.getValue(), baseConf.get(dep.getKey()));
            }
        }

        for (Entry<String, Pair<String, String>> dep : mrToTezIOParamMap.entrySet()) {
            if (baseConf.get(dep.getKey()) != null) {
                conf.unset(dep.getKey());
                LOG.info("Setting " + dep.getValue() + " to "
                        + baseConf.get(dep.getKey()) + " from MR setting "
                        + dep.getKey());
                conf.setIfUnset(dep.getValue().first, baseConf.get(dep.getKey()));
                conf.setIfUnset(dep.getValue().second, baseConf.get(dep.getKey()));
            }
        }
    }

}
