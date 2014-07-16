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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler;
import org.apache.pig.classification.InterfaceAudience;
import org.apache.tez.common.TezJobConfig;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.mapreduce.hadoop.DeprecatedKeys;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;

@InterfaceAudience.Private
public class MRToTezHelper {

    private static final Log LOG = LogFactory.getLog(MRToTezHelper.class);

    private static List<String> mrSettingsToRetain = new ArrayList<String>();

    private MRToTezHelper() {
    }

    static {
        populateMRSettingsToRetain();
    }

    private static void populateMRSettingsToRetain() {

        // FileInputFormat
        mrSettingsToRetain.add(FileInputFormat.INPUT_DIR);
        mrSettingsToRetain.add(FileInputFormat.SPLIT_MAXSIZE);
        mrSettingsToRetain.add(FileInputFormat.SPLIT_MINSIZE);
        mrSettingsToRetain.add(FileInputFormat.PATHFILTER_CLASS);
        mrSettingsToRetain.add(FileInputFormat.NUM_INPUT_FILES);
        mrSettingsToRetain.add(FileInputFormat.INPUT_DIR_RECURSIVE);

        // FileOutputFormat
        mrSettingsToRetain.add("mapreduce.output.basename");
        mrSettingsToRetain.add(FileOutputFormat.COMPRESS);
        mrSettingsToRetain.add(FileOutputFormat.COMPRESS_CODEC);
        mrSettingsToRetain.add(FileOutputFormat.COMPRESS_TYPE);
        mrSettingsToRetain.add(FileOutputFormat.OUTDIR);
        mrSettingsToRetain.add(FileOutputCommitter.SUCCESSFUL_JOB_OUTPUT_DIR_MARKER);
    }

    public static TezConfiguration getDAGAMConfFromMRConf(
            Configuration tezConf) {

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

        dagAMConf.setIfUnset(TezConfiguration.TEZ_AM_LAUNCH_CMD_OPTS,
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

        if (tezConf.get("mapreduce.job.credentials.binary") != null) {
            dagAMConf.setIfUnset(TezJobConfig.TEZ_CREDENTIALS_PATH,
                    tezConf.get("mapreduce.job.credentials.binary"));
        }

        return dagAMConf;
    }

    /**
     * Process the mapreduce configuration settings and
     *    - copy as is the still required ones (like those used by FileInputFormat/FileOutputFormat)
     *    - convert and set equivalent tez runtime settings
     *    - handle compression related settings
     *
     * @param conf Configuration on which the mapreduce settings will have to be transferred
     * @param mrConf Configuration that contains mapreduce settings
     */
    public static void processMRSettings(Configuration conf, Configuration mrConf) {
        for (String mrSetting : mrSettingsToRetain) {
            if (mrConf.get(mrSetting) != null) {
                conf.set(mrSetting, mrConf.get(mrSetting));
            }
        }
        JobControlCompiler.configureCompression(conf);
        convertMRToTezRuntimeConf(conf, mrConf);
    }

    /**
     * Convert MR settings to Tez settings and set on conf.
     *
     * @param conf  Configuration on which MR equivalent Tez settings should be set
     * @param mrConf Configuration that contains MR settings
     */
    private static void convertMRToTezRuntimeConf(Configuration conf, Configuration mrConf) {
        for (Entry<String, String> dep : DeprecatedKeys.getMRToTezRuntimeParamMap().entrySet()) {
            if (mrConf.get(dep.getKey()) != null) {
                conf.unset(dep.getKey());
                LOG.info("Setting " + dep.getValue() + " to "
                        + mrConf.get(dep.getKey()) + " from MR setting "
                        + dep.getKey());
                conf.setIfUnset(dep.getValue(), mrConf.get(dep.getKey()));
            }
        }
    }

}
