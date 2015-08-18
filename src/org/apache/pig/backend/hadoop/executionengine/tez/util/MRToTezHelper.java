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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.split.JobSplitWriter;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MRConfiguration;
import org.apache.pig.classification.InterfaceAudience;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.mapreduce.hadoop.DeprecatedKeys;
import org.apache.tez.mapreduce.hadoop.InputSplitInfo;
import org.apache.tez.mapreduce.hadoop.InputSplitInfoDisk;
import org.apache.tez.mapreduce.hadoop.InputSplitInfoMem;

@InterfaceAudience.Private
public class MRToTezHelper {

    private static final Log LOG = LogFactory.getLog(MRToTezHelper.class);
    private static final String JOB_SPLIT_RESOURCE_NAME = MRJobConfig.JOB_SPLIT;
    private static final String JOB_SPLIT_METAINFO_RESOURCE_NAME = MRJobConfig.JOB_SPLIT_METAINFO;

    private static List<String> mrSettingsToRetain = new ArrayList<String>();

    private static List<String> mrSettingsToRemove = new ArrayList<String>();

    private MRToTezHelper() {
    }

    static {
        populateMRSettingsToRetain();
        populateMRSettingsToRemove();
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
        mrSettingsToRetain.add(MRConfiguration.OUTPUT_BASENAME);
        mrSettingsToRetain.add(FileOutputFormat.COMPRESS);
        mrSettingsToRetain.add(FileOutputFormat.COMPRESS_CODEC);
        mrSettingsToRetain.add(FileOutputFormat.COMPRESS_TYPE);
        mrSettingsToRetain.add(FileOutputFormat.OUTDIR);
        mrSettingsToRetain.add(FileOutputCommitter.SUCCESSFUL_JOB_OUTPUT_DIR_MARKER);
    }

    private static void populateMRSettingsToRemove() {

        // TODO: Add all unwanted MR config once Tez UI starts showing config

        // FileInputFormat.listStatus() on a task can cause job failure when run from Oozie
        mrSettingsToRemove.add(MRJobConfig.MAPREDUCE_JOB_CREDENTIALS_BINARY);
    }

    private static void removeUnwantedMRSettings(Configuration tezConf) {

        Iterator<Entry<String, String>> iter = tezConf.iterator();
        while (iter.hasNext()) {
            Entry<String, String> next = iter.next();
            for (String mrSetting : mrSettingsToRemove) {
                if (next.getKey().equals(mrSetting)) {
                    iter.remove();
                    break;
                }
            }
        }
    }

    public static TezConfiguration getDAGAMConfFromMRConf(
            Configuration tezConf) {

        // Set Tez parameters based on MR parameters.
        TezConfiguration dagAMConf = new TezConfiguration(tezConf);
        Map<String, String> mrParamToDAGParamMap = DeprecatedKeys
                .getMRToDAGParamMap();

        for (Entry<String, String> entry : mrParamToDAGParamMap.entrySet()) {
            if (dagAMConf.get(entry.getKey()) != null) {
                dagAMConf.setIfUnset(entry.getValue(), dagAMConf.get(entry.getKey()));
                dagAMConf.unset(entry.getKey());
                if (LOG.isDebugEnabled()) {
                    LOG.debug("MR->DAG Translating MR key: " + entry.getKey()
                            + " to Tez key: " + entry.getValue()
                            + " with value " + dagAMConf.get(entry.getValue()));
                }
            }
        }

        String env = tezConf.get(MRJobConfig.MR_AM_ADMIN_USER_ENV);
        if (tezConf.get(MRJobConfig.MR_AM_ENV) != null) {
            env = (env == null) ? tezConf.get(MRJobConfig.MR_AM_ENV)
                                : env + "," + tezConf.get(MRJobConfig.MR_AM_ENV);
        }

        if (env != null) {
            dagAMConf.setIfUnset(TezConfiguration.TEZ_AM_LAUNCH_ENV, env);
        }

        dagAMConf.setIfUnset(TezConfiguration.TEZ_AM_LAUNCH_CMD_OPTS,
                org.apache.tez.mapreduce.hadoop.MRHelpers
                        .getJavaOptsForMRAM(tezConf));

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

        dagAMConf.setIfUnset(TezConfiguration.TEZ_AM_VIEW_ACLS,
                tezConf.get(MRJobConfig.JOB_ACL_VIEW_JOB, MRJobConfig.DEFAULT_JOB_ACL_VIEW_JOB));

        dagAMConf.setIfUnset(TezConfiguration.TEZ_AM_MODIFY_ACLS,
                tezConf.get(MRJobConfig.JOB_ACL_MODIFY_JOB, MRJobConfig.DEFAULT_JOB_ACL_MODIFY_JOB));

        dagAMConf.setIfUnset(TezConfiguration.TEZ_AM_MAX_APP_ATTEMPTS, ""
                + dagAMConf.getInt(MRJobConfig.MR_AM_MAX_ATTEMPTS,
                        MRJobConfig.DEFAULT_MR_AM_MAX_ATTEMPTS));

        if (tezConf.get(MRConfiguration.JOB_CREDENTIALS_BINARY) != null) {
            dagAMConf.setIfUnset(TezConfiguration.TEZ_CREDENTIALS_PATH,
                    tezConf.get(MRConfiguration.JOB_CREDENTIALS_BINARY));
        }

        if (tezConf.get(MRJobConfig.JOB_CANCEL_DELEGATION_TOKEN) != null) {
            dagAMConf.setIfUnset(TezConfiguration.TEZ_CANCEL_DELEGATION_TOKENS_ON_COMPLETION,
                    tezConf.get(MRJobConfig.JOB_CANCEL_DELEGATION_TOKEN));
        }

        // Hardcoding at AM level instead of setting per vertex till TEZ-2710 is available
        dagAMConf.setIfUnset(TezConfiguration.TEZ_TASK_SCALE_MEMORY_RESERVE_FRACTION, "0.5");

        removeUnwantedMRSettings(dagAMConf);

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
        removeUnwantedMRSettings(conf);
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

    /**
     * Write input splits (job.split and job.splitmetainfo) to disk
     */
    public static InputSplitInfoDisk writeInputSplitInfoToDisk(
            InputSplitInfoMem infoMem, Path inputSplitsDir, JobConf jobConf,
            FileSystem fs) throws IOException, InterruptedException {

        InputSplit[] splits = infoMem.getNewFormatSplits();
        JobSplitWriter.createSplitFiles(inputSplitsDir, jobConf, fs, splits);

        return new InputSplitInfoDisk(
                JobSubmissionFiles.getJobSplitFile(inputSplitsDir),
                JobSubmissionFiles.getJobSplitMetaFile(inputSplitsDir),
                splits.length, infoMem.getTaskLocationHints(),
                jobConf.getCredentials());
    }

    /**
     * Exact copy of private method from from org.apache.tez.mapreduce.hadoop.MRInputHelpers
     *
     * Update provided localResources collection with the required local
     * resources needed by MapReduce tasks with respect to Input splits.
     *
     * @param fs Filesystem instance to access status of splits related files
     * @param inputSplitInfo Information on location of split files
     * @param localResources LocalResources collection to be updated
     * @throws IOException
     */
    public static void updateLocalResourcesForInputSplits(
        FileSystem fs,
        InputSplitInfo inputSplitInfo,
        Map<String, LocalResource> localResources) throws IOException {
      if (localResources.containsKey(JOB_SPLIT_RESOURCE_NAME)) {
        throw new RuntimeException("LocalResources already contains a"
            + " resource named " + JOB_SPLIT_RESOURCE_NAME);
      }
      if (localResources.containsKey(JOB_SPLIT_METAINFO_RESOURCE_NAME)) {
        throw new RuntimeException("LocalResources already contains a"
            + " resource named " + JOB_SPLIT_METAINFO_RESOURCE_NAME);
      }

      FileStatus splitFileStatus =
          fs.getFileStatus(inputSplitInfo.getSplitsFile());
      FileStatus metaInfoFileStatus =
          fs.getFileStatus(inputSplitInfo.getSplitsMetaInfoFile());
      localResources.put(JOB_SPLIT_RESOURCE_NAME,
          LocalResource.newInstance(
              ConverterUtils.getYarnUrlFromPath(inputSplitInfo.getSplitsFile()),
              LocalResourceType.FILE,
              LocalResourceVisibility.APPLICATION,
              splitFileStatus.getLen(), splitFileStatus.getModificationTime()));
      localResources.put(JOB_SPLIT_METAINFO_RESOURCE_NAME,
          LocalResource.newInstance(
              ConverterUtils.getYarnUrlFromPath(
                  inputSplitInfo.getSplitsMetaInfoFile()),
              LocalResourceType.FILE,
              LocalResourceVisibility.APPLICATION,
              metaInfoFileStatus.getLen(),
              metaInfoFileStatus.getModificationTime()));
    }

}
