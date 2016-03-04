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
import java.util.HashMap;
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
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.mapreduce.hadoop.DeprecatedKeys;
import org.apache.tez.mapreduce.hadoop.InputSplitInfo;
import org.apache.tez.mapreduce.hadoop.InputSplitInfoDisk;
import org.apache.tez.mapreduce.hadoop.InputSplitInfoMem;

@InterfaceAudience.Private
public class MRToTezHelper {

    private static final Log LOG = LogFactory.getLog(MRToTezHelper.class);
    private static final String JOB_SPLIT_RESOURCE_NAME = MRJobConfig.JOB_SPLIT;
    private static final String JOB_SPLIT_METAINFO_RESOURCE_NAME = MRJobConfig.JOB_SPLIT_METAINFO;

    private static Map<String, String> mrAMParamToTezAMParamMap = new HashMap<String, String>();
    private static Map<String, String> mrMapParamToTezVertexParamMap = new HashMap<String, String>();
    private static Map<String, String> mrReduceParamToTezVertexParamMap = new HashMap<String, String>();

    private static List<String> mrSettingsToRetain = new ArrayList<String>();

    private static List<String> mrSettingsToRemove = new ArrayList<String>();

    private MRToTezHelper() {
    }

    static {
        populateMRToTezParamsMap();
        populateMRSettingsToRetain();
        populateMRSettingsToRemove();
    }

    private static void populateMRToTezParamsMap() {

        //AM settings
        mrAMParamToTezAMParamMap.put(MRJobConfig.MR_AM_VMEM_MB, TezConfiguration.TEZ_AM_RESOURCE_MEMORY_MB);
        mrAMParamToTezAMParamMap.put(MRJobConfig.MR_AM_CPU_VCORES, TezConfiguration.TEZ_AM_RESOURCE_CPU_VCORES);
        mrAMParamToTezAMParamMap.put(MRJobConfig.MR_AM_MAX_ATTEMPTS, TezConfiguration.TEZ_AM_MAX_APP_ATTEMPTS);
        mrAMParamToTezAMParamMap.put(MRConfiguration.JOB_CREDENTIALS_BINARY, TezConfiguration.TEZ_CREDENTIALS_PATH);
        mrAMParamToTezAMParamMap.put(MRJobConfig.JOB_CANCEL_DELEGATION_TOKEN, TezConfiguration.TEZ_CANCEL_DELEGATION_TOKENS_ON_COMPLETION);

        //Map settings
        mrMapParamToTezVertexParamMap.put(MRJobConfig.MAP_MAX_ATTEMPTS, TezConfiguration.TEZ_AM_TASK_MAX_FAILED_ATTEMPTS);
        mrMapParamToTezVertexParamMap.put(MRJobConfig.MAP_SPECULATIVE, TezConfiguration.TEZ_AM_SPECULATION_ENABLED);
        mrMapParamToTezVertexParamMap.put(MRJobConfig.MAP_LOG_LEVEL, TezConfiguration.TEZ_TASK_LOG_LEVEL);
        //TezConfiguration.TEZ_AM_VERTEX_MAX_TASK_CONCURRENCY TEZ-2914 in Tez 0.8
        mrMapParamToTezVertexParamMap.put("mapreduce.job.running.map.limit", "tez.am.vertex.max-task-concurrency");
        //TezConfiguration.TEZ_TASK_PROGRESS_STUCK_INTERVAL_MS TEZ-808 in Tez 0.8
        mrMapParamToTezVertexParamMap.put(MRJobConfig.TASK_TIMEOUT, "tez.am.progress.stuck.interval-ms");

        //Reduce settings
        mrReduceParamToTezVertexParamMap.put(MRJobConfig.REDUCE_MAX_ATTEMPTS, TezConfiguration.TEZ_AM_TASK_MAX_FAILED_ATTEMPTS);
        mrReduceParamToTezVertexParamMap.put(MRJobConfig.REDUCE_SPECULATIVE, TezConfiguration.TEZ_AM_SPECULATION_ENABLED);
        mrReduceParamToTezVertexParamMap.put(MRJobConfig.REDUCE_LOG_LEVEL, TezConfiguration.TEZ_TASK_LOG_LEVEL);
        mrReduceParamToTezVertexParamMap.put("mapreduce.job.running.reduce.limit", "tez.am.vertex.max-task-concurrency");
        mrReduceParamToTezVertexParamMap.put("mapreduce.job.running.map.limit", "tez.am.vertex.max-task-concurrency");
        mrReduceParamToTezVertexParamMap.put(MRJobConfig.TASK_TIMEOUT, "tez.am.progress.stuck.interval-ms");
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

        // FileInputFormat.listStatus() on a task can cause job failure when run from Oozie
        mrSettingsToRemove.add(MRJobConfig.MAPREDUCE_JOB_CREDENTIALS_BINARY);

        mrSettingsToRemove.add(MRJobConfig.CACHE_ARCHIVES);
        mrSettingsToRemove.add(MRJobConfig.CACHE_ARCHIVES_SIZES);
        mrSettingsToRemove.add(MRJobConfig.CACHE_ARCHIVES_TIMESTAMPS);
        mrSettingsToRemove.add(MRJobConfig.CACHE_ARCHIVES_VISIBILITIES);
        mrSettingsToRemove.add(MRJobConfig.CACHE_FILES);
        mrSettingsToRemove.add(MRJobConfig.CACHE_FILES_SIZES);
        mrSettingsToRemove.add(MRJobConfig.CACHE_FILE_TIMESTAMPS);
        mrSettingsToRemove.add(MRJobConfig.CACHE_FILE_VISIBILITIES);
        mrSettingsToRemove.add(MRJobConfig.CLASSPATH_FILES);
    }

    private static void removeUnwantedSettings(Configuration tezConf, boolean isAMConf) {

        // It is good to clean up as much of the unapplicable settings as possible.
        // Tez has configs set on multiple places AM, DAG, Vertex, VertexManager
        // Plugin, Tasks (Processor, Edge, every input and output, combiner)
        // If conf size is bigger, it places heavy pressurce on AM memory and is
        // inefficient while sending over RPC to tasks

        for (String mrSetting : mrSettingsToRemove) {
            tezConf.unset(mrSetting);
        }

        Iterator<Entry<String, String>> iter = new Configuration(tezConf).iterator();
        while (iter.hasNext()) {
            String key = iter.next().getKey();
            if (!isAMConf) {
                // Keep the setting in AM conf to be able to connect back to the
                // Oozie launcher job and look at the parameter values passed,
                // but get rid of for others
                if (key.startsWith("oozie.")) {
                    tezConf.unset(key);
                    continue;
                }
            }
            if (key.startsWith("dfs.datanode")) {
                tezConf.unset(key);
            } else if (key.startsWith("dfs.namenode")) {
                tezConf.unset(key);
            } else if (key.startsWith("yarn.nodemanager")) {
                tezConf.unset(key);
            } else if (key.startsWith("mapreduce.jobhistory")) {
                tezConf.unset(key);
            } else if (key.startsWith("mapreduce.jobtracker")) {
                tezConf.unset(key);
            } else if (key.startsWith("mapreduce.tasktracker")) {
                tezConf.unset(key);
            }
        }
    }

    public static TezConfiguration getDAGAMConfFromMRConf(
            Configuration tezConf) {

        // Set Tez parameters based on MR parameters.
        TezConfiguration dagAMConf = new TezConfiguration(tezConf);


        convertMRToTezConf(dagAMConf, dagAMConf, DeprecatedKeys.getMRToDAGParamMap());
        convertMRToTezConf(dagAMConf, dagAMConf, mrAMParamToTezAMParamMap);

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

        dagAMConf.setIfUnset(TezConfiguration.TEZ_AM_VIEW_ACLS,
                tezConf.get(MRJobConfig.JOB_ACL_VIEW_JOB, MRJobConfig.DEFAULT_JOB_ACL_VIEW_JOB));

        dagAMConf.setIfUnset(TezConfiguration.TEZ_AM_MODIFY_ACLS,
                tezConf.get(MRJobConfig.JOB_ACL_MODIFY_JOB, MRJobConfig.DEFAULT_JOB_ACL_MODIFY_JOB));

        // Hardcoding at AM level instead of setting per vertex till TEZ-2710 is available
        dagAMConf.setIfUnset(TezConfiguration.TEZ_TASK_SCALE_MEMORY_RESERVE_FRACTION, "0.5");

        removeUnwantedSettings(dagAMConf, true);

        return dagAMConf;
    }

    /**
     * Set config with Scope.Vertex in TezConfiguration on the vertex
     *
     * @param vertex Vertex on which config is to be set
     * @param isMapVertex Whether map or reduce vertex. i.e root or intermediate/leaf vertex
     * @param conf Config that contains the tez or equivalent mapreduce settings.
     */
    public static void setVertexConfig(Vertex vertex, boolean isMapVertex,
            Configuration conf) {
        Map<String, String> configMapping = isMapVertex ? mrMapParamToTezVertexParamMap
                : mrReduceParamToTezVertexParamMap;
        for (Entry<String, String> dep : configMapping.entrySet()) {

            String value = conf.get(dep.getValue(), conf.get(dep.getKey()));
            if (value != null) {
                vertex.setConf(dep.getValue(), value);
                LOG.debug("Setting " + dep.getValue() + " to " + value
                        + " for the vertex " + vertex.getName());
            }
        }
    }

    /**
     * Process the mapreduce configuration settings and
     *    - copy as is the still required ones (like those used by FileInputFormat/FileOutputFormat)
     *    - convert and set equivalent tez runtime settings
     *    - handle compression related settings
     *
     * @param tezConf Configuration on which the mapreduce settings will have to be transferred
     * @param mrConf Configuration that contains mapreduce settings
     */
    public static void processMRSettings(Configuration tezConf, Configuration mrConf) {
        for (String mrSetting : mrSettingsToRetain) {
            if (mrConf.get(mrSetting) != null) {
                tezConf.set(mrSetting, mrConf.get(mrSetting));
            }
        }
        JobControlCompiler.configureCompression(tezConf);
        convertMRToTezConf(tezConf, mrConf, DeprecatedKeys.getMRToTezRuntimeParamMap());
        removeUnwantedSettings(tezConf, false);
    }

    /**
     * Convert MR settings to Tez settings and set on conf.
     *
     * @param tezConf  Configuration on which MR equivalent Tez settings should be set
     * @param mrConf Configuration that contains MR settings
     * @param mrToTezConfigMapping  Mapping of MR config to equivalent Tez config
     */
    private static void convertMRToTezConf(Configuration tezConf, Configuration mrConf, Map<String, String> mrToTezConfigMapping) {
        for (Entry<String, String> dep : mrToTezConfigMapping.entrySet()) {
            if (mrConf.get(dep.getKey()) != null) {
                if (tezConf.get(dep.getValue()) == null) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Setting " + dep.getValue() + " to "
                                + mrConf.get(dep.getKey()) + " from MR setting "
                                + dep.getKey());
                    }
                    tezConf.set(dep.getValue(), mrConf.get(dep.getKey()));
                }
                tezConf.unset(dep.getKey());
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
