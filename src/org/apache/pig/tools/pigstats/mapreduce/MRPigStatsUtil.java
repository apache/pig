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

package org.apache.pig.tools.pigstats.mapreduce;

import java.io.IOException;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.NativeMapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.shims.HadoopShims;
import org.apache.pig.classification.InterfaceAudience.Private;
import org.apache.pig.impl.PigContext;
import org.apache.pig.tools.pigstats.PigStats.JobGraph;
import org.apache.pig.tools.pigstats.mapreduce.SimplePigStats;
import org.apache.pig.tools.pigstats.mapreduce.MRScriptState;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.PigStatsUtil;
import org.apache.pig.tools.pigstats.JobStats;



/**
 * A utility class for Pig Statistics
 */
public class MRPigStatsUtil extends PigStatsUtil {

    public static final String MULTI_STORE_RECORD_COUNTER
            = "Output records in ";
    public static final String MULTI_STORE_COUNTER_GROUP
            = "MultiStoreCounters";
    public static final String TASK_COUNTER_GROUP
            = "org.apache.hadoop.mapred.Task$Counter";
    public static final String FS_COUNTER_GROUP
            = HadoopShims.getFsCounterGroupName();
    public static final String MAP_INPUT_RECORDS
            = "MAP_INPUT_RECORDS";
    public static final String MAP_OUTPUT_RECORDS
            = "MAP_OUTPUT_RECORDS";
    public static final String REDUCE_INPUT_RECORDS
            = "REDUCE_INPUT_RECORDS";
    public static final String REDUCE_OUTPUT_RECORDS
            = "REDUCE_OUTPUT_RECORDS";
    public static final String HDFS_BYTES_WRITTEN
            = "HDFS_BYTES_WRITTEN";
    public static final String HDFS_BYTES_READ
            = "HDFS_BYTES_READ";
    public static final String MULTI_INPUTS_RECORD_COUNTER
            = "Input records from ";
    public static final String MULTI_INPUTS_COUNTER_GROUP
            = "MultiInputCounters";

    private static final Log LOG = LogFactory.getLog(MRPigStatsUtil.class);

    // Restrict total string size of a counter name to 64 characters.
    // Leave 24 characters for prefix string.
    private static final int COUNTER_NAME_LIMIT = 40;

    /**
     * Returns the count for the given counter name in the counter group
     * 'MultiStoreCounters'
     *
     * @param job the MR job
     * @param jobClient the Hadoop job client
     * @param counterName the counter name
     * @return the count of the given counter name
     */
    public static long getMultiStoreCount(Job job, JobClient jobClient,
            String counterName) {
        long value = -1;
        try {
            RunningJob rj = jobClient.getJob(job.getAssignedJobID());
            if (rj != null) {
                Counters.Counter counter = rj.getCounters().getGroup(
                        MULTI_STORE_COUNTER_GROUP).getCounterForName(counterName);
                value = counter.getValue();
            }
        } catch (IOException e) {
            LOG.warn("Failed to get the counter for " + counterName, e);
        }
        return value;
    }

    /**
     * Returns the counter name for the given {@link POStore}
     *
     * @param store the POStore
     * @return the counter name
     */
    public static String getMultiStoreCounterName(POStore store) {
        String shortName = getShortName(store.getSFile().getFileName());
        return (shortName == null) ? null
                : MULTI_STORE_RECORD_COUNTER + "_" + store.getIndex() + "_" + shortName;
    }

    /**
     * Returns the counter name for the given input file name
     *
     * @param fname the input file name
     * @return the counter name
     */
    public static String getMultiInputsCounterName(String fname, int index) {
        String shortName = getShortName(fname);
        return (shortName == null) ? null
                : MULTI_INPUTS_RECORD_COUNTER + "_" + index + "_" + shortName;
    }

    private static final String SEPARATOR = "/";
    private static final String SEMICOLON = ";";

    private static String getShortName(String uri) {
        int scolon = uri.indexOf(SEMICOLON);
        int slash;
        if (scolon!=-1) {
            slash = uri.lastIndexOf(SEPARATOR, scolon);
        } else {
            slash = uri.lastIndexOf(SEPARATOR);
        }
        String shortName = null;
        if (scolon==-1) {
            shortName = uri.substring(slash+1);
        }
        if (slash < scolon) {
            shortName = uri.substring(slash+1, scolon);
        }
        if (shortName != null && shortName.length() > COUNTER_NAME_LIMIT) {
            shortName = shortName.substring(shortName.length()
                    - COUNTER_NAME_LIMIT);
        }
        return shortName;
    }

    /**
     * Starts collecting statistics for the given MR plan
     *
     * @param pc the Pig context
     * @param client the Hadoop job client
     * @param jcc the job compiler
     * @param plan the MR plan
     */
    public static void startCollection(PigContext pc, JobClient client,
            JobControlCompiler jcc, MROperPlan plan) {
        SimplePigStats ps = (SimplePigStats)PigStats.get();
        ps.initialize(pc, client, jcc, plan);

        MRScriptState.get().emitInitialPlanNotification(plan);
        MRScriptState.get().emitLaunchStartedNotification(plan.size());
    }

    /**
     * Stops collecting statistics for a MR plan
     *
     * @param display if true, log collected statistics in the Pig log
     *      file at INFO level
     */
    public static void stopCollection(boolean display) {
        SimplePigStats ps = (SimplePigStats)PigStats.get();
        ps.finish();
        if (!ps.isSuccessful()) {
            LOG.error(ps.getNumberFailedJobs() + " map reduce job(s) failed!");
            String errMsg = ps.getErrorMessage();
            if (errMsg != null) {
                LOG.error("Error message: " + errMsg);
            }
        }
        MRScriptState.get().emitLaunchCompletedNotification(
                ps.getNumberSuccessfulJobs());
        if (display) ps.display();
    }

    /**
     * Add stats for a new Job, which doesn't yet need to be completed.
     *
     * @param job the job being run
     * @return JobStats for the job
     */
    public static JobStats addJobStats(Job job) {
        SimplePigStats ps = (SimplePigStats)PigStats.get();
        return ps.addMRJobStats(job);
    }

    /**
     * Logs the statistics in the Pig log file at INFO level
     */
    public static void displayStatistics() {
        ((SimplePigStats)PigStats.get()).display();
    }

    /**
     * Updates the {@link JobGraph} of the {@link PigStats}. The initial
     * {@link JobGraph} is created without job ids using {@link MROperPlan},
     * before any job is submitted for execution. The {@link JobGraph} then
     * is updated with job ids after jobs are executed.
     *
     * @param jobMroMap the map that maps {@link Job}s to {@link MapReduceOper}s
     */
    public static void updateJobMroMap(Map<Job, MapReduceOper> jobMroMap) {
        SimplePigStats ps = (SimplePigStats)PigStats.get();
        for (Map.Entry<Job, MapReduceOper> entry : jobMroMap.entrySet()) {
            MapReduceOper mro = entry.getValue();
            ps.mapMROperToJob(mro, entry.getKey());
        }
    }

    /**
     * Updates the statistics after a patch of jobs is done
     *
     * @param jc the job control
     */
    public static void accumulateStats(JobControl jc) {
        SimplePigStats ps = (SimplePigStats)PigStats.get();
        MRScriptState ss = MRScriptState.get();

        for (Job job : jc.getSuccessfulJobs()) {
            MRJobStats js = addSuccessJobStats(ps, job);
            if (js != null) {
                ss.emitjobFinishedNotification(js);
            }
        }

        for (Job job : jc.getFailedJobs()) {
            MRJobStats js = addFailedJobStats(ps, job);
            if (js != null) {
                js.setErrorMsg(job.getMessage());
                ss.emitJobFailedNotification(js);
            }
        }
    }

    @Private
    public static void setBackendException(Job job, Exception e) {
        JobID jobId = job.getAssignedJobID();
        if (jobId == null) {
            return;
        }
        PigStats.get().setBackendException(jobId.toString(), e);
    }

    private static MRJobStats addFailedJobStats(SimplePigStats ps, Job job) {
        if (ps.isJobSeen(job)) return null;

        MRJobStats js = ps.addMRJobStats(job);
        if (js == null) {
            LOG.warn("unable to add failed job stats");
        } else {
            js.setSuccessful(false);
            js.addOutputStatistics();
            js.addInputStatistics();
        }
        return js;
    }

    public static MRJobStats addNativeJobStats(PigStats ps, NativeMapReduceOper mr,
            boolean success) {
        return addNativeJobStats(ps, mr, success, null);
    }

    public static MRJobStats addNativeJobStats(PigStats ps, NativeMapReduceOper mr,
            boolean success, Exception e) {
        if (ps.isEmbedded()) {
            throw new IllegalArgumentException();
        }
        MRJobStats js = ((SimplePigStats)ps).addMRJobStatsForNative(mr);
        if(js == null) {
            LOG.warn("unable to add native job stats");
        } else {
            js.setSuccessful(success);
            if(e != null)
                js.setBackendException(e);
        }
        return js;
    }

    private static MRJobStats addSuccessJobStats(SimplePigStats ps, Job job) {
        if (ps.isJobSeen(job)) return null;

        MRJobStats js = ps.addMRJobStats(job);
        if (js == null) {
            LOG.warn("unable to add job stats");
        } else {
            js.setSuccessful(true);

            js.addMapReduceStatistics(ps.getJobClient(), job.getJobConf());

            JobClient client = ps.getJobClient();
            RunningJob rjob = null;
            try {
                rjob = client.getJob(job.getAssignedJobID());
            } catch (IOException e) {
                LOG.warn("Failed to get running job", e);
            }
            if (rjob == null) {
                LOG.warn("Failed to get RunningJob for job "
                        + job.getAssignedJobID());
            } else {
                js.addCounters(rjob);
            }

            js.addOutputStatistics();

            js.addInputStatistics();
        }
        return js;
    }

}
