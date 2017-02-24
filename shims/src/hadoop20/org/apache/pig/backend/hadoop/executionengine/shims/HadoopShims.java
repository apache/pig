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
package org.apache.pig.backend.hadoop.executionengine.shims;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapred.TaskReport;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.pig.PigConfiguration;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MRConfiguration;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigOutputCommitter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop20.PigJobControl;

/**
 * We need to make Pig work with both hadoop 20 and hadoop 23 (PIG-2125). However,
 * there is API differences between hadoop 20 and 23. Here we use a shims layer to
 * hide these API differences. A dynamic shims layer is not possible due to some
 * static dependencies. We adopt a static shims approach. For different hadoop version,
 * we need to recompile.
 *
 * This class wrapping all static method. PigMapReduce, PigMapBase, MiniCluster wrapping hadoop
 * version dependant implementation of PigGenericMapReduce, PigGenericMapBase and MiniGenericCluster.
 **/
public class HadoopShims {

    private static Log LOG = LogFactory.getLog(HadoopShims.class);

    static public JobContext cloneJobContext(JobContext original) throws IOException, InterruptedException {
        JobContext newContext = new JobContext(original.getConfiguration(), original.getJobID());
        return newContext;
    }

    static public TaskAttemptContext createTaskAttemptContext(Configuration conf,
            TaskAttemptID taskId) {
        TaskAttemptContext newContext = new TaskAttemptContext(conf,
                taskId);
        return newContext;
    }

    static public JobContext createJobContext(Configuration conf,
            JobID jobId) {
        JobContext newJobContext = new JobContext(
                conf, jobId);
        return newJobContext;
    }

    static public boolean isMap(TaskAttemptID taskAttemptID) {
        return taskAttemptID.isMap();
    }

    static public TaskAttemptID getNewTaskAttemptID() {
        return new TaskAttemptID();
    }

    static public TaskAttemptID createTaskAttemptID(String jtIdentifier, int jobId, boolean isMap,
            int taskId, int id) {
        return new TaskAttemptID(jtIdentifier, jobId, isMap, taskId, id);
    }

    static public void storeSchemaForLocal(Job job, POStore st) throws IOException {
        JobContext jc = HadoopShims.createJobContext(job.getJobConf(),
                new org.apache.hadoop.mapreduce.JobID());
        JobContext updatedJc = PigOutputCommitter.setUpContext(jc, st);
        PigOutputCommitter.storeCleanup(st, updatedJc.getConfiguration());
    }

    static public String getFsCounterGroupName() {
        return "FileSystemCounters";
    }

    static public void commitOrCleanup(OutputCommitter oc, JobContext jc) throws IOException {
        oc.cleanupJob(jc);
    }

    public static JobControl newJobControl(String groupName, int timeToSleep) {
        return new PigJobControl(groupName, timeToSleep);
    }

    public static long getDefaultBlockSize(FileSystem fs, Path path) {
        return fs.getDefaultBlockSize();
    }

    public static Counters getCounters(Job job) throws IOException {
        JobClient jobClient = job.getJobClient();
        return jobClient.getJob(job.getAssignedJobID()).getCounters();
    }

    public static boolean isJobFailed(TaskReport report) {
        float successfulProgress = 1.0f;
        // if the progress reported is not 1.0f then the map or reduce
        // job failed
        // this comparison is in place for the backward compatibility
        // for Hadoop 0.20
        return report.getProgress() != successfulProgress;
    }

    public static void unsetConf(Configuration conf, String key) {
        // Not supported in Hadoop 0.20/1.x
    }

    /**
     * Fetch mode needs to explicitly set the task id which is otherwise done by Hadoop
     * @param conf
     * @param taskAttemptID
     */
    public static void setTaskAttemptId(Configuration conf, TaskAttemptID taskAttemptID) {
        conf.set(MRConfiguration.TASK_ID, taskAttemptID.toString());
    }

    /**
     * Returns whether the give path has a FileSystem implementation.
     *
     * @param path path
     * @param conf configuration
     * @return true if the give path's scheme has a FileSystem implementation,
     *         false otherwise
     */
    public static boolean hasFileSystemImpl(Path path, Configuration conf) {
        String scheme = path.toUri().getScheme();
        if (scheme != null) {
            String fsImpl = conf.get("fs." + scheme + ".impl");
            if (fsImpl == null) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns the progress of a Job j which is part of a submitted JobControl
     * object. The progress is for this Job. So it has to be scaled down by the
     * num of jobs that are present in the JobControl.
     *
     * @param j The Job for which progress is required
     * @return Returns the percentage progress of this Job
     * @throws IOException
     */
    public static double progressOfRunningJob(Job j)
            throws IOException {
        RunningJob rj = j.getJobClient().getJob(j.getAssignedJobID());
        if (rj == null && j.getState() == Job.SUCCESS)
            return 1;
        else if (rj == null)
            return 0;
        else {
            return (rj.mapProgress() + rj.reduceProgress()) / 2;
        }
    }

    public static void killJob(Job job) throws IOException {
        RunningJob runningJob = job.getJobClient().getJob(job.getAssignedJobID());
        if (runningJob != null)
            runningJob.killJob();
    }

    public static Iterator<TaskReport> getTaskReports(Job job, TaskType type) throws IOException {
        if (job.getJobConf().getBoolean(PigConfiguration.PIG_NO_TASK_REPORT, false)) {
            LOG.info("TaskReports are disabled for job: " + job.getAssignedJobID());
            return null;
        }
        JobClient jobClient = job.getJobClient();
        TaskReport[] reports = null;
        if (type == TaskType.MAP) {
            reports = jobClient.getMapTaskReports(job.getAssignedJobID());
        } else {
            reports = jobClient.getReduceTaskReports(job.getAssignedJobID());
        }
        return reports == null ? null : Arrays.asList(reports).iterator();
    }
    
    public static boolean isHadoopYARN() {
        return false;
    }
}
