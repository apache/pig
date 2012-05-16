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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigOutputCommitter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop20.PigJobControl;
import org.apache.pig.impl.PigContext;

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
    static public JobContext cloneJobContext(JobContext original) throws IOException, InterruptedException {
        JobContext newContext = new JobContext(original.getConfiguration(), original.getJobID());
        return newContext;
    }

    static public TaskAttemptContext createTaskAttemptContext(Configuration conf,
                                TaskAttemptID taskId) {
        TaskAttemptContext newContext = new TaskAttemptContext(new Configuration(conf),
            taskId);
        return newContext;
    }

    static public JobContext createJobContext(Configuration conf,
            JobID jobId) {
        JobContext newJobContext = new JobContext(
                new Configuration(conf), jobId);
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
}
