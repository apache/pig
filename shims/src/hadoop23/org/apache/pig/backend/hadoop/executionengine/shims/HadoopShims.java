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
import org.apache.hadoop.mapreduce.ContextFactory;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop23.PigJobControl;
import org.apache.pig.impl.PigContext;

public class HadoopShims {
    static public JobContext cloneJobContext(JobContext original) throws IOException, InterruptedException {
        JobContext newContext = ContextFactory.cloneContext(original, original.getConfiguration());
        return newContext;
    }

    static public TaskAttemptContext createTaskAttemptContext(Configuration conf,
                                TaskAttemptID taskId) {
        TaskAttemptContext newContext = new TaskAttemptContextImpl(conf, taskId);
        return newContext;
    }

    static public JobContext createJobContext(Configuration conf,
            JobID jobId) {
        JobContext newContext = new JobContextImpl(conf, jobId);
        return newContext;
    }

    static public boolean isMap(TaskAttemptID taskAttemptID) {
        TaskType type = taskAttemptID.getTaskType();
        if (type==TaskType.MAP)
            return true;

        return false;
    }

    static public TaskAttemptID getNewTaskAttemptID() {
        TaskAttemptID taskAttemptID = new TaskAttemptID("", 1, TaskType.MAP,
                1, 1);
        return taskAttemptID;
    }

    static public TaskAttemptID createTaskAttemptID(String jtIdentifier, int jobId, boolean isMap,
            int taskId, int id) {
        if (isMap) {
            return new TaskAttemptID(jtIdentifier, jobId, TaskType.MAP, taskId, id);
        } else {
            return new TaskAttemptID(jtIdentifier, jobId, TaskType.REDUCE, taskId, id);
        }
    }

    static public void storeSchemaForLocal(Job job, POStore st) {
        // Doing nothing for hadoop 23
    }

    static public String getFsCounterGroupName() {
        return "org.apache.hadoop.mapreduce.FileSystemCounter";
    }

    static public void commitOrCleanup(OutputCommitter oc, JobContext jc) throws IOException {
        oc.commitJob(jc);
    }

    public static JobControl newJobControl(String groupName, int timeToSleep) {
      return new PigJobControl(groupName, timeToSleep);
    }
}
