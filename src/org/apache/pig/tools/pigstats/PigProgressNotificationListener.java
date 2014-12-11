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

package org.apache.pig.tools.pigstats;

import org.apache.pig.PigRunner;
import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;
import org.apache.pig.impl.plan.OperatorPlan;

/**
 * Should be implemented by an object that wants to receive notifications
 * from {@link PigRunner}.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface PigProgressNotificationListener extends java.util.EventListener {

    /**
     * Invoked before any Hadoop jobs (or a Tez DAG) are run with the plan that is to be executed.
     *
     * @param scriptId the unique id of the script
     * @param plan the OperatorPlan that is to be executed
     */
    public void initialPlanNotification(String scriptId, OperatorPlan<?> plan);

    /**
     * Invoked just before launching Hadoop jobs (or tez DAGs) spawned by the script.
     * @param scriptId the unique id of the script
     * @param numJobsToLaunch the total number of Hadoop jobs (or Tez DAGs) spawned by the script
     */
    public void launchStartedNotification(String scriptId, int numJobsToLaunch);

    /**
     * Invoked just before submitting a batch of Hadoop jobs (or Tez DAGs).
     * @param scriptId the unique id of the script
     * @param numJobsSubmitted the number of Hadoop jobs (or Tez DAGs) in the batch
     */
    public void jobsSubmittedNotification(String scriptId, int numJobsSubmitted);

    /**
     * Invoked after a Hadoop job (or Tez DAG) is started.
     * @param scriptId the unique id of the script
     * @param assignedJobId the Hadoop job id (or Tez DAG job id)
     */
    public void jobStartedNotification(String scriptId, String assignedJobId);

    /**
     * Invoked just after a Hadoop job (or Tez DAG) is completed successfully.
     * @param scriptId the unique id of the script
     * @param jobStats the {@link JobStats} object associated with the Hadoop job (or Tez DAG)
     */
    public void jobFinishedNotification(String scriptId, JobStats jobStats);

    /**
     * Invoked when a Hadoop job fails.
     * @param scriptId the unique id of the script
     * @param jobStats the {@link JobStats} object associated with the Hadoop job
     */
    public void jobFailedNotification(String scriptId, JobStats jobStats);

    /**
     * Invoked just after an output is successfully written.
     * @param scriptId the unique id of the script
     * @param outputStats the {@link OutputStats} object associated with the output
     */
    public void outputCompletedNotification(String scriptId, OutputStats outputStats);

    /**
     * Invoked to update the execution progress.
     * @param scriptId the unique id of the script
     * @param progress the percentage of the execution progress
     */
    public void progressUpdatedNotification(String scriptId, int progress);

    /**
     * Invoked just after all Hadoop jobs (Tez DAGs) spawned by the script are completed.
     * @param scriptId the unique id of the script
     * @param numJobsSucceeded the total number of Hadoop jobs (Tez DAGs) succeeded
     */
    public void launchCompletedNotification(String scriptId, int numJobsSucceeded);
}
