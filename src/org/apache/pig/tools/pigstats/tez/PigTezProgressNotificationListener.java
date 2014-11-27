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

package org.apache.pig.tools.pigstats.tez;

import org.apache.pig.PigRunner;
import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;
import org.apache.pig.impl.plan.OperatorPlan;
import org.apache.pig.tools.pigstats.PigProgressNotificationListener;

/**
 * Should be implemented by an object that wants to receive notifications
 * from {@link PigRunner}.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class PigTezProgressNotificationListener implements PigProgressNotificationListener {

    /**
     * Invoked just before launching a Tez DAG spawned by the script.
     *
     * @param scriptId the unique id of the script
     * @param dagId the unique name of the Tez DAG
     * @param dagPlan the OperatorPlan that is to be executed
     * @param numVerticesToLaunch the total number of vertices spawned by the Tez DAG
     */
    public abstract void dagLaunchNotification(String scriptId, String dagId,
            OperatorPlan<?> dagPlan, int numVerticesToLaunch);

    /**
     * Invoked after a Tez DAG is started.
     *
     * @param scriptId the unique id of the script
     * @param dagId the unique name of the Tez DAG
     * @param assignedApplicationId
     *            the YARN application id for the Tez DAG. More than one Tez DAG
     *            can share same application ID if session reuse is turned on.
     *            Session reuse is turned on by default
     *
     */
    public abstract void dagStartedNotification(String scriptId, String dagId, String assignedApplicationId);

    /**
     * Invoked to update the execution progress.
     *
     * @param scriptId the unique id of the script
     * @param dagId the unique name of the Tez DAG
     * @param numVerticesCompleted the number of vertices completed so far
     * @param progress the percentage of the execution progress based on total number of tasks of all vertices
     */
    public abstract void dagProgressNotification(String scriptId, String dagId, int numVerticesCompleted, int progress);

    /**
     * Invoked just after the Tez DAGs is completed (successful or failed).
     * @param scriptId the unique id of the script
     * @param dagId the unique name of the Tez DAG
     * @param success true if the Tez DAG was successful, false otherwise
     * @param tezDAGStats the stats information for the DAG
     */
    public abstract void dagCompletedNotification(String scriptId, String dagId, boolean success, TezDAGStats tezDAGStats);
}
