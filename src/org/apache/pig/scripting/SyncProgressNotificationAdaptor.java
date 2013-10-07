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

package org.apache.pig.scripting;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.OutputStats;
import org.apache.pig.tools.pigstats.PigProgressNotificationListener;

class SyncProgressNotificationAdaptor implements PigProgressNotificationListener {

    private static final Log LOG = LogFactory.getLog(SyncProgressNotificationAdaptor.class);

    private List<PigProgressNotificationListener> listeners;

    public SyncProgressNotificationAdaptor(
            List<PigProgressNotificationListener> listeners) {
        this.listeners = listeners;
    }

    @Override
    public void jobFailedNotification(String scriptId, JobStats jobStats) {
        synchronized (listeners) {
            for (PigProgressNotificationListener listener : listeners) {
                listener.jobFailedNotification(scriptId, jobStats);
            }
        }        
    }

    @Override
    public void jobFinishedNotification(String scriptId, JobStats jobStats) {
        synchronized (listeners) {
            for (PigProgressNotificationListener listener : listeners) {
                listener.jobFinishedNotification(scriptId, jobStats);
            }
        }        
    }

    @Override
    public void jobStartedNotification(String scriptId, String assignedJobId) {
        synchronized (listeners) {
            for (PigProgressNotificationListener listener : listeners) {
                listener.jobStartedNotification(scriptId, assignedJobId);
            }
        }        
    }

    @Override
    public void jobsSubmittedNotification(String scriptId, int numJobsSubmitted) {
        synchronized (listeners) {
            for (PigProgressNotificationListener listener : listeners) {
                listener.jobsSubmittedNotification(scriptId, numJobsSubmitted);
            }
        }        
    }

    @Override
    public void launchCompletedNotification(String scriptId,
            int numJobsSucceeded) {
        synchronized (listeners) {
            for (PigProgressNotificationListener listener : listeners) {
                listener.launchCompletedNotification(scriptId, numJobsSucceeded);
            }
        }        
    }

    @Override
    public void initialPlanNotification(String scriptId, MROperPlan plan) {
        synchronized (listeners) {
            for (PigProgressNotificationListener listener : listeners) {
                try {
                    listener.initialPlanNotification(scriptId, plan);
                } catch (NoSuchMethodError e) {
                    LOG.warn("PigProgressNotificationListener implementation doesn't "
                           + "implement initialPlanNotification(..) method: "
                           + listener.getClass().getName(), e);
                }
            }
        }
    }

    @Override
    public void launchStartedNotification(String scriptId, int numJobsToLaunch) {
        synchronized (listeners) {
            for (PigProgressNotificationListener listener : listeners) {
                listener.launchStartedNotification(scriptId, numJobsToLaunch);
            }
        }        
    }

    @Override
    public void outputCompletedNotification(String scriptId,
            OutputStats outputStats) {
        synchronized (listeners) {
            for (PigProgressNotificationListener listener : listeners) {
                listener.outputCompletedNotification(scriptId, outputStats);
            }
        }        
    }

    @Override
    public void progressUpdatedNotification(String scriptId, int progress) {
        synchronized (listeners) {
            for (PigProgressNotificationListener listener : listeners) {
                listener.progressUpdatedNotification(scriptId, progress);
            }
        }
    }
}
