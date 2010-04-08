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

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.mortbay.log.Log;

public abstract class PigStatsUtil {

    public static final String MULTI_STORE_RECORD_COUNTER 
            = "Output records in ";
    public static final String MULTI_STORE_COUNTER_GROUP 
            = "MultiStoreCounters";
    
    @SuppressWarnings("deprecation")
    public static long getMultiStoreCount(Job job, JobClient jobClient,
            String counterName) {
        long value = 0;
        try {
            RunningJob rj = jobClient.getJob(job.getAssignedJobID());
            if (rj != null) {
                Counters.Counter counter = rj.getCounters().getGroup(
                        MULTI_STORE_COUNTER_GROUP).getCounterForName(counterName);
                value = counter.getValue();
            }
        } catch (IOException e) {
            Log.warn("Failed to get the counter for " + counterName);
        }
        return value;        
    }
    
    public static String getMultiStoreCounterName(POStore store) {
        return MULTI_STORE_RECORD_COUNTER +
                new Path(store.getSFile().getFileName()).getName();
    }
}
