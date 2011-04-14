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
package org.apache.pig.penny.apps.la;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.penny.Location;
import org.apache.pig.penny.MonitorAgent;
import org.apache.pig.penny.NoSuchLocationException;



public class LAMonitorAgent extends MonitorAgent {

    private static final double multiplier = 100.0;        // generate alert if tuple latency > multiplier * avg latency
    private static final int MIN_TUPLES_FOR_AVG = 5;    // minimum number of tuples upon which to base an accurate average
    
    private Tuple lastTuple = null;                        // most recent tuple we saw
    private long lastTime = -1;                            // start time of most recent tuple
    private double totalProcessingTime = 0;                // cumulative processing time of all tuples
    private long numTuples = 0;                            // cumulative number of tuples seen
    
    private Map<Long, Tuple> pendingTuples;                // first few tuples, if we don't know yet whether they are atypical
    
    public void finish() {
        // no-op
    }

    public Set<Integer> furnishFieldsToMonitor() {
        return null;
    }

    public void init(Serializable[] args) {
        pendingTuples = new HashMap<Long, Tuple>();
    }

    public Set<String> observeTuple(Tuple t, Set<String> tags) {
        
        // look at time to process last tuple
        if (lastTuple != null) {
            long timeToProcessLastTuple = System.nanoTime() - lastTime;
            if (numTuples >= MIN_TUPLES_FOR_AVG) {
                if (alarmingProcessingTime(timeToProcessLastTuple)) {
                    sendAlert(lastTuple);
                }
            } else {
                pendingTuples.put(timeToProcessLastTuple, lastTuple);
                notifyPeers(timeToProcessLastTuple);
            }
            
            updateStats(timeToProcessLastTuple);
        }

        // initialize state for tracking time of the new tuple "t"
        lastTuple = t;
        lastTime = System.nanoTime();
        
        return NO_TAGS;
    }
    
    private void updateStats(long tupleProcessingTime) {
        totalProcessingTime += tupleProcessingTime;
        numTuples++;

        // if cross threshold of having enough tuples for average, drain the "pendingTuples" buffer
        if (numTuples == MIN_TUPLES_FOR_AVG) {
            for (Entry<Long, Tuple> e : pendingTuples.entrySet()) {
                if (alarmingProcessingTime(e.getKey())) sendAlert(e.getValue());
            }
            pendingTuples.clear();
        }
        
    }
    
    private void sendAlert(Tuple t) {
        communicator().sendToCoordinator(t);
    }
    
    private boolean alarmingProcessingTime(long time) {
        double avgProcessingTime = totalProcessingTime / numTuples;
        return (time > avgProcessingTime * multiplier);
    }
    
    private void notifyPeers(long tupleProcessingTime) {
        // send a time measurement to peers
        try {
            communicator().sendToAgents(communicator().myLocation().asLogical(), tupleProcessingTime);
        } catch (NoSuchLocationException e) {
            throw new RuntimeException(e);
        }
    }
    
    public void receiveMessage(Location source, Tuple message) {
        // if a peer (other than self) transmits a measurement, incorporate it into my running average
        if (!source.equals(communicator().myLocation())) {
            try {
                updateStats((Long) message.get(0));
            } catch (ExecException e) {
                throw new RuntimeException("Error parsing message: " + message);
            }
        }
    }

}
