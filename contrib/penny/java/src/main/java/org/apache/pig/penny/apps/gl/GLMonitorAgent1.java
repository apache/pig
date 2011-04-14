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
package org.apache.pig.penny.apps.gl;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.apache.pig.data.Tuple;
import org.apache.pig.penny.Location;
import org.apache.pig.penny.MonitorAgent;



public class GLMonitorAgent1 extends MonitorAgent {

    private final Random rand = new Random(System.currentTimeMillis());
    
    private double sampleRate;
    
    private int taintCount = 0;
    
    public void finish() {
    }

    public Set<Integer> furnishFieldsToMonitor() {
        return null;
    }

    public void init(Serializable[] args) {
        sampleRate = (Double) args[0];
    }

    public Set<String> observeTuple(Tuple t, Set<String> tags) {
        if (rand.nextDouble() < sampleRate) {
            String assignedTag = communicator().myLocation() + "_" + (taintCount++);        // give each tuple a unique taint tag
            communicator().sendToCoordinator("in", t, assignedTag);            
            Set<String> assignedTags = new HashSet<String>();
            assignedTags.add(assignedTag);
            return assignedTags;
        } else {
            return FILTER_OUT;
        }
    }
    
    
    public void receiveMessage(Location source, Tuple message) {
    }

}
