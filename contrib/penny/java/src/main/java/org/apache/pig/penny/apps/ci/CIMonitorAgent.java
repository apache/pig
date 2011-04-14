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
package org.apache.pig.penny.apps.ci;

import java.io.Serializable;
import java.util.Set;

import org.apache.pig.data.Tuple;
import org.apache.pig.penny.Location;
import org.apache.pig.penny.MonitorAgent;



public class CIMonitorAgent extends MonitorAgent {

    private int firstToMonitor;                // first tuple # to monitor
    private int skipSize;                    // # of tuples to skip between monitoring
    private int tupleNum = 0;                // # of tuples seen so far
    
    public Set<Integer> furnishFieldsToMonitor() {
        return null;        // monitor all fields
    }

    public void init(Serializable[] args) {
        firstToMonitor = (Integer) args[0];
        skipSize = (Integer) args[1];
    }

    public Set<String> observeTuple(Tuple t, Set<String> tags) {
        if ((tupleNum >= firstToMonitor) && (tupleNum % skipSize == 0)) {
            communicator().sendToCoordinator(tupleNum, t);
        }
        tupleNum++;
        return NO_TAGS;
    }

    public void receiveMessage(Location source, Tuple message) {
    }
    
    public void finish() {
    }

}
