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
package org.apache.pig.penny.apps.dh;

import java.io.Serializable;
import java.util.Set;
import java.util.TreeMap;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.penny.Location;
import org.apache.pig.penny.MonitorAgent;



public class DHMonitorAgent extends MonitorAgent {

    private int fieldNo, min, max, bucketSize;
    
    private TreeMap<Integer, Integer> histogram = new TreeMap<Integer, Integer>();        // bucket# -> count

    public void finish() {
        communicator().sendToCoordinator(histogramAsTuple());
    }

    public Set<Integer> furnishFieldsToMonitor() {
        return null;
    }

    public void init(Serializable[] args) {
        fieldNo = (Integer) args[0];
        min = (Integer) args[1];
        max = (Integer) args[2];
        bucketSize = (Integer) args[3];
    }

    public Set<String> observeTuple(Tuple t, Set<String> tags) throws ExecException {
        int val = Integer.parseInt((t.get(fieldNo)).toString());
        if (val < min || val > max) throw new ExecException("Number out of range for histogram construction.");
        int bucket = (val-min)/bucketSize;
        if (histogram.containsKey(bucket)) histogram.put(bucket, histogram.get(bucket)+1);
        else histogram.put(bucket, 1);
        
        return tags;
    }

    public void receiveMessage(Location source, Tuple message) {
    }
    
    private Tuple histogramAsTuple() {
        Tuple t = new DefaultTuple();
        for (int bucket : histogram.keySet()) {
            t.append(bucket);
            t.append(histogram.get(bucket));
        }
        return t;
    }

}
