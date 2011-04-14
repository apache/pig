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
package org.apache.pig.penny.apps.lp;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.penny.Coordinator;
import org.apache.pig.penny.Location;



public class LPCoordinator extends Coordinator {
    
    private final Map<String, TreeMap<String, Long>> times = new HashMap<String, TreeMap<String, Long>>();
    private long startTime;

    public void init(Serializable[] args) {
        startTime = System.currentTimeMillis();
    }
    
    public Object finish() {
        for (String tag : times.keySet()) {
            TreeMap<String, Long> thisTagTimes = times.get(tag);
            System.out.println(" --- LATENCY TRACE:");
            for (String alias : thisTagTimes.keySet()) {
                System.out.println("\t Alias " + alias + ": " + (thisTagTimes.get(alias) - startTime) + " ms");
            }
        }
        return null;
    }

    public void receiveMessage(Location source, Tuple message) {
        long timestamp = System.currentTimeMillis();
        
        String alias = source.logId();
        Tuple tags = message;
        
        for (int i = 0; i < tags.size(); i++) {
            String tag;
            try {
                tag = (String) tags.get(i);
            } catch (ExecException e) {
                throw new RuntimeException(e);
            }
            if (!times.containsKey(tag)) times.put(tag, new TreeMap<String, Long>());
            times.get(tag).put(alias, timestamp);        // for now, just keep track of the latest time (in future, could do median or something)
        }
    }
    
}
