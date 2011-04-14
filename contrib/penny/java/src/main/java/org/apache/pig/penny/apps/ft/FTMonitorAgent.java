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
package org.apache.pig.penny.apps.ft;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.penny.Location;
import org.apache.pig.penny.MonitorAgent;



public class FTMonitorAgent extends MonitorAgent {
    
    private static final Set<String> ftTag;
    static {
        ftTag = new HashSet<String>();
        ftTag.add("t");
    }

    private Integer triggerField;            // field to look for triggerValue (or null if we're not triggering any forward tracing from this agent)
    private String triggerValue;            // value to trigger on (or null if we're not triggering any forward tracing from this agent) -- for now use String; in general should be any Object
    
    public void finish() {
    }

    public Set<Integer> furnishFieldsToMonitor() {
        return null;
    }

    public void init(Serializable[] args) {
        triggerField = (args.length > 0)? ((Integer) args[0]) : null;
        triggerValue = (args.length > 1)? ((String) args[1]) : null;
    }

    public Set<String> observeTuple(Tuple t, Set<String> tags) throws ExecException {
        if (triggerField != null && t.get(triggerField).toString().equals(triggerValue)) tags = ftTag;        // create taint, if record matches trigger value
        if (!tags.isEmpty()) communicator().sendToCoordinator(t);                                            // notify coordinator of tainted tuple
        return tags;                                                                                        // propagate taint forward
    }

    public void receiveMessage(Location source, Tuple message) {
    }
    
}
