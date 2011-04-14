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
import java.util.HashSet;
import java.util.Set;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.penny.Location;
import org.apache.pig.penny.MonitorAgent;



public class LPMonitorAgent extends MonitorAgent {
    
    private final static int NUM_SAMPLES = 5;
    private boolean injectTags;
    private int tupleCount = 0;
    
    public void finish() {
    }

    public Set<Integer> furnishFieldsToMonitor() {
        return null;
    }

    public void init(Serializable[] args) {
        injectTags = (Boolean) args[0];
    }

    public Set<String> observeTuple(Tuple t, Set<String> tags) throws ExecException {
        if (injectTags && (tupleCount++ < NUM_SAMPLES)) {
            String tag = communicator().myLocation() + "_" + (tupleCount++);            // assign a unique tag to every sampled tuple
            Set<String> injectedTags = new HashSet<String>();
            injectedTags.add(tag);
            Tuple tagAsTuple = new DefaultTuple();
            tagAsTuple.append(tag);
            communicator().sendToCoordinator(tagAsTuple);
            return injectedTags;
        } else if (!tags.isEmpty()) {
            Tuple tagsAsTuple = new DefaultTuple();
            for (String tag : tags) tagsAsTuple.append(tag);
            communicator().sendToCoordinator(tagsAsTuple);
            return tags;
        } else {
            return tags;
        }
    }

    public void receiveMessage(Location source, Tuple message) {
    }

}
