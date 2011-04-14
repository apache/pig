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
package org.apache.pig.penny.apps.bt;

import java.io.Serializable;
import java.util.Set;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.penny.Location;
import org.apache.pig.penny.MonitorAgent;



public class BTMatchTaintMonitorAgent extends MonitorAgent {

    private String traceTuple;
    
    @Override
    public void finish() {
    }

    @Override
    public Set<Integer> furnishFieldsToMonitor() {
        return null;
    }

    @Override
    public void init(Serializable[] args) {
        this.traceTuple = (String) args[0];
    }

    @Override
    public Set<String> observeTuple(Tuple t, Set<String> tags) throws ExecException {
        if (t.toString().equals(traceTuple)) {
            Tuple tagsAsTuple = new DefaultTuple();
            for (String tag : tags) tagsAsTuple.append(tag);
            communicator().sendToCoordinator("matched_tags", tagsAsTuple);
        }
        return FILTER_OUT;
    }

    @Override
    public void receiveMessage(Location source, Tuple message) {
    }

}
