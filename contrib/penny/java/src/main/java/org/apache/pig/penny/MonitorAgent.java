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
package org.apache.pig.penny;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

public abstract class MonitorAgent {
    
    ///// tag constants
    public static final Set<String> NO_TAGS = new HashSet<String>();
    public static final Set<String> FILTER_OUT = null;
    
    
    private Communicator communicator;

    public final void initialize(Communicator communicator) {
        this.communicator = communicator;
    }
    
    protected Communicator communicator() {
        if (communicator == null) {
            throw new IllegalStateException("Agent must be initialized before getting communicator");
        }
        return communicator;
    }
    
    
    ///// Abstract methods that app-writer implements:
    
    /**
     * Furnish set of fields to monitor. (Null means monitor all fields ('*').)
     */
    public abstract Set<Integer> furnishFieldsToMonitor();
    
    /**
     * Initialize, using any arguments passed from higher layer.
     */
    public abstract void init(Serializable[] args);
    
    /**
     * Observe a tuple (record) that passes through the monitoring point.
     * 
     * @param t                the tuple
     * @param tag            t's tags
     * @return                FILTER_OUT to remove the tuple from the data stream; NO_TAGS to let it pass through and not give it any tags; a set of tags to let it pass through and assign those tags
     */
    public abstract Set<String> observeTuple(Tuple t, Set<String> tags) throws ExecException;
    
    /**
     * Process an incoming (synchronous or asynchronous) message.
     */
    public abstract void receiveMessage(Location source, Tuple message);
    
    /**
     * No more tuples are going to pass through the monitoring point. Finish any ongoing processing.
     */
    public abstract void finish();

}
