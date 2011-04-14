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

import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;

public abstract class Communicator {
    
    /**
     * Find out my (physical) location.
     */
    public abstract Location myLocation();
    
    /**
     * Send an message to the coordinator, asynchronously.
     */
    public abstract void sendToCoordinator(Tuple message);
    
    /**
     * Send a message to immediate downstream neighbor(s), synchronously.
     * If downstream neighbor(s) span a task boundary, all instances will receive it; otherwise only same-task instances will receive it.
     * If there is no downstream neighbor, an exception will be thrown.
     */
    public abstract void sendDownstream(Tuple message) throws NoSuchLocationException;
    
    /**
     * Send a message to immediate upstream neighbor(s), synchronously.
     * If upstream neighbor(s) are non-existent or span a task boundary, an exception will be thrown.
     */
    public abstract void sendUpstream(Tuple message) throws NoSuchLocationException;
    
    /**
     * Send a message to current/future instances of a given logical location.
     * Instances that have already terminated will not receive the message (obviously).
     * Instances that are currently executing will receive it asynchronously (or perhaps not at all, if they terminate before the message arrives).
     * Instances that have not yet started will receive the message prior to beginning processing of tuples.
     */
    public abstract void sendToAgents(LogicalLocation destination, Tuple message) throws NoSuchLocationException;
    
    public void sendToCoordinator(Object ... message) {
        sendToCoordinator(makeTuple(message));
    }
    
    public void sendDownstream(Object ... message) throws NoSuchLocationException {
        sendDownstream(makeTuple(message));
    }

    public void sendUpstream(Object ... message) throws NoSuchLocationException {
        sendUpstream(makeTuple(message));
    }
    
    public void sendToAgents(LogicalLocation destination, Object ... message) throws NoSuchLocationException {
        sendToAgents(destination, makeTuple(message));
    }

    private static Tuple makeTuple(Object[] items) {
        Tuple t = new DefaultTuple();
        for (int i = 0; i < items.length; i++) {
            t.append(items[i]);
        }
        return t;
    }
    
}
