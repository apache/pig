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
package org.apache.pig.penny.impl.pig;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.pig.penny.ClassWithArgs;



/**
 * The arguments that get passed to the monitoring agent UDF.
 */
public class MonitorAgentUDFArgs implements Serializable {
    
    public final String alias;
    public final String monitorClassName;
    public final Serializable[] monitorClassArgs;
    public final String masterHost;
    public final int masterPort;
    public final List<String> logicalIds;
    public final Set<String> withinTaskUpstreamNeighbors;
    public final Set<String> withinTaskDownstreamNeighbors;
    public final Set<String> crossTaskDownstreamNeighbors;
    public final Map<String, List<Integer>> incomingCrossTaskKeyFields;
    public final List<Integer> outgoingCrossTaskKeyFields;
    
    public MonitorAgentUDFArgs(String alias, ClassWithArgs monitorClass, InetSocketAddress masterAddr, List<String> logicalIds, Set<String> withinTaskUpstreamNeighbors, Set<String> withinTaskDownstreamNeighbors, Set<String> crossTaskDownstreamNeighbors, Map<String, List<Integer>> incomingCrossTaskKeyFields, List<Integer> outgoingCrossTaskKeyFields) {
        this.alias = alias;
        this.monitorClassName = monitorClass.theClass().getCanonicalName();
        this.monitorClassArgs = monitorClass.args();
        this.masterHost = masterAddr.getAddress().getCanonicalHostName();
        this.masterPort = masterAddr.getPort();
        this.withinTaskUpstreamNeighbors = withinTaskUpstreamNeighbors;
        this.withinTaskDownstreamNeighbors = withinTaskDownstreamNeighbors;
        this.crossTaskDownstreamNeighbors = crossTaskDownstreamNeighbors;
        this.logicalIds = logicalIds;
        this.incomingCrossTaskKeyFields = incomingCrossTaskKeyFields;
        this.outgoingCrossTaskKeyFields = outgoingCrossTaskKeyFields;
    }
    
}
