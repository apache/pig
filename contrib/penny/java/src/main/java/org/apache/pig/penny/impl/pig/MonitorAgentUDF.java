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

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.penny.LogicalLocation;
import org.apache.pig.penny.MonitorAgent;
import org.apache.pig.penny.impl.harnesses.MonitorAgentHarness;


/**
 * Pig UDF wrapper for monitor agent.
 */
public class MonitorAgentUDF extends EvalFunc<DataBag> {

    private MonitorAgentHarness harness;
    private boolean initialized = false;
    
    public MonitorAgentUDF(String encodedArgs) throws Exception {
        MonitorAgentUDFArgs args = (MonitorAgentUDFArgs) ObjectSerializer.deserialize(encodedArgs);
        
        MonitorAgent monitorAgent = (MonitorAgent) Class.forName(args.monitorClassName).newInstance();
        monitorAgent.init(args.monitorClassArgs);
        harness = new MonitorAgentHarness(
                monitorAgent, 
                new LogicalLocation(args.alias), 
                new InetSocketAddress(args.masterHost, args.masterPort),
                args.logicalIds,
                args.withinTaskUpstreamNeighbors,
                args.withinTaskDownstreamNeighbors,
                args.crossTaskDownstreamNeighbors,
                args.incomingCrossTaskKeyFields,
                args.outgoingCrossTaskKeyFields);
    }
    
    private void init() throws IOException {
        try {
            harness.initialize();
        } catch(IOException e) {
            throw e;
        } catch(Exception e) {
            throw new IOException("Initialization error", e);
        }
        initialized = true;
    }

    @Override
    public DataBag exec(Tuple t) throws IOException {
        if (!initialized) init();
        boolean retainOutput = harness.processTuple(t);
        DataBag output = BagFactory.getInstance().newDefaultBag();
        if (retainOutput) output.add(t);
        return output;
    }
    
    @Override
    public void finish() {
        if (!initialized) {
            try {
                init();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        harness.finish();
    }

    @Override
    public Schema outputSchema(Schema input) {
        return input;
    }
}
