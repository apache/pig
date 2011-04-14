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
package org.apache.pig.penny.apps.gl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.pig.penny.ClassWithArgs;
import org.apache.pig.penny.ParsedPigScript;
import org.apache.pig.penny.PennyServer;



/**
 * Golden logic testing app.
 */
public class Main {
    
    public static void main(String[] args) throws Exception {
        
        PennyServer pennyServer = new PennyServer();
        String pigScriptFilename = args[0];
        ParsedPigScript parsedPigScript = pennyServer.parse(pigScriptFilename);
        
        String testAlias = args[1];                                // which alias is being tested
        double sampleRate = Double.parseDouble(args[2]);        // what fraction of data to sample for testing
        String goldenLogicClass = args[3];                        // class that gives the "golden" (correct) output for a given input tuple

        if (!parsedPigScript.aliases().contains(testAlias)) throw new IllegalArgumentException("Invalid alias.");

        List<String> upstreamAliases = parsedPigScript.inEdges(testAlias);
        if (upstreamAliases.size() != 1) throw new IllegalArgumentException("Unable to perform testing of given alias.");
        String prevAlias = upstreamAliases.iterator().next();
        
        Map<String, ClassWithArgs> monitorClasses = new HashMap<String, ClassWithArgs>();
        monitorClasses.put(prevAlias, new ClassWithArgs(GLMonitorAgent1.class, sampleRate));
        monitorClasses.put(testAlias, new ClassWithArgs(GLMonitorAgent2.class));
        parsedPigScript.trace(new ClassWithArgs(GLCoordinator.class, goldenLogicClass), monitorClasses);
        
    }
            
}
