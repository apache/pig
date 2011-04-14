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
package org.apache.pig.penny.apps.op;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.pig.penny.ClassWithArgs;
import org.apache.pig.penny.ParsedPigScript;
import org.apache.pig.penny.PennyServer;



/**
 * Get a breakdown of the overhead of each piece of the script.
 * (For now, assumes a linear operator chain (no joins or splits)).
 */
public class Main {
    
    private static final int NUM_TRIALS = 3;            // number of repeated runs to average across
    
    public static void main(String[] args) throws Exception {
        
        PennyServer pennyServer = new PennyServer();
        String pigScriptFilename = args[0];
        ParsedPigScript parsedPigScript = pennyServer.parse(pigScriptFilename);
        
        if (!parsedPigScript.isChain()) throw new RuntimeException("Overhead profiling currently only supports linear chain scripts (no joins, splits, etc.).");

        System.out.println("***** OVERHEAD PROFILER STARTING ...");
        
        Map<Integer, Long> runningTimes = new HashMap<Integer, Long>();                // running time of a script prefix ending at line N

        List<String> runs = new ArrayList<String>();
        String loadAlias = getLoadAlias(parsedPigScript);
        for (String alias = loadAlias; alias != null; alias = parsedPigScript.outEdge(alias)) {
            runs.add(alias);
        }
        runs.add(null);        // whole thing
        
        for (int prefix = 0; prefix < runs.size(); prefix++) {
            System.out.println("*** LAUNCHING PROFILE RUN " + (prefix+1) + " of " + runs.size() + " ...");
        
            Map<String, ClassWithArgs> monitorClasses = new HashMap<String, ClassWithArgs>();
            if (runs.get(prefix) != null) monitorClasses.put(runs.get(prefix), new ClassWithArgs(OPMonitorAgent.class));
            
            long totalRunningTime = 0;
            for (int i = 0; i < NUM_TRIALS; i++) {
                System.out.println("* TRIAL " + (i+1) + " of " + NUM_TRIALS + " ...");
                
                long startTime = System.currentTimeMillis();
                parsedPigScript.truncateAndTrace(OPCoordinator.class, monitorClasses, runs.get(prefix));
                totalRunningTime += System.currentTimeMillis() - startTime;
            }
            long avgRunningTime = totalRunningTime/NUM_TRIALS;
            runningTimes.put(prefix, avgRunningTime);
        }
        
        System.out.println("***** RUNNING TIME BREAKDOWN:");
        for (int prefix = 0; prefix < runs.size(); prefix++) {
            String alias = runs.get(prefix);
            String stepName = "Step " + ((alias == null)? "[store]" : alias);
            long deltaTime = (prefix == 0)? runningTimes.get(prefix) : (runningTimes.get(prefix) - runningTimes.get(prefix-1));
            System.out.println(stepName + ": " + (deltaTime/1000.0) + " seconds");
        }
    }

    private static String getLoadAlias(ParsedPigScript pps) {
        for (String alias : pps.aliases()) {
            if (pps.operator(alias).equals("LOLoad")) return alias;
        }
        throw new RuntimeException("No load operator found.");
    }    
}
