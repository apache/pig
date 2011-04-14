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
package org.apache.pig.penny.apps.ci;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.pig.data.Tuple;
import org.apache.pig.penny.ClassWithArgs;
import org.apache.pig.penny.ParsedPigScript;
import org.apache.pig.penny.PennyServer;
import org.apache.pig.penny.PhysicalLocation;



/**
 * Crash investigator app.
 */
public class Main {
    
    public static void main(String[] args) throws Exception {
        
        System.out.println("***** CRASH INVESTIGATOR STARTING ...");
        
        PennyServer pennyServer = new PennyServer();
        String pigScriptFilename = args[0];
        ParsedPigScript parsedPigScript = pennyServer.parse(pigScriptFilename);

        Map<String, Integer> lowerBounds = new HashMap<String, Integer>();            // lower bound on tuple num for each logical location
        Map<PhysicalLocation, Tuple> latestTuples = null;
        for (int skipSize = 100; skipSize > 0; skipSize /= 10) {
            System.out.println("***** TRYING SCRIPT ...");
            
            Map<String, ClassWithArgs> monitorClasses = new HashMap<String, ClassWithArgs>();
            for (String alias : parsedPigScript.aliases()) {
                int lowerBound = (lowerBounds.containsKey(alias))? lowerBounds.get(alias) : 0;
                monitorClasses.put(alias, new ClassWithArgs(CIMonitorAgent.class, lowerBound, skipSize));
            }
            latestTuples = (Map<PhysicalLocation, Tuple>) parsedPigScript.trace(CICoordinator.class, monitorClasses);
            
            // update lower bounds
            lowerBounds.clear();
            for (Entry<PhysicalLocation, Tuple> entry : latestTuples.entrySet()) {
                String alias = entry.getKey().logId();
                int tupleNum = (Integer) entry.getValue().get(0);
                if (lowerBounds.containsKey(alias)) lowerBounds.put(alias, Math.min(lowerBounds.get(alias), tupleNum));
                else lowerBounds.put(alias, tupleNum);
            }
        }
        
        Map<String, Set<Tuple>> crashCulprits = new HashMap<String, Set<Tuple>>();
        for (Entry<PhysicalLocation, Tuple> entry : latestTuples.entrySet()) {
            String alias = entry.getKey().logId();
            Tuple t = (Tuple) entry.getValue().get(1);
            if (!crashCulprits.containsKey(alias)) crashCulprits.put(alias, new HashSet<Tuple>());
            crashCulprits.get(alias).add(t);
        }
        
        System.out.println("\n\n***** CRASH SUSPECTS:");
        for (String alias : parsedPigScript.aliases()) {
            if (crashCulprits.containsKey(alias)) {
                System.out.println("\nAlias " + alias + ":");
                for (Tuple t : crashCulprits.get(alias)) {
                    System.out.println("\t" + t);
                }
            }
        }
    }

}
