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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.pig.data.Tuple;
import org.apache.pig.penny.ClassWithArgs;
import org.apache.pig.penny.ParsedPigScript;
import org.apache.pig.penny.PennyServer;



/**
 * Backward tracing app.
 */
public class Main {
    
    public static void main(String[] args) throws Exception {
        
        PennyServer pennyServer = new PennyServer();
        String pigScriptFilename = args[0];
        ParsedPigScript parsedPigScript = pennyServer.parse(pigScriptFilename);
        
        String traceAlias = args[1];                            // script alias of tuple to get trace of
        String traceTuple = args[2];                            // tuple to trace (has to match exactly tuple.toSTring())

    // Ibis change : start
        if (!(parsedPigScript.aliases().contains(traceAlias) && parsedPigScript.operator(traceAlias).equals("LOLoad"))) throw new IllegalArgumentException("Invalid alias.");
    // Ibis change : end
    
        Map<String, ClassWithArgs> monitorClasses = new HashMap<String, ClassWithArgs>();
        for (String alias : parsedPigScript.aliases()) {
      // Ibis change : start
            if (parsedPigScript.operator(alias).equals("LOLoad")) {
      // Ibis change : end
                monitorClasses.put(alias, new ClassWithArgs(BTInjectTaintMonitorAgent.class));                    
            } else if (alias.equals(traceAlias)) {
                monitorClasses.put(alias, new ClassWithArgs(BTMatchTaintMonitorAgent.class, traceTuple));
            } else {
                monitorClasses.put(alias, new ClassWithArgs(BTPropagateTaintMonitorAgent.class));
            }
        }
        Collection<Tuple> traceResults = (Collection<Tuple>) parsedPigScript.trace(BTCoordinator.class, monitorClasses);
        System.out.println("*** TRACE RESULTS:" + traceResults);
    }
            
}
