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
package org.apache.pig.penny.apps.ft;

import java.util.HashMap;
import java.util.Map;

import org.apache.pig.penny.ClassWithArgs;
import org.apache.pig.penny.ParsedPigScript;
import org.apache.pig.penny.PennyServer;



/**
 * Forward tracing app.
 */
public class Main {
    
    public static void main(String[] args) throws Exception {
        
        PennyServer pennyServer = new PennyServer();
        String pigScriptFilename = args[0];
        ParsedPigScript parsedPigScript = pennyServer.parse(pigScriptFilename);
        
        String triggerAlias = args[1];                            // script alias at which to trigger forward tracing
        int triggerField = Integer.parseInt(args[2]);            // field number to use for triggering forward tracing
        String triggerValue = args[3];                            // data value to use for triggering forward tracing (for now, use a String -- in general should be any Object)
        
        if (!parsedPigScript.aliases().contains(triggerAlias)) throw new IllegalArgumentException("Invalid trigger alias.");

        Map<String, ClassWithArgs> monitorClasses = new HashMap<String, ClassWithArgs>();
        for (String alias : parsedPigScript.aliases()) {
            if (alias.equals(triggerAlias)) {
                monitorClasses.put(alias, new ClassWithArgs(FTMonitorAgent.class, triggerField, triggerValue));
            } else {
                monitorClasses.put(alias, new ClassWithArgs(FTMonitorAgent.class));
            }
        }
        parsedPigScript.trace(FTCoordinator.class, monitorClasses);
        
    }
            
}
