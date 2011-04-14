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
package org.apache.pig.penny.apps.lp;

import java.util.HashMap;
import java.util.Map;

import org.apache.pig.penny.ClassWithArgs;
import org.apache.pig.penny.ParsedPigScript;
import org.apache.pig.penny.PennyServer;



/**
 * Latency profiles app.
 */
public class Main {
    
    public static void main(String[] args) throws Exception {
        
        PennyServer pennyServer = new PennyServer();
        String pigScriptFilename = args[0];
        ParsedPigScript parsedPigScript = pennyServer.parse(pigScriptFilename);
    
        Map<String, ClassWithArgs> monitorClasses = new HashMap<String, ClassWithArgs>();
        for (String alias : parsedPigScript.aliases()) {
            monitorClasses.put(alias, new ClassWithArgs(LPMonitorAgent.class, parsedPigScript.operator(alias).equals("LOLoad")));
        }
        parsedPigScript.trace(LPCoordinator.class, monitorClasses);
        
    }
            
}
