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
package org.apache.pig.penny.apps.dh;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.pig.penny.ClassWithArgs;
import org.apache.pig.penny.ParsedPigScript;
import org.apache.pig.penny.PennyServer;

/**
 * Data summaries app. that computes a histogram of one of the fields of one of the intermediate data sets.
 */
public class Main {
    
    public static void main(String[] args) throws Exception {
        
        PennyServer pennyServer = new PennyServer();
        String pigScriptFilename = args[0];
        ParsedPigScript parsedPigScript = pennyServer.parse(pigScriptFilename);
        
        String alias = args[1];                                // which alias to create histogram for
        int fieldNo = Integer.parseInt(args[2]);            // which field to create histogram for
        int min = Integer.parseInt(args[3]);                // min field value
        int max = Integer.parseInt(args[4]);                // max field value
        int bucketSize = Integer.parseInt(args[5]);            // histogram bucket size
        
        if (!parsedPigScript.aliases().contains(alias)) throw new IllegalArgumentException("No such alias.");
        
        Map<String, ClassWithArgs> monitorClasses = new HashMap<String, ClassWithArgs>();
        monitorClasses.put(alias, new ClassWithArgs(DHMonitorAgent.class, fieldNo, min, max, bucketSize));
        TreeMap<Integer, Integer> histogram = (TreeMap<Integer, Integer>) parsedPigScript.trace(DHCoordinator.class, monitorClasses);
        System.out.println("Histogram: " + histogram);
    }
            
}
