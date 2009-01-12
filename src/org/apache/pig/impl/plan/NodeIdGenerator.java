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

package org.apache.pig.impl.plan;

import java.util.Map;
import java.util.HashMap;

public class NodeIdGenerator {

    private Map<String, Long> scopeToIdMap;
    private static NodeIdGenerator theGenerator = new NodeIdGenerator();
    
    private NodeIdGenerator() {
        scopeToIdMap = new HashMap<String, Long>();
    }
    
    public static NodeIdGenerator getGenerator() {
        return theGenerator;
    }
    
    public long getNextNodeId(String scope) {
        Long val = scopeToIdMap.get(scope);
        
        long nextId = 0;
        
        if (val != null) {
            nextId = val.longValue();
        }

        scopeToIdMap.put(scope, nextId + 1);
        
        return nextId;
    }

    public static void reset(String scope) {
        theGenerator.scopeToIdMap.put(scope, 0L) ;
    }
}
