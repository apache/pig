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
package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.partitioners;
import java.util.HashMap;
import java.util.Map.Entry;

/**
 * 
 */

public class CountingMap<K> extends HashMap<K, Integer> {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    private long totalCount = 0;
    
    @Override
    public Integer put(K arg0, Integer arg1) {
        if(!containsKey(arg0)){
            totalCount += arg1;
            return super.put(arg0, arg1);
        }
        else{
            totalCount += arg1;
            return super.put(arg0, get(arg0)+arg1);
        }
    }
    
    public void display(){
        System.out.println();
        System.out.println("-------------------------");
        
        for (Entry<K,Integer> ent : entrySet()) {
            System.out.println(ent.getKey() + ": " + ent.getValue());
        }
        
        System.out.println("-------------------------------");
    }

    public long getTotalCount() {
        return totalCount;
    }
}