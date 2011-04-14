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

import java.io.Serializable;
import java.util.TreeMap;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.penny.Coordinator;
import org.apache.pig.penny.Location;



public class DHCoordinator extends Coordinator {
    
    private TreeMap<Integer, Integer> histogram = new TreeMap<Integer, Integer>();        // bucket# -> count

    public void init(Serializable[] args) {
    }
    
    public Object finish() {
        return histogram;
    }

    public void receiveMessage(Location source, Tuple message) {
        try {
            for (int i = 0; i < message.size(); i += 2) {
                int bucket = (Integer) message.get(i);
                int count = (Integer) message.get(i+1);
                if (histogram.containsKey(bucket)) histogram.put(bucket, histogram.get(bucket) + count);
                else histogram.put(bucket, count);
            }
        } catch (ExecException e) {
            throw new RuntimeException(e);
        }
    }
    
}
