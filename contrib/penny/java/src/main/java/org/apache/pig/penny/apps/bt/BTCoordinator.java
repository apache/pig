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

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.penny.Coordinator;
import org.apache.pig.penny.Location;



public class BTCoordinator extends Coordinator {
    
    private Set<String> matchedTags = new HashSet<String>();
    private Map<String, Collection<Tuple>> taggedTuples = new HashMap<String, Collection<Tuple>>();

    @Override
    public Object finish() {
        Collection<Tuple> traceResults = new LinkedList<Tuple>();
        for (String tag : matchedTags) {
            traceResults.addAll(taggedTuples.get(tag));
        }
        return traceResults;
    }

    @Override
    public void init(Serializable[] args) {
    }

    @Override
    public void receiveMessage(Location source, Tuple message) {
        try {
            if (message.get(0).equals("matched_tags")) {
                Tuple tagsAsTuple = (Tuple) message.get(1);
                for (int i = 0; i < tagsAsTuple.size(); i++) {
                    matchedTags.add((String) tagsAsTuple.get(i));
                }
            } else {
                Tuple t = (Tuple) message.get(0);
                Tuple tagsAsTuple = (Tuple) message.get(1);
                for (int i = 0; i < tagsAsTuple.size(); i++) {
                    String tag = (String) tagsAsTuple.get(i);
                    if (!taggedTuples.containsKey(tag)) taggedTuples.put(tag, new LinkedList<Tuple>());
                    taggedTuples.get(tag).add(t);
                }            
            }
        } catch (ExecException e) {
            throw new RuntimeException(e);
        }
    }

}
