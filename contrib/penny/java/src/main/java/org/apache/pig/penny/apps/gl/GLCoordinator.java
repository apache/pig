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
package org.apache.pig.penny.apps.gl;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.penny.Coordinator;
import org.apache.pig.penny.Location;



public class GLCoordinator extends Coordinator {
    
    private final Map<String, Tuple> inputTuples = new HashMap<String, Tuple>();
    private final Map<String, Set<Tuple>> outputTuples = new HashMap<String, Set<Tuple>>();

    private GoldenLogic goldenLogic;

    public void init(Serializable[] args) {
        try {
            this.goldenLogic = (GoldenLogic) Class.forName((String) args[0]).newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Object finish() {
        for (String tag : inputTuples.keySet()) {
            Tuple in = inputTuples.get(tag);
            Set<Tuple> actualOut = outputTuples.get(tag);
            Set<Tuple> goldenOut;
            try {
                goldenOut = new HashSet<Tuple>(goldenLogic.run(in));
            } catch (ExecException e) {
                throw new RuntimeException(e);
            }
            
            if (!actualOut.equals(goldenOut)) {
                System.out.println("**** DISCREPANCY BETWEEN ACTUAL AND GOLDEN OUTPUT FOR INPUT TUPLE " + in + ":");
                System.out.println("\tGOLDEN OUTPUT: " + goldenOut);
                System.out.println("\tACTUAL OUTPUT: " + actualOut);
            }
        }
        return null;
    }

    public void receiveMessage(Location source, Tuple message) {
        String position;
        Tuple tuple;
        String tag;
        
        try {
            position = (String) message.get(0);
            tuple = (Tuple) message.get(1);
            tag = (String) message.get(2);
        } catch (ExecException e) {
            throw new RuntimeException(e);
        }
        
        if (!outputTuples.containsKey(tag)) outputTuples.put(tag, new HashSet<Tuple>());

        if (position.equals("in")) inputTuples.put(tag, tuple);
        else outputTuples.get(tag).add(tuple);
    }

}
