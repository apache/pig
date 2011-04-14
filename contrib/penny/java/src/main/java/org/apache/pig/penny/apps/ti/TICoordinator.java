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
package org.apache.pig.penny.apps.ti;

import java.io.Serializable;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.penny.Coordinator;
import org.apache.pig.penny.Location;



public class TICoordinator extends Coordinator {
    
    private int minCard;                    // threshold for alert
    private int cardinality = 0;            // current running total
    
    public void init(Serializable[] args) {
        minCard = (Integer) args[0];
    }
    
    public Object finish() {
        if (cardinality < minCard) System.out.println("*** INTEGRITY ALERT: DATA SET CARDINALITY IS ONLY " + cardinality);
        return null;
    }

    public void receiveMessage(Location source, Tuple message) {
        try {
            cardinality += (Integer) message.get(0);
        } catch (ExecException e) {
            throw new RuntimeException(e);
        }
    }
    
}
