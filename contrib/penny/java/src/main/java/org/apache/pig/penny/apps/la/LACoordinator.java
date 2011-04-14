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
package org.apache.pig.penny.apps.la;

import java.io.Serializable;

import org.apache.pig.data.Tuple;
import org.apache.pig.penny.Coordinator;
import org.apache.pig.penny.Location;



public class LACoordinator extends Coordinator {

    public void init(Serializable[] args) {
    }

    public Object finish() {
        return null;
    }

    public void receiveMessage(Location source, Tuple message) {
        System.out.println("*** HIGH-LATENCY RECORD ALERT AT ALIAS " + source.logId() + ": " + truncate(message));
    }
    
    private String truncate(Tuple t) {
        String s = t.toString();
        return s.substring(0, Math.min(s.length(), 100));
    }

}
