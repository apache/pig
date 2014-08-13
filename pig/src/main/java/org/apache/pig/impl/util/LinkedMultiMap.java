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
package org.apache.pig.impl.util;

import java.util.ArrayList;
import java.util.LinkedHashMap;

public class LinkedMultiMap<K,V> extends MultiMap<K,V> {

    /** This class simply extends MultiMap to use LinkedHashMap instead of HashMap.
     *  This ensures while iterating over keys of MultiMap we get keys in 
     *  in same order as they were inserted.
     */
    private static final long serialVersionUID = 1L;

    public LinkedMultiMap() {
        
        mMap = new LinkedHashMap<K, ArrayList<V>>();
    }
    
    /**
     * @param size Initial size of the map
     */
    public LinkedMultiMap(int size) {
    
        mMap = new LinkedHashMap<K, ArrayList<V>>(size);
    }
 }
