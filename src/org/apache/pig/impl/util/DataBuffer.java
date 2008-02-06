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

import java.util.*;

import org.apache.pig.data.Datum;
import org.apache.pig.impl.eval.collector.DataCollector;



public class DataBuffer extends DataCollector {
    
    public DataBuffer(){
        super(null);
    }
    
    List<Datum> buf = Collections.synchronizedList(new LinkedList<Datum>());

    @Override
    public void add(Datum d){
        if (d != null) buf.add(d);
    }
    
    public Datum removeFirst(){
        if (buf.isEmpty())
            return null;
        else
            return buf.remove(0);
    }
    
    /**
     * This is a sequence we want to do frequently to accomodate the simple eval case, i.e., cases
     * where we know that running an eval spec one item should produce one and only one item.
     */
    public Datum removeFirstAndAssertEmpty(){
        Datum d;
        if (isStale() || (d = removeFirst()) == null){
            throw new RuntimeException("Simple eval used but buffer found to be empty or stale");
        }
        if (!buf.isEmpty())
            throw new RuntimeException("Simple eval used but buffer found to have more than one datum");
        return d;
    }
    
    public boolean isEmpty() {
        return buf.isEmpty();
    }

}
