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
package org.apache.pig.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.AccumulativeTupleBuffer;

public class AccumulativeBag implements DataBag {
    private static final long serialVersionUID = 1L;	 
     
    private transient AccumulativeTupleBuffer buffer;
    private int index;
    
    public AccumulativeBag(AccumulativeTupleBuffer buffer, int index) {
        this.buffer = buffer;
        this.index = index;
    }
    
    public void add(Tuple t) {
        throw new RuntimeException("AccumulativeBag does not support add operation");
    }

    public void addAll(DataBag b) {
        throw new RuntimeException("AccumulativeBag does not support add operation");
    }

    public void clear() {
        throw new RuntimeException("AccumulativeBag does not support clear operation");
    }

    public boolean isDistinct() {		
        return false;
    }

    public boolean isSorted() {		
        return false;
    }	
    
    public AccumulativeTupleBuffer getTuplebuffer() {
        return buffer;
    }

    public Iterator<Tuple> iterator() {				
        return buffer.getTuples(index);
    }

    public void markStale(boolean stale) {		

    }

    public long size() {		
        int size = 0;
        for (Iterator<Tuple> it = iterator(); it.hasNext(); it.next(), ++size);
        return size;
    }

    public long getMemorySize() {	
        return 0;
    }

    public long spill() {		
        return 0;
    }

    public void readFields(DataInput datainput) throws IOException {
        throw new IOException("AccumulativeBag does not support readFields operation");
    }

    public void write(DataOutput dataoutput) throws IOException {
        throw new IOException("AccumulativeBag does not support write operation");
    }

    public int compareTo(Object other) {
        throw new RuntimeException("AccumulativeBag does not support compareTo() operation");
    }
    
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        
        return false;
    }
        
    public int hashCode() {
        assert false : "hashCode not designed";
        return 42;
    }

}
