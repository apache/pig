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
package org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.data.Tuple;

/**
 * This interface is used during Reduce phrase to process tuples
 * in batch mode. It is used by POPackage when all of the UDFs can be
 * called in accumulative mode. Tuples are not pulled all at once,
 * instead, each time, only a specified number of tuples are pulled out
 * of iterator and put in an buffer. Then this buffer is wrapped into 
 * a bag to be passed to the operators in reduce plan.
 * 
 * The purpose of doing this is to reduce memory usage and avoid spilling.
 */
public interface AccumulativeTupleBuffer {
    
    /**
     * Pull next batch of tuples from iterator and put them into this buffer
     */
    public void nextBatch() throws IOException;
    
    /**
     * Whether there are more tuples to pull out of iterator
     */
    public boolean hasNextBatch() ;
    
    /**
     * Clear internal buffer, this should be called after all data are retreived
     */
    public void clear();
    
    /**
     * Get iterator of tuples in the buffer
     * @param index  the index of tuple
     */
    public Iterator<Tuple> getTuples(int index);
}
