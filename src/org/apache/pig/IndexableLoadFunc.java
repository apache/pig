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
package org.apache.pig;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;
import org.apache.pig.data.Tuple;

/**
 * This class is intended for use by LoadFunc implementations
 * which have an internal index for sorted data and can use the index
 * to support merge join in Pig. Interaction with the index 
 * is abstracted away by the methods in this interface which the Pig
 * runtime will call in a particular sequence to get the records it
 * needs to perform the merge based join.
 * 
 * The sequence of calls made from the Pig runtime are:
 * <ol>
 * <li>{@link LoadFunc#setUDFContextSignature(String)}
 * <li>{@link IndexableLoadFunc#initialize(Configuration)}
 * <li>{@link LoadFunc#setLocation(String, org.apache.hadoop.mapreduce.Job)}
 * <li>{@link IndexableLoadFunc#seekNear(Tuple)}
 * <li>{@link LoadFunc#getNext} called multiple times to retrieve data and perform the join
 * <li>{@link IndexableLoadFunc#close}
 * </ol>
 * @since Pig 0.6
 * 
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving // Set to evolving because we don't have this working with outer join
                             // yet, and we may need to change it some for that.
public interface IndexableLoadFunc {
    
    /**
     * This method is called by Pig run time to allow the
     * IndexableLoadFunc to perform any initialization actions
     * @param conf The job configuration object
     * @throws IOException
     */
    public abstract void initialize(Configuration conf) throws IOException;

    /**
     * This method is called by the Pig runtime to indicate
     * to the LoadFunc to position its underlying input stream
     * near the keys supplied as the argument. Specifically:
     * 1) if the keys are present in the input stream, the loadfunc
     * implementation should position its read position to 
     * a record where the key(s) is/are the biggest key(s) less than
     * the key(s) supplied in the argument OR to the record with the
     * first occurrence of the keys(s) supplied.
     * 2) if the key(s) are absent in the input stream, the implementation
     * should position its read position to a record where the key(s)
     * is/are the biggest key(s) less than the key(s) supplied OR to the
     * first record where the key(s) is/are the smallest key(s) greater
     * than the keys(s) supplied. 
     * The description above holds for descending order data in 
     * a similar manner with "biggest" and "less than" replaced with
     * "smallest" and "greater than" and vice versa.
     *  
     * @param keys Tuple with join keys (which are a prefix of the sort
     * keys of the input data). For example if the data is sorted on
     * columns in position 2,4,5 any of the following Tuples are
     * valid as an argument value:
     * (fieldAt(2))
     * (fieldAt(2), fieldAt(4))
     * (fieldAt(2), fieldAt(4), fieldAt(5))
     * 
     * The following are some invalid cases:
     * (fieldAt(4))
     * (fieldAt(2), fieldAt(5))
     * (fieldAt(4), fieldAt(5))
     * 
     * @throws IOException When the loadFunc is unable to position
     * to the required point in its input stream
     */
    public abstract void seekNear(Tuple keys) throws IOException;
    
    
    /**
     * A method called by the Pig runtime to give an opportunity
     * for implementations to perform cleanup actions like closing
     * the underlying input stream. This is necessary since while
     * performing a join the Pig run time may determine than no further
     * join is possible with remaining records and may indicate to the
     * IndexableLoader to cleanup by calling this method.
     * 
     * @throws IOException if the loadfunc is unable to perform
     * its close actions.
     */
    public abstract void close() throws IOException;
}
