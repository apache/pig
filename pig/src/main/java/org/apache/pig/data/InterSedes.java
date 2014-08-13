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

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;

/**
 * A class to handle reading and writing of intermediate results of data
 * types. The serialization format used by this class more efficient than 
 * what was used in DataReaderWriter . 
 * The format used by the functions in this class is subject to change, so it
 * should be used ONLY to store intermediate results within a pig query.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface InterSedes {
    
    /**
     * Get the next object from DataInput in
     * @param in
     * @return Next object from DataInput in
     * @throws IOException
     * @throws ExecException
     */
    public Object readDatum(DataInput in)
    throws IOException, ExecException;
    
    /**
     * Get the next object from DataInput in of the type of type argument
     * The type information has been read from DataInput.
     * @param in
     * @param type
     * @return Next object from DataInput in
     * @throws IOException
     * @throws ExecException
     */
    public Object readDatum(DataInput in, byte type)
    throws IOException, ExecException;
    
    /**
     * The type of next object has been determined to be of type Tuple,
     * add the columns that belong to the tuple to given tuple argument t
     * @param in
     * @param t
     * @throws IOException
     */
    public void addColsToTuple(DataInput in, Tuple t)
    throws IOException;

    /**
     * Write given object val to DataOutput out
     * @param out
     * @param val
     * @throws IOException
     */
    public void writeDatum(DataOutput out, Object val) 
    throws IOException;
    
    /**
     * Write given object val of DataType type to DataOutput out
     * @param out output
     * @param val value to write
     * @param type type, as defined in {@link DataType}
     * @throws IOException
     */
    public void writeDatum(DataOutput out, Object val, byte type)
    throws IOException;

    public Class<? extends TupleRawComparator> getTupleRawComparatorClass();
}

