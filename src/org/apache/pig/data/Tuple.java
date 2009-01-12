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

import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.io.WritableComparable;

import org.apache.pig.backend.executionengine.ExecException;

/**
 * An ordered list of Data.  A tuple has fields, numbered 0 through
 * (number of fields - 1).  The entry in the field can be any datatype,
 * or it can be null.
 *
 * Tuples are constructed only by a TupleFactory.  A DefaultTupleFactory
 * is provided by the system.  If a user wishes to use their own type of
 * Tuple, they should also provide an implementation of TupleFactory to
 * construct their types of Tuples.
 *
 * Fields are numbered from 0.
 */
public interface Tuple extends WritableComparable, Serializable {
       
    /**
     * Marker for indicating whether the value this object holds
     * is a null
     */
    public static byte NULL = 0x00;
    
    /**
     * Marker for indicating whether the value this object holds
     * is not a null
     */
    public static byte NOTNULL = 0x01;
    
    /**
     * Make this tuple reference the contents of another.  This method does not copy
     * the underlying data.   It maintains references to the data from the original
     * tuple (and possibly even to the data structure holding the data).
     * @param t Tuple to reference.
     */
    void reference(Tuple t);

    /**
     * Find the size of the tuple.  Used to be called arity().
     * @return number of fields in the tuple.
     */
    int size();

    /**
     * Find out if a given field is null.
     * @param fieldNum Number of field to check for null.
     * @return true if the field is null, false otherwise.
     * @throws ExecException if the field number given is greater
     * than or equal to the number of fields in the tuple.
     */
    boolean isNull(int fieldNum) throws ExecException;

    /**
     * Find the type of a given field.
     * @param fieldNum Number of field to get the type for.
     * @return type, encoded as a byte value.  The values are taken from
     * the class DataType.  If the field is null, then DataType.UNKNOWN
     * will be returned.
     * @throws ExecException if the field number is greater than or equal to
     * the number of fields in the tuple.
     */
    byte getType(int fieldNum) throws ExecException;

    /**
     * Get the value in a given field.
     * @param fieldNum Number of the field to get the value for.
     * @return value, as an Object.
     * @throws ExecException if the field number is greater than or equal to
     * the number of fields in the tuple.
     */
    Object get(int fieldNum) throws ExecException;

    /**
     * Get all of the fields in the tuple as a list.
     * @return List&lt;Object&gt; containing the fields of the tuple
     * in order.
     */
    List<Object> getAll();

    /**
     * Set the value in a given field.
     * @param fieldNum Number of the field to set the value for.
     * @param val Object to put in the indicated field.
     * @throws ExecException if the field number is greater than or equal to
     * the number of fields in the tuple.
     */
    void set(int fieldNum, Object val) throws ExecException;

    /**
     * Append a field to a tuple.  This method is not efficient as it may
     * force copying of existing data in order to grow the data structure.
     * Whenever possible you should construct your Tuple with the
     * newTuple(int) method and then fill in the values with set(), rather
     * than construct it with newTuple() and append values.
     * @param val Object to append to the tuple.
     */
    void append(Object val);

    /**
     * Determine the size of tuple in memory.  This is used by data bags
     * to determine their memory size.  This need not be exact, but it
     * should be a decent estimation.
     * @return estimated memory size.
     */
    long getMemorySize();

    /** 
     * Write a tuple of atomic values into a string.  All values in the
     * tuple must be atomic (no bags, tuples, or maps).
     * @param delim Delimiter to use in the string.
     * @return A string containing the tuple.
     * @throws ExecException if a non-atomic value is found.
     */
    String toDelimitedString(String delim) throws ExecException;
    
    /**
     * @return true if this Tuple is null
     */
    public boolean isNull();
    
    /**
     * @param isNull boolean indicating whether this tuple is null
     */
    public void setNull(boolean isNull);
}
