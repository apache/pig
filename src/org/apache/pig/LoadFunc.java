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
import java.net.URL;
import java.util.Map;

import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.logicalLayer.schema.Schema;


/**
 * This interface is used to implement functions to parse records
 * from a dataset.  This also includes functions to cast raw byte data into various
 * datatypes.  These are external functions because we want loaders, whenever
 * possible, to delay casting of datatypes until the last possible moment (i.e.
 * don't do it on load).  This means we need to expose the functionality so that
 * other sections of the code can call back to the loader to do the cast.
 */
public interface LoadFunc {
    /**
     * Specifies a portion of an InputStream to read tuples. Because the
     * starting and ending offsets may not be on record boundaries it is up to
     * the implementor to deal with figuring out the actual starting and ending
     * offsets in such a way that an arbitrarily sliced up file will be processed
     * in its entirety.
     * <p>
     * A common way of handling slices in the middle of records is to start at
     * the given offset and, if the offset is not zero, skip to the end of the
     * first record (which may be a partial record) before reading tuples.
     * Reading continues until a tuple has been read that ends at an offset past
     * the ending offset.
     * <p>
     * <b>The load function should not do any buffering on the input stream</b>. Buffering will
     * cause the offsets returned by is.getPos() to be unreliable.
     *  
     * @param fileName the name of the file to be read
     * @param is the stream representing the file to be processed, and which can also provide its position.
     * @param offset the offset to start reading tuples.
     * @param end the ending offset for reading.
     * @throws IOException
     */
    public void bindTo(String fileName,
                       BufferedPositionedInputStream is,
                       long offset,
                       long end) throws IOException;

    /**
     * Retrieves the next tuple to be processed.
     * @return the next tuple to be processed or null if there are no more tuples
     * to be processed.
     * @throws IOException
     */
    public Tuple getNext() throws IOException;
    
    
    /**
     * Cast data from bytes to integer value.  
     * @param b byte array to be cast.
     * @return Integer value.
     * @throws IOException if the value cannot be cast.
     */
    public Integer bytesToInteger(byte[] b) throws IOException;

    /**
     * Cast data from bytes to long value.  
     * @param b byte array to be cast.
     * @return Long value.
     * @throws IOException if the value cannot be cast.
     */
    public Long bytesToLong(byte[] b) throws IOException;

    /**
     * Cast data from bytes to float value.  
     * @param b byte array to be cast.
     * @return Float value.
     * @throws IOException if the value cannot be cast.
     */
    public Float bytesToFloat(byte[] b) throws IOException;

    /**
     * Cast data from bytes to double value.  
     * @param b byte array to be cast.
     * @return Double value.
     * @throws IOException if the value cannot be cast.
     */
    public Double bytesToDouble(byte[] b) throws IOException;

    /**
     * Cast data from bytes to chararray value.  
     * @param b byte array to be cast.
     * @return String value.
     * @throws IOException if the value cannot be cast.
     */
    public String bytesToCharArray(byte[] b) throws IOException;

    /**
     * Cast data from bytes to map value.  
     * @param b byte array to be cast.
     * @return Map value.
     * @throws IOException if the value cannot be cast.
     */
    public Map<Object, Object> bytesToMap(byte[] b) throws IOException;

    /**
     * Cast data from bytes to tuple value.  
     * @param b byte array to be cast.
     * @return Tuple value.
     * @throws IOException if the value cannot be cast.
     */
    public Tuple bytesToTuple(byte[] b) throws IOException;

    /**
     * Cast data from bytes to bag value.  
     * @param b byte array to be cast.
     * @return Bag value.
     * @throws IOException if the value cannot be cast.
     */
    public DataBag bytesToBag(byte[] b) throws IOException;

    /**
     * Indicate to the loader fields that will be needed.  This can be useful for
     * loaders that access data that is stored in a columnar format where indicating
     * columns to be accessed a head of time will save scans.  If the loader
     * function cannot make use of this information, it is free to ignore it.
     * @param schema Schema indicating which columns will be needed.
     */
    public void fieldsToRead(Schema schema);

    /**
     * Find the schema from the loader.  This function will be called at parse time
     * (not run time) to see if the loader can provide a schema for the data.  The
     * loader may be able to do this if the data is self describing (e.g. JSON).  If
     * the loader cannot determine the schema, it can return a null.
     * LoadFunc implementations which need to open the input "fileName", can use 
     * FileLocalizer.open(String fileName, ExecType execType, DataStorage storage) to get
     * an InputStream which they can use to initialize their loader implementation. They
     * can then use this to read the input data to discover the schema. Note: this will
     * work only when the fileName represents a file on Local File System or Hadoop file 
     * system
     * @param fileName Name of the file to be read.(this will be the same as the filename 
     * in the "load statement of the script)
     * @param execType - execution mode of the pig script - one of ExecType.LOCAL or ExecType.MAPREDUCE
     * @param storage - the DataStorage object corresponding to the execType
     * @return a Schema describing the data if possible, or null otherwise.
     * @throws IOException.
     */
    public Schema determineSchema(String fileName, ExecType execType, DataStorage storage) throws IOException;
}
