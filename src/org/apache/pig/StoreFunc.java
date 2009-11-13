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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.pig.data.Tuple;


/**
* This interface is used to implement functions to write records
* from a dataset.
* 
*
*/

public interface StoreFunc {

    /**
     * This method is called by the Pig runtime in the front end to convert the
     * output location to an absolute path if the location is relative. The
     * StoreFunc implementation is free to choose how it converts a relative 
     * location to an absolute location since this may depend on what the location
     * string represent (hdfs path or some other data source)
     * 
     * @param location location as provided in the "store" statement of the script
     * @param curDir the current working direction based on any "cd" statements
     * in the script before the "store" statement. If there are no "cd" statements
     * in the script, this would be the home directory - 
     * <pre>/user/<username> </pre>
     * @return the absolute location based on the arguments passed
     * @throws IOException if the conversion is not possible
     */
    String relToAbsPathForStoreLocation(String location, Path curDir) throws IOException;

    /**
     * Return the OutputFormat associated with StoreFunc.  This will be called
     * on the front end during planning and not on the backend during
     * execution. 
     * @return the {@link OutputFormat} associated with StoreFunc
     * @throws IOException if an exception occurs while constructing the 
     * OutputFormat
     *
     */
    OutputFormat getOutputFormat() throws IOException;

    /**
     * Communicate to the store function the location used in Pig Latin to refer 
     * to the object(s) being stored.  That is, if the PL script is
     * <b>store A into 'bla'</b>
     * then 'bla' is the location.  This location should be either a file name
     * or a URI.  If it does not have a URI scheme Pig will assume it is a 
     * filename.  This will be 
     * called during planning on the front end, not during execution on
     * the backend.
     * @param location Location indicated in store statement.
     * @param job The {@link Job} object
     * @throws IOException if the location is not valid.
     */
    void setStoreLocation(String location, Job job) throws IOException;
 
    /**
     * Set the schema for data to be stored.  This will be called on the
     * front end during planning. A Store function should implement this function to
     * check that a given schema is acceptable to it.  For example, it
     * can check that the correct partition keys are included;
     * a storage function to be written directly to an OutputFormat can
     * make sure the schema will translate in a well defined way.  
     * @param s to be checked
     * @throws IOException if this schema is not acceptable.  It should include
     * a detailed error message indicating what is wrong with the schema.
     */
    void checkSchema(ResourceSchema s) throws IOException;

    /**
     * Initialize StoreFunc to write data.  This will be called during
     * execution before the call to putNext.
     * @param writer RecordWriter to use.
     * @throws IOException if an exception occurs during initialization
     */
    void prepareToWrite(RecordWriter writer) throws IOException;

    /**
     * Write a tuple the output stream to which this instance was
     * previously bound.
     * 
     * @param t the tuple to store.
     * @throws IOException if an exception occurs during the write
     */
    void putNext(Tuple t) throws IOException;

}
