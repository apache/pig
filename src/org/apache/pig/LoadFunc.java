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
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;


/**
 * This interface is used to implement functions to parse records
 * from a dataset.
 */
public interface LoadFunc {
    /**
     * This method is called by the Pig runtime in the front end to convert the
     * input location to an absolute path if the location is relative. The
     * loadFunc implementation is free to choose how it converts a relative 
     * location to an absolute location since this may depend on what the location
     * string represent (hdfs path or some other data source)
     * 
     * @param location location as provided in the "load" statement of the script
     * @param curDir the current working direction based on any "cd" statements
     * in the script before the "load" statement. If there are no "cd" statements
     * in the script, this would be the home directory - 
     * <pre>/user/<username> </pre>
     * @return the absolute location based on the arguments passed
     * @throws IOException if the conversion is not possible
     */
    String relativeToAbsolutePath(String location, Path curDir) throws IOException;

    /**
     * Communicate to the loader the location of the object(s) being loaded.  
     * The location string passed to the LoadFunc here is the return value of 
     * {@link LoadFunc#relativeToAbsolutePath(String, Path)}
     * 
     * This method will be called in the backend multiple times. Implementations
     * should bear in mind that this method is called multiple times and should
     * ensure there are no inconsistent side effects due to the multiple calls.
     * 
     * @param location Location as returned by 
     * {@link LoadFunc#relativeToAbsolutePath(String, Path)}
     * @param job the {@link Job} object
     * @throws IOException if the location is not valid.
     */
    void setLocation(String location, Job job) throws IOException;
    
    /**
     * This will be called during planning on the front end. This is the
     * instance of InputFormat (rather than the class name) because the 
     * load function may need to instantiate the InputFormat in order 
     * to control how it is constructed.
     * @return the InputFormat associated with this loader.
     * @throws IOException if there is an exception during InputFormat 
     * construction
     */
    InputFormat getInputFormat() throws IOException;

    /**
     * This will be called on the front end during planning and not on the back 
     * end during execution.
     * @return the {@link LoadCaster} associated with this loader. Returning null 
     * indicates that casts from byte array are not supported for this loader. 
     * construction
     * @throws IOException if there is an exception during LoadCaster 
     */
    LoadCaster getLoadCaster() throws IOException;

    /**
     * Initializes LoadFunc for reading data.  This will be called during execution
     * before any calls to getNext.  The RecordReader needs to be passed here because
     * it has been instantiated for a particular InputSplit.
     * @param reader {@link RecordReader} to be used by this instance of the LoadFunc
     * @param split The input {@link PigSplit} to process
     * @throws IOException if there is an exception during initialization
     */
    void prepareToRead(RecordReader reader, PigSplit split) throws IOException;

    /**
     * Retrieves the next tuple to be processed.
     * @return the next tuple to be processed or null if there are no more tuples
     * to be processed.
     * @throws IOException if there is an exception while retrieving the next
     * tuple
     */
    Tuple getNext() throws IOException;

}
