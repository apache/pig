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
     * Communicate to the loader the load string used in Pig Latin to refer to the 
     * object(s) being loaded.  That is, if the PL script is
     * <b>A = load 'bla'</b>
     * then 'bla' is the load string.  In general Pig expects these to be
     * a path name, a glob, or a URI.  If there is no URI scheme present,
     * Pig will assume it is a file name.  This will be 
     * called during planning on the front end at which time an empty Job object
     * will be passed as the second argument.
     * 
     * This method will also be called in the backend multiple times and in those
     * calls the Job object will actually have job information. Implementations
     * should bear in mind that this method is called multiple times and should
     * ensure there are no inconsistent side effects due to the multiple calls.
     * 
     * @param location Location indicated in load statement.
     * @param job the {@link Job} object
     * @throws IOException if the location is not valid.
     */
    void setLocation(String location, Job job) throws IOException;
    
    /**
     * Return the InputFormat associated with this loader.  This will be
     * called during planning on the front end.  The LoadFunc need not
     * carry the InputFormat information to the backend, as it will
     * be provided with the appropriate RecordReader there.  This is the
     * instance of InputFormat (rather than the class name) because the 
     * load function may need to instantiate the InputFormat in order 
     * to control how it is constructed.
     */
    InputFormat getInputFormat();

    /**
     * Return the LoadCaster associated with this loader.  Returning
     * null indicates that casts from byte array are not supported
     * for this loader.  This will be called on the front end during
     * planning and not on the back end during execution.
     */
    LoadCaster getLoadCaster();

    /**
     * Initializes LoadFunc for reading data.  This will be called during execution
     * before any calls to getNext.  The RecordReader needs to be passed here because
     * it has been instantiated for a particular InputSplit.
     * @param reader RecordReader to be used by this instance of the LoadFunc
     * @param split The input split to process
     */
    void prepareToRead(RecordReader reader, PigSplit split);

    /**
     * Called after all reading is finished.
     */
    void doneReading();

    /**
     * Retrieves the next tuple to be processed.
     * @return the next tuple to be processed or null if there are no more tuples
     * to be processed.
     * @throws IOException
     */
    Tuple getNext() throws IOException;

}
