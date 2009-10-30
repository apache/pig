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
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputCommitter;
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
     * Return the OutputFormat associated with StoreFunc.  This will be called
     * on the front end during planning and not on the backend during
     * execution.  OutputFormat information need not be carried to the back end
     * as the appropriate RecordWriter will be provided to the StoreFunc.
     */
    OutputFormat getOutputFormat();

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
     * front end during planning.  If the store function wishes to record
     * the schema it will need to carry it to the backend.
     * Even if a store function cannot
     * record the schema, it may need to implement this function to
     * check that a given schema is acceptable to it.  For example, it
     * can check that the correct partition keys are included;
     * a storage function to be written directly to an OutputFormat can
     * make sure the schema will translate in a well defined way.  
     * @param s to be checked/set
     * @throws IOException if this schema is not acceptable.  It should include
     * a detailed error message indicating what is wrong with the schema.
     */
    void setSchema(ResourceSchema s) throws IOException;

    /**
     * Initialize StoreFunc to write data.  This will be called during
     * execution before the call to putNext.
     * @param writer RecordWriter to use.
     */
    void prepareToWrite(RecordWriter writer);

    /**
     * XXX FIXME: do we really need this - there is already
     * {@link OutputCommitter#commitTask(org.apache.hadoop.mapreduce.TaskAttemptContext)}
     * Called when all writing is finished.  This will be called on the backend,
     * once for each writing task.
     */
    void doneWriting();

    /**
     * Write a tuple the output stream to which this instance was
     * previously bound.
     * 
     * @param t the tuple to store.
     * @throws IOException
     */
    void putNext(Tuple t) throws IOException;

    
    /**
     * XXX FIXME: do we really need this - there is already 
     * {@link OutputCommitter#cleanupJob(org.apache.hadoop.mapreduce.JobContext)}
     * Called when writing all of the data is finished.  This can be used
     * to commit information to a metadata system, clean up tmp files, 
     * close connections, etc.  This call will be made on the front end
     * after all back end processing is finished.
     * @param job The job object
     */
    void allFinished(Job job);



}
