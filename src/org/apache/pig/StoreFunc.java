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

import org.apache.pig.data.Tuple;


/**
* This interface is used to implement functions to write records
* from a dataset.
* 
*
*/

public interface StoreFunc {
    
    /**
     * Specifies the OutputStream to write to. This will be called before
     * store(Tuple) is invoked.
     * 
     * @param os The stream to write tuples to.
     * @throws IOException
     */
    public abstract void bindTo(OutputStream os) throws IOException;

    /**
     * Write a tuple the output stream to which this instance was
     * previously bound.
     * 
     * @param f the tuple to store.
     * @throws IOException
     */
    public abstract void putNext(Tuple f) throws IOException;

    /**
     * Do any kind of post processing because the last tuple has been
     * stored. DO NOT CLOSE THE STREAM in this method. The stream will be
     * closed later outside of this function.
     * 
     * @throws IOException
     */
    public abstract void finish() throws IOException;
    
    /**
     * Specify a backend specific class to use to prepare for
     * storing output.  In the Hadoop case, this can return an
     * OutputFormat that will be used instead of PigOutputFormat.  The 
     * framework will call this function and if a Class is returned
     * that implements OutputFormat it will be used. For more details on how
     * the OutputFormat should interact with Pig, see 
     * {@link org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigOutputFormat#getRecordWriter(org.apache.hadoop.fs.FileSystem, org.apache.hadoop.mapred.JobConf, String, org.apache.hadoop.util.Progressable)}
     * @return Backend specific class used to prepare for storing output.
     * If the {@link StoreFunc} implementation does not have a class to prepare
     * for storing output, it can return null and a default Pig implementation
     * will be used to prepare for storing output.
     * @throws IOException if the class does not implement the expected
     * interface(s).
     */
    public Class getStorePreparationClass() throws IOException;

    
}
