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
import org.apache.hadoop.mapreduce.Job;

import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;

/**
 * This interface defines how to retrieve metadata related to data to be loaded.
 * If a given loader does not implement this interface, it will be assumed that it
 * is unable to provide metadata about the associated data.
 * @since Pig 0.7
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface LoadMetadata {

    /**
     * Get a schema for the data to be loaded.  
     * @param location Location as returned by 
     * {@link LoadFunc#relativeToAbsolutePath(String, org.apache.hadoop.fs.Path)}
     * @param job The {@link Job} object - this should be used only to obtain 
     * cluster properties through {@link Job#getConfiguration()} and not to set/query
     * any runtime job information.  
     * @return schema for the data to be loaded. This schema should represent
     * all tuples of the returned data.  If the schema is unknown or it is
     * not possible to return a schema that represents all returned data,
     * then null should be returned. The schema should not be affected by pushProjection, ie.
     * getSchema should always return the original schema even after pushProjection
     * @throws IOException if an exception occurs while determining the schema
     */
    ResourceSchema getSchema(String location, Job job) throws 
    IOException;

    /**
     * Get statistics about the data to be loaded.  If no statistics are
     * available, then null should be returned. If the implementing class also extends
     * {@link LoadFunc}, then {@link LoadFunc#setLocation(String, org.apache.hadoop.mapreduce.Job)}
     * is guaranteed to be called before this method.
     * @param location Location as returned by 
     * {@link LoadFunc#relativeToAbsolutePath(String, org.apache.hadoop.fs.Path)}
     * @param job The {@link Job} object - this should be used only to obtain 
     * cluster properties through {@link Job#getConfiguration()} and not to set/query
     * any runtime job information.  
     * @return statistics about the data to be loaded.  If no statistics are
     * available, then null should be returned.
     * @throws IOException if an exception occurs while retrieving statistics
     */
    ResourceStatistics getStatistics(String location, Job job) 
    throws IOException;

    /**
     * Find what columns are partition keys for this input.
     * @param location Location as returned by 
     * {@link LoadFunc#relativeToAbsolutePath(String, org.apache.hadoop.fs.Path)}
     * @param job The {@link Job} object - this should be used only to obtain 
     * cluster properties through {@link Job#getConfiguration()} and not to set/query
     * any runtime job information.  
     * @return array of field names of the partition keys. Implementations 
     * should return null to indicate that there are no partition keys
     * @throws IOException if an exception occurs while retrieving partition keys
     */
    String[] getPartitionKeys(String location, Job job) 
    throws IOException;

    /**
     * Set the filter for partitioning.  It is assumed that this filter
     * will only contain references to fields given as partition keys in
     * getPartitionKeys. So if the implementation returns null in 
     * {@link #getPartitionKeys(String, Job)}, then this method is not
     * called by Pig runtime. This method is also not called by the Pig runtime
     * if there are no partition filter conditions. 
     * @param partitionFilter that describes filter for partitioning
     * @throws IOException if the filter is not compatible with the storage
     * mechanism or contains non-partition fields.
     */
    void setPartitionFilter(Expression partitionFilter) throws IOException;

}
