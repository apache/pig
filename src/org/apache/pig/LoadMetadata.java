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

import org.apache.pig.impl.plan.OperatorPlan;

/**
 * This interface defines how to retrieve metadata related to data to be loaded.
 * If a given loader does not implement this interface, it will be assumed that it
 * is unable to provide metadata about the associated data.
 */
public interface LoadMetadata {

    /**
     * Get a schema for the data to be loaded.  This schema should represent
     * all tuples of the returned data.  If the schema is unknown or it is
     * not possible to return a schema that represents all returned data,
     * then null should be returned.
     * This method will be called after a 
     * {@link LoadFunc#setLocation(String, org.apache.hadoop.mapreduce.Job)}
     * call is made on the Loader implementing {@link LoadFunc} and {@link LoadMetadata}
     */
    ResourceSchema getSchema();

    /**
     * Get statistics about the data to be loaded.  If no statistics are
     * available, then null should be returned.
     */
    ResourceStatistics getStatistics();

    /**
     * Find what columns are partition keys for this input.
     * This function assumes that setLocation has already been called.
     * @return array of field names of the partition keys.
     */
    String[] getPartitionKeys();

    /**
     * Set the filter for partitioning.  It is assumed that this filter
     * will only contain references to fields given as partition keys in
     * getPartitionKeys
     * @param plan that describes filter for partitioning
     * @throws IOException if the filter is not compatible with the storage
     * mechanism or contains non-partition fields.
     */
    void setParitionFilter(OperatorPlan plan) throws IOException;

}