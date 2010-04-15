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
package org.apache.hadoop.owl.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.owl.mapreduce.OwlInputFormat.OwlOperation;
import org.apache.hadoop.owl.protocol.OwlLoaderInfo;
import org.apache.hadoop.owl.protocol.OwlSchema;
import org.apache.pig.data.Tuple;

/** The abstract class to be implemented by underlying storage drivers to enable data access from Owl through
 *  OwlInputFormat.
 */
public abstract class OwlInputStorageDriver {

    /**
     * Returns the InputFormat to use with this Storage Driver. 
     * @param loaderInfo the loader info object containing parameters required for initialization of InputFormat
     * @return the InputFormat instance
     */
    public abstract InputFormat<BytesWritable, Tuple> getInputFormat(OwlLoaderInfo loaderInfo);

    /**
     * Returns true if this InputFormat supports specified operation.
     * @param operation the operation to check for
     * @return true, if specified operation is supported
     */
    public abstract boolean isFeatureSupported(OwlOperation operation) throws IOException;

    /**
     * Set the data location for the input.
     * @param jobContext the job context object
     * @param location the data location
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public abstract void setInputPath(JobContext jobContext, String location) throws IOException;

    /**
     * Set the predicate filter to be pushed down to the storage driver.
     * @param jobContext the job context object
     * @param predicate the predicate filter, an arbitrary AND/OR filter
     * @return true, if filtering for the specified predicate is supported. Default implementation in
     *               OwlInputStorageDriver  always returns false.
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public boolean setPredicate(JobContext jobContext, String predicate) throws IOException {
        return false;
    }

    /**
     * Set the schema of the data as originally published in Owl. The storage driver might validated that this matches with
     * the schema it has (like Zebra) or it will use this to create a Tuple matching the output schema.
     * @param jobContext the job context object
     * @param schema the schema published in Owl for this data
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public abstract void setOriginalSchema(JobContext jobContext, OwlSchema schema) throws IOException;

    /**
     * Set the consolidated schema for the Tuple data returned by the storage driver. All tuples returned by the RecordReader should
     * have this schema. Nulls should be inserted for columns not present in the data.
     * @param jobContext the job context object
     * @param schema the schema to use as the consolidated schema
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public abstract void setOutputSchema(JobContext jobContext, OwlSchema schema) throws IOException;

    /**
     * Sets the partition key values for the current partition. The storage driver is passed this so that the storage
     * driver can add the partition key values to the output Tuple if the partition key values are not present on disk. 
     * @param jobContext the job context object
     * @param partitionValues the partition values having a map with partition key name as key and the OwlKeyValue as value
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public abstract void setPartitionValues(JobContext jobContext, OwlPartitionValues partitionValues) throws IOException;

}
