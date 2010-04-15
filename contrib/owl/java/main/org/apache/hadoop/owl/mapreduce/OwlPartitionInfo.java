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

import java.io.Serializable;

import org.apache.hadoop.owl.protocol.OwlLoaderInfo;
import org.apache.hadoop.owl.protocol.OwlSchema;

/** The Class used to serialize the partition information read from the Owl server */
class OwlPartitionInfo implements Serializable {

    /** The serialization version */
    private static final long serialVersionUID = 1L;     

    /** The partition schema. */
    private OwlSchema partitionSchema;

    /** The information about the to use. */
    private OwlLoaderInfo loaderInfo;

    /** The data location. */
    private String location;

    /** The map of partition key names and their values. */
    private OwlPartitionValues partitionValues;  

    /**
     * Instantiates a new owl partition info.
     * @param partitionSchema the partition schema
     * @param loaderInfo the loader info
     * @param location the location
     */
    public OwlPartitionInfo(OwlSchema partitionSchema,
            OwlLoaderInfo loaderInfo, String location) {
        this.partitionSchema = partitionSchema;
        this.loaderInfo = loaderInfo;
        this.location = location;
    }

    /**
     * Gets the value of partitionSchema.
     * @return the partitionSchema
     */
    public OwlSchema getPartitionSchema() {
        return partitionSchema;
    }

    /**
     * Gets the value of loaderInfo.
     * @return the loaderInfo
     */
    public OwlLoaderInfo getLoaderInfo() {
        return loaderInfo;
    }

    /**
     * Gets the value of location.
     * @return the location
     */
    public String getLocation() {
        return location;
    }

    /**
     * Sets the partition values.
     * @param partitionValues the new partition values
     */
    public void setPartitionValues(OwlPartitionValues partitionValues) {
        this.partitionValues = partitionValues;
    }

    /**
     * Gets the partition values.
     * @return the partition values
     */
    public OwlPartitionValues getPartitionValues() {
        return partitionValues;
    }
}
