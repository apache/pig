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

import java.util.EnumSet;
import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.owl.mapreduce.OwlInputFormat.OwlOperation;
import org.apache.hadoop.owl.protocol.OwlSchema;
import org.apache.hadoop.owl.protocol.OwlTableName;

/** The class used to serialize and store the information read from the Owl server */ 
class OwlJobInfo implements Serializable{

    /** The serialization version */
    private static final long serialVersionUID = 1L;     

    /** The table name. */
    private OwlTableName tableName;

    /** The table schema. */
    private OwlSchema tableSchema;

    /** The list of partitions matching the filter. */
    private List<OwlPartitionInfo> partitions;

    /** The enum set of supported OwlOperations */    
    private EnumSet<OwlOperation> supportedFeatures;

    /**
     * Instantiates a new owl job info.
     * @param tableName the table name
     * @param tableSchema the table schema
     * @param partitions the partitions
     */
    public OwlJobInfo(OwlTableName tableName, OwlSchema tableSchema,
            List<OwlPartitionInfo> partitions) {
        this.tableName = tableName;
        this.tableSchema = tableSchema;
        this.partitions = partitions;
    }

    /**
     * Gets the value of tableName
     * @return the tableName
     */
    public OwlTableName getTableName() {
        return tableName;
    }

    /**
     * Gets the value of tableSchema
     * @return the tableSchema
     */
    public OwlSchema getTableSchema() {
        return tableSchema;
    }

    /**
     * Gets the value of partitions
     * @return the partitions
     */
    public List<OwlPartitionInfo> getPartitions() {
        return partitions;
    }

    /**
     * Sets the list of supported features.
     * @param supportedFeatures the new supported features
     */
    public void setSupportedFeatures(EnumSet<OwlOperation> supportedFeatures) {
        this.supportedFeatures = supportedFeatures;
    }

    /**
     * Gets the supported features list.
     * @return the supported features
     */
    public EnumSet<OwlOperation> getSupportedFeatures() {
        return supportedFeatures;
    }
}
