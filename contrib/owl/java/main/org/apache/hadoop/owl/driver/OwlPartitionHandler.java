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
package org.apache.hadoop.owl.driver;

import java.util.List;

import org.apache.hadoop.owl.client.OwlClient;
import org.apache.hadoop.owl.common.ErrorType;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.common.OwlSchemaUtil;
import org.apache.hadoop.owl.protocol.OwlKeyValue;
import org.apache.hadoop.owl.protocol.OwlLoaderInfo;
import org.apache.hadoop.owl.protocol.OwlPartition;
import org.apache.hadoop.owl.protocol.OwlResultObject;
import org.apache.hadoop.owl.protocol.OwlSchema;
import org.apache.hadoop.owl.protocol.OwlTableName;

/**
 * This class handles the Partition object access operations for the OwlDriver.
 */
class OwlPartitionHandler {

    /** The client to talk to the Owl server. */
    private OwlClient client;

    /**
     * Instantiates a new OwlPartition handler.
     * @param client the client
     */
    OwlPartitionHandler(OwlClient client) {
        this.client = client;
    }

    /**
     * Add partition (publish) with specified values to the OwlTable.
     * @param tableName the owl table name
     * @param partitionKeyValues the partition key values
     * @param propKeyValues the property key values
     * @param location the data location
     * @param schema the schema
     * @param loaderInfo the loader info
     * @throws OwlException
     */
    void publish(OwlTableName tableName, List<OwlKeyValue> partitionKeyValues, List<OwlKeyValue> propKeyValues,
            String location, OwlSchema schema, OwlLoaderInfo loaderInfo) throws OwlException {
        OwlTableHandler.validateTableName(tableName);
        OwlTableHandler.validateStringValue("Location", location);
        OwlTableHandler.validateValue("LoaderInfo", loaderInfo);
        OwlTableHandler.validateStringValue("StorageDriver", loaderInfo.getInputDriverClass());

        StringBuffer buffer = new StringBuffer();
        buffer.append("publish dataelement to owltable " + tableName.getTableName() + " within owldatabase " + tableName.getDatabaseName());

        //Append partition key info
        if( partitionKeyValues != null && partitionKeyValues.size() > 0 ) {
            buffer.append(" partition (");
            buffer.append( AlterOwlTableCommand.getKeyValueDefinition(partitionKeyValues));
            buffer.append(" )");
        }

        //Append property key info
        if( propKeyValues != null && propKeyValues.size() > 0 ) {
            buffer.append(" property (");
            buffer.append( AlterOwlTableCommand.getKeyValueDefinition(partitionKeyValues));
            buffer.append(" )");
        }

        //Append the schema if given
        if( schema != null ) {
            buffer.append(" schema \"" + OwlSchemaUtil.getSchemaString(schema) + "\"");
        }else{
            throw new OwlException(ErrorType.INVALID_OWL_SCHEMA, "A partition level schema must be provided when publishing.");
        }

        //Append the data location
        buffer.append(" delocation \"" + location + "\"");

        //Append the loader info
        buffer.append(" loader \"" + loaderInfo.toString() + "\"");

        client.execute(buffer.toString());
    }

    /**
     * Drop the specified partition.
     * @param tableName the owl table name
     * @param partitionKeyValues the partition key values
     * @throws OwlException
     */
    void dropPartition(OwlTableName tableName, List<OwlKeyValue> partitionKeyValues) throws OwlException {
        OwlTableHandler.validateTableName(tableName);

        StringBuffer buffer = new StringBuffer();
        buffer.append("alter owltable " + tableName.getTableName() + " within owldatabase " + tableName.getDatabaseName());  
        buffer.append(" drop");

        if(partitionKeyValues != null && partitionKeyValues.size() > 0) {
            buffer.append(" partition (");
            buffer.append(AlterOwlTableCommand.getKeyValueDefinition(partitionKeyValues));
            buffer.append(" )");
        }

        client.execute(buffer.toString());
    }

    /**
     * Fetch all partitions in the the OwlTable which match the specified filter.
     * @param owlTableName the table to fetch partitions from
     * @param partitionFilter the filter string
     * @param partitionKeyAtLevel the partition level to fetch, null indicates leaf level
     * @return the list of partitions
     * @throws OwlException the owl exception
     */
    List<OwlPartition> fetchAll(OwlTableName owlTableName, String partitionFilter, String partitionKeyAtLevel) throws OwlException {
        OwlTableHandler.validateTableName(owlTableName);

        StringBuffer command = new StringBuffer("select partition objects from owltable " + owlTableName.getTableName() 
                + " within owldatabase " + owlTableName.getDatabaseName());

        if( partitionFilter != null && partitionFilter.trim().length() > 0 ) {
            command.append(" with partition (" + partitionFilter + ")");
        }
        if (partitionKeyAtLevel != null && partitionKeyAtLevel.trim().length() > 0){
            command.append(" at partition level (" + partitionKeyAtLevel + ")");
        }

        OwlResultObject result = client.execute(command.toString());

        @SuppressWarnings("unchecked") List<OwlPartition> partitions = (List<OwlPartition>) result.getOutput();
        return partitions;
    }
}
