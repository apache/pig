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
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.protocol.OwlDatabase;
import org.apache.hadoop.owl.protocol.OwlGlobalKey;
import org.apache.hadoop.owl.protocol.OwlKeyValue;
import org.apache.hadoop.owl.protocol.OwlLoaderInfo;
import org.apache.hadoop.owl.protocol.OwlPartition;
import org.apache.hadoop.owl.protocol.OwlSchema;
import org.apache.hadoop.owl.protocol.OwlTable;
import org.apache.hadoop.owl.protocol.OwlTableName;

/**
 * The main entry point to access OwlBase.
 */
public class OwlDriver {


    /** The client to talk to the Owl server. */
    private OwlClient client;

    /**
     * Create a new OwlDriver object.
     * @param serverUrl the server url
     * @throws OwlException 
     */
    public OwlDriver(String serverUrl) throws OwlException {
        this.client = new OwlClient(serverUrl);
    }

    /**
     * Create the specified Database.
     * @param database the database to create
     * @throws OwlException 
     */
    public void createOwlDatabase(OwlDatabase database) throws OwlException {
        OwlDatabaseHandler handler = new OwlDatabaseHandler(client);
        handler.create(database);
    }

    /**
     * Perform the alter operation specified in the command object, no-op for OB2.
     * @param alterCommand the alter command
     * @throws OwlException 
     */
    public void alterOwlDatabase(AlterOwlDatabaseCommand alterCommand) throws OwlException {
        OwlDatabaseHandler handler = new OwlDatabaseHandler(client);
        handler.alter(alterCommand);
    }

    /**
     * Drop the specified Database.
     * @param database the database to drop
     * @throws OwlException 
     */
    public void dropOwlDatabase(OwlDatabase database) throws OwlException {
        OwlDatabaseHandler handler = new OwlDatabaseHandler(client);
        handler.drop(database);
    }

    /**
     * List databases in the metadata.
     * @return the list of database names
     * @throws OwlException 
     */
    public List<String> showOwlDatabases() throws OwlException {
        OwlDatabaseHandler handler = new OwlDatabaseHandler(client);
        return handler.fetchAll();
    }

    /**
     * Get a database from the metadata.
     * @param databaseName the name of the database to fetch
     * @return the owl database
     * @throws OwlException 
     */
    public OwlDatabase getOwlDatabase(String databaseName) throws OwlException {
        OwlDatabaseHandler handler = new OwlDatabaseHandler(client);
        return handler.fetch(databaseName);   
    }

    /**
     * Create the specified OwlTable.
     * @param table the table to create
     * @throws OwlException 
     */
    public void createOwlTable(OwlTable table) throws OwlException {
        OwlTableHandler handler = new OwlTableHandler(client);
        handler.create(table);
    }

    /**
     * Perform the alter operation specified in the command object.
     * @param alterCommand the alter command
     * @throws OwlException 
     */
    public void alterOwlTable(AlterOwlTableCommand alterCommand) throws OwlException {
        OwlTableHandler handler = new OwlTableHandler(client);
        handler.alter(alterCommand);
    }

    /**
     * Drop the specified OwlTable.
     * @param table the table to drop
     * @throws OwlException 
     */
    public void dropOwlTable(OwlTable table) throws OwlException {
        OwlTableHandler handler = new OwlTableHandler(client);
        handler.drop(table);
    }

    /**
     * List OwlTables in a database.
     * @param databaseName the database name
     * @return the list of table names
     * @throws OwlException 
     */
    public List<OwlTableName> showOwlTables(String databaseName) throws OwlException {
        OwlTableHandler handler = new OwlTableHandler(client);
        return handler.fetchAll(databaseName);    
    }

    /**
     * List OwlTables in a database with name matching the specified name.
     * @param databaseName the database name
     * @param tableNameLike the table name to match using a LIKE comparison, for example "%abc%"
     * @return the list of table names
     * @throws OwlException 
     */
    public List<OwlTableName> showOwlTables(String databaseName, String tableNameLike) throws OwlException {
        OwlTableHandler handler = new OwlTableHandler(client);
        return handler.fetchLike(databaseName, tableNameLike);    
    }

    /**
     * Get an OwlTable from the metadata.
     * @param tableName the name of the table to fetch
     * @return the owl table
     * @throws OwlException 
     */
    public OwlTable getOwlTable(OwlTableName tableName) throws OwlException {
        OwlTableHandler handler = new OwlTableHandler(client);
        return handler.fetch(tableName); 
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
    public void publish(OwlTableName tableName, List<OwlKeyValue> partitionKeyValues, List<OwlKeyValue> propKeyValues,
            String location, OwlSchema schema, OwlLoaderInfo loaderInfo) throws OwlException {
        OwlPartitionHandler handler = new OwlPartitionHandler(client);
        handler.publish(tableName, partitionKeyValues, propKeyValues, location, schema, loaderInfo);
    }

    /**
     * Drop the partition specified by the given partition keys (all sub-partitions are recursively deleted).
     * @param tableName the owl table name
     * @param partitionKeyValues the partition key values
     * @throws OwlException
     */
    public void dropPartition(OwlTableName tableName, List<OwlKeyValue> partitionKeyValues) throws OwlException {
        OwlPartitionHandler handler = new OwlPartitionHandler(client);
        handler.dropPartition(tableName, partitionKeyValues);
    }

    /**
     * Get the list of leaf level partitions matching criteria from metadata
     * @param tableName the name of the table to fetch partitions from
     * @param partitionFilter the partition filter, arbitrary AND/OR string
     * @return List of OwlPartition matching the criteria.
     * @throws OwlException
     */
    public List<OwlPartition> getPartitions(OwlTableName tableName, String partitionFilter) throws OwlException {
        OwlPartitionHandler handler = new OwlPartitionHandler(client);
        return handler.fetchAll(tableName, partitionFilter, null);
    }

    /**
     * Get the list of partitions at the specified partition level matching given filter.
     * @param tableName the name of the table to fetch partitions from
     * @param partitionFilter the partition filter, arbitrary AND/OR string
     * @param partitionKey the partition key name for the partition level to get
     * @return List of OwlPartition matching the criteria.
     * @throws OwlException
     */
    public List<OwlPartition> getPartitions(OwlTableName tableName,
            String partitionFilter, String partitionKey) throws OwlException {
        OwlPartitionHandler handler = new OwlPartitionHandler(client);
        return handler.fetchAll(tableName, partitionFilter, partitionKey);
    }

    /** 
     * Get the list of GlobalKey's from the metadata
     * @return the list of table names
     * @throws OwlException 
     */
    public List<String> showGlobalKeys() throws OwlException {
        OwlGlobalKeyHandler handler = new OwlGlobalKeyHandler( client );
        return handler.fetchAll();
    }

    /** 
     * Get the GlobalKey instance corresponding to given name
     * @param keyName the global key name
     * @return the global key
     * @throws OwlException 
     */
    public OwlGlobalKey getGlobalKey(String keyName) throws OwlException {
        OwlGlobalKeyHandler handler = new OwlGlobalKeyHandler( client );
        return handler.fetch( keyName );
    }

}

