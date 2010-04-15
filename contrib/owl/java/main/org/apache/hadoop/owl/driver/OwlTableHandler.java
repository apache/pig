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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.ArrayList;
import java.util.TimeZone;

import org.apache.hadoop.owl.client.OwlClient;
import org.apache.hadoop.owl.common.ErrorType;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.common.OwlUtil;
import org.apache.hadoop.owl.common.OwlSchemaUtil;
import org.apache.hadoop.owl.protocol.OwlKeyListValue;
import org.apache.hadoop.owl.protocol.OwlPartitionKey;
import org.apache.hadoop.owl.protocol.OwlPropertyKey;
import org.apache.hadoop.owl.protocol.OwlTable;
import org.apache.hadoop.owl.protocol.OwlResultObject;
import org.apache.hadoop.owl.protocol.OwlTableName;
import org.apache.hadoop.owl.protocol.OwlKey.DataType;


/**
 * This class handles the OwlTable object access operations for the OwlDriver.
 */
class OwlTableHandler {

    /** The client to talk to the Owl server. */
    private OwlClient client;

    /**
     * Instantiates a new OwlTable handler.
     * @param client the client
     */
    OwlTableHandler(OwlClient client) {
        this.client = client;
    }

    /**
     * Creates the OwlTable.
     * @param table the table
     * @throws OwlException the owl exception
     */
    void create(OwlTable table) throws OwlException {
        validateValue("Table", table);
        validateTableName(table.getName());

        String command = getTableDefinition(table);
        client.execute(command);
    }

    /**
     * Drops the OwlTable.
     * @param table the table
     * @throws OwlException the owl exception
     */
    void drop(OwlTable table) throws OwlException {
        validateValue("Table", table);
        validateTableName(table.getName());

        String command = "drop owltable " + table.getName().getTableName() +
        " within owldatabase " + table.getName().getDatabaseName();
        client.execute(command);
    }


    /**
     * Alter the OwlTable.
     * @param alterCommand the alter command object
     * @throws OwlException 
     */
    void alter(AlterOwlTableCommand alterCommand) throws OwlException {
        String command = alterCommand.buildCommand();
        client.execute(command);
    }


    /**
     * Fetch all tables in the specified database in the Owl metadata.
     * @param databaseName the database to fetch owl tables from
     * @return the list of table names
     * @throws OwlException the owl exception
     */
    List<OwlTableName> fetchAll(String databaseName) throws OwlException {
        validateStringValue("DatabaseName", databaseName);

        String command = "select owltable objects where owldatabase in (" + databaseName + ")";
        OwlResultObject result = client.execute(command);

        @SuppressWarnings("unchecked") List<OwlTable> tables = (List<OwlTable>) result.getOutput();

        List<OwlTableName> tableNames = new ArrayList<OwlTableName>();
        for(OwlTable table : tables) {
            tableNames.add(table.getName());
        }

        return tableNames;
    }

    /**
     * Fetch all tables in the specified database 
     * in the Owl metadata which satisfy the given LIKE criteria.
     * @param databaseName (optional) the database to fetch owl tables from
     * @param tableNameLike the table name to match
     * @return the list of table names
     * @throws OwlException the owl exception
     */
    public List<OwlTableName> fetchLike(String databaseName, String likeTableNameStr) throws OwlException {
        validateStringValue("TableNameLike", likeTableNameStr);

        String command;
        if(databaseName != null && databaseName.trim().length() > 0) {
            command = "select OwlTable objects within OwlDatabase " + databaseName + " where OwlTable like \"" + likeTableNameStr + "\"";
        }
        else {
            command = "select OwlTable objects where OwlTable like \"" + likeTableNameStr + "\"";
        }
        OwlResultObject result = client.execute(command);
        @SuppressWarnings("unchecked") List<OwlTable> tables = (List<OwlTable>) result.getOutput();
        List<OwlTableName> tableNames = new ArrayList<OwlTableName>();
        for(OwlTable table : tables) {
            tableNames.add(table.getName());
        }
        return tableNames;
    }

    /**
     * Fetch the OwlTable corresponding to given name.
     * @param tableName the table name
     * @return the owl table object
     * @throws OwlException the owl exception
     */
    OwlTable fetch(OwlTableName tableName) throws OwlException {
        validateTableName(tableName);

        String command = "describe owltable " + tableName.getTableName() + " within owldatabase " + tableName.getDatabaseName();
        OwlResultObject result = client.execute(command);

        OwlTable table = (OwlTable) result.getOutput().get(0);
        return table;
    }


    /**
     * Gets the table definition as a Owl DDL string.
     * @param table the table
     * @return the table definition
     * @throws OwlException 
     */
    @SuppressWarnings("boxing")
    static String getTableDefinition(OwlTable table) throws OwlException {
        validateValue("OwlTable", table);
        validateTableName(table.getName());

        StringBuffer command = new StringBuffer("create owltable type basic " +
                table.getName().getTableName() + " within owldatabase " + table.getName().getDatabaseName());

        //Add all partition keys to command string
        OwlPartitionKey[] partitionKeys = getPartitionKeys(table);
        for(int i = 0;i < partitionKeys.length;i++) {
            command.append(getPartitionKeyDefinition(partitionKeys[i]));
        }

        //Add all property keys to command string
        if( table.getPropertyKeys() != null ) {
            for(OwlPropertyKey key : table.getPropertyKeys()) {
                command.append(getPropertyKeyDefinition(key));
            }
        }

        //Add schema description if any
        if( table.getSchema() != null ) {
            command.append(" schema \"" + OwlSchemaUtil.getSchemaString(table.getSchema()) + "\"");
        }else{
            throw new OwlException(ErrorType.NO_SCHEMA_PROVIDED);
        }

        return command.toString();
    }

    /**
     * Gets the partition keys for the table as an sorted array and also validate the partition definition.
     * @param table the table
     * @return the partition keys
     * @throws OwlException the owl exception
     */
    static OwlPartitionKey[] getPartitionKeys(OwlTable table) throws OwlException {

        if( table.getPartitionKeys() == null ) {
            return new OwlPartitionKey[0];
        }

        OwlPartitionKey[] partitionKeys = new OwlPartitionKey[table.getPartitionKeys().size()];

        for(OwlPartitionKey key : table.getPartitionKeys()) {
            if( key.getLevel() < 1 || key.getLevel() > table.getPartitionKeys().size() ) {
                throw new OwlException(ErrorType.ERROR_INVALID_PARTITION_LEVEL, "Level " + key.getLevel());
            }

            if( partitionKeys[key.getLevel() - 1] != null) {
                throw new OwlException(ErrorType.ERROR_INVALID_PARTITION_LEVEL, "Duplicate level " + key.getLevel());
            }

            partitionKeys[key.getLevel() - 1] = key;
        }

        return partitionKeys;
    }

    /**
     * Gets the property key definition as a string.
     * @param key the key
     * @return the property key definition
     * @throws OwlException 
     */
    static String getPropertyKeyDefinition(OwlPropertyKey key) throws OwlException {
        StringBuffer buffer = new StringBuffer();
        validateStringValue("Property Key Name", key.getName());

        String typeName = key.getDataType().name();
        if( key.getDataType() == DataType.INT ) {
            typeName = "INTEGER";
        }
        buffer.append(" define property key "  + key.getName() + " : " + typeName);
        return buffer.toString();
    }

    /**
     * Gets the partition key definition as a string.
     * @param key the key
     * @return the partition key definition
     * @throws OwlException 
     */
    static String getPartitionKeyDefinition(OwlPartitionKey key) throws OwlException {
        StringBuffer buffer = new StringBuffer();

        if( key.getPartitioningType() == OwlPartitionKey.PartitioningType.LIST ) {
            //List partitioning
            buffer.append(getListPartitionDefinition(key));
        } else if( key.getPartitioningType() == OwlPartitionKey.PartitioningType.INTERVAL ) {
            //Interval partitioning
            buffer.append(getIntervalPartitionDefinition(key));
        }

        return buffer.toString();
    }

    /**
     * Gets the list partition definition as a string.
     * @param key the key
     * @return the list partition definition
     * @throws OwlException 
     */
    private static Object getListPartitionDefinition(OwlPartitionKey key) throws OwlException {

        StringBuffer buffer = new StringBuffer();
        buffer.append(" partitioned by LIST");
        validateStringValue("Partition Key Name", key.getName());

        if( key.getListValues() != null && key.getListValues().size() != 0 ) {
            //Bounded list, add the valid values to the buffer
            buffer.append(" (");

            for(int i = 0; i < key.getListValues().size();i++) {
                OwlKeyListValue value = key.getListValues().get(i);

                if( value.getIntValue() != null ) {
                    buffer.append(String.valueOf(value.getIntValue()));
                } else if( value.getStringValue() != null ) {
                    buffer.append("\"" + value.getStringValue() + "\"");
                }

                if( i != key.getListValues().size() - 1 ) {
                    buffer.append(", ");
                }
            }

            buffer.append(")");
        }

        String typeName = key.getDataType().name();
        if( key.getDataType() == DataType.INT ) {
            typeName = "INTEGER";
        }

        buffer.append(" with partition key "  + key.getName() + " : " + typeName);
        return buffer.toString();
    }

    /**
     * Gets the interval partition definition as a string.
     * @param key the key
     * @return the interval partition definition
     * @throws OwlException 
     */
    private static Object getIntervalPartitionDefinition(OwlPartitionKey key) throws OwlException {
        StringBuffer buffer = new StringBuffer();
        validateStringValue("Partition Key Name", key.getName());

        DateFormat fmt = new SimpleDateFormat(OwlUtil.DATE_FORMAT);
        fmt.setTimeZone(TimeZone.getTimeZone("GMT"));
        String dateString = fmt.format(key.getIntervalStart());

        buffer.append(" partitioned by INTERVAL");
        buffer.append(" (\"" + dateString + "\", " + key.getIntervalFrequency() + " " + key.getIntervalFrequencyUnit().name() + ")"); 
        buffer.append(" with partition key "  + key.getName());

        return buffer.toString();
    }

    /**
     * Validate whether specified field is null.
     * @param fieldName the field name, appended to exception thrown
     * @param fieldValue the field value to check
     * @throws OwlException the owl exception
     */
    static void validateValue(String fieldName, Object fieldValue) throws OwlException {
        if( fieldValue == null ) {
            throw new OwlException(ErrorType.INVALID_FIELD_VALUE,
                    "Null value for field <" + fieldName + ">");
        }
    }

    /**
     * Validate whether specified string value is null or empty.
     * @param fieldName the field name, appended to exception thrown
     * @param fieldValue the string value to check
     * @throws OwlException the owl exception
     */
    static void validateStringValue(String fieldName, String fieldValue) throws OwlException {
        if( fieldValue == null || fieldValue.trim().length() == 0 ) {
            throw new OwlException(ErrorType.INVALID_FIELD_VALUE, 
                    "Field <" + fieldName + "> Value <" + fieldValue + ">");
        }
    }

    /**
     * Check whether specified table name is null or empty.
     * @param tableName the table name
     * @throws OwlException the owl exception
     */
    static void validateTableName(OwlTableName tableName) throws OwlException {
        validateValue("OwlTableName", tableName);
        validateStringValue("Database Name", tableName.getDatabaseName());
        validateStringValue("Table Name", tableName.getTableName());
    }

}
