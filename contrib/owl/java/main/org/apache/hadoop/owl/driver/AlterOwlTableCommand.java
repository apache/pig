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

import org.apache.hadoop.owl.common.ErrorType;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.protocol.OwlKeyValue;
import org.apache.hadoop.owl.protocol.OwlPropertyKey;
import org.apache.hadoop.owl.protocol.OwlSchema;
import org.apache.hadoop.owl.protocol.OwlTableName;
import org.apache.hadoop.owl.protocol.OwlKey.DataType;

/** Alter functions for OwlTable object*/
public abstract class AlterOwlTableCommand {
    /** The table for which this alter is to be done. */
    OwlTableName tableName;

    /**
     * Create a alter table command instance for the specified OwlTable.
     * @param tableName the table name
     * @throws OwlException 
     */
    protected AlterOwlTableCommand(OwlTableName tableName) {
        this.tableName = tableName;
    }

    private static void validateTableName(OwlTableName tableName) throws OwlException {
        if( tableName == null ) 
            throw new OwlException( ErrorType.INVALID_TABLE_NAME, "<null>" );
    }

    /**
     * Add new property key to the OwlTable (which applies to all partitions)
     * @param tableName the table name
     * @param propertyKeys the property keys
     * @throws OwlException
     */
    public static AlterOwlTableCommand createForAddPropertyKey(OwlTableName tableName, 
            List<OwlPropertyKey> propertyKeys) throws OwlException {
        validateTableName( tableName );

        if( propertyKeys == null || propertyKeys.size() == 0 )
            throw new OwlException( ErrorType.MISSING_PROPERTY_KEY, "AddPropertyKeyValue" );

        for( OwlPropertyKey kv : propertyKeys ) {
            DataType type = kv.getDataType();
            if( type == DataType.LONG )
                throw new OwlException( ErrorType.ERROR_INVALID_KEY_DATATYPE, type.toString() );
        }

        return new AlterOwlTableCommandAddPropertyKey( tableName, propertyKeys );
    }

    /**
     * Update the property key values for specified partition. If partition key values are not provided,
     * then the operation is applied to the table.
     * @param tableName the table name
     * @param partitionKeyValues the partition key values
     * @param propertyKeyValues the property key values
     * @throws OwlException
     */
    public static AlterOwlTableCommand createForUpdatePropertyKeyValue(OwlTableName tableName,
            List<OwlKeyValue> partitionKeyValues, List<OwlKeyValue> propertyKeyValues) throws OwlException {
        validateTableName( tableName );

        if( propertyKeyValues == null || propertyKeyValues.size() == 0 )
            throw new OwlException( ErrorType.MISSING_PROPERTY_KEY, "UpdatePropertyKeyValue" );

        return new AlterOwlTableCommandUpdatePropertyKeyValue( tableName, partitionKeyValues, propertyKeyValues );
    }

    /**
     * Delete the specified property key values for specified partition. If partition key values are not provided,
     * then the operation is applied to the table.
     * 
     * (This is not for dropping the key itself.)
     * @param tableName the table name
     * @param partitionKeyValues the partition key values
     * @param propertyKeys the property key value list
     * @throws OwlException
     */
    public static AlterOwlTableCommand createForDeletePropertyKeyValue(OwlTableName tableName,
            List<OwlKeyValue> partitionKeyValues, List<OwlKeyValue> propertyKeys) throws OwlException {
        validateTableName( tableName );

        if( propertyKeys == null || propertyKeys.size() == 0 )
            throw new OwlException( ErrorType.MISSING_PROPERTY_KEY, "DeletePropertyKeyValue" );

        return new AlterOwlTableCommandDeletePropertyKeyValue( tableName, partitionKeyValues, propertyKeys );
    }

    /**
     * Alter the OwlTable level schema to the given schema. This can be used for updating the
     * table level schema to add/drop columns and to change the ordering of columns. The data 
     * types of existing columns cannot be changed.
     * @param tableName the table name
     * @param schema the new schema to set for the OwlTable
     * @throws OwlException
     */
    public static AlterOwlTableCommand createForSetSchema(OwlTableName tableName,
            OwlSchema schema) throws OwlException {
        validateTableName( tableName );

        return new AlterOwlTableCommandSetSchema( tableName, schema);
    }

    /**
     * Gets the command corresponding to the alter being performed .
     * @return the command
     * @throws OwlException 
     */
    abstract String buildCommand() throws OwlException;

    /**
     * Gets the DDL string corresponding to the given key value list.
     * @param keyValues the key values
     * @return the key value definition
     */
    static String getKeyValueDefinition(List<OwlKeyValue> keyValues) {
        if( keyValues == null || keyValues.size() == 0 ) {
            return "";
        }

        StringBuilder buffer = new StringBuilder();

        for(int i = 0;i < keyValues.size();i++) {
            OwlKeyValue keyValue = keyValues.get(i);
            buffer.append(keyValue.getKeyName());
            buffer.append(" = ");

            if( keyValue.getIntValue() != null ) {
                buffer.append(String.valueOf(keyValue.getIntValue()));
            } else {
                buffer.append("\"" + keyValue.getStringValue() + "\"");
            }

            if( i != (keyValues.size() - 1) ) {
                buffer.append(", ");
            }
        }

        return buffer.toString();
    }
}

class AlterOwlTableCommandAddPropertyKey extends AlterOwlTableCommand {
    private List<OwlPropertyKey> propertyKeys;

    AlterOwlTableCommandAddPropertyKey(OwlTableName tableName, List<OwlPropertyKey> propertyKeys) throws OwlException {
        super( tableName );
        this.propertyKeys = propertyKeys;
    }

    @Override
    public String buildCommand() throws OwlException {
        StringBuilder buffer = new StringBuilder();
        buffer.append("alter owltable ");
        buffer.append(tableName.getTableName() + " within owldatabase " + tableName.getDatabaseName() );
        buffer.append( " at partition level (1) " );
        buffer.append(" add propertykey (" );

        for( int i = 0; i < propertyKeys.size(); i++ ) {
            if( i != 0 )
                buffer.append( ", " );
            OwlPropertyKey kvPair = propertyKeys.get( i );
            buffer.append( kvPair.getName() + ":" );
            DataType dataType = kvPair.getDataType();
            if( dataType == DataType.STRING ) {
                buffer.append(DataType.STRING.name());
            } else if( dataType == DataType.INT ) {
                buffer.append( DataType.INT.name() );
            } 
        }

        buffer.append( " )" );

        return buffer.toString();
    }
}

class AlterOwlTableCommandUpdatePropertyKeyValue extends AlterOwlTableCommand {
    private List<OwlKeyValue> partitionKeyValues;
    private List<OwlKeyValue> propertyKeyValues;

    AlterOwlTableCommandUpdatePropertyKeyValue(OwlTableName tableName,
            List<OwlKeyValue> partitionKeyValues, List<OwlKeyValue> propertyKeyValues ) {
        super( tableName );
        this.partitionKeyValues = partitionKeyValues;
        this.propertyKeyValues = propertyKeyValues;
    }

    @Override
    String buildCommand() throws OwlException {
        StringBuilder buffer = new StringBuilder();
        buffer.append("alter owltable ");

        buffer.append( tableName.getTableName() + " within owldatabase " + tableName.getDatabaseName() );

        // Add partition info if given.
        if( partitionKeyValues != null && partitionKeyValues.size() > 0 ) {
            buffer.append( " with partition( " );
            buffer.append( AlterOwlTableCommand.getKeyValueDefinition( partitionKeyValues ) );
            buffer.append( " )"  );
        }

        buffer.append( " modify property ( " );
        buffer.append( AlterOwlTableCommand.getKeyValueDefinition( propertyKeyValues ) );
        buffer.append( " )" );

        return buffer.toString();
    }

}

class AlterOwlTableCommandDeletePropertyKeyValue extends AlterOwlTableCommand {
    private List<OwlKeyValue> partitionKeyValues;
    private List<OwlKeyValue> propertyKeyValues;

    AlterOwlTableCommandDeletePropertyKeyValue(OwlTableName tableName,
            List<OwlKeyValue> partitionKeyValues, List<OwlKeyValue> propertyKeys) {
        super(tableName);
        this.partitionKeyValues = partitionKeyValues;
        this.propertyKeyValues = propertyKeys;
    }

    @Override
    String buildCommand() throws OwlException {
        StringBuilder buffer = new StringBuilder();
        buffer.append("alter owltable ");
        buffer.append( tableName.getTableName() + " within owldatabase " + tableName.getDatabaseName() );

        // Add partition info if given.
        if( partitionKeyValues != null && partitionKeyValues.size() > 0 ) {
            buffer.append( " with partition( " );
            buffer.append( AlterOwlTableCommand.getKeyValueDefinition( partitionKeyValues ) );
            buffer.append( " )"  );
        }

        buffer.append( " drop property ( " );
        buffer.append( AlterOwlTableCommand.getKeyValueDefinition( propertyKeyValues ) );
        buffer.append( " )" );

        return buffer.toString();
    }

}

class AlterOwlTableCommandSetSchema extends AlterOwlTableCommand {
    private OwlSchema schema;

    AlterOwlTableCommandSetSchema(OwlTableName tableName,
            OwlSchema schema) {
        super(tableName);
        this.schema = schema;
    }

    @Override
    String buildCommand() throws OwlException {
        StringBuilder buffer = new StringBuilder();
        buffer.append("alter owltable ");
        buffer.append( tableName.getTableName() + " within owldatabase " + tableName.getDatabaseName() );

        buffer.append( " set schema \"" );
        buffer.append(schema.getSchemaString());
        buffer.append( "\"" );

        return buffer.toString();
    }
}

