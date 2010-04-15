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
package org.apache.hadoop.owl.logical;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.owl.common.LogHandler;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.common.ErrorType;
import org.apache.hadoop.owl.common.OwlUtil;
import org.apache.hadoop.owl.entity.OwlColumnSchemaEntity;
import org.apache.hadoop.owl.entity.OwlSchemaEntity;
import org.apache.hadoop.owl.entity.OwlTableEntity;
import org.apache.hadoop.owl.entity.PartitionKeyEntity;
import org.apache.hadoop.owl.protocol.ColumnType;
import org.apache.hadoop.owl.protocol.OwlKey;
import org.apache.hadoop.owl.schema.*;
import org.apache.hadoop.owl.schema.Schema.ColumnSchema;

public class SchemaUtil {

    public static void validateSchema(String schema) throws OwlException{
        try{
            new Schema(schema);
        }catch( TokenMgrError tmg) { 
            throw new OwlException (ErrorType.ZEBRA_TOKENMRGERROR_EXCEPTION, tmg); 
        }catch (Exception e){
            throw new OwlException (ErrorType.ZEBRA_SCHEMA_EXCEPTION, e); 
        }
    }

    public static String consolidateSchema(String baseSchema, String schemaToAppend) throws OwlException{
        if (schemaToAppend == null){
            return baseSchema;
        }
        if (baseSchema == null){
            return schemaToAppend;
        }
        try {
            // construct zebra schema object of baseSchema
            Schema base = new Schema(baseSchema);
            Schema toAppend = new Schema(schemaToAppend);
            LogHandler.getLogger("server").warn("Orange base schema [" + baseSchema +"], append schema ["+schemaToAppend+"]");
            // union schemas 
            base.unionSchema(toAppend);
            baseSchema = base.toString();
        }  catch( TokenMgrError tmg) { 
            throw new OwlException (ErrorType.ZEBRA_TOKENMRGERROR_EXCEPTION, tmg); 
        }  
        catch (Exception e){
            throw new OwlException ( ErrorType.ZEBRA_SCHEMA_EXCEPTION, e); 
        }
        LogHandler.getLogger("server").warn("Orange result schema [ "+ baseSchema + "]");
        return baseSchema; 
    }


    /**
     * Checks if the schema contains the partitioning keys and the data types match. If ignoreAbsence is false (for create),
     * adds the partition keys to the schema.
     * @param schema the schema to validate
     * @param table the table for the schema
     * @param ignoreAbsence ignore if owl schema does not contain partition key (for publish)
     * @throws OwlException the owl exception
     */
    public static void checkPartitionKeys(OwlSchemaEntity schema, OwlTableEntity table, boolean ignoreAbsence) throws OwlException {

        //TODO: This check can be moved to the backend but that requires the OwlSchemaEntiy to have a field for the OwlTable id

        Map<String, OwlColumnSchemaEntity> columnMap = new HashMap<String, OwlColumnSchemaEntity>();
        for(OwlColumnSchemaEntity column : schema.getOwlColumnSchema() ) {
            columnMap.put(OwlUtil.toLowerCase(column.getName()), column);
        }

        ArrayList<OwlColumnSchemaEntity> columnList = new ArrayList<OwlColumnSchemaEntity>(schema.getOwlColumnSchema());

        boolean invalidDataType = false;

        for(int i = (table.getPartitionKeys().size() - 1);i >= 0; i--) {
            PartitionKeyEntity partitionKey = table.getPartitionKeys().get(i);

            OwlColumnSchemaEntity column = columnMap.get(partitionKey.getName().toLowerCase());

            if( column == null ) {
                if( ignoreAbsence ) {
                    //For publish, we allow schema to not have the partition keys
                    continue;
                }
                else {
                    //Schema does not contain partition key for create operation, add it
                    //The partition keys get added in reverse order so that original order is maintained
                    OwlColumnSchemaEntity newCol = new OwlColumnSchemaEntity();
                    newCol.setName(partitionKey.getName());
                    newCol.setColumnType(OwlKey.DataType.fromCode(partitionKey.getDataType()).name());

                    columnList.add(0, newCol);
                    continue;
                }
            }

            switch(OwlKey.DataType.fromCode(partitionKey.getDataType())) {
            case INT:
                if( ColumnType.getTypeByName(column.getColumnType()) !=  ColumnType.INT ) {
                    invalidDataType = true;
                }
                break;

            case STRING:
                if( ColumnType.getTypeByName(column.getColumnType()) !=  ColumnType.STRING ) {
                    invalidDataType = true;
                }
                break;

            case LONG:
                if( ColumnType.getTypeByName(column.getColumnType()) !=  ColumnType.LONG ) {
                    invalidDataType = true;
                }
                break;
            }

            if( invalidDataType ) {
                //Data type mismatch between schema and table definition
                throw new OwlException(ErrorType.INVALID_SCHEMA_PARTITION_DATATYPE, "Partition key " + partitionKey.getName());
            }

        }

        schema.setOwlColumnSchema(columnList);

    }


}
