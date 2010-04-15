/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.owl.logical;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.owl.backend.BackendUtil;
import org.apache.hadoop.owl.backend.IntervalUtil;
import org.apache.hadoop.owl.backend.OwlBackend;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.entity.DatabaseEntity;
import org.apache.hadoop.owl.entity.DataElementEntity;
import org.apache.hadoop.owl.entity.GlobalKeyEntity;
import org.apache.hadoop.owl.entity.KeyBaseEntity;
import org.apache.hadoop.owl.entity.KeyListValueEntity;
import org.apache.hadoop.owl.entity.KeyValueEntity;
import org.apache.hadoop.owl.entity.OwlColumnSchemaEntity;
import org.apache.hadoop.owl.entity.OwlSchemaEntity;
import org.apache.hadoop.owl.entity.OwlTableEntity;
import org.apache.hadoop.owl.entity.OwlTableKeyValueEntity;
import org.apache.hadoop.owl.entity.PartitionEntity;
import org.apache.hadoop.owl.entity.PartitionKeyEntity;
import org.apache.hadoop.owl.entity.PropertyKeyEntity;
import org.apache.hadoop.owl.protocol.ColumnType;
import org.apache.hadoop.owl.protocol.OwlColumnSchema;
import org.apache.hadoop.owl.protocol.OwlDatabase;
import org.apache.hadoop.owl.protocol.OwlDataElement;
import org.apache.hadoop.owl.protocol.OwlGlobalKey;
import org.apache.hadoop.owl.protocol.OwlKey;
import org.apache.hadoop.owl.protocol.OwlKeyListValue;
import org.apache.hadoop.owl.protocol.OwlKeyValue;
import org.apache.hadoop.owl.protocol.OwlLoaderInfo;
import org.apache.hadoop.owl.protocol.OwlPartition;
import org.apache.hadoop.owl.protocol.OwlPartitionKey;
import org.apache.hadoop.owl.protocol.OwlPartitionProperty;
import org.apache.hadoop.owl.protocol.OwlPropertyKey;
import org.apache.hadoop.owl.protocol.OwlSchema;
import org.apache.hadoop.owl.protocol.OwlTable;
import org.apache.hadoop.owl.protocol.OwlTableName;
import org.apache.hadoop.owl.protocol.OwlKey.KeyType;

/** Utility functions for converting entity objects to bean objects. Static convert functions are the main entry points*/
public class ConvertEntity {


    /**
     * Convert a global key entity to a global key bean.
     * @param globalKey the global key entity to convert
     * @return the global key bean
     * @throws OwlException
     */
    public static OwlGlobalKey convert(GlobalKeyEntity globalKey) throws OwlException {
        return new OwlGlobalKey(globalKey.getName(), OwlKey.DataType.fromCode(globalKey.getDataType()));
    }


    /**
     * Convert a database entity to a database bean.
     * 
     * @param database
     *            the database entity to convert
     * 
     * @return the owl database bean
     */
    public static OwlDatabase convert(DatabaseEntity database) {
        return new OwlDatabase(database.getOwner(), database.getCreatedAt(),
                database.getLastModifiedAt(), database.getName(), database.getLocation());
    }

    /**
     * Convert a Owltable entity to a OwlTable bean.
     * 
     * @param table
     *            the table entity to convert
     * @param databaseName
     *            the database name
     * @param globalKeys
     *            the global keys
     * 
     * @return the owl table bean
     * 
     * @throws OwlException
     *             the owl exception
     */
    @SuppressWarnings("boxing")
    public static OwlTable convert(OwlTableEntity table, String databaseName, List<GlobalKeyEntity> globalKeys, OwlBackend backend) throws OwlException {

        List<OwlPropertyKey> propKeys = new ArrayList<OwlPropertyKey>();
        List<OwlPartitionKey> partKeys = new ArrayList<OwlPartitionKey>();
        List<OwlKeyValue> keyValues = new ArrayList<OwlKeyValue>();


        //Create property key list
        if( table.getPropertyKeys() != null ) {
            for(PropertyKeyEntity propKey : table.getPropertyKeys() ) {
                OwlPropertyKey pk = new OwlPropertyKey(propKey.getName(),
                        OwlKey.DataType.fromCode(propKey.getDataType()));

                propKeys.add(pk);
            }
        }

        //Create partition key list
        if( table.getPartitionKeys() != null ) {
            for(PartitionKeyEntity partKey : table.getPartitionKeys() ) {

                List<OwlKeyListValue> listValues = new ArrayList<OwlKeyListValue>();
                if( partKey.getListValues() != null ) {
                    for(KeyListValueEntity listValue : partKey.getListValues() ) {
                        listValues.add(new OwlKeyListValue(listValue.getIntValue(), listValue.getStringValue()));
                    }
                }

                OwlPartitionKey pk = new OwlPartitionKey(partKey.getName(),
                        OwlKey.DataType.fromCode(partKey.getDataType()),
                        partKey.getPartitionLevel(),
                        OwlPartitionKey.PartitioningType.fromCode(partKey.getPartitioningType()),
                        partKey.getIntervalStart(),
                        partKey.getIntervalFrequency(),
                        (partKey.getIntervalFrequencyUnit() == null) ? null :
                            OwlPartitionKey.IntervalFrequencyUnit.fromCode(partKey.getIntervalFrequencyUnit()),
                            listValues);

                partKeys.add(pk);
            }
        }

        //Create key value list
        if( table.getKeyValues() != null ) {
            for(OwlTableKeyValueEntity kv : table.getKeyValues() ) {
                KeyBaseEntity key = BackendUtil.getKeyForValue(kv, table, globalKeys);

                OwlKeyValue value = new OwlKeyValue(key.getName(),
                        BackendUtil.getKeyType(kv),
                        OwlKey.DataType.fromCode(key.getDataType()),
                        kv.getIntValue(),
                        kv.getStringValue());

                keyValues.add(value);
            }
        }

        // get OwlSchema object
        // find the OwlSchemaEntity
        OwlSchema schemaBean = null;
        if( table.getSchemaId() != null ) {
            List<OwlSchemaEntity>owe = backend.find(OwlSchemaEntity.class, "id = " + table.getSchemaId());
            schemaBean = convert(owe.get(0), backend) ;
        }

        //Create the OwlTable bean 
        return new OwlTable(table.getOwner(), table.getCreatedAt(), table.getLastModifiedAt(),
                new OwlTableName(databaseName, table.getName()),
                propKeys, partKeys, keyValues,
                schemaBean);
    }

    /** Convert OwlSchemaEntity to OwlSchema
     */
    public static OwlSchema convert (OwlSchemaEntity se, OwlBackend backend) throws OwlException{
        OwlSchema schemaBean = new OwlSchema();
        ArrayList<OwlColumnSchema> columnBeanList = new ArrayList<OwlColumnSchema>();

        for(OwlColumnSchemaEntity cse : se.getOwlColumnSchema()) {
            columnBeanList.add(convert(cse, backend));
        }

        schemaBean.setColumnSchema(columnBeanList);
        return schemaBean;
    }

    /** Convert OwlColumnSchemaEntity to OwlColumnSchema
     */
    private static OwlColumnSchema convert (OwlColumnSchemaEntity columnEntity, OwlBackend backend) throws OwlException {

        OwlColumnSchema columnBean = new OwlColumnSchema();
        columnBean.setName(columnEntity.getName());
        columnBean.setType(ColumnType.getTypeByName(columnEntity.getColumnType()));
        columnBean.setColumnNumber(columnEntity.getColumnNumber());

        //Handle nested schemas
        if( columnEntity.getSubSchema() != null && columnBean.getSchema() == null) {
            OwlSchema os = convert(columnEntity.getSubSchema(), backend);
            columnBean.setSchema(os);
        }

        return columnBean;
    }

    /**
     * Convert the data element entity to a bean.
     * 
     * @param dataElement
     *            the data element entity to convert
     * @param parentPartition
     *            the parent partition, null if non partitioned owltable
     * @param parentTable
     *            the parent table
     * @param globalKeys
     *            the global keys
     * 
     * @return the data element bean
     * 
     * @throws OwlException
     *             the owl exception
     */
    public static OwlDataElement convert(DataElementEntity dataElement, PartitionEntity parentPartition,
            OwlTableEntity parentTable, List<GlobalKeyEntity> globalKeys, OwlBackend backend) throws OwlException {

        List<OwlKeyValue> keyValues = new ArrayList<OwlKeyValue>();

        if( parentPartition != null && parentPartition.getKeyValues() != null ) {

            //Create key value list
            for(KeyValueEntity kv : parentPartition.getKeyValues() ) {
                KeyBaseEntity key = BackendUtil.getKeyForValue(kv, parentTable, globalKeys);

                OwlKeyValue value = new OwlKeyValue(key.getName(),
                        BackendUtil.getKeyType(kv),
                        OwlKey.DataType.fromCode(key.getDataType()),
                        kv.getIntValue(),
                        kv.getStringValue());

                if( key.getDataType() == OwlKey.DataType.LONG.getCode() ) {
                    //For LONG datatype, the integer value in the entity is converted to a long value in the bean
                    value.setIntValue(null);
                    value.setLongValue(
                            IntervalUtil.getTimeFromOffset((PartitionKeyEntity) key, kv.getIntValue().intValue()));
                }

                keyValues.add(value);
            }
        }

        // get OwlSchema object
        // find the OwlSchemaEntity
        OwlSchema schemaBean = null;
        if( dataElement.getSchemaId() != null ) {
            List<OwlSchemaEntity>owe = backend.find(OwlSchemaEntity.class, "id = " + dataElement.getSchemaId());
            schemaBean = convert(owe.get(0), backend) ;
        }

        //Create the DataElement bean
        return new OwlDataElement(dataElement.getOwner(), dataElement.getCreatedAt(),
                dataElement.getLastModifiedAt(), dataElement.getLocation(),keyValues, new OwlLoaderInfo(dataElement.getLoader()),
                schemaBean);
    }


    /**
     * Convert partition to the OwlPartitionProperty bean. The partition keys values are not
     * written into the return list, only property and global keys values are written.
     * 
     * @param partition
     *            the partition to convert
     * @param parentTable
     *            the parent table
     * @param globalKeys
     *            the global keys
     * 
     * @return the partition property keys bean list
     * 
     * @throws OwlException
     *             the owl exception
     */
    public static OwlPartitionProperty convert(PartitionEntity partition,
            OwlTableEntity parentTable, List<GlobalKeyEntity> globalKeys) throws OwlException {

        List<OwlKeyValue> keyValues = new ArrayList<OwlKeyValue>();

        if( partition.getKeyValues() != null ) {

            //Create key value list
            for(KeyValueEntity kv : partition.getKeyValues() ) {

                KeyType keyType = BackendUtil.getKeyType(kv);
                if( keyType == KeyType.PROPERTY || keyType == KeyType.GLOBAL ) {
                    //If property or global key, then add to return list
                    KeyBaseEntity key = BackendUtil.getKeyForValue(kv, parentTable, globalKeys);

                    OwlKeyValue value = new OwlKeyValue(key.getName(),
                            BackendUtil.getKeyType(kv),
                            OwlKey.DataType.fromCode(key.getDataType()),
                            kv.getIntValue(),
                            kv.getStringValue());

                    keyValues.add(value);
                }
            }
        }

        //Create the PartitionProperty bean
        return new OwlPartitionProperty(keyValues);
    }


    /**
     * Convert partition entity (and its data elements) to partition bean.
     * @param backend the backend object
     * @param partition the partition to convert
     * @param otable the owl table
     * @param globalKeys the global keys
     * @return the list of owl partitions
     * @throws OwlException the owl exception
     */
    public static List<OwlPartition> convertPartition(OwlBackend backend, PartitionEntity partition,
            OwlTableEntity otable, List<GlobalKeyEntity> globalKeys) throws OwlException {

        List<OwlKeyValue> keyValues = new ArrayList<OwlKeyValue>();

        if( partition.getKeyValues() != null ) {

            //Create key value list
            for(KeyValueEntity kv : partition.getKeyValues() ) {
                KeyBaseEntity key = BackendUtil.getKeyForValue(kv, otable, globalKeys);

                OwlKeyValue value = new OwlKeyValue(key.getName(),
                        BackendUtil.getKeyType(kv),
                        OwlKey.DataType.fromCode(key.getDataType()),
                        kv.getIntValue(),
                        kv.getStringValue());

                if( key.getDataType() == OwlKey.DataType.LONG.getCode() ) {
                    //For LONG datatype, the integer value in the entity is converted to a long value in the bean
                    value.setIntValue(null);
                    value.setLongValue(
                            IntervalUtil.getTimeFromOffset((PartitionKeyEntity) key, kv.getIntValue().intValue()));
                }

                keyValues.add(value);
            }
        }

        List<OwlPartition> retVal = new ArrayList<OwlPartition>();

        if( partition.getPartitionLevel() == otable.getPartitionKeys().size() ) {
            //Leaf level partition, find matching dataelements

            List<DataElementEntity> delist = backend.find(DataElementEntity.class, "partitionId = " + partition.getId());   

            for(DataElementEntity dataElement : delist ) {

                // Get OwlSchema object
                OwlSchema schemaBean = null;
                if( dataElement.getSchemaId() != null ) {
                    List<OwlSchemaEntity>owe = backend.find(OwlSchemaEntity.class, "id = " + dataElement.getSchemaId());
                    schemaBean = convert(owe.get(0), backend) ;
                }

                OwlPartition partBean = new OwlPartition(dataElement.getOwner(),
                        dataElement.getCreatedAt(), dataElement.getLastModifiedAt(),
                        true, partition.getPartitionLevel(), dataElement.getLocation(),
                        keyValues, new OwlLoaderInfo(dataElement.getLoader()), schemaBean);
                retVal.add(partBean);
            }
        } else {
            //Not a leaf level partition, the schema/loader info will be null
            OwlPartition partBean = new OwlPartition(partition.getOwner(),
                    partition.getCreatedAt(), partition.getLastModifiedAt(),
                    false, partition.getPartitionLevel(), null,
                    keyValues, null, null);
            retVal.add(partBean);
        }

        return retVal; 
    }

    /**
     * Convert data element entity to a partition bean.
     * @param backend the backend
     * @param de the dataelement
     * @param otable the owltable
     * @param globalKeys the global keys
     * @return the owl partition
     * @throws OwlException the owl exception
     */
    public static OwlPartition convertPartition(OwlBackend backend,
            DataElementEntity de, OwlTableEntity otable,
            List<GlobalKeyEntity> globalKeys) throws OwlException {

        // Get OwlSchema object
        OwlSchema schemaBean = null;
        if( de.getSchemaId() != null ) {
            List<OwlSchemaEntity>owe = backend.find(OwlSchemaEntity.class, "id = " + de.getSchemaId());
            schemaBean = convert(owe.get(0), backend) ;
        }

        List<OwlKeyValue> keyValues = new ArrayList<OwlKeyValue>();
        //For non partitioned owltable, the level is 0 and isLeaf is true
        OwlPartition partBean = new OwlPartition(de.getOwner(),
                de.getCreatedAt(), de.getLastModifiedAt(),
                true, 0, de.getLocation(),
                keyValues, new OwlLoaderInfo(de.getLoader()), schemaBean);

        return partBean;
    }
}


