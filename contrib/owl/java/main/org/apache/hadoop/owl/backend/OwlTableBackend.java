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

package org.apache.hadoop.owl.backend;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.owl.common.ErrorType;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.common.OwlUtil;
import org.apache.hadoop.owl.entity.GlobalKeyEntity;
import org.apache.hadoop.owl.entity.KeyBaseEntity;
import org.apache.hadoop.owl.entity.OwlEntity;
import org.apache.hadoop.owl.entity.OwlTableEntity;
import org.apache.hadoop.owl.entity.OwlResourceEntity;
import org.apache.hadoop.owl.entity.OwlTableKeyValueEntity;
import org.apache.hadoop.owl.entity.PartitionKeyEntity;
import org.apache.hadoop.owl.entity.PropertyKeyEntity;
import org.apache.hadoop.owl.orm.OwlEntityManager;

/**
 * This class implements the services exposed for OwlTable resource.
 */
public class OwlTableBackend extends OwlGenericBackend<OwlTableEntity> {

    /**
     * Instantiates a new owltable backend.
     * 
     * @param sessionBackend the backend to use for session context
     */
    public OwlTableBackend(OwlGenericBackend<? extends OwlResourceEntity> sessionBackend) {
        super(OwlEntityManager.createEntityManager(OwlTableEntity.class, sessionBackend.getEntityManager()));
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.owl.backend.OwlBackend#validateCreate(org.apache.hadoop.owl.entity.OwlResourceEntity)
     */
    @Override
    protected void validateCreate(OwlResourceEntity entity) throws OwlException {
        validateTableDefinition(entity);
        validateListValues(entity);
    }


    /* (non-Javadoc)
     * @see org.apache.hadoop.owl.backend.OwlBackend#validateUpdate(org.apache.hadoop.owl.entity.OwlResourceEntity)
     */
    @Override
    protected void validateUpdate(OwlResourceEntity entity) throws OwlException {
        validateTableDefinition(entity);
        validateListValues(entity);
    }

    /**
     * Validate table definition.
     * 
     * @param entity
     *            the entity
     * 
     * @throws OwlException
     *             the owl exception
     */
    private void validateTableDefinition(OwlResourceEntity entity) throws OwlException {

        OwlTableEntity table = (OwlTableEntity) entity;

        int partitionCount = 0;
        List<String> names = new ArrayList<String>(); //list of all key names
        // validate the length of owltablename
        String name = table.getName();
        if ((name != null)&&(!OwlUtil.validateLength(name, OwlUtil.IDENTIFIER_LIMIT))){
            throw new OwlException(ErrorType.ERROR_IDENTIFIER_LENGTH_VALIDATION_FAILED, 
                    "The length of owltable name [" + name +"] is exceeding the limits.");
        }

        // get a list of global keys.
        GlobalKeyBackend gkBackend = new GlobalKeyBackend(this);
        List<GlobalKeyEntity> globalKeys = gkBackend.find(null);

        //Validate partition keys
        if( table.getPartitionKeys() != null && table.getPartitionKeys().size() != 0 ) {

            partitionCount = table.getPartitionKeys().size();
            boolean[] partFound = new boolean[partitionCount];

            for(PartitionKeyEntity partKey : table.getPartitionKeys() ) {

                //Check if partition key level is valid
                if( partKey.getPartitionLevel() < 1 || partKey.getPartitionLevel() > partitionCount ) {
                    throw new OwlException(ErrorType.ERROR_INVALID_PARTITION_LEVEL, "level " + partKey.getPartitionLevel());
                }

                //Check if partition name is unique
                if( names.contains(partKey.getName())) {
                    throw new OwlException(ErrorType.INVALID_DUPLICATE_KEY_NAME, partKey.getName());
                }
                // validate the length of partitionkeyname
                String partKeyName = partKey.getName();
                if ((partKeyName != null )&&(!OwlUtil.validateLength(partKeyName, OwlUtil.IDENTIFIER_LIMIT))){
                    throw new OwlException(ErrorType.ERROR_IDENTIFIER_LENGTH_VALIDATION_FAILED, 
                            "The length of partition key name [" + partKeyName +"] is exceeding the limits.");
                }

                // validate -- each partition key name is not duplicated with any existing global key name
                for ( GlobalKeyEntity gkey : globalKeys){
                    if (gkey.getName().equalsIgnoreCase(partKeyName)){
                        throw new OwlException (ErrorType.ERROR_DUPLICATED_RESOURCENAME, "Partition key name ["+partKeyName+"] is duplicated of existing global key name.");
                    }
                }

                // all validation is done. We can now safely add this partition key into key list
                names.add(partKey.getName());

                if( partFound[partKey.getPartitionLevel() - 1] == true ) {
                    throw new OwlException(ErrorType.ERROR_INVALID_PARTITION_LEVEL, "duplicate level " + partKey.getPartitionLevel());
                }

                partFound[partKey.getPartitionLevel() - 1] = true;
            }

            //Check if any partition levels were missing
            for(int i = 0;i < partitionCount;i++) {
                if( partFound[i] == false ) {
                    throw new OwlException(ErrorType.ERROR_PARTITION_KEY_MISSING, "level " + (i + 1));
                }
            }
        }

        //Validate property keys
        if( table.getPropertyKeys() != null && table.getPropertyKeys().size() != 0 ) {
            for(PropertyKeyEntity propKey : table.getPropertyKeys() ) {

                //Check if prop key name is unique
                if( names.contains(propKey.getName())) {
                    throw new OwlException(ErrorType.INVALID_DUPLICATE_KEY_NAME, propKey.getName());
                }   

                // validate the length of propertykeyname
                String propKeyName = propKey.getName();
                if ((propKeyName!=null)&&(!OwlUtil.validateLength(propKeyName, OwlUtil.IDENTIFIER_LIMIT))){
                    throw new OwlException(ErrorType.ERROR_IDENTIFIER_LENGTH_VALIDATION_FAILED, 
                            "The length of property key name [" + propKeyName +"] is exceeding the limits.");
                }

                // validate -- each partition key name is not duplicated with any existing global key name
                for ( GlobalKeyEntity gkey : globalKeys){
                    if ((gkey.getName()).equalsIgnoreCase(propKeyName)){
                        throw new OwlException (ErrorType.ERROR_DUPLICATED_RESOURCENAME, "Property key name ["+propKeyName+"] is duplicated of existing global key name.");
                    }
                }
                names.add(propKey.getName());
            }
        }

    }

    /**
     * Validate whether key values specified satisfy the bounded list.
     * 
     * @param entity the entity
     * @throws OwlException 
     */
    private void validateListValues(OwlResourceEntity entity) throws OwlException {
        OwlTableEntity table = (OwlTableEntity) entity;

        if( table.getKeyValues() != null ) {
            GlobalKeyBackend gkBackend = new GlobalKeyBackend(this);
            List<GlobalKeyEntity> globalKeys = gkBackend.find(null);

            //Check each key value if it satisfies the bounds 
            for(OwlTableKeyValueEntity kv : table.getKeyValues()) {
                BackendUtil.validateKeyValue(kv, table, globalKeys);
            }

        }
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.owl.backend.OwlBackend#deleteResource(org.apache.hadoop.owl.entity.OwlResourceEntity)
     */
    @Override
    protected void deleteResource(OwlResourceEntity entity) throws OwlException {
        OwlTableEntity owlTable = (OwlTableEntity) entity;

        //Delete all partitions within this owltable first
        PartitionBackend partitionBackend = new PartitionBackend(this, false);
        partitionBackend.delete("owlTableId = " + owlTable.getId());

        //Delete all data elements within this owltable, in case it is a non-partitioned owltable
        DataElementBackend deBackend = new DataElementBackend(this);
        deBackend.delete("owlTableId = " + owlTable.getId());

        Integer schemaId = owlTable.getSchemaId();

        //The property key foreign key from owltablekeyvalue table causes the dropping 
        //of the owltable to fail. Clear up the key values before dropping the table.
        owlTable.setKeyValues(null);  // FIXME: need to explicitly delete the old keyvalues
        owlTable.setSchemaId(null);
        update(owlTable);

        //Delete tables schema if present
        if( schemaId != null ) {
            OwlSchemaBackend schemaBackend = new OwlSchemaBackend(this);
            schemaBackend.delete("id = " + schemaId);
        }
    }

}

