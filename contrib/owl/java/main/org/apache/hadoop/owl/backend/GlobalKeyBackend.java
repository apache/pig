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
import org.apache.hadoop.owl.entity.DatabaseEntity;
import org.apache.hadoop.owl.entity.GlobalKeyEntity;
import org.apache.hadoop.owl.entity.OwlResourceEntity;
import org.apache.hadoop.owl.entity.OwlTableEntity;
import org.apache.hadoop.owl.entity.PartitionKeyEntity;
import org.apache.hadoop.owl.entity.PropertyKeyEntity;
import org.apache.hadoop.owl.orm.OwlEntityManager;
import org.apache.log4j.Logger;

/**
 * This class implements the services exposed for GlobalKey resource.
 */
public class GlobalKeyBackend extends OwlGenericBackend<GlobalKeyEntity> {

    /**
     * Instantiates a new global key backend.
     * 
     * @param sessionBackend the backend to use for session context
     */
    public GlobalKeyBackend(OwlGenericBackend<? extends OwlResourceEntity> sessionBackend) {
        super(OwlEntityManager.createEntityManager(GlobalKeyEntity.class, sessionBackend.getEntityManager()));
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.owl.backend.OwlBackend#validateCreate(org.apache.hadoop.owl.entity.OwlResourceEntity)
     */
    @Override
    protected void validateCreate(OwlResourceEntity entity) throws OwlException {
        validateGlobalKey(entity);
    }

    /**
     * Validate global key
     * @param entity
     * @throws OwlException
     */
    private void validateGlobalKey(OwlResourceEntity entity) throws OwlException {
        // validate the length of global key name
        GlobalKeyEntity gKey = (GlobalKeyEntity) entity;
        String keyName = gKey.getName();
        if ((keyName != null) && (!OwlUtil.validateLength(keyName, OwlUtil.IDENTIFIER_LIMIT))){
            throw new OwlException(ErrorType.ERROR_IDENTIFIER_LENGTH_VALIDATION_FAILED, 
                    "The length of global key name [" + keyName +"] is exceeding the limits.");
        }

        // validation -- the uniqueness of the global key name. 
        // Global key name can not be the same with any existing tables' partition keys and property keys.

        // get every owltable. note that we want to make sure that we are still in the same transaction.
        OwlTableBackend owltablebackend = new OwlTableBackend(this);
        List<OwlTableEntity> owlTableList = owltablebackend.find(null);
        for (OwlTableEntity otable : owlTableList){
            // get the list of partition keys for every owltable
            List<PartitionKeyEntity> partitionKeyEntityList =  otable.getPartitionKeys();

            // for each partition key, we check if there is a key name duplication with the global key
            for (PartitionKeyEntity pke : partitionKeyEntityList){
                if((pke.getName()).equalsIgnoreCase(keyName)){
                    throw new OwlException (ErrorType.ERROR_DUPLICATED_RESOURCENAME, "Global key,["+keyName+"] has key name duplication with a partition key " +
                            "on owl table [" + otable.getName() + "]");
                }
            }

            // get the list of property keys for every owltable
            List<PropertyKeyEntity> propertyKeyEntityList = otable.getPropertyKeys();
            // for each property key, we check if there is a key name duplication with the global key
            for (PropertyKeyEntity proK : propertyKeyEntityList ){
                if((proK.getName()).equalsIgnoreCase(keyName)){
                    throw new OwlException (ErrorType.ERROR_DUPLICATED_RESOURCENAME, "Global key,["+keyName+"] has key name duplication with a property key" +
                            "on owl table [" + otable.getName() + "]");
                }
            }
        }
    }


    /* (non-Javadoc)
     * @see org.apache.hadoop.owl.backend.OwlBackend#validateUpdate(org.apache.hadoop.owl.entity.OwlResourceEntity)
     */
    @Override
    protected void validateUpdate(OwlResourceEntity entity) throws OwlException {
        validateGlobalKey(entity);
    }

}

