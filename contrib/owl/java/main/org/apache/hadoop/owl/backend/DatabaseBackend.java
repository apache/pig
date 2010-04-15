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

import java.util.List;

import org.apache.hadoop.owl.common.ErrorType;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.common.OwlUtil;
import org.apache.hadoop.owl.entity.DatabaseEntity;
import org.apache.hadoop.owl.entity.OwlResourceEntity;
import org.apache.hadoop.owl.entity.OwlTableEntity;
import org.apache.hadoop.owl.orm.OwlEntityManager;

/**
 * This class implements the services exposed for Database resource.
 */
public class DatabaseBackend extends OwlGenericBackend<DatabaseEntity> {

    /**
     * Instantiates a new database backend.
     * 
     * @param sessionBackend the backend to use for session context
     */
    public DatabaseBackend(OwlGenericBackend<? extends OwlResourceEntity> sessionBackend) {
        super(OwlEntityManager.createEntityManager(DatabaseEntity.class, sessionBackend.getEntityManager()));
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.owl.backend.OwlBackend#validateCreate(org.apache.hadoop.owl.entity.OwlResourceEntity)
     */
    @Override
    protected void validateCreate(OwlResourceEntity entity) throws OwlException {
        validateDatabase(entity);
    }

    /**
     * Validate database
     * @param entity
     * @throws OwlException
     */
    private void validateDatabase(OwlResourceEntity entity) throws OwlException {
        DatabaseEntity odbEntity = (DatabaseEntity) entity;
        // validate the length of databaseName
        String databaseName = odbEntity.getName();
        if ((databaseName != null )&&(!OwlUtil.validateLength(databaseName, OwlUtil.IDENTIFIER_LIMIT))){
            throw new OwlException(ErrorType.ERROR_IDENTIFIER_LENGTH_VALIDATION_FAILED, 
                    "The length of database name [" + databaseName +"] is exceeding the limits.");
        }
        // validate the length of db location
        String externalStorageIdentifier = odbEntity.getLocation();
        if ((externalStorageIdentifier != null)&&(! OwlUtil.validateLength(externalStorageIdentifier, OwlUtil.LOCATION_LIMIT))){
            throw new OwlException(ErrorType.ERROR_IDENTIFIER_LENGTH_VALIDATION_FAILED, 
                    "The length of storage location [" + externalStorageIdentifier +"] is exceeding the limits.");
        }
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.owl.backend.OwlBackend#validateUpdate(org.apache.hadoop.owl.entity.OwlResourceEntity)
     */
    @Override
    protected void validateUpdate(OwlResourceEntity entity) throws OwlException {
        validateDatabase(entity);
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.owl.backend.OwlBackend#deleteResource(org.apache.hadoop.owl.entity.OwlResourceEntity)
     */
    @Override
    protected void deleteResource(OwlResourceEntity entity) throws OwlException {
        DatabaseEntity database = (DatabaseEntity) entity;

        //Fail if database has any owl tables
        OwlTableBackend tableBackend = new OwlTableBackend(this);
        List<OwlTableEntity> tables = tableBackend.find("databaseId = " + database.getId());

        if( tables.size() > 0 ) {
            throw new OwlException(ErrorType.ERROR_DATABASE_NONEMPTY);
        }
    }

}

