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
import org.apache.hadoop.owl.common.LogHandler;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.common.OwlUtil;
import org.apache.hadoop.owl.entity.DataElementEntity;
import org.apache.hadoop.owl.entity.OwlResourceEntity;
import org.apache.hadoop.owl.entity.PartitionEntity;
import org.apache.hadoop.owl.orm.OwlEntityManager;

/**
 * This class implements the services exposed for DataElement resource.
 */
public class DataElementBackend extends OwlGenericBackend<DataElementEntity> {

    /**
     * Instantiates a new data element backend.
     * 
     * @param sessionBackend the backend to use for session context
     */
    public DataElementBackend(OwlGenericBackend<? extends OwlResourceEntity> sessionBackend) {
        super(OwlEntityManager.createEntityManager(DataElementEntity.class, sessionBackend.getEntityManager()));
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.owl.backend.OwlBackend#validateCreate(org.apache.hadoop.owl.entity.OwlResourceEntity)
     */
    @Override
    protected void validateCreate(OwlResourceEntity entity) throws OwlException {
        validateDataElement(entity);
    }

    /**
     * Validate data element
     * @param entity
     * @throws OwlException
     */
    private void validateDataElement(OwlResourceEntity entity) throws OwlException {
        // Validation -- make sure the data location is limited to 1024 characters
        DataElementEntity deEntity = (DataElementEntity)entity;
        String location = deEntity.getLocation();
        if ((location != null)&&(!OwlUtil.validateLength(location, OwlUtil.LOCATION_LIMIT))){
            throw new OwlException(ErrorType.ERROR_IDENTIFIER_LENGTH_VALIDATION_FAILED, 
                    "The length of deLocation [" + location +"] is exceeding its limits.");
        }

    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.owl.backend.OwlBackend#validateUpdate(org.apache.hadoop.owl.entity.OwlResourceEntity)
     */
    @Override
    protected void validateUpdate(OwlResourceEntity entity) throws OwlException {
        validateDataElement(entity);
    }

}

