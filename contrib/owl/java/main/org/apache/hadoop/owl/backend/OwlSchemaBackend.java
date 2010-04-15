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

import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.entity.OwlColumnSchemaEntity;
import org.apache.hadoop.owl.entity.OwlSchemaEntity;
import org.apache.hadoop.owl.entity.OwlResourceEntity;
import org.apache.hadoop.owl.orm.OwlEntityManager;

/**
 * This class implements the services exposed for OwlSchema resource.
 */
public class OwlSchemaBackend extends OwlGenericBackend<OwlSchemaEntity> {

    /**
     * Instantiates a new OwlSchema backend.
     * 
     * @param sessionBackend the backend to use for session context
     */
    public OwlSchemaBackend(OwlGenericBackend<? extends OwlResourceEntity> sessionBackend) {
        super(OwlEntityManager.createEntityManager(OwlSchemaEntity.class, sessionBackend.getEntityManager()));
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.owl.backend.OwlGenericBackend#find(java.lang.String)
     */
    //Overridden so than the nested schema objects can be populated into the schema entity
    @Override
    public List<OwlSchemaEntity> find(String filter) throws OwlException {
        List<OwlSchemaEntity> schemas = super.find(filter);
        for(OwlSchemaEntity schema : schemas) {
            schema.fixupSchema(this);
        }

        return schemas;
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.owl.backend.OwlGenericBackend#createResource(org.apache.hadoop.owl.entity.OwlResourceEntity)
     */
    @Override
    protected void createResource(
            OwlResourceEntity entity) throws OwlException {

        OwlSchemaEntity schema = (OwlSchemaEntity) entity;  

        for(OwlColumnSchemaEntity columnEntity : schema.getOwlColumnSchema() ) {
            if( columnEntity.getSubSchema() != null ) {
                createNestedSchema(columnEntity);
            }
        }
    }

    /**
     * Creates the nested schema. Recursively calls itself so that subschemas get created
     * first and the subschemas id is updated in the column.
     * @param column the current column schema 
     * @throws OwlException the owl exception
     */
    @SuppressWarnings("boxing")
    private void createNestedSchema(OwlColumnSchemaEntity column) throws OwlException {
        OwlSchemaEntity schema = column.getSubSchema();

        for(OwlColumnSchemaEntity columnEntity : schema.getOwlColumnSchema() ) {
            if( columnEntity.getSubSchema() != null ) {
                createNestedSchema(columnEntity);
            }
        }

        if( column.getSubSchemaId()== null ) {
            schema = create(schema);
            column.setSubSchema(schema);
        }
        column.setSubSchemaId(schema.getId());
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.owl.backend.OwlBackend#deleteResource(org.apache.hadoop.owl.entity.OwlResourceEntity)
     */
    @Override
    protected void deleteResource(
            OwlResourceEntity entity) throws OwlException {
        OwlSchemaEntity schema = (OwlSchemaEntity) entity;
        schema.fixupSchema(this);

        for(OwlColumnSchemaEntity columnEntity : schema.getOwlColumnSchema() ) {
            if( columnEntity.getSubSchemaId() != null ) {
                dropNestedSchema(columnEntity);
            }
        }
    }

    /**
     * Drops the nested schema. Recursively calls itself so that subschemas get dropped first.
     * @param column the current column schema 
     * @throws OwlException the owl exception
     */
    private void dropNestedSchema(OwlColumnSchemaEntity column) throws OwlException {
        OwlSchemaEntity schema = column.getSubSchema();

        for(OwlColumnSchemaEntity columnEntity : schema.getOwlColumnSchema() ) {
            if( columnEntity.getSubSchema() != null ) {
                dropNestedSchema(columnEntity);
                columnEntity.setSubSchemaId(null);
            }
        }

        delete("id = " + schema.getId());
    }
}

