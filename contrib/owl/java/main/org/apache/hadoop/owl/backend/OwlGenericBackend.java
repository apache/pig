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
import org.apache.hadoop.owl.entity.OwlEntity;
import org.apache.hadoop.owl.entity.OwlResourceEntity;
import org.apache.hadoop.owl.common.OwlUtil.Verb;
import org.apache.hadoop.owl.orm.OwlEntityManager;

/**
 * This class is the base class for all backend classes implementing find/create/update/delete
 * operations for the top level resources. This classes uses a generic type T as the OwlResourceEntity
 * type. Each derived class extends the base class for a particular resource type. The
 * derived classes implement a set of callback abstract functions which can be used to
 * customize the operations as appropriate for each resource type. 
 */
public class OwlGenericBackend<T extends OwlResourceEntity> {

    /** The entity manager for this backend */
    protected OwlEntityManager<T> entityManager = null; 

    /**
     * Initializes a OwlResource Backend instance.
     * @param sessionBackend the session backend for this class 
     */
    @SuppressWarnings("unchecked")
    OwlGenericBackend() throws OwlException {
        this.entityManager = OwlEntityManager.createEntityManager((Class<T>) OwlResourceEntity.class);
    }

    /**
     * Initializes a OwlResource Backend instance.
     * @param entityManager the entity manager to use for this session
     */
    OwlGenericBackend(OwlEntityManager<T> entityManager) {
        this.entityManager = entityManager;
    }


    /** Gets the entity manager for this backend
     * @return the entity manager
     */
    public OwlEntityManager<T> getEntityManager() {
        return entityManager;
    }


    /**
     * The generic functionality for creating a resource. This function calls
     * helper functions which can be overridden in the resource specific backend classes.
     * 
     * @param inputResource the OwlResource to create
     * @return the newly create OwlResource object
     * @throws OwlException the meta data exception
     */
    public T create(T inputResource) throws OwlException {

        try {
            long currentTime = entityManager.getCurrentTime();

            ValidateInput.validateResource(inputResource, Verb.CREATE);
            //Call the resource specific callback functions
            validateCreate(inputResource);
            createResource(inputResource);

            inputResource.setVersion(1);
            inputResource.setCreatedAt(currentTime);
            inputResource.setLastModifiedAt(currentTime);

            //Do the insert into database
            entityManager.insertEntity(inputResource);
            entityManager.flush(); //required so that id's get updated

            return inputResource;

        } catch (Exception exception) {
            //rollback transaction, create OwlException
            throw BackendError.handleBackendException(
                    entityManager,
                    exception,
                    ErrorType.ERROR_RESOURCE_CREATE
            );
        }
    }


    /**
     * The generic functionality for updating a resource. This function calls helper
     * functions which can be overridden in the resource specific backend classes.
     * 
     * @param inputResource the OwlResourceEntity to update
     * @return the updated Resource
     * @throws OwlException the meta data exception
     */
    public T update(
            T inputResource
    ) throws OwlException {

        try {
            ValidateInput.validateResource(inputResource, Verb.UPDATE);

            //Fetch entity to be updated
            T entity = entityManager.fetchById(inputResource.getId());

            long currentTime = entityManager.getCurrentTime();

            if( entity.getVersion() != inputResource.getVersion() ) {
                OwlException mex = new OwlException(ErrorType.ERROR_MISMATCH_VERSON_NUMBER,
                        inputResource.getVersion() + " as against " + entity.getVersion());
                LogHandler.getLogger("server").debug(mex.getMessage());
                throw mex;
            }

            //Call the resource specific callback functions
            validateUpdate(inputResource);
            updateResource(inputResource);

            inputResource.setLastModifiedAt(currentTime);
            inputResource.setVersion(entity.getVersion() + 1);

            //persist updated entity to database
            entityManager.updateEntity(inputResource); 
            entityManager.flush(); //required so that id's get updated

            return inputResource;

        } catch (Exception exception) {
            //rollback transaction, create OwlException
            throw BackendError.handleBackendException(
                    entityManager,
                    exception,
                    ErrorType.ERROR_RESOURCE_UPDATE
            );
        }
    }


    /**
     * The generic functionality for deleting a resource. This function calls helper
     * functions which can be overridden in the resource specific backend classes.
     * 
     * @param filter the filter for resource to delete
     * @throws OwlException the meta data exception
     */
    public void delete(String filter) throws OwlException {
        System.out.println("BECK:OwlGenericBackend.delete called on : filter[" + filter + "]");
        try {
            ValidateInput.checkUnsafeUpdate(filter);

            //Fetch entities to be deleted
            List<T> entities = entityManager.fetchByFilter(filter);

            for(OwlResourceEntity entity : entities) {
                //Call the resource specific callback functions
                validateDelete(entity);
                deleteResource(entity);

                //delete the entity from database
                entityManager.deleteEntity(entity); 
            }

        } catch (Exception exception) {
            //rollback transaction, create OwlException
            throw BackendError.handleBackendException(
                    entityManager,
                    exception,
                    ErrorType.ERROR_RESOURCE_DELETE
            );
        }
    }


    /**
     * The generic functionality for finding a resource. This function is invoked from
     * OwlResource.findResource.
     * 
     * @param filter the filter for resource to update, currently restricted to "id=N"
     * @return the list of found Resources
     * @throws OwlException the meta data exception
     */

    public List<T> find(String filter) throws OwlException {

        List<T> entities = entityManager.fetchByFilter(filter);

        //Since we do prefetching in some cases, we need to clear the entities so that the prefetched data
        //does not get persisted (@see org.apache.hadoop.owl.orm.DataUnitEntityManager#fetchByFilter(java.lang.String))
        //entityManager.clear();
        //TODO fix for new transaction semantics

        return entities;
    }


    /**
     * Validates a create, each backend can provides its specific implementation of this. This function is called during a
     * create operation, after the entity object has been created from the input bean, and before the resource specific
     * operations (createEntity) have been done.
     * 
     * @param entity the entity being created
     * @throws OwlException the meta data exception
     */
    protected void validateCreate(
            OwlResourceEntity entity) throws OwlException {
    }

    /**
     * Validates a update, each backend can provide its specific implementation of this. This function is called during a
     * update operation, after the entities being updated are fetched and the merge has been done, and before the resource
     * specific operations (updateEntity) have been done.
     * 
     * @param entity the entity being updated
     * @throws OwlException the meta data exception
     */
    protected void validateUpdate(
            OwlResourceEntity entity) throws OwlException {
    }

    /**
     * Validates a delete, each backend can provide its specific implementation of this. This function is called during a
     * delete operation, after the entities to be deleted are fetched, and before the resource specific
     * operations (deleteEntity) have been done.
     * 
     * @param entity the entity being deleted
     * @throws OwlException the meta data exception
     */
    protected void validateDelete(
            OwlResourceEntity entity) throws OwlException {
    }

    /**
     * The resource specific operations for a create, each backend can provide its specific implementation of this.
     * This function is called during a create operation, after the resource specific validations are done and
     * before the actual insert to database is done.
     * 
     * @param entity the entity being created
     * @throws OwlException the meta data exception
     */
    protected void createResource(
            OwlResourceEntity entity) throws OwlException {
    }

    /**
     * The resource specific operations for a update, each backend can provides its specific implementation of this.
     * This function is called during a update operation, after the resource specific validations are done and
     * before the actual update to database is done.
     * 
     * @param entity the entity being updated
     * @throws OwlException the meta data exception
     */
    protected void updateResource(
            OwlResourceEntity entity) throws OwlException {
    }

    /**
     * The resource specific operations for a delete, each backend can provide its specific implementation of this.
     * This function is called during a delete operation, after the resource specific validations are done and
     * before the actual delete from database is done.
     * 
     * @param entity the entity being deleted
     * @throws OwlException the meta data exception
     */
    protected void deleteResource(
            OwlResourceEntity entity) throws OwlException {
    }

    /**
     * A method to delete dependant entities, each backend can provide its own specific implementation of this.
     * @param entity
     */
    public void deleteDependantEntity(OwlEntity entity) throws OwlException {
        entityManager.deleteEntity(entity);
    }

}

