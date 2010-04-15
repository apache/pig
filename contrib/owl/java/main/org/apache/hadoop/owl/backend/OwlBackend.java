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

import java.lang.reflect.Constructor;
import java.util.List;

import org.apache.hadoop.owl.common.ErrorType;
import org.apache.hadoop.owl.common.LogHandler;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.entity.OwlEntity;
import org.apache.hadoop.owl.entity.OwlResourceEntity;

/**
 * This class is the interface from the logical layer to the backend layer. A generic session backend is created
 * when this is created. So every new OwlBackend created should be closed to avoid resource leakage. Closing
 * transaction does not automatically close the backend instance.
 */
public class OwlBackend {

    /** The session backend object. */
    OwlGenericBackend<? extends OwlResourceEntity> sessionBackend = null;

    /**
     * Instantiates a new owl backend.
     * @throws OwlException the owl exception
     */
    public OwlBackend() throws OwlException {
        sessionBackend = new OwlGenericBackend<OwlResourceEntity>();
    }

    /**
     * Begin a transaction.
     * @throws OwlException the owl exception
     */
    public void beginTransaction() throws OwlException {
        sessionBackend.getEntityManager().beginTransaction();
    }

    /**
     * Commit the active transaction.
     * @throws OwlException the owl exception
     */
    public void commitTransaction() throws OwlException {
        sessionBackend.getEntityManager().commitTransaction();
    }

    /**
     * Rollback the transaction.
     * @throws OwlException the owl exception
     */
    public void rollbackTransaction() throws OwlException {
        sessionBackend.getEntityManager().rollbackTransaction();
    }

    /**
     * Close the backend instance. Required to avoid resource leakage.
     * @throws OwlException the owl exception
     */
    public void close() throws OwlException {
        if( sessionBackend != null ) {
            try {
                sessionBackend.getEntityManager().clear();
            } finally {            
                sessionBackend.getEntityManager().close();
                sessionBackend = null;
            }
        }
    }

    /**
     * Creates the specified resource.
     * 
     * @param resource the resource to create
     * @return the created resource
     * @throws OwlException the owl exception
     */
    public <T extends OwlResourceEntity> T create(T resource) throws OwlException {
        OwlGenericBackend<T> backend = createBackendInstance(resource.getClass());
        return backend.create(resource);
    }

    /**
     * Update the specified resource
     * 
     * @param resource the resource
     * @return the list of updated resources
     * @throws OwlException the owl exception
     */
    public <T extends OwlResourceEntity> T update(T resource) throws OwlException {
        OwlGenericBackend<T> backend = createBackendInstance(resource.getClass());
        return backend.update(resource);
    }

    /**
     * Delete the resources matching the filter.
     * 
     * @param resourceClass the resource class
     * @param filter the filter
     * @throws OwlException the owl exception
     */
    public <T extends OwlResourceEntity> void delete(Class<? extends OwlResourceEntity> resourceClass, String filter) throws OwlException {
        OwlGenericBackend<T> backend = createBackendInstance(resourceClass);
        backend.delete(filter);
    }

    /**
     * Deletes a dependant entity object
     * @param parentResourceClass The parent Resource Class for the entity object
     * @param entity The entity under consideration
     * @throws OwlException the owl exception
     */
    public <T extends OwlResourceEntity> void deleteDependantEntity(Class<? extends OwlResourceEntity> parentResourceClass, OwlEntity entity) throws OwlException{
        OwlGenericBackend<T> backend = createBackendInstance(parentResourceClass);
        backend.deleteDependantEntity(entity);
    }

    /**
     * Finds resources matching specified filter.
     * 
     * @param resourceClass the resource class
     * @param filter the filter
     * @return the list of matching resources
     * @throws OwlException the owl exception
     */
    public <T extends OwlResourceEntity> List<T> find(Class<? extends OwlResourceEntity> resourceClass, String filter) throws OwlException {
        OwlGenericBackend<T> backend = createBackendInstance(resourceClass);
        return backend.find(filter);
    }

    /**
     * Gets the backend class.
     * 
     * @param resourceClass the resource entity class 
     * @return the backend class
     * @throws OwlException the owl exception
     */
    private <T extends OwlResourceEntity> Class<? extends OwlGenericBackend<T>> getBackendClass(
            Class<? extends OwlResourceEntity> resourceClass) throws OwlException {

        String resourceClassName = resourceClass.getSimpleName();
        String resourceName = resourceClassName.substring(0, resourceClassName.length() - "Entity".length());

        try {
            @SuppressWarnings("unchecked") Class<? extends OwlGenericBackend<T>> backendClass = 
                (Class<? extends OwlGenericBackend<T>>) Class.forName(
                        OwlGenericBackend.class.getPackage().getName() + "." + resourceName + "Backend");
            return backendClass;

        } catch(Exception cnfe) {
            throw new OwlException(ErrorType.ERROR_UNRECOGNIZED_RESOURCE_TYPE, resourceName, cnfe);
        }
    }

    /**
     * Create a backend instance of type corresponding to the resource class passed in.
     * 
     * @param resourceClass the resource entity class
     * @return the backend instance
     * @throws OwlException the owl exception
     */
    private <T extends OwlResourceEntity> OwlGenericBackend<T> createBackendInstance(
            Class<? extends OwlResourceEntity> resourceClass) throws OwlException {
        try {
            Class<? extends OwlGenericBackend<T>> backendClass = getBackendClass(resourceClass);

            Constructor<? extends OwlGenericBackend<T>> constructor = 
                backendClass.getConstructor(OwlGenericBackend.class);
            return constructor.newInstance(sessionBackend);
        } catch (Exception e) {
            throw new OwlException(ErrorType.ERROR_UNRECOGNIZED_RESOURCE_TYPE, resourceClass.getSimpleName(), e);
        }
    }

}
