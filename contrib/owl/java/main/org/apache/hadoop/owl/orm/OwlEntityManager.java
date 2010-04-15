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

package org.apache.hadoop.owl.orm;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.Query;

import org.apache.hadoop.owl.common.ErrorType;
import org.apache.hadoop.owl.common.LogHandler;
import org.apache.hadoop.owl.common.OwlConfig;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.entity.OwlEntity;
import org.apache.hadoop.owl.entity.OwlTableEntity;
import org.apache.hadoop.owl.entity.PartitionEntity;

/**
 * The base class for all the OwlResource Managers. Exposes basic functionality and hides the OR mapping internals.
 */
public class OwlEntityManager<T extends OwlEntity> {

    /** The entity class for which this entity Manager is created */
    protected Class<? extends OwlEntity> entityClass;

    /** The JPA EntityManager to use for this OwlResource */
    protected EntityManager entityManager;

    /** The JPA EntityTransaction for this resource */
    protected EntityTransaction entityTransaction; 

    /** Is this OwlResource Entity Manager the owner of the persistence session */
    private boolean isSessionOwner;

    /**
     * Initializes a OwlEntityManager instance
     * @param entityClass the entity class for which this entity Manager is created
     */
    protected OwlEntityManager(Class<T> entityClass) throws OwlException {

        try {
            this.entityClass = entityClass;

            //Get the entity manager factory from the singleton wrapper
            EntityManagerFactory entityManagerFactory = 
                PersistenceWrapper.getInstance().getEntityManagerFactory();

            entityManager = entityManagerFactory.createEntityManager();
            entityTransaction = entityManager.getTransaction();

            isSessionOwner = true;
        }
        catch(Exception cause) {
            //JDO uses multiple nested exceptions, so it is easier to check directly if ClassNotFound is the cause

            boolean classFound = false; 
            try {
                if( Class.forName(OwlConfig.getJdbcDriver()) != null ) {
                    classFound = true;
                }
            } catch(Exception e2) {
            }

            LogHandler.getLogger("server").error(cause.getMessage());

            if( classFound == false ) {
                throw new OwlException(ErrorType.ERROR_DB_JDBC_DRIVER, "Driver " + OwlConfig.getJdbcDriver() +
                        " Url " + OwlConfig.getJdbcUrl(), cause);
            } else {
                throw new OwlException(ErrorType.ERROR_DB_INIT, "Driver " + OwlConfig.getJdbcDriver() +
                        " Url " + OwlConfig.getJdbcUrl(), cause);
            }
        }
    }

    /**
     * Initializes a OwlEntityManager instance using existing OwlEntityManager
     * as the session owner.
     *
     * @param entityClass the entity class for which this entity Manager is created
     * @param owlEntityManager the session owner OwlEntityManager
     */
    protected OwlEntityManager(
            Class<? extends OwlEntity> entityClass,
            OwlEntityManager<? extends OwlEntity> owlEntityManager) {
        this.entityClass = entityClass;

        this.entityManager = owlEntityManager.entityManager;
        this.entityTransaction = owlEntityManager.entityTransaction;

        //This entity manager is reusing an existing JPA session, so it is not the owner.
        isSessionOwner = false;
    }

    /**
     * Creates an OwlEntityManager instance.
     *
     * @param entityClassType the entity class for which this entity Manager is created
     * @return the created entity manager
     * @throws OwlException
     */
    @SuppressWarnings("unchecked")
    public static <E extends OwlEntity> OwlEntityManager<E> createEntityManager(Class<E> entityClassType) throws OwlException {
        if( entityClassType.equals(PartitionEntity.class) ) {
            return (OwlEntityManager<E>) new PartitionEntityManager();
        } else if( entityClassType.equals(OwlTableEntity.class) ) {
            return (OwlEntityManager<E>) new OwlTableEntityManager();
        }
        return new OwlEntityManager<E>(entityClassType);
    }

    /**
     * Creates an OwlEntityManager instance using existing OwlEntityManager
     * as the session owner.
     *
     * @param entityClassType the entity class for which this entity Manager is created
     * @param owlEntityManager the session owner OwlEntityManager
     * @return the created entity manager
     */
    @SuppressWarnings("unchecked")
    public static <E extends OwlEntity>OwlEntityManager<E> createEntityManager(Class<E> entityClassType,
            OwlEntityManager<? extends OwlEntity> owlEntityManager) {

        if( entityClassType.equals(PartitionEntity.class) ) {
            return (OwlEntityManager<E>) new PartitionEntityManager(owlEntityManager);
        } else if( entityClassType.equals(OwlTableEntity.class) ) {
            return (OwlEntityManager<E>) new OwlTableEntityManager(owlEntityManager);
        }

        return new OwlEntityManager<E>(entityClassType, owlEntityManager);
    }

    /**
     * Close the resource entity manager. Rolls back active transactions and
     * closes the base entity manager. If not a session owner, does nothing.
     * 
     * @throws OwlException
     *             the meta data exception
     */
    public void close() throws OwlException  {
        try {

            if( ! isSessionOwner ) {
                //This entity manager is not the session owner, do not close the
                //transaction, just return. The owner will do the cleanup.
                entityTransaction = null;
                entityManager = null;
                return;
            }

            if( entityTransaction != null ) {
                if( entityTransaction.isActive() ) {
                    rollbackTransaction();

                    LogHandler.getLogger("server").warn("Entity manager closed with transaction active.");
                }
            }

            if( entityManager != null) {
                entityManager.close();
            }

            entityTransaction = null;
            entityManager = null;
        }
        catch(Exception e) {
            //TODO review for db cleanup (restart tomcat ?)
            LogHandler.getLogger("server").error(e.getMessage()); // TODO:fatal instead of error?
            throw new OwlException(ErrorType.ERROR_DB_CLOSE, e);
        }
    }

    /**
     * Begin transaction.
     * 
     * @throws OwlException
     *             the meta data exception
     */
    public void beginTransaction() throws OwlException  {
        try {
            entityTransaction.begin();
        }
        catch(Exception e) {
            LogHandler.getLogger("server").warn(e.getMessage());
            throw new OwlException(ErrorType.ERROR_DB_BEGIN, e);
        }
    }

    /**
     * Commit transaction.
     * 
     * @throws OwlException
     *             the meta data exception
     */
    public void commitTransaction() throws OwlException  {
        try {
            entityTransaction.commit();
        }
        catch(Exception e) {
            LogHandler.getLogger("server").warn(e.getMessage());
            throw new OwlException(ErrorType.ERROR_DB_COMMIT, e);
        }
    }

    /**
     * Rollback transaction.
     * 
     * @throws OwlException
     *             the meta data exception
     */
    public void rollbackTransaction() throws OwlException  {
        try {
            entityTransaction.rollback();
        }
        catch(Exception e) {
            LogHandler.getLogger("server").warn(e.getMessage());
            throw new OwlException(ErrorType.ERROR_DB_ROLLBACK, e);
        }
    }

    /**
     * Check if transaction is active.
     * 
     * @return is transaction active
     * @throws OwlException
     *             the meta data exception
     */
    public boolean isActive() throws OwlException  {
        try {
            return entityTransaction.isActive();
        }
        catch(Exception e) {
            LogHandler.getLogger("server").warn(e.getMessage());
            throw new OwlException(ErrorType.ERROR_DB_CHECKTRANS, e);
        }
    }

    /**
     * Insert entity.
     * 
     * @param entity the entity to insert
     * 
     * @throws OwlException
     *             the meta data exception
     */
    public void insertEntity(OwlEntity entity) throws OwlException {
        try {
            entityManager.persist(entity);
        }
        catch(Exception e) {
            LogHandler.getLogger("server").warn(e.getMessage());
            throw new OwlException(ErrorType.ERROR_DB_INSERT, e);
        }
    }

    /**
     * Delete entity.
     * 
     * @param entity  the entity to delete
     * 
     * @throws OwlException
     *             the meta data exception
     */
    public void deleteEntity(OwlEntity entity) throws OwlException {
        try {
            entityManager.remove(entity);
        }
        catch(Exception e) {
            LogHandler.getLogger("server").warn(e.getMessage());
            throw new OwlException(ErrorType.ERROR_DB_DELETE, e);
        }
    }

    /**
     * Update entity.
     * 
     * @param entity the entity to update
     *
     * @throws OwlException
     *             the meta data exception
     */
    public void updateEntity(OwlEntity entity) throws OwlException {
        try {
            entityManager.merge(entity);
        }
        catch(Exception e) {    
            LogHandler.getLogger("server").warn(e.getMessage());
            throw new OwlException(ErrorType.ERROR_DB_UPDATE, e);
        }
    }

    /**
     * Refresh entity from database.
     * 
     * @param entity the entity to refresh
     *
     * @throws OwlException
     *             the meta data exception
     */
    public void refreshEntity(OwlEntity entity) throws OwlException {
        try {
            entityManager.refresh(entity);
        }
        catch(Exception e) {    
            LogHandler.getLogger("server").warn(e.getMessage());
            throw new OwlException(ErrorType.ERROR_DB_REFRESH, e);
        }
    }

    /**
     * Flush changes to database.
     * 
     * @throws OwlException
     *             the meta data exception
     */
    public void flush() throws OwlException {
        try {
            entityManager.flush();
        }
        catch(Exception e) {    
            LogHandler.getLogger("server").warn(e.getMessage());
            throw new OwlException(ErrorType.ERROR_DB_FLUSH, e);
        }
    }

    /**
     * Clear the entities from this entity manager, required to ensure that changes to entities
     * are not persisted if not required.
     * 
     * @throws OwlException
     *             the meta data exception
     */
    public void clear() throws OwlException {
        try {
            entityManager.clear();
        }
        catch(Exception e) {    
            LogHandler.getLogger("server").warn(e.getMessage());
            throw new OwlException(ErrorType.ERROR_DB_CLEAR, e);
        }
    }

    /**
     * Fetch by id.
     * 
     * @param id id of Entity to be fetched
     * 
     * @return the fetched entity
     * 
     * @throws OwlException
     *             the meta data exception
     */
    @SuppressWarnings("unchecked")
    public T fetchById(int id) throws OwlException {
        try {
            Object obj = entityManager.find(entityClass, Integer.valueOf(id));

            return (T) obj;
        }
        catch(Exception e) {
            LogHandler.getLogger("server").warn(e.getMessage());
            throw new OwlException(ErrorType.ERROR_DB_FETCH_BY_ID, "entity " + entityClass.getSimpleName() + " id " + id, e);
        }
    }


    /**
     * Fetch entities by filter.
     * 
     * @param filter the filter to use 
     * @return the list of resource entities fetched
     * 
     * @throws OwlException meta data exception
     */
    public List<T> fetchByFilter(String filter) throws OwlException {
        return fetchByFilter(null, filter);
    }


    /**
     * Fetch entities by filter .
     * 
     * @param from the list of entities to query on, the from clause
     * @param filter the filter to use, the where clause
     * 
     * @return the list of resource entities fetched
     * 
     * @throws OwlException meta data exception
     */
    @SuppressWarnings("unchecked")
    public List<T> fetchByFilter(String from, String filter) throws OwlException {

        String queryString = null;

        if( from == null && filter != null ) {
            //Optimize for the "id=N" case
            List<T> list = fetchByFilterOptimized(filter);

            if( list != null ) {
                return list;
            }
            //Could not optimize, run the query with filter
        }

        try {
            queryString = "select distinct entity from ";

            if( from != null ) {
                //user specified from clause
                //TODO verify that from has class name and entity
                queryString += from;
            } else {
                queryString += entityClass.getSimpleName() + " entity";
            }

            if( filter != null && filter.trim().length() > 0) {
                queryString = queryString + " where " + filter;
            }

            LogHandler.getLogger("server").debug("queryString:["+queryString+"]");
            Query query = entityManager.createQuery(queryString);
            return query.getResultList();
        }
        catch(Exception e) {
            LogHandler.getLogger("server").warn(e.getMessage());
            throw new OwlException(ErrorType.ERROR_DB_QUERY_EXEC, "[" + queryString + "]", e);
        }
    }

    /**
     * Optimized fetch by filter. Checks if the filter is of "id=N" format, if so does an
     * EntityManager.find instead of running a query. This will help increase the usage of
     * the L1 cache maintained by DataNucleus since find can be done from the cache.
     * @param filter the filter
     * @return the list of objects fetched
     * @throws OwlException 
     */
    private List<T> fetchByFilterOptimized(String filter) throws OwlException {

        int id = KeyQueryBuilder.parseId(filter, "id", true);
        if( id == -1 ) {
            //Could not optimize
            return null;
        }

        List<T> list = new ArrayList<T>();
        T object = fetchById(id);
        list.add(object);

        return list;
    }

    /**
     * Get the current database timestamp .
     * 
     * @return the database timestamp as a long value
     * 
     * @throws OwlException meta data exception
     */
    public long getCurrentTime() throws OwlException {

        //FIXME: cache difference b/w System.currentTimeMillis() and db time, and sync only every 5 mins or so.
        // return System.currentTimeMillis();

        String queryString = null;

        try {
            PersistenceWrapper.DatabaseType databaseType = PersistenceWrapper.getDataBaseType();

            if( databaseType == PersistenceWrapper.DatabaseType.MYSQL )  {
                queryString = "select CURRENT_TIMESTAMP()";
            } else if( databaseType == PersistenceWrapper.DatabaseType.ORACLE )  {
                queryString = "select localtimestamp(3) from dual";
            } else { //TODO maybe make this query a configurable param, currently default to derby format
                queryString = "values CURRENT TIMESTAMP";
            }

            Query query = entityManager.createNativeQuery(queryString);

            Timestamp currentTime = (Timestamp) query.getSingleResult();
            return currentTime.getTime();
        }
        catch(Exception e) {
            LogHandler.getLogger("server").warn(e.getMessage());
            throw new OwlException(ErrorType.ERROR_DB_QUERY_EXEC, "[" + queryString + "]", e);
        }
    }

}
