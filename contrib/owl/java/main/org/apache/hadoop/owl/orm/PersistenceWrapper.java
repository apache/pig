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

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;

import org.apache.hadoop.owl.common.LogHandler;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.common.OwlConfig;

/** A Singleton wrapper class for calling Persistence.createEntityManagerFactory. The constructor of this class
 *  calls Persistence.createEntityManagerFactory. This way a synchronized call to create the EntityManagerFactory is avoided
 *  in the non error case.
 */
public class PersistenceWrapper {

    /** Enum to find which type of database is being accessed */
    public enum DatabaseType {
        DERBY,
        MYSQL,
        ORACLE,
        OTHER
    }

    /** Persistence Unit name as defined in META-INF/persistence.xml */
    private static final String       persistenceUnit      = "Owl";

    /**
     * The JPA EntityManagerFactory reference, this is cached across rest calls.
     * This becomes a singleton since the wrapper which creates it is a singleton.
     */
    volatile private EntityManagerFactory      entityManagerFactory = null;

    /** The static singleton instance variable */
    private static PersistenceWrapper instance             = new PersistenceWrapper();

    /** The private constructor */
    private PersistenceWrapper() {
        try {
            initialize();
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    /** Initializes a persistence session 
     * @throws OwlException the exception creating the entity manager factory
     */
    private void initialize() throws OwlException{
        Map<String, String> customProperties = OwlConfig.getDbConnectivityProperties();

        if( customProperties == null ) {
            customProperties = new HashMap<String, String>();
        }

        //Mysql requires lowercase, derby requires uppercase (which is default in persistence.xml)
        DatabaseType dbType = getDataBaseType();
        if( dbType == DatabaseType.MYSQL ) {
            customProperties.put("datanucleus.identifier.case", "LowerCase");
        } else if( dbType == DatabaseType.DERBY ) {
            System.setProperty("derby.locks.waitTimeout", "300"); //set the lock wait timeout to 5 minutes for derby
        }else if( dbType == DatabaseType.ORACLE ) {
            System.setProperty("oracle.jdbc.J2EE13Compliant", "true");
        }

        if ((dbType == DatabaseType.DERBY) || (dbType == DatabaseType.MYSQL)){
            // DataNucleus default isolation level is read-committed, but that can cause non-repeatable reads
            // repeatable read is a good performance-correctness tradeoff point. 
            // Also, MySql Replication seems to need at least repeatable-read to generate binlogs
            // not doing so blindly for all db types though, because not all jdbc drivers seem to support it.
            customProperties.put("datanucleus.transactionIsolation", "repeatable-read");
        } else if(dbType == DatabaseType.ORACLE) {
            // can't use repeatable-read for oracle:
            // ORA-17030 : READ_COMMITTED and SERIALIZABLE are the only valid transaction levels
            // See http://www.oracle.com/technology/oramag/oracle/05-nov/o65asktom.html for details
            // customProperties.put("datanucleus.transactionIsolation", "serializable");
            // Update : Can't use serializable either, we get 
            // ORA-08177 : "can't serialize access for this transaction" for some accesses
            // TODO: investigate and verify - could it be because each entitymanager we have winds up having its own transaction?
        }

        Log logger = LogHandler.getLogger("server");
        logger.info("custom properties used:");
        for (Map.Entry<String, String> entry : customProperties.entrySet()){
            logger.info("    key:[" + entry.getKey() + "], value:[" + entry.getValue() + "]");
        }

        entityManagerFactory = Persistence.createEntityManagerFactory(persistenceUnit, customProperties);
    }

    /**
     * Gets the single instance of PersistenceWrapper.
     * @return single instance of PersistenceWrapper
     */
    public static PersistenceWrapper getInstance() {
        return instance;
    }

    /**
     * Gets the entity manager factory. If an exception had occurred during the factory creation, retry
     * to initialize.
     * @return the entity manager factory instance
     * @throws Exception the exception which occurred during entity manager factory creation
     */
    public EntityManagerFactory getEntityManagerFactory() throws Exception {
        if( entityManagerFactory == null ) {
            synchronized(this) {
                if( entityManagerFactory == null ) {
                    initialize();
                }
            }
        }

        return entityManagerFactory;
    }

    /**
     * Gets the data base type. Looks at the JDBC driver class name to find the database type.
     * @return the data base type enum
     */
    public static DatabaseType getDataBaseType() {
        try {
            String driver = OwlConfig.getJdbcDriver();

            if( driver != null ) {
                String driverLowerCase = driver.toLowerCase();

                if( driverLowerCase.contains("derby") ) {
                    return DatabaseType.DERBY;
                } else if( driverLowerCase.contains("mysql") ) {
                    return DatabaseType.MYSQL;
                } else if( driverLowerCase.contains("oracle") ) {
                    return DatabaseType.ORACLE;
                }

            }
        } catch(OwlException me) {
            return DatabaseType.OTHER;
        }

        return DatabaseType.OTHER;
    }
}
