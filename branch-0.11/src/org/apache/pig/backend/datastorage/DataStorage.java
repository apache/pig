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

package org.apache.pig.backend.datastorage;

import java.util.Properties;
import java.util.Map;
import java.io.IOException;

/**
 * DataStorage provides an abstraction of a generic container.
 * Special instances of it can be a file system.
 *
 */

public interface DataStorage {
        
        public static final String USED_BYTES_KEY = "pig.used.bytes";
        public static final String RAW_CAPACITY_KEY = "pig.raw.capacity.bytes";    // replication is disregarded
        public static final String RAW_USED_KEY = "pig.raw.used.capacity.bytes";   // replication is disregarded
        
        //
        // TODO: more keys
        //
        
        /**
         * Place holder for possible initialization activities.
         */
        public void init();

        /**
         * Clean-up and releasing of resources.
         */
        public void close() throws IOException;
        
        /**
         * Provides configuration information about the storage itself.
         * For instance global data-replication policies if any, default
         * values, ... Some of such values could be overridden at a finer 
         * granularity (e.g. on a specific object in the Data Storage)
         * 
         * @return - configuration information
         */
        public Properties getConfiguration();
        
        /**
         * Provides a way to change configuration parameters
         * at the Data Storage level. For instance, change the 
         * data replication policy.
         * 
         * @param newConfiguration - the new configuration settings
         * @throws when configuration conflicts are detected
         * 
         */
        public void updateConfiguration(Properties newConfiguration) 
             throws DataStorageException;
        
        /**
         * Provides statistics on the Storage: capacity values, how much 
         * storage is in use...
         * @return statistics on the Data Storage
         */
        public Map<String, Object> getStatistics() throws IOException;
                
        /**
         * Creates an entity handle for an object (no containment
         * relation) from a String
         *
         * @param name of the object
         * @return an object descriptor
         * @throws DataStorageException if name does not conform to naming 
         *         convention enforced by the Data Storage.
         */
        public ElementDescriptor asElement(String name) 
            throws DataStorageException;

        public ElementDescriptor asElement(ElementDescriptor element)
            throws DataStorageException;
        
        public ElementDescriptor asElement(String parent,
                                                      String child) 
            throws DataStorageException;

        public ElementDescriptor asElement(ContainerDescriptor parent,
                                                      String child) 
            throws DataStorageException;

        public ElementDescriptor asElement(ContainerDescriptor parent,
                                                      ElementDescriptor child)
            throws DataStorageException;
 
        public boolean isContainer(String name) throws DataStorageException;
                
        /**
         * Created an entity handle for a container.
         * 
         * @param name of the container
         * @return a container descriptor
         * @throws DataStorageException if name does not conform to naming 
         *         convention enforced by the Data Storage.
         */
        public ContainerDescriptor asContainer(String name) 
            throws DataStorageException;
        
        public ContainerDescriptor asContainer(ContainerDescriptor container)
            throws DataStorageException;
        
        public ContainerDescriptor asContainer(String parent,
                                                          String child) 
            throws DataStorageException;

        public ContainerDescriptor asContainer(ContainerDescriptor parent,
                                                          String child) 
            throws DataStorageException;
        
        public ContainerDescriptor asContainer(ContainerDescriptor parent,
                                                          ContainerDescriptor child) 
            throws DataStorageException;

        public ElementDescriptor[] asCollection(String pattern)
            throws DataStorageException;
                                                                     
        public void setActiveContainer(ContainerDescriptor container);
        
        public ContainerDescriptor getActiveContainer();
}
