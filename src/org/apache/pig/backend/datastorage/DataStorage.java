package org.apache.pig.backend.datastorage;

import java.util.Properties;
import java.io.IOException;

public interface DataStorage {
        
        public static final String DEFAULT_REPLICATION_FACTOR_KEY = "default.replication.factor";
        public static final String USED_BYTES_KEY = "used.bytes";

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
        public Properties getStatistics() throws IOException;
                
        /**
         * Creates an entity handle for an object (no containment
         * relation) from a String
         *
         * @param name of the object
         * @return an object descriptor
         * @throws DataStorageException if name does not conform to naming 
         *         convention enforced by the Data Storage.
         */
        public DataStorageElementDescriptor asElement(String name) 
            throws DataStorageException;

        public DataStorageElementDescriptor asElement(DataStorageElementDescriptor element)
            throws DataStorageException;
        
        public DataStorageElementDescriptor asElement(String parent,
                                                      String child) 
            throws DataStorageException;

        public DataStorageElementDescriptor asElement(DataStorageContainerDescriptor parent,
                                                      String child) 
            throws DataStorageException;

        public DataStorageElementDescriptor asElement(DataStorageContainerDescriptor parent,
                                                      DataStorageElementDescriptor child)
            throws DataStorageException;
        
        /**
         * Created an entity handle for a container.
         * 
         * @param name of the container
         * @return a container descriptor
         * @throws DataStorageException if name does not conform to naming 
         *         convention enforced by the Data Storage.
         */
        public DataStorageContainerDescriptor asContainer(String name) 
            throws DataStorageException;
        
        public DataStorageContainerDescriptor asContainer(DataStorageContainerDescriptor container)
            throws DataStorageException;
        
        public DataStorageContainerDescriptor asContainer(String parent,
                                                          String child) 
            throws DataStorageException;

        public DataStorageContainerDescriptor asContainer(DataStorageContainerDescriptor parent,
                                                          String child) 
            throws DataStorageException;
        
        public DataStorageContainerDescriptor asContainer(DataStorageContainerDescriptor parent,
                                                          DataStorageContainerDescriptor child) 
            throws DataStorageException;

        public void setActiveContainer(DataStorageContainerDescriptor container);
        
        public DataStorageContainerDescriptor getActiveContainer();
}
