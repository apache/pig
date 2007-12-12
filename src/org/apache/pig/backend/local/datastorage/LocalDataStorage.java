package org.apache.pig.backend.local.datastorage;

import java.io.IOException;
import java.io.File;
import java.util.Properties;

import org.apache.pig.backend.datastorage.*;

public class LocalDataStorage implements DataStorage {

    protected File workingDir;
    
    public LocalDataStorage() {
        workingDir = new File(System.getProperty("user.dir"));
    }
    
    @Override
    public void init() {
        ;
    }

    @Override
    public void close() throws IOException {
        ;
    }
    
    @Override
    public Properties getConfiguration() {
        Properties config = new Properties();
        
        config.put(DEFAULT_REPLICATION_FACTOR_KEY, (new Integer(1)).toString());
        
        return config;
    }
    
    @Override
    public void updateConfiguration(Properties newConfiguration) 
         throws DataStorageException {
        ;
    }
    
    @Override
    public Properties getStatistics() throws IOException {
        Properties stats = new Properties();
        
        //TODO determine used bytes in order to set the value of the
        // key USED_BYTES_KEY
        
        return stats;
    }
            
    @Override
    public LocalFile asElement(String name) 
            throws DataStorageException {
        return new LocalFile(this, name);
    }

    @Override
    public LocalFile asElement(DataStorageElementDescriptor element)
            throws DataStorageException {
        return new LocalFile(this, element.toString());
    }
    
    @Override
    public LocalFile asElement(String parent,
                               String child) 
            throws DataStorageException {
        return new LocalFile(this,parent, child);
    }

    @Override
    public LocalFile asElement(DataStorageContainerDescriptor parent,
                               String child) 
            throws DataStorageException {
        return new LocalFile(this, parent.toString(), child);
    }

    @Override
    public LocalFile asElement(DataStorageContainerDescriptor parent,
                               DataStorageElementDescriptor child)
            throws DataStorageException {
        return new LocalFile(this, parent.toString(), child.toString());
    }
    
    @Override
    public LocalDir asContainer(String name) 
            throws DataStorageException {
        return new LocalDir(this, name);
    }
    
    @Override
    public LocalDir asContainer(DataStorageContainerDescriptor container)
            throws DataStorageException {
        return new LocalDir(this, container.toString());
    }
    
    @Override
    public LocalDir asContainer(String parent,
                                String child) 
            throws DataStorageException {
        return new LocalDir(this, parent, child);
    }

    @Override
    public LocalDir asContainer(DataStorageContainerDescriptor parent,
                                String child) 
            throws DataStorageException {
        return new LocalDir(this, parent.toString(), child);
    }
    
    @Override
    public LocalDir asContainer(DataStorageContainerDescriptor parent,
                                DataStorageContainerDescriptor child) 
        throws DataStorageException {
        return new LocalDir(this, parent.toString(), child.toString());
    }

    @Override
    public void setActiveContainer(DataStorageContainerDescriptor container) {
    	this.workingDir = new File(container.toString());
    }
    
    @Override
    public DataStorageContainerDescriptor getActiveContainer() {
        return new LocalDir(this, this.workingDir.getPath());
    }
    
    public File getWorkingDir() {
        return this.workingDir;
    }
}
