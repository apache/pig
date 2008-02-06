package org.apache.pig.backend.local.datastorage;

import java.io.IOException;
import java.io.File;
import java.util.Properties;
import java.util.Map;

import org.apache.pig.backend.datastorage.*;

public class LocalDataStorage implements DataStorage {

    protected File workingDir;
    
    public LocalDataStorage() {
        workingDir = new File(System.getProperty("user.dir"));
    }
    
    public void init() {
        ;
    }

    public void close() throws IOException {
        ;
    }
    
    public Properties getConfiguration() {
        Properties config = new Properties();
        
        config.put(DEFAULT_REPLICATION_FACTOR_KEY, (new Integer(1)).toString());
        
        return config;
    }
    
    public void updateConfiguration(Properties newConfiguration) 
         throws DataStorageException {
        ;
    }
    
    public Map<String, Object> getStatistics() throws IOException {
        throw new UnsupportedOperationException();
    }
            
    public LocalPath asElement(String name) 
            throws DataStorageException {
        if (this.isContainer(name)) {
            return new LocalDir(this, name);
        }
        else {            
            return new LocalFile(this, name);
        }
    }

    public LocalPath asElement(ElementDescriptor element)
            throws DataStorageException {
        return asElement(element.toString());
    }
    
    public LocalPath asElement(String parent,
                               String child) 
            throws DataStorageException {
        return asElement((new File(parent, child).toString()));
    }

    public LocalPath asElement(ContainerDescriptor parent,
                               String child) 
            throws DataStorageException {
        return asElement(parent.toString(), child);
    }

    public LocalPath asElement(ContainerDescriptor parent,
                               ElementDescriptor child)
            throws DataStorageException {
        return asElement(parent.toString(), child.toString());
    }
    
    public LocalDir asContainer(String name) 
            throws DataStorageException {
        return new LocalDir(this, name);
    }
    
    public LocalDir asContainer(ContainerDescriptor container)
            throws DataStorageException {
        return new LocalDir(this, container.toString());
    }
    
    public LocalDir asContainer(String parent,
                                String child) 
            throws DataStorageException {
        return new LocalDir(this, parent, child);
    }

    public LocalDir asContainer(ContainerDescriptor parent,
                                String child) 
            throws DataStorageException {
        return new LocalDir(this, parent.toString(), child);
    }
    
    public LocalDir asContainer(ContainerDescriptor parent,
                                ContainerDescriptor child) 
        throws DataStorageException {
        return new LocalDir(this, parent.toString(), child.toString());
    }

    public boolean isContainer(String name) throws DataStorageException {
        boolean isContainer = false;
        File file = new File(name);
        
        if (file.exists() && file.isDirectory()) {
            isContainer = true;
        }
        
        return isContainer;
    }
    
    public void setActiveContainer(ContainerDescriptor container) {
        this.workingDir = new File(container.toString());
    }
    
    public ContainerDescriptor getActiveContainer() {
        return new LocalDir(this, this.workingDir.getPath());
    }
    
    public LocalPath[] asCollection(String pattern) throws DataStorageException {
        throw new UnsupportedOperationException();
    }
    
    public File getWorkingDir() {
        return this.workingDir;
    }
}
