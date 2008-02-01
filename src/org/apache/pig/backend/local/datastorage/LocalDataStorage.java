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
    public Map<String, Object> getStatistics() throws IOException {
    	throw new UnsupportedOperationException();
    }
            
    @Override
    public LocalPath asElement(String name) 
            throws DataStorageException {
        if (this.isContainer(name)) {
            return new LocalDir(this, name);
        }
        else {            
            return new LocalFile(this, name);
        }
    }

    @Override
    public LocalPath asElement(ElementDescriptor element)
            throws DataStorageException {
        return asElement(element.toString());
    }
    
    @Override
    public LocalPath asElement(String parent,
                               String child) 
            throws DataStorageException {
        return asElement((new File(parent, child).toString()));
    }

    @Override
    public LocalPath asElement(ContainerDescriptor parent,
                               String child) 
            throws DataStorageException {
        return asElement(parent.toString(), child);
    }

    @Override
    public LocalPath asElement(ContainerDescriptor parent,
                               ElementDescriptor child)
            throws DataStorageException {
        return asElement(parent.toString(), child.toString());
    }
    
    @Override
    public LocalDir asContainer(String name) 
            throws DataStorageException {
        return new LocalDir(this, name);
    }
    
    @Override
    public LocalDir asContainer(ContainerDescriptor container)
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
    public LocalDir asContainer(ContainerDescriptor parent,
                                String child) 
            throws DataStorageException {
        return new LocalDir(this, parent.toString(), child);
    }
    
    @Override
    public LocalDir asContainer(ContainerDescriptor parent,
                                ContainerDescriptor child) 
        throws DataStorageException {
        return new LocalDir(this, parent.toString(), child.toString());
    }

    @Override
    public boolean isContainer(String name) throws DataStorageException {
        boolean isContainer = false;
        File file = new File(name);
        
        if (file.exists() && file.isDirectory()) {
            isContainer = true;
        }
        
        return isContainer;
    }
    
    @Override
    public void setActiveContainer(ContainerDescriptor container) {
    	this.workingDir = new File(container.toString());
    }
    
    @Override
    public ContainerDescriptor getActiveContainer() {
        return new LocalDir(this, this.workingDir.getPath());
    }
    
    @Override
    public LocalPath[] asCollection(String pattern) throws DataStorageException {
    	throw new UnsupportedOperationException();
    }
    
    public File getWorkingDir() {
        return this.workingDir;
    }
}
