package org.apache.pig.backend.hadoop.datastorage;

import java.net.URI;
import java.io.IOException;
import java.util.Properties;
import java.util.Enumeration;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.dfs.DistributedFileSystem;
import org.apache.hadoop.conf.Configuration;

import org.apache.pig.backend.datastorage.*;


public class HDataStorage implements DataStorage {

    
    private FileSystem fs;
    
    public HDataStorage(URI uri, Configuration conf) throws IOException {
        this(uri, new HConfiguration(conf));
    }
    
    public HDataStorage(URI uri, HConfiguration conf) throws IOException {
        fs = FileSystem.get(uri, conf.getConfiguration());
    }

    public HDataStorage(Configuration conf) throws IOException {
        this(new HConfiguration(conf));
    }
    
    public HDataStorage(HConfiguration conf) throws IOException {
        fs = FileSystem.get(conf.getConfiguration());
    }

    public void init() { }
    
    public void close() throws IOException {
        fs.close();
    }
    
    public Properties getConfiguration() {
        Properties props = new HConfiguration(fs.getConf());
                
        short defaultReplication = fs.getDefaultReplication();
        props.setProperty(DEFAULT_REPLICATION_FACTOR_KEY,
                          (new Short(defaultReplication)).toString());
        
        return props;
    }
    
    public void updateConfiguration(Properties newConfiguration) 
            throws DataStorageException {        
        if (newConfiguration == null) {
            return;
        }
        
        Enumeration<Object> newKeys = newConfiguration.keys();
        
        while (newKeys.hasMoreElements()) {
            String key = (String) newKeys.nextElement();
            String value = null;
            
            value = newConfiguration.getProperty(key);
            
            fs.getConf().set(key,value);
        }
    }
    
    public Map<String, Object> getStatistics() throws IOException {
        Map<String, Object> stats = new HashMap<String, Object>();

        long usedBytes = fs.getUsed();
        stats.put(USED_BYTES_KEY , new Long(usedBytes).toString());
        
        if (fs instanceof DistributedFileSystem) {
            DistributedFileSystem dfs = (DistributedFileSystem) fs;
            
            long rawCapacityBytes = dfs.getRawCapacity();
            stats.put(RAW_CAPACITY_KEY, new Long(rawCapacityBytes).toString());
            
            long rawUsedBytes = dfs.getRawUsed();
            stats.put(RAW_USED_KEY, new Long(rawUsedBytes).toString());
        }
        
        return stats;
    }
    
    public ElementDescriptor asElement(String name) 
            throws DataStorageException {
        if (this.isContainer(name)) {
            return new HDirectory(this, name);
        }
        else {
            return new HFile(this, name);
        }
    }
    
    public ElementDescriptor asElement(ElementDescriptor element)
            throws DataStorageException {
        return asElement(element.toString());
    }
    
    public ElementDescriptor asElement(String parent,
                                                  String child) 
            throws DataStorageException {
        return asElement((new Path(parent, child)).toString());
    }

    public ElementDescriptor asElement(ContainerDescriptor parent,
                                                  String child) 
            throws DataStorageException {
        return asElement(parent.toString(), child);
    }

    public ElementDescriptor asElement(ContainerDescriptor parent,
                                                  ElementDescriptor child) 
            throws DataStorageException {
        return asElement(parent.toString(), child.toString());
    }

    public ContainerDescriptor asContainer(String name) 
            throws DataStorageException {
        return new HDirectory(this, name);
    }
    
    public ContainerDescriptor asContainer(ContainerDescriptor container)
            throws DataStorageException {
        return new HDirectory(this, container.toString());
    }
    
    public ContainerDescriptor asContainer(String parent,
                                                      String child) 
            throws DataStorageException {
        return new HDirectory(this, parent, child);
    }

    public ContainerDescriptor asContainer(ContainerDescriptor parent,
                                                      String child) 
            throws DataStorageException {
        return new HDirectory(this, parent.toString(), child);
    }
    
    public ContainerDescriptor asContainer(ContainerDescriptor parent,
                                                      ContainerDescriptor child)
            throws DataStorageException {
        return new HDirectory(this, parent.toString(), child.toString());
    }
    
    public void setActiveContainer(ContainerDescriptor container) {
        fs.setWorkingDirectory(new Path(container.toString()));
    }
    
    public ContainerDescriptor getActiveContainer() {
        return new HDirectory(this, fs.getWorkingDirectory());
    }

    public boolean isContainer(String name) throws DataStorageException {
        boolean isContainer = false;
        Path path = new Path(name);
        
        try {
            if ((this.fs.exists(path)) && (! this.fs.isFile(path))) {
                isContainer = true;
            }
        }
        catch (IOException e) {
            throw new DataStorageException("Unable to check name " + name, e);
        }
        
        return isContainer;
    }
    
    public HPath[] asCollection(String pattern) throws DataStorageException {
        try {
            Path[] paths = this.fs.globPaths(new Path(pattern));
            
            HPath[] hpaths = new HPath[ paths.length ];
            
            for (int i = 0; i < paths.length; ++i) {
                hpaths[ i ] = ((HPath)this.asElement(paths[ i ].toString()));
            }
            
            
            return hpaths;
        }
        catch (IOException e) {
            throw new DataStorageException("Failed to obtain glob for " + pattern, e);
        }
    }
    
    public FileSystem getHFS() {
        return fs;
    }
}
