package org.apache.pig.backend.hadoop.datastorage;

import java.net.URI;
import java.io.IOException;
import java.util.Properties;
import java.util.Enumeration;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

    @Override
    public void init() { }
    
    @Override
    public void close() throws IOException {
        fs.close();
    }
    
    @Override
    public Properties getConfiguration() {
        Properties props = new HConfiguration(fs.getConf());
                
        short defaultReplication = fs.getDefaultReplication();
        props.setProperty(DEFAULT_REPLICATION_FACTOR_KEY,
                          (new Short(defaultReplication)).toString());
        
        return props;
    }
    
    @Override
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
    
    @Override
    public Properties getStatistics() throws IOException {
        Properties stats = new Properties();
        long bytes = fs.getUsed();
        
        stats.setProperty(USED_BYTES_KEY , new Long(bytes).toString());
        
        return stats;
    }
    
    @Override
    public DataStorageElementDescriptor asElement(String name) 
            throws DataStorageException {
        return new HFile(this, name);
    }
    
    @Override
    public DataStorageElementDescriptor asElement(DataStorageElementDescriptor element)
            throws DataStorageException {
        return new HFile(this, element.toString());
    }
    
    @Override
    public DataStorageElementDescriptor asElement(String parent,
                                                  String child) 
            throws DataStorageException {
        return new HFile(this, parent, child);
    }

    @Override
    public DataStorageElementDescriptor asElement(DataStorageContainerDescriptor parent,
                                                  String child) 
            throws DataStorageException {
        return new HFile(this, parent.toString(), child);
    }

    @Override
    public DataStorageElementDescriptor asElement(DataStorageContainerDescriptor parent,
                                                  DataStorageElementDescriptor child) 
            throws DataStorageException {
        return new HFile(this, parent.toString(), child.toString());
    }

    @Override
    public DataStorageContainerDescriptor asContainer(String name) 
            throws DataStorageException {
        return new HDirectory(this, name);
    }
    
    @Override
    public DataStorageContainerDescriptor asContainer(DataStorageContainerDescriptor container)
            throws DataStorageException {
        return new HDirectory(this, container.toString());
    }
    
    @Override
    public DataStorageContainerDescriptor asContainer(String parent,
                                                      String child) 
            throws DataStorageException {
        return new HDirectory(this, parent, child);
    }

    @Override
    public DataStorageContainerDescriptor asContainer(DataStorageContainerDescriptor parent,
                                                      String child) 
            throws DataStorageException {
        return new HDirectory(this, parent.toString(), child);
    }
    
    @Override
    public DataStorageContainerDescriptor asContainer(DataStorageContainerDescriptor parent,
                                                      DataStorageContainerDescriptor child)
            throws DataStorageException {
        return new HDirectory(this, parent.toString(), child.toString());
    }
    
    @Override
    public void setActiveContainer(DataStorageContainerDescriptor container) {
        fs.setWorkingDirectory(new Path(container.toString()));
    }
    
    @Override
    public DataStorageContainerDescriptor getActiveContainer() {
        return new HDirectory(this, fs.getWorkingDirectory());
    }

    public FileSystem getHFS() {
        return fs;
    }
}
