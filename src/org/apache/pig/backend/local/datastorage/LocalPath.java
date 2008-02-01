package org.apache.pig.backend.local.datastorage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.File;
import java.util.Map;
import java.util.HashMap;
import java.util.Properties;

import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.backend.datastorage.ElementDescriptor;
import org.apache.pig.backend.datastorage.SeekableInputStream;

public abstract class LocalPath implements ElementDescriptor {

    protected DataStorage fs;
    protected File path;

    protected File getCurPath() {
    	File path;
    	
    	if (this.path.isAbsolute()) {
    		path = this.path;
    	}
    	else {
    		path = new File(fs.getActiveContainer().toString(),
    						this.path.getPath());
    	}
    	
    	return path;
    }
    
    public LocalPath(LocalDataStorage fs, String path) {
        this.fs = fs;
        this.path = new File(path);
    }
    
    public LocalPath(LocalDataStorage fs, File path) {
        this.fs = fs;
        this.path = new File(path.getPath());
    }
    
    public LocalPath(LocalDataStorage fs, String parent, String child) {
        this.fs = fs;
        this.path = new File(parent, child);
    }
    
    public LocalPath(LocalDataStorage fs, File parent, File child) {
        this.fs = fs;
        this.path = new File(parent.getPath(),
        		             child.getPath());
    }
    
    public LocalPath(LocalDataStorage fs, File parent, String child) {
        this(fs, parent.getPath(), child);
    }
    
    public LocalPath(LocalDataStorage fs, String parent, File child) {
        this(fs, parent, child.getPath());
    }
    
    @Override
    public DataStorage getDataStorage() {
        return fs;
    }
    
    public File getPath() {
        return this.path;
    }

    @Override
    public abstract OutputStream create(Properties configuration) 
            throws IOException;

    @Override
    public OutputStream create() 
            throws IOException {
        return create(null);
    }

    @Override
    public abstract void copy(ElementDescriptor dstName,
                              Properties dstConfiguration,
                              boolean removeSrc) 
            throws IOException;
        
    @Override
    public void copy(ElementDescriptor dstName,
                     boolean removeSrc) throws IOException {
        copy(dstName, null, removeSrc);
    }
                
    @Override
    public abstract InputStream open() throws IOException;

    @Override
    public abstract SeekableInputStream sopen() throws IOException;
        
    @Override
    public boolean exists() throws IOException {
        return getCurPath().exists();
    }
    
    @Override
    public void rename(ElementDescriptor newName) 
            throws IOException {
        if (! this.path.renameTo(((LocalPath)newName).path)) {
            throw new IOException("Unalbe to rename " + this.path +
                                  "to " + ((LocalPath)newName).path);
        }
    }

    @Override
    public void delete() throws IOException {
        getCurPath().delete();
    }

    @Override
    public Properties getConfiguration() throws IOException {
        Properties props = new Properties();
        
        props.put(BLOCK_REPLICATION_KEY, "1");
        
        return props;
    }

    @Override
    public void updateConfiguration(Properties newConfig) 
            throws IOException {
        ;
    }
        
    @Override
    public Map<String, Object> getStatistics() throws IOException {
        Map<String, Object> stats = new HashMap<String, Object>();

        long size = this.path.length();
        stats.put(LENGTH_KEY , (new Long(size)).toString());

        long lastModified = this.path.lastModified();
        stats.put(MODIFICATION_TIME_KEY, (new Long(lastModified)).toString());
        
        return stats;
    }

    @Override
    public int compareTo(ElementDescriptor other) {
        return this.path.compareTo(((LocalPath)other).path);
    }
    
    public String toString() {
        return this.path.toString();
    }
}
