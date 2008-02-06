
package org.apache.pig.backend.hadoop.datastorage;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.conf.Configuration;

import org.apache.pig.backend.datastorage.*;

public abstract class HPath implements ElementDescriptor {

    protected Path path;
    protected HDataStorage fs;

    public HPath(HDataStorage fs, Path parent, Path child) {
        this.path = new Path(parent, child);
        this.fs = fs;
    }

    public HPath(HDataStorage fs, String parent, String child) {
        this(fs, new Path(parent), new Path(child));
    }
    
    public HPath(HDataStorage fs, Path parent, String child) {
        this(fs, parent, new Path(child));
    }

    public HPath(HDataStorage fs, String parent, Path child) {
        this(fs, new Path(parent), child);
    }
        
    public HPath(HDataStorage fs, String pathString) {
        this(fs, new Path(pathString));
    }
        
    public HPath(HDataStorage fs, Path path) {
        this.path = path;
        this.fs = fs;
    }

    public DataStorage getDataStorage() {
        return fs;
    }
    
    public abstract OutputStream create(Properties configuration) 
             throws IOException;
    
    public void copy(ElementDescriptor dstName,
                     Properties dstConfiguration,
                     boolean removeSrc)
            throws IOException {
        FileSystem srcFS = this.fs.getHFS();
        FileSystem dstFS = ((HPath)dstName).fs.getHFS();
        
        Path srcPath = this.path;
        Path dstPath = ((HPath)dstName).path;
        
        boolean result = FileUtil.copy(srcFS,
                                       srcPath,
                                       dstFS,
                                       dstPath,
                                       false,
                                       new Configuration());
        
        if (!result) {
            throw new IOException("Failed to copy from: " + this.toString() +
                                  " to: " + dstName.toString());
        }
    }
    
    public abstract InputStream open() throws IOException;

    public abstract SeekableInputStream sopen() throws IOException;

    public boolean exists() throws IOException {
        return fs.getHFS().exists(path);
    }
    
    public void rename(ElementDescriptor newName) 
             throws IOException {
        if (newName != null) {
            fs.getHFS().rename(path, ((HPath)newName).path);
        }
    }

    public void delete() throws IOException {
        // the file is removed and not placed in the trash bin
        fs.getHFS().delete(path);
    }

    public Properties getConfiguration() throws IOException {
        HConfiguration props = new HConfiguration();

        long blockSize = fs.getHFS().getFileStatus(path).getBlockSize();

        short replication = fs.getHFS().getFileStatus(path).getReplication();
        
        props.setProperty(BLOCK_SIZE_KEY, (new Long(blockSize)).toString());
        props.setProperty(BLOCK_REPLICATION_KEY, (new Short(replication)).toString());
        
        return props;
    }

    public void updateConfiguration(Properties newConfig) throws IOException {
        if (newConfig == null) {
            return;
        }
        
        String blkReplStr = newConfig.getProperty(BLOCK_REPLICATION_KEY);
        
        fs.getHFS().setReplication(path, 
                                   new Short(blkReplStr).shortValue());    
    }

    public Map<String, Object> getStatistics() throws IOException {
        HashMap<String, Object> props = new HashMap<String, Object>();
        
        Long length = new Long(fs.getHFS().getFileStatus(path).getLen());

        Long modificationTime = new Long(fs.getHFS().getFileStatus(path).
                                         getModificationTime());

        props.put(LENGTH_KEY, length.toString());
        props.put(MODIFICATION_TIME_KEY, modificationTime.toString());
        
        return props;
    }

    public OutputStream create() throws IOException {
        return create(null);
    }

    public void copy(ElementDescriptor dstName,
                     boolean removeSrc) 
            throws IOException {
        copy(dstName, null, removeSrc);
    }
    
    public Path getPath() {
        return path;
    }
    
    public FileSystem getHFS() {
        return fs.getHFS();
    }
    
    @Override
    public String toString() {
        return path.toString();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (! (obj instanceof HPath)) {
            return false;
        }
        
        return this.path.equals(((HPath)obj).path);  
    }
    
    public int compareTo(ElementDescriptor other) {
        return path.compareTo(((HPath)other).path);
    }
    
    @Override
    public int hashCode() {
        return this.path.hashCode();
    }
}
