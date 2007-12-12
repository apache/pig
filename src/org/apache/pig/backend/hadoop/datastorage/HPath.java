
package org.apache.pig.backend.hadoop.datastorage;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

import org.apache.pig.backend.datastorage.*;

public abstract class HPath implements DataStorageElementDescriptor {

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

    @Override
    public DataStorage getDataStorage() {
        return fs;
    }
    
    @Override
    public abstract OutputStream create(Properties configuration) 
             throws IOException;
    
    @Override
    public abstract void copy(DataStorageElementDescriptor dstName,
                              Properties dstConfiguration,
                              boolean removeSrc)
        throws IOException;
    
    @Override
    public abstract InputStream open() throws IOException;

    @Override
    public abstract SeekableInputStream sopen() throws IOException;

    @Override
    public boolean exists() throws IOException {
        return fs.getHFS().exists(path);
    }
    
    @Override
    public void rename(DataStorageElementDescriptor newName) 
             throws IOException {
        if (newName != null) {
            fs.getHFS().rename(path, ((HPath)newName).path);
        }
    }

    @Override
    public void delete() throws IOException {
        fs.getHFS().delete(path);
    }

    @Override
    public Properties getConfiguration() throws IOException {
        HConfiguration props = new HConfiguration();

        long blockSize = fs.getHFS().getFileStatus(path).getBlockSize();

        short replication = fs.getHFS().getFileStatus(path).getReplication();
        
        props.setProperty(BLOCK_SIZE_KEY, (new Long(blockSize)).toString());
        props.setProperty(BLOCK_REPLICATION_KEY, (new Short(replication)).toString());
        
        return props;
    }

    @Override
    public void updateConfiguration(Properties newConfig) throws IOException {
        if (newConfig == null) {
            return;
        }
        
        String blkReplStr = newConfig.getProperty(BLOCK_REPLICATION_KEY);
        
        fs.getHFS().setReplication(path, 
                                   new Short(blkReplStr).shortValue());    
    }

    @Override
    public Properties getStatistics() throws IOException {
        HConfiguration props = new HConfiguration();
        
        Long length = new Long(fs.getHFS().getFileStatus(path).getLen());

        Long modificationTime = new Long(fs.getHFS().getFileStatus(path).
                                         getModificationTime());

        props.setProperty(LENGTH_KEY, length.toString());
        props.setProperty(MODIFICATION_TIME_KEY, modificationTime.toString());
        
        return props;
    }

    @Override
    public int compareTo(DataStorageElementDescriptor other) {
        return path.compareTo(other);
    }

    @Override
    public OutputStream create() throws IOException {
        return create(null);
    }

    @Override
    public void copy(DataStorageElementDescriptor dstName,
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
    
    public String toString() {
        return path.toString();
    }
}
