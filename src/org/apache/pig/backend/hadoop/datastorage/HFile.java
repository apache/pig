package org.apache.pig.backend.hadoop.datastorage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

import org.apache.hadoop.fs.Path;

import org.apache.pig.backend.datastorage.DataStorageElementDescriptor;
import org.apache.pig.backend.datastorage.DataStorageContainerDescriptor;
import org.apache.pig.backend.datastorage.DataStorageException;
import org.apache.pig.backend.datastorage.SeekableInputStream;

public class HFile extends HPath {
    
    public HFile(HDataStorage fs, Path parent, Path child) {
        super(fs, parent, child);
    }

    public HFile(HDataStorage fs, String parent, String child) {
        super(fs, parent, child);
    }
    
    public HFile(HDataStorage fs, Path parent, String child) {
        super(fs, parent, child);
    }

    public HFile(HDataStorage fs, String parent, Path child) {
        super(fs, parent, child);
    }
        
    public HFile(HDataStorage fs, String pathString) {
        super(fs, pathString);
    }
        
    public HFile(HDataStorage fs, Path path) {
        super(fs, path);
    }
    
    @Override
    public OutputStream create(Properties configuration) 
             throws IOException {
        return fs.getHFS().create(path, false);
    }
    
    @Override
    public void copy(DataStorageElementDescriptor dstName,
                     Properties dstConfiguration,
                     boolean removeSrc) 
            throws IOException {
        if (dstName == null) {
            return;
        }
        
        if (!exists()) {
            throw new IOException("Source does not exist " +
                                  this);
        }

        if (dstName.exists()) {
            if (dstName instanceof DataStorageContainerDescriptor) {
                try {
                    dstName = dstName.getDataStorage().
                                      asElement((DataStorageContainerDescriptor) dstName,
                                                getPath().getName());
                }
                catch (DataStorageException e) {
                    throw new IOException("Unable to generate element name (src: " + 
                                           this + ", dst: " + dstName + ")",
                                          e);
                }
            }
        }
        
        InputStream in = null;
        OutputStream out = null;
        
        in = this.open();
        out = dstName.create(dstConfiguration);
            
        byte[] data = new byte[4 * 1024];
        int bc;
        while((bc = in.read(data)) != -1) {
            out.write(data, 0, bc);
        }
        
        out.close();
            
        if (removeSrc) {
            delete();
        }
    }
    
    @Override
    public InputStream open() throws IOException {
        return fs.getHFS().open(path);
    }

    @Override
    public SeekableInputStream sopen() throws IOException {
        return new HSeekableInputStream(fs.getHFS().open(path),
                                        fs.getHFS().getContentLength(path));
    }
}
