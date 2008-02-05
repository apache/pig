
package org.apache.pig.backend.local.datastorage;

import java.io.File;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.util.Properties;

import org.apache.pig.backend.datastorage.*;

public class LocalFile extends LocalPath {

    public LocalFile(LocalDataStorage fs, String path) {
        super(fs, path);
    }
    
    public LocalFile(LocalDataStorage fs, File path) {
        super(fs, path);
    }

    public LocalFile(LocalDataStorage fs, String parent, String child) {
        super(fs, parent, child);
    }
    
    public LocalFile(LocalDataStorage fs, File parent, File child) {
        super(fs,
              parent.getPath(),
              child.getPath());
    }
    
    public LocalFile(LocalDataStorage fs, File parent, String child) {
        this(fs, parent.getPath(), child);
    }
    
    public LocalFile(LocalDataStorage fs, String parent, File child) {
        this(fs, parent, child.getPath());
    }
        
    @Override
    public OutputStream create(Properties configuration) 
            throws IOException {
        if (! getCurPath().createNewFile()) {
            throw new IOException("Failed to create file " + this.path);
        }
        
        return new FileOutputStream(getCurPath());
    }    
    
    @Override
    public void copy(ElementDescriptor dstName,
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
            if (dstName instanceof ContainerDescriptor) {
                try {
                    dstName = dstName.getDataStorage().
                                      asElement((ContainerDescriptor) dstName,
                                                path.getName());
                }
                catch (DataStorageException e) {
                    IOException ioe = new IOException("Unable to generate element name (src: " + 
                                           this + ", dst: " + dstName + ")");
                    ioe.initCause(e);
                    throw ioe;
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

    public InputStream open (Properties configuration) throws IOException {
    	return open();
    }
    
    public InputStream open () throws IOException {
        return new FileInputStream(this.path);
    }
    
    public SeekableInputStream sopen(Properties configuration) throws IOException {
    	return sopen();
    }
    
    public SeekableInputStream sopen() throws IOException {
        try {
            return new LocalSeekableInputStream(this.path);
        }
        catch (FileNotFoundException e) {
            IOException ioe = new IOException("Unable to find " + this.path);
            ioe.initCause(e);
            throw ioe;
        }
    }
}
