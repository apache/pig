package org.apache.pig.backend.local.datastorage;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Iterator;

import org.apache.pig.backend.datastorage.DataStorageElementDescriptor;
import org.apache.pig.backend.datastorage.DataStorageContainerDescriptor;
import org.apache.pig.backend.datastorage.DataStorageException;
import org.apache.pig.backend.datastorage.ImmutableOutputStream;
import org.apache.pig.backend.datastorage.SeekableInputStream;

public class LocalDir extends LocalPath
                      implements DataStorageContainerDescriptor {

    public LocalDir(LocalDataStorage fs, String path) {
        super(fs, path);
    }
    
    public LocalDir(LocalDataStorage fs, File path) {
        super(fs, path);
    }

    public LocalDir(LocalDataStorage fs, String parent, String child) {
        super(fs, parent, child);
    }
    
    public LocalDir(LocalDataStorage fs, File parent, File child) {
        super(fs,
              parent.getPath(),
              child.getPath());
    }
    
    public LocalDir(LocalDataStorage fs, File parent, String child) {
        this(fs, parent.getPath(), child);
    }
    
    public LocalDir(LocalDataStorage fs, String parent, File child) {
        this(fs, parent, child.getPath());
    }
    
    @Override
    public OutputStream create(Properties configuration) 
            throws IOException {
        if (! getCurPath().mkdirs()) {
            throw new IOException("Unable to create dirs for: " + this.path);
        }
        
        return new ImmutableOutputStream(path.toString());
    }

    @Override
    public void copy(DataStorageElementDescriptor dstName,
                     Properties dstConfiguration,
                     boolean removeSrc) 
            throws IOException {
        copy((DataStorageContainerDescriptor) dstName,
                dstConfiguration,
                removeSrc);
       }
       
       
    public void copy(DataStorageContainerDescriptor dstName,
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
            throw new IOException("Destination already exists " +
                                  dstName);
        }
           
        dstName.create();
           
        Iterator<DataStorageElementDescriptor> elems = iterator();
       
        try {
            while (elems.hasNext()) {
                DataStorageElementDescriptor curElem = elems.next();
               
                if (curElem instanceof DataStorageContainerDescriptor) {
                    DataStorageContainerDescriptor dst =
                        dstName.getDataStorage().asContainer(dstName,
                                                             ((LocalPath)curElem).getPath().getName());
                       
                    curElem.copy(dst, dstConfiguration, removeSrc);
                       
                    if (removeSrc) {
                        curElem.delete();
                    }
                }
                else {
                    DataStorageElementDescriptor dst = 
                        dstName.getDataStorage().asElement(dstName,
                                                 ((LocalPath)curElem).getPath().getName());
                       
                    curElem.copy(dst, dstConfiguration, removeSrc);
                }
            }
        }
        catch (DataStorageException e) {
            throw new IOException("Failed to copy " + this + " to " + dstName, e);
        }

        if (removeSrc) {
            delete();
        }
    }

    @Override
    public InputStream open() throws IOException {
        throw new IOException("Cannot open dir " + path);
    }

    @Override
    public SeekableInputStream sopen() throws IOException {
        throw new IOException("Cannot sopen dir " + path);
    }

    @Override
    public Iterator<DataStorageElementDescriptor> iterator() {
        LinkedList<DataStorageElementDescriptor> elements =
            new LinkedList<DataStorageElementDescriptor>();
        
        try {
            File[] files = getCurPath().listFiles();
            
            for (File f : files) {
                if (f.isFile()) {
                    elements.add(fs.asElement(f.getPath()));
                }
                else {
                    elements.add(fs.asContainer(f.getPath()));                    
                }
            }
        }
        catch (DataStorageException e) {
        }
        
        return elements.iterator();
    }
}
