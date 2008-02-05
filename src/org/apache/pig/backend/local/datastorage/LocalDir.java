package org.apache.pig.backend.local.datastorage;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Iterator;

import org.apache.pig.backend.datastorage.ElementDescriptor;
import org.apache.pig.backend.datastorage.ContainerDescriptor;
import org.apache.pig.backend.datastorage.DataStorageException;
import org.apache.pig.backend.datastorage.ImmutableOutputStream;
import org.apache.pig.backend.datastorage.SeekableInputStream;

public class LocalDir extends LocalPath
                      implements ContainerDescriptor {

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
    public void copy(ElementDescriptor dstName,
                     Properties dstConfiguration,
                     boolean removeSrc) 
            throws IOException {
    	try {
    		if (! dstName.getDataStorage().isContainer(dstName.toString())) {
    			dstName = dstName.getDataStorage().asContainer(dstName.toString());
    		}
    	}
    	catch (DataStorageException e) {
    		IOException ioe = new IOException("Failed to get container for " + dstName.toString());
    		ioe.initCause(e);
    		throw ioe;
    	}

        copy((ContainerDescriptor) dstName,
                dstConfiguration,
                removeSrc);
       }
       
       
    public void copy(ContainerDescriptor dstName,
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
           
        Iterator<ElementDescriptor> elems = iterator();
       
        try {
            while (elems.hasNext()) {
                ElementDescriptor curElem = elems.next();
               
                if (curElem instanceof ContainerDescriptor) {
                    ContainerDescriptor dst =
                        dstName.getDataStorage().asContainer(dstName,
                                                             ((LocalPath)curElem).getPath().getName());
                       
                    curElem.copy(dst, dstConfiguration, removeSrc);
                       
                    if (removeSrc) {
                        curElem.delete();
                    }
                }
                else {
                    ElementDescriptor dst = 
                        dstName.getDataStorage().asElement(dstName,
                                                 ((LocalPath)curElem).getPath().getName());
                       
                    curElem.copy(dst, dstConfiguration, removeSrc);
                }
            }
        }
        catch (DataStorageException e) {
            IOException ioe = new IOException("Failed to copy " + this + " to " + dstName);
            ioe.initCause(e);
            throw ioe;
        }

        if (removeSrc) {
            delete();
        }
    }

    public InputStream open(Properties configuration) throws IOException {
    	return open();
    }
    
    public InputStream open() throws IOException {
        throw new IOException("Cannot open dir " + path);
    }

    public SeekableInputStream sopen(Properties configuration) throws IOException {
    	return sopen();
    }
    
    public SeekableInputStream sopen() throws IOException {
        throw new IOException("Cannot sopen dir " + path);
    }

    public Iterator<ElementDescriptor> iterator() {
        LinkedList<ElementDescriptor> elements =
            new LinkedList<ElementDescriptor>();
        
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
