package org.apache.pig.backend.hadoop.datastorage;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Properties;

import org.apache.hadoop.fs.Path;

import org.apache.pig.backend.datastorage.*;

public class HDirectory extends HPath
                        implements DataStorageContainerDescriptor {

    public HDirectory(HDataStorage fs, Path parent, Path child) {
        super(fs, parent, child);
    }

    public HDirectory(HDataStorage fs, String parent, String child) {
        super(fs, parent, child);
    }
    
    public HDirectory(HDataStorage fs, Path parent, String child) {
        super(fs, parent, child);
    }

    public HDirectory(HDataStorage fs, String parent, Path child) {
        super(fs, parent, child);
    }
        
    public HDirectory(HDataStorage fs, Path path) {
        super(fs, path);
    }
    
    public HDirectory(HDataStorage fs, String pathString) {
        super(fs, pathString);
    }

    @Override
    public OutputStream create(Properties configuration) 
            throws IOException {
        fs.getHFS().mkdirs(path);
        
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
                                                             ((HPath)curElem).getPath().getName());
                    
                    curElem.copy(dst, dstConfiguration, removeSrc);
                    
                    if (removeSrc) {
                        curElem.delete();
                    }
                }
                else {
                    DataStorageElementDescriptor dst = 
                        dstName.getDataStorage().asElement(dstName,
                                                           ((HPath)curElem).getPath().getName());
                    
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

    public Iterator<DataStorageElementDescriptor> iterator() {
        LinkedList<DataStorageElementDescriptor> elements =
            new LinkedList<DataStorageElementDescriptor>();
        
        try {
            Path[] paths = fs.getHFS().listPaths(new Path[]{path});
            
            for (Path p : paths) {
                if (fs.getHFS().isFile(p)) {
                    elements.add(fs.asElement(p.toString()));
                }
                else {
                    elements.add(fs.asContainer(p.toString()));                    
                }
            }
        }
        catch (IOException e) {
        }
        catch (DataStorageException e) {
        }
        
        return elements.iterator();
    }
}
