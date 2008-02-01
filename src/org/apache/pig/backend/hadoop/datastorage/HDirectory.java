package org.apache.pig.backend.hadoop.datastorage;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;

import org.apache.pig.backend.datastorage.*;

public class HDirectory extends HPath
                        implements ContainerDescriptor {

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
                                                             ((HPath)curElem).getPath().getName());
                    
                    curElem.copy(dst, dstConfiguration, removeSrc);
                    
                    if (removeSrc) {
                        curElem.delete();
                    }
                }
                else {
                    ElementDescriptor dst = 
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
    public InputStream open(Properties configuration) throws IOException {
    	return open();
    }
    
    @Override
    public InputStream open() throws IOException {
        throw new IOException("Cannot open dir " + path);
    }

    @Override
    public SeekableInputStream sopen(Properties configuration) throws IOException {
    	return sopen();
    }
    
    @Override
    public SeekableInputStream sopen() throws IOException {
        throw new IOException("Cannot sopen dir " + path);
    }

    public Iterator<ElementDescriptor> iterator() {
        LinkedList<ElementDescriptor> elements =
            new LinkedList<ElementDescriptor>();

	try {
	    FileStatus fileStat[] = fs.getHFS().listStatus(path);
	    for (int j = 0; j < fileStat.length; ++j) {
               if (fileStat[j].isDir()) {
	           elements.add(fs.asContainer(fileStat[j].getPath().toString()));
	       }
	       else {
	           elements.add(fs.asElement(fileStat[j].getPath().toString()));
               }
            }
        }
        catch (IOException e) {
        }
        catch (DataStorageException e) {
        }

/*        
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
*/        
        return elements.iterator();
    }
}
