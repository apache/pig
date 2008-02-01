package org.apache.pig.backend.hadoop.datastorage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

import org.apache.hadoop.fs.Path;

import org.apache.pig.backend.datastorage.ElementDescriptor;
import org.apache.pig.backend.datastorage.ContainerDescriptor;
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
    public InputStream open(Properties configuration) throws IOException {
        return open();
    }
    
    @Override
    public InputStream open() throws IOException {
        return fs.getHFS().open(path);
    }

    @Override
    public SeekableInputStream sopen(Properties configuration) throws IOException {
    	return sopen();
    }
    
	@Override
    public SeekableInputStream sopen() throws IOException {
        return new HSeekableInputStream(fs.getHFS().open(path),
                                        fs.getHFS().getContentLength(path));
    }
}
