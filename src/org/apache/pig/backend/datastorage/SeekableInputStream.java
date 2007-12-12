package org.apache.pig.backend.datastorage;

import java.io.InputStream;
import java.io.IOException;

public abstract class SeekableInputStream extends InputStream {
    
    public enum FLAGS {
        SEEK_SET,
        SEEK_CUR,
        SEEK_END,
    }
    
    /**
     * Seeks to a given offset as specified by whence flags.
     * If whence is SEEK_SET, offset is added to beginning of stream
     * If whence is SEEK_CUR, offset is added to current position inside stream
     * If whence is SEEK_END, offset is added to end of file position
     * 
     * @param offset
     * @param whence
     * @throws IOException
     */
    public abstract void seek(long offset, FLAGS whence) throws IOException;
    
    /**
     * Returns current offset
     * 
     * @return offset
     * @throws IOException
     */
    public abstract long tell() throws IOException;
}
