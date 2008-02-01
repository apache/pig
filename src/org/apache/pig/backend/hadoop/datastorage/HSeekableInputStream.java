package org.apache.pig.backend.hadoop.datastorage;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;

import org.apache.pig.backend.datastorage.SeekableInputStream;

public class HSeekableInputStream extends SeekableInputStream {

    protected FSDataInputStream input;
    protected long contentLength;
    
    HSeekableInputStream(FSDataInputStream input,
                         long contentLength) {
        this.input = input;
        this.contentLength = contentLength;
    }
    
    @Override
    public void seek(long offset, FLAGS whence) throws IOException {
        long targetPos;
        
        switch (whence) {
        case SEEK_SET: {
            targetPos = offset;
            break;
        }
        case SEEK_CUR: {
            targetPos = input.getPos() + offset;
            break;
        }
        case SEEK_END: {
            targetPos = contentLength + offset;
            break;
        }
        default: {
            throw new IOException("Invalid seek option: " + whence);
        }
        }
        
        input.seek(targetPos);
    }
    
    @Override
    public long tell() throws IOException {
        return input.getPos();
    }
    
    @Override
    public int read() throws IOException {
        return input.read();
    }
    
    @Override
    public int read(byte[] b) throws IOException {
        return input.read(b);
    }
        
    @Override
    public int read(byte[] b, int off, int len ) throws IOException {
        return input.read(b, off, len);
    }
    
    @Override
    public int available() throws IOException {
        return input.available();
    }
    
    @Override
    public long skip(long n) throws IOException {
        return input.skip(n);
    }
    
    @Override
    public void close() throws IOException {
        input.close();
    }
    
    @Override
    public void mark(int readlimit) {
        input.mark(readlimit);
    }
    
    @Override
    public void reset() throws IOException {
        input.reset();
    }
    
    @Override
    public boolean markSupported() {
        return input.markSupported();
    }
}
