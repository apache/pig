/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.pig.test.utils;

import java.io.RandomAccessFile;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.pig.backend.datastorage.*;

public class LocalSeekableInputStream extends SeekableInputStream {

    protected RandomAccessFile file;
    protected long curMark;
    
    public LocalSeekableInputStream(File file) throws FileNotFoundException {
        this.file = new RandomAccessFile(file, "r");
        this.curMark = 0;
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
            targetPos = this.file.getFilePointer() + offset;
            break;
        }
        case SEEK_END: {
            targetPos = this.file.length() + offset;
            break;
        }
        default: {
            throw new IOException("Invalid seek option: " + whence);
        }
        }
        
        this.file.seek(targetPos);
    }
    
    @Override
    public long tell() throws IOException {
        return this.file.getFilePointer();
    }
    
    @Override
    public int read() throws IOException {
        return this.file.read();
    }
    
    @Override
    public int read(byte[] b) throws IOException {
        return this.file.read(b);
    }
        
    @Override
    public int read(byte[] b, int off, int len ) throws IOException {
        return this.file.read(b, off, len);
    }
    
    @Override
    public int available() throws IOException {
    	return (int)( this.file.length() - this.file.getFilePointer() );
    }
    
    @Override
    public long skip(long n) throws IOException {
        long skipped = 0;
        
        if (n > 0) {
            skipped = this.file.length() - tell();

            seek(n, FLAGS.SEEK_CUR);
        }
        
        return skipped;
    }
    
    @Override
    public void close() throws IOException {
        this.file.close();
    }
    
    @Override
    public void mark(int readlimit) {
        try {
            this.curMark = tell();
        }
        catch (IOException e) {
            ;
        }
    }
    
    @Override
    public void reset() throws IOException {
        seek(this.curMark, FLAGS.SEEK_SET);
    }
    
    @Override
    public boolean markSupported() {
        return true;
    }
}
