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

package org.apache.pig.backend.hadoop.datastorage;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;

import org.apache.pig.PigException;
import org.apache.pig.backend.datastorage.SeekableInputStream;
import org.apache.pig.backend.executionengine.ExecException;

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
            int errCode = 2098;
            String msg = "Invalid seek option: " + whence;
            throw new ExecException(msg, errCode, PigException.BUG);
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
