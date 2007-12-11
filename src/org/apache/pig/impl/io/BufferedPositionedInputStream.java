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

package org.apache.pig.impl.io;

import java.io.IOException;
import java.io.InputStream;

import org.apache.tools.bzip2r.CBZip2InputStream;

public class BufferedPositionedInputStream extends InputStream {
	long pos;
    InputStream in;
    
	public BufferedPositionedInputStream(InputStream in, long pos) {
		this.in = in;
		this.pos = pos;
	}
	
	public BufferedPositionedInputStream(InputStream in){
		this(in,0);
	}
	
	@Override
	public int read() throws IOException {
        int c = in.read();
        pos++;
        return c;
	}

    @Override
    public int read(byte b[], int off, int len) throws IOException {
        int read = in.read(b, off, len);
        pos += read;
        return read;
    }
	
    @Override
	public long skip(long n) throws IOException {
        long rc = in.skip(n);
        pos += rc;
        return rc;
    }
    
    /**
     * Returns the current position in the tracked InputStream.
     */
    public long getPosition() throws IOException{
        if (in instanceof CBZip2InputStream)
        	return ((CBZip2InputStream)in).getPos();
    	return pos;
    }


}
