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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;

import org.apache.tools.bzip2r.CBZip2InputStream;

public class BufferedPositionedInputStream extends InputStream {
    long pos;
    InputStream in;
    static final int bufSize = 1024;
    
    public BufferedPositionedInputStream(InputStream in, long pos) {
		// Don't buffer a bzip stream as it will cause problems for split
		// records.
		if (in instanceof CBZip2InputStream) this.in = in;
		else this.in = new BufferedInputStream(in, bufSize);
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

    /*
     * Preallocated array for readline buffering
     */
    private byte barray[] = new byte[1024];

    /*
     * Preallocated ByteBuffer for readline buffering
     */
    private ByteBuffer bbuff = ByteBuffer.wrap(barray);

    /*
     * Preallocated char array for decoding bytes
     */
    private char carray[] = new char[1024];

    /*
     * Preallocated CharBuffer for decoding bytes
     */
    private CharBuffer cbuff = CharBuffer.wrap(carray);

    public String readLine(Charset charset, byte delimiter) throws IOException {
        CharsetDecoder decoder = charset.newDecoder();
        decoder.onMalformedInput(CodingErrorAction.REPLACE);
        decoder.onUnmappableCharacter(CodingErrorAction.REPLACE);
        int delim = delimiter&0xff;
        int rc;
        int offset = 0;
        StringBuilder sb = null;
        CoderResult res;
        while ((rc = read())!=-1) {
            if (rc == delim) {
                break;
            }
            barray[offset++] = (byte)rc;
            if (barray.length == offset) {
                bbuff.position(0);
                bbuff.limit(barray.length);
                cbuff.position(0);
                cbuff.limit(carray.length);
                res = decoder.decode(bbuff, cbuff, false);
                if (res.isError()) {
                    throw new IOException("Decoding error: " + res.toString());
                }
                offset = bbuff.remaining();
                switch (offset) {
                default:
                    System.arraycopy(barray, bbuff.position(), barray, 0, bbuff
                            .remaining());
                    break;
                case 2:
                    barray[1] = barray[barray.length - 1];
                    barray[0] = barray[barray.length - 2];
                    break;
                case 1:
                    barray[0] = barray[barray.length - 1];
                    break;
                case 0:
                }
                if (sb == null) {
                    sb = new StringBuilder(cbuff.position());
                }
                sb.append(carray, 0, cbuff.position());
            }
        }
        if (sb == null) {
            if (rc == -1 && offset == 0) {
                // We are at EOF with nothing read
                return null;
            }
            sb = new StringBuilder();
        }
        bbuff.position(0);
        bbuff.limit(offset);
        cbuff.position(0);
        cbuff.limit(carray.length);
        res = decoder.decode(bbuff, cbuff, true);
        if (res.isError()) {
            System.out.println("Error");
        }
        sb.append(carray, 0, cbuff.position());
        cbuff.position(0);
        res = decoder.flush(cbuff);
        if (res.isError()) {
            System.out.println("Error");
        }
        sb.append(carray, 0, cbuff.position());
        return sb.toString();
    }

    public void close() throws IOException {
        super.close();
        in.close();
    }
}
