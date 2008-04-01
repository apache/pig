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
package org.apache.pig.builtin;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.pig.LoadFunc;
import org.apache.pig.StoreFunc;
import org.apache.pig.data.DataAtom;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.BufferedPositionedInputStream;

/**
 * {@link BinaryStorage} is a simple, as-is, serializer/deserializer pair.
 * 
 * It is {@link LoadFunc} which loads all the given data from the given 
 * {@link InputStream} into a single {@link Tuple} and a {@link StoreFunc}
 * which writes out all input data as a single <code>Tuple</code>. 
 * 
 * <code>BinaryStorage</code> is intended to work in cases where input files
 * are to be sent in-whole for processing without any splitting and 
 * interpretation of their data.
 */
public class BinaryStorage implements LoadFunc, StoreFunc {
    // LoadFunc
    private static final int DEFAULT_BUFFER_SIZE = 64*1024;
    protected int bufferSize = DEFAULT_BUFFER_SIZE;

    protected BufferedPositionedInputStream in = null;
    protected long offset = 0;
    protected long end = Long.MAX_VALUE;

    // StoreFunc
    OutputStream out;
    
    /**
     * Create a <code>BinaryStorage</code> with default buffer size for reading
     * inputs.
     */
    public BinaryStorage() {}
    
    /**
     * Create a <code>BinaryStorage</code> with the given buffer-size for 
     * reading inputs.
     * 
     * @param bufferSize buffer size to be used
     */
    public BinaryStorage(int bufferSize) {
        this.bufferSize = bufferSize;
    }
    
    public void bindTo(String fileName, BufferedPositionedInputStream in,
            long offset, long end) throws IOException {
        this.in = in;
        this.offset = offset;
        this.end = end;
    }

    public Tuple getNext() throws IOException {
        // Sanity check
        if (in == null || in.getPosition() > end) {
            return null;
        }
     
        // Copy all data into the buffer
        byte[] buffer = new byte[bufferSize];
        int off = 0;
        int len = bufferSize;
        int n = 0;
        while (len > 0 && (n = in.read(buffer, off, len)) != -1) {
            off += n;
            len -= n;
        }
        
        if (n == -1) {
            // Copy out the part-buffer and send it
            byte[] copy = new byte[off];
            System.arraycopy(buffer, 0, copy, 0, copy.length);
            buffer = copy;
        }

        // Create a new Tuple with one DataAtom field and return it, 
        // ensure that we return 'null' if we didn't get any data
        if (off > 0) {
            return new Tuple(new DataAtom(buffer));
        }
        
        return null;
    }

    public void bindTo(OutputStream out) throws IOException {
        this.out = out;
    }

    public void finish() throws IOException {}

    public void putNext(Tuple f) throws IOException {
        // Pick up the first field of the Tuple, then it's 
        // raw-bytes and send it out
        DataAtom dAtom = (DataAtom)(f.getAtomField(0));
        byte[] data = dAtom.getValueBytes();
        if (data.length > 0) {
            out.write(dAtom.getValueBytes());
            out.flush();
        }
    }
    
    public String toString() {
        return "BinaryStorage(" + bufferSize + ")";
    }
    
    public boolean equals(Object obj) {
        return true;
    }
}
