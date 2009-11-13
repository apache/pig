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
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.pig.ExecType;
import org.apache.pig.LoadCaster;
import org.apache.pig.LoadFunc;
import org.apache.pig.ResourceSchema;
import org.apache.pig.StoreFunc;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DefaultTupleFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.logicalLayer.schema.Schema;

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
// XXX : FIXME - make this work with new load-store redesign
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
            return DefaultTupleFactory.getInstance().newTuple(new DataByteArray(buffer));
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
        //DataAtom dAtom = (DataAtom)(f.getAtomField(0));
        //byte[] data = dAtom.getValueBytes();
        byte[] data;
        try {
            data = ((DataByteArray)f.get(0)).get();
        } catch (ExecException e) {
            throw e;
        }
        if (data.length > 0) {
            out.write(data);
            out.flush();
        }
    }
    
    public String toString() {
        return "BinaryStorage(" + bufferSize + ")";
    }
    
    public boolean equals(Object obj) {
        return true;
    }

    /* (non-Javadoc)
     * @see org.apache.pig.LoadFunc#getInputFormat()
     */
    @Override
    public InputFormat getInputFormat() {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.pig.LoadFunc#getLoadCaster()
     */
    @Override
    public LoadCaster getLoadCaster() {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.pig.LoadFunc#prepareToRead(org.apache.hadoop.mapreduce.RecordReader, org.apache.hadoop.mapreduce.InputSplit)
     */
    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) {
        // TODO Auto-generated method stub
        
    }

    /* (non-Javadoc)
     * @see org.apache.pig.LoadFunc#setLocation(java.lang.String, org.apache.hadoop.mapreduce.Job)
     */
    @Override
    public void setLocation(String location, Job job) throws IOException {
        // TODO Auto-generated method stub
        
    }

    /* (non-Javadoc)
     * @see org.apache.pig.StoreFunc#getOutputFormat()
     */
    @Override
    public OutputFormat getOutputFormat() {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.pig.StoreFunc#prepareToWrite(org.apache.hadoop.mapreduce.RecordWriter)
     */
    @Override
    public void prepareToWrite(RecordWriter writer) {
        // TODO Auto-generated method stub
        
    }

    /* (non-Javadoc)
     * @see org.apache.pig.StoreFunc#setSchema(org.apache.pig.ResourceSchema)
     */
    @Override
    public void checkSchema(ResourceSchema s) throws IOException {
        // TODO Auto-generated method stub
        
    }

    /* (non-Javadoc)
     * @see org.apache.pig.StoreFunc#setStoreLocation(java.lang.String, org.apache.hadoop.mapreduce.Job)
     */
    @Override
    public void setStoreLocation(String location, Job job) throws IOException {
        // TODO Auto-generated method stub
        
    }

    /* (non-Javadoc)
     * @see org.apache.pig.LoadFunc#relativeToAbsolutePath(java.lang.String, org.apache.hadoop.fs.Path)
     */
    @Override
    public String relativeToAbsolutePath(String location, Path curDir)
            throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.pig.StoreFunc#relToAbsPathForStoreLocation(java.lang.String, org.apache.hadoop.fs.Path)
     */
    @Override
    public String relToAbsPathForStoreLocation(String location, Path curDir)
            throws IOException {
        // TODO Auto-generated method stub
        return null;
    }
}
