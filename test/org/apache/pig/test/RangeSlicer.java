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

package org.apache.pig.test;

import java.io.IOException;
import java.net.URL;
import java.util.Map;

import org.apache.pig.ExecType;
import org.apache.pig.LoadFunc;
import org.apache.pig.Slice;
import org.apache.pig.Slicer;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * Makes slices each containing a single value from 0 to value - 1.
 */
public class RangeSlicer
    implements Slicer, LoadFunc
{
    int numslices = 0;

    public RangeSlicer(String num) {
        numslices = Integer.parseInt(num);
    }

    /**
     * Each slice generates a single value,
     * its index in the sequence of slices.
     */
    public Slice[] slice (DataStorage store, String location)
        throws IOException
    {
        Slice[] slices = new Slice[numslices];
        for (int i = 0; i < slices.length; i++) {
            slices[i] = new SingleValueSlice(i);
        }
        return slices;
    }

    public void validate(DataStorage store, String location) throws IOException {
        if (!location.matches(".*/tmp/foo.*")) {
            throw new IOException("Wrong Path");
        }
    }

    /**
     * A Slice that returns a single value from next.
     */
    public static class SingleValueSlice
        implements Slice
    {
        public int val;

        private transient boolean read;

        public SingleValueSlice (int value)
        {
            this.val = value;
        }

        public void close ()
            throws IOException
        {}

        public long getLength ()
        {
            return 1;
        }

        public String[] getLocations ()
        {
            return new String[0];
        }

        public long getStart() {
            return 0;
        }
        
        public long getPos ()
            throws IOException
        {
            return read ? 1 : 0;
        }

        public float getProgress ()
            throws IOException
        {
            return read ? 1 : 0;
        }

        public void init (DataStorage store)
            throws IOException
        {}

        public boolean next (Tuple value)
            throws IOException
        {
            if (!read) {
                Tuple t = TupleFactory.getInstance().newTuple();
                t.append(val);
                value.reference(t);
                read = true;
                return true;
            }
            return false;
        }

        private static final long serialVersionUID = 1L;
    }

    /* (non-Javadoc)
     * @see org.apache.pig.LoadFunc#bindTo(java.lang.String, org.apache.pig.impl.io.BufferedPositionedInputStream, long, long)
     */
    public void bindTo(String fileName, BufferedPositionedInputStream is,
            long offset, long end) throws IOException {      
        
    }

    /* (non-Javadoc)
     * @see org.apache.pig.LoadFunc#bytesToBag(byte[])
     */
    public DataBag bytesToBag(byte[] b) throws IOException {
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.pig.LoadFunc#bytesToCharArray(byte[])
     */
    public String bytesToCharArray(byte[] b) throws IOException {
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.pig.LoadFunc#bytesToDouble(byte[])
     */
    public Double bytesToDouble(byte[] b) throws IOException {
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.pig.LoadFunc#bytesToFloat(byte[])
     */
    public Float bytesToFloat(byte[] b) throws IOException {
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.pig.LoadFunc#bytesToInteger(byte[])
     */
    public Integer bytesToInteger(byte[] b) throws IOException {
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.pig.LoadFunc#bytesToLong(byte[])
     */
    public Long bytesToLong(byte[] b) throws IOException {
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.pig.LoadFunc#bytesToMap(byte[])
     */
    public Map<Object, Object> bytesToMap(byte[] b) throws IOException {
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.pig.LoadFunc#bytesToTuple(byte[])
     */
    public Tuple bytesToTuple(byte[] b) throws IOException {
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.pig.LoadFunc#determineSchema(java.lang.String, org.apache.pig.ExecType, org.apache.pig.backend.datastorage.DataStorage)
     */
    public Schema determineSchema(String fileName, ExecType execType,
            DataStorage storage) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.pig.LoadFunc#fieldsToRead(org.apache.pig.impl.logicalLayer.schema.Schema)
     */
    public void fieldsToRead(Schema schema) {
        
    }

    /* (non-Javadoc)
     * @see org.apache.pig.LoadFunc#getNext()
     */
    public Tuple getNext() throws IOException {
        return null;
    }
}
