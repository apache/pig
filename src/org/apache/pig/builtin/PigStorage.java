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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Iterator;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

import org.apache.pig.ExecType;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigException;
import org.apache.pig.StoreFunc;
import org.apache.pig.ReversibleLoadStoreFunc;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * A load function that parses a line of input into fields using a delimiter to set the fields. The
 * delimiter is given as a regular expression. See String.split(delimiter) and
 * http://java.sun.com/j2se/1.5.0/docs/api/java/util/regex/Pattern.html for more information.
 */
public class PigStorage extends Utf8StorageConverter
        implements ReversibleLoadStoreFunc {
    protected BufferedPositionedInputStream in = null;
    protected final Log mLog = LogFactory.getLog(getClass());
        
    long                end            = Long.MAX_VALUE;
    private byte recordDel = '\n';
    private byte fieldDel = '\t';
    private ByteArrayOutputStream mBuf = null;
    private ArrayList<Object> mProtoTuple = null;
    private int os;
    private static final int OS_UNIX = 0;
    private static final int OS_WINDOWS = 1;
    private static final String UTF8 = "UTF-8";
    
    public PigStorage() {
        os = OS_UNIX;
        if (System.getProperty("os.name").toUpperCase().startsWith("WINDOWS"))
            os = OS_WINDOWS;
    }

    /**
     * Constructs a Pig loader that uses specified regex as a field delimiter.
     * 
     * @param delimiter
     *            the single byte character that is used to separate fields.
     *            ("\t" is the default.)
     */
    public PigStorage(String delimiter) {
        this();
        if (delimiter.length() == 1) {
            this.fieldDel = (byte)delimiter.charAt(0);
        } else if (delimiter.length() > 1 && delimiter.charAt(0) == '\\') {
            switch (delimiter.charAt(1)) {
            case 't':
                this.fieldDel = (byte)'\t';
                break;

            case 'x':
            case 'u':
                this.fieldDel =
                    Integer.valueOf(delimiter.substring(2)).byteValue();
                break;

            default:                
                throw new RuntimeException("Unknown delimiter " + delimiter);
            }
        } else {            
            throw new RuntimeException("PigStorage delimeter must be a single character");
        }
    }

    public Tuple getNext() throws IOException {
        if (in == null || in.getPosition() > end) {
            return null;
        }

        if (mBuf == null) mBuf = new ByteArrayOutputStream(4096);
        mBuf.reset();
        while (true) {
            // BufferedPositionedInputStream is buffered, so I don't need
            // to buffer.
            int b = in.read();

            if (b == fieldDel) {
                readField();
            } else if (b == recordDel) {
                readField();
                //Tuple t =  mTupleFactory.newTuple(mProtoTuple);
                Tuple t =  mTupleFactory.newTupleNoCopy(mProtoTuple);
                mProtoTuple = null;
                return t;
            } else if (b == -1) {
                // hit end of file
                return null;
            } else {
                mBuf.write(b);
            }
        }
    }

    public void bindTo(String fileName, BufferedPositionedInputStream in, long offset, long end) throws IOException {
        this.in = in;
        this.end = end;
        
        // Since we are not block aligned we throw away the first
        // record and cound on a different instance to read it
        if (offset != 0) {
            getNext();
        }
    }
    
    OutputStream mOut;
    public void bindTo(OutputStream os) throws IOException {
        mOut = os;
    }

    private void putField(Object field) throws IOException {
        //string constants for each delimiter
        String tupleBeginDelim = "(";
        String tupleEndDelim = ")";
        String bagBeginDelim = "{";
        String bagEndDelim = "}";
        String mapBeginDelim = "[";
        String mapEndDelim = "]";
        String fieldDelim = ",";
        String mapKeyValueDelim = "#";

        switch (DataType.findType(field)) {
        case DataType.NULL:
            break; // just leave it empty

        case DataType.BOOLEAN:
            mOut.write(((Boolean)field).toString().getBytes());
            break;

        case DataType.INTEGER:
            mOut.write(((Integer)field).toString().getBytes());
            break;

        case DataType.LONG:
            mOut.write(((Long)field).toString().getBytes());
            break;

        case DataType.FLOAT:
            mOut.write(((Float)field).toString().getBytes());
            break;

        case DataType.DOUBLE:
            mOut.write(((Double)field).toString().getBytes());
            break;

        case DataType.BYTEARRAY: {
            byte[] b = ((DataByteArray)field).get();
            mOut.write(b, 0, b.length);
            break;
                                 }

        case DataType.CHARARRAY:
            // oddly enough, writeBytes writes a string
            mOut.write(((String)field).getBytes(UTF8));
            break;

        case DataType.MAP:
            boolean mapHasNext = false;
            Map<Object, Object> m = (Map<Object, Object>)field;
            mOut.write(mapBeginDelim.getBytes(UTF8));
            for(Object o: m.keySet()) {
                if(mapHasNext) {
                    mOut.write(fieldDelim.getBytes(UTF8));
                } else {
                    mapHasNext = true;
                }
                putField(o);
                mOut.write(mapKeyValueDelim.getBytes(UTF8));
                putField(m.get(o));
            }
            mOut.write(mapEndDelim.getBytes(UTF8));
            break;

        case DataType.TUPLE:
            boolean tupleHasNext = false;
            Tuple t = (Tuple)field;
            mOut.write(tupleBeginDelim.getBytes(UTF8));
            for(int i = 0; i < t.size(); ++i) {
                if(tupleHasNext) {
                    mOut.write(fieldDelim.getBytes(UTF8));
                } else {
                    tupleHasNext = true;
                }
                try {
                    putField(t.get(i));
                } catch (ExecException ee) {
                    throw ee;
                }
            }
            mOut.write(tupleEndDelim.getBytes(UTF8));
            break;

        case DataType.BAG:
            boolean bagHasNext = false;
            mOut.write(bagBeginDelim.getBytes(UTF8));
            Iterator<Tuple> tupleIter = ((DataBag)field).iterator();
            while(tupleIter.hasNext()) {
                if(bagHasNext) {
                    mOut.write(fieldDelim.getBytes(UTF8));
                } else {
                    bagHasNext = true;
                }
                putField((Object)tupleIter.next());
            }
            mOut.write(bagEndDelim.getBytes(UTF8));
            break;
            
        default: {
            int errCode = 2108;
            String msg = "Could not determine data type of field: " + field;
            throw new ExecException(msg, errCode, PigException.BUG);
        }
        
        }
    }

    public void putNext(Tuple f) throws IOException {
        // I have to convert integer fields to string, and then to bytes.
        // If I use a DataOutputStream to convert directly from integer to
        // bytes, I don't get a string representation.
        int sz = f.size();
        for (int i = 0; i < sz; i++) {
            Object field;
            try {
                field = f.get(i);
            } catch (ExecException ee) {
                throw ee;
            }

            putField(field);

            if (i == sz - 1) {
                // last field in tuple.
                mOut.write(recordDel);
            } else {
                mOut.write(fieldDel);
            }
        }
    }

    public void finish() throws IOException {
    }

    private void readField() {
        if (mProtoTuple == null) mProtoTuple = new ArrayList<Object>();
        if (mBuf.size() == 0) {
            // NULL value
            mProtoTuple.add(null);
        } else {
            // TODO, once this can take schemas, we need to figure out
            // if the user requested this to be viewed as a certain
            // type, and if so, then construct it appropriately.
            byte[] array = mBuf.toByteArray();
            if (array[array.length-1]=='\r' && os==OS_WINDOWS) {
                // This is a java 1.6 function.  Until pig officially moves to
                // 1.6 we can't use this.
                // array = Arrays.copyOf(array, array.length-1);
                byte[] tmp = new byte[array.length - 1];
                for (int i = 0; i < array.length - 1; i++) tmp[i] = array[i];
                array = tmp;
            }
                
            if (array.length==0)
                mProtoTuple.add(null);
            else
                mProtoTuple.add(new DataByteArray(array));
        }
        mBuf.reset();
    }

    /* (non-Javadoc)
     * @see org.apache.pig.LoadFunc#determineSchema(java.lang.String, org.apache.pig.ExecType, org.apache.pig.backend.datastorage.DataStorage)
     */
    public Schema determineSchema(String fileName, ExecType execType,
            DataStorage storage) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    public void fieldsToRead(Schema schema) {
        // do nothing
    }
    
    public boolean equals(Object obj) {
        return equals((PigStorage)obj);
    }

    public boolean equals(PigStorage other) {
        return this.fieldDel == other.fieldDel;
    }

    /* (non-Javadoc)
     * @see org.apache.pig.StoreFunc#getStorePreparationClass()
     */
    @Override
    public Class getStorePreparationClass() throws IOException {
        // TODO Auto-generated method stub
        return null;
    }


}
