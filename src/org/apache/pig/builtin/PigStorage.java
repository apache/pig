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
import java.util.Map;

import org.apache.pig.LoadFunc;
import org.apache.pig.StoreFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
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
public class PigStorage implements LoadFunc, StoreFunc {
    protected BufferedPositionedInputStream in = null;
        
    long                end            = Long.MAX_VALUE;
    private byte recordDel = '\n';
    private byte fieldDel = '\t';
    private ByteArrayOutputStream mBuf = null;
    private ArrayList<Object> mProtoTuple = null;
    private TupleFactory mTupleFactory = TupleFactory.getInstance();
    
    public PigStorage() {
    }

    /**
     * Constructs a Pig loader that uses specified regex as a field delimiter.
     * 
     * @param delimiter
     *            the single byte character that is used to separate fields.
     *            ("\t" is the default.)
     */
    public PigStorage(String delimiter) {
        this.fieldDel = (byte)delimiter.charAt(0);
        //mBuf = new ByteArrayOutputStream(4096);
        //mProtoTuple = new ArrayList<Object>();
    }

    public Tuple getNext() throws IOException {
        if (in == null || in.getPosition() > end) {
            return null;
        }

        if (mBuf == null) mBuf = new ByteArrayOutputStream(4096);
        mBuf.reset();
        while (true) {
            // Hadoop's FSDataInputStream (which my input stream is based
            // on at some point) is buffered, so I don't need to buffer.
            int b = in.read();

            if (b == fieldDel) {
                readField();
            } else if (b == recordDel) {
                readField();
                Tuple t =  mTupleFactory.newTuple(mProtoTuple);
                mProtoTuple.clear();
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
                throw new RuntimeException(ee);
            }
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
                mOut.write(((String)field).getBytes());
                break;

            case DataType.MAP:
            case DataType.TUPLE:
            case DataType.BAG:
                throw new IOException("Cannot store a non-flat tuple " +
                    "using PigStorage");
                
            default:
                throw new RuntimeException("Unknown datatype " + 
                    DataType.findType(field));
            }

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
            mProtoTuple.add(new DataByteArray(mBuf.toByteArray()));
        }
        mBuf.reset();
    }

	public DataBag bytesToBag(byte[] b) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	public Boolean bytesToBoolean(byte[] b) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	public String bytesToCharArray(byte[] b) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	public Double bytesToDouble(byte[] b) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	public Float bytesToFloat(byte[] b) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	public Integer bytesToInteger(byte[] b) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	public Long bytesToLong(byte[] b) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	public Map<Object, Object> bytesToMap(byte[] b) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	public Tuple bytesToTuple(byte[] b) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	public Schema determineSchema(URL fileName) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	public void fieldsToRead(Schema schema) {
		// TODO Auto-generated method stub
		
	}

}
