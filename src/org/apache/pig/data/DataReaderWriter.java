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
package org.apache.pig.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;

/**
 * A class to handle reading and writing of intermediate results of data
 * types.  This class could also be used for storing permanent results.
 */
public class DataReaderWriter {
    private static TupleFactory mTupleFactory = TupleFactory.getInstance();
    private static BagFactory mBagFactory = BagFactory.getInstance();
    static final int UNSIGNED_SHORT_MAX = 65535;
    static final String UTF8 = "UTF-8";

    public static Tuple bytesToTuple(DataInput in) throws IOException {
        // Don't use Tuple.readFields, because it requires you to
        // create a tuple with no size and then append fields.
        // That's less efficient than allocating the tuple size up
        // front and then filling in the spaces.
        // Read the size.
        int sz = in.readInt();
        // if sz == 0, we construct an "empty" tuple -
        // presumably the writer wrote an empty tuple!
        if (sz < 0) {
            throw new IOException("Invalid size " + sz + " for a tuple");
        }
        Tuple t = mTupleFactory.newTuple(sz);
        for (int i = 0; i < sz; i++) {
            t.set(i, readDatum(in));
        }
        return t;

    }
    
    public static DataBag bytesToBag(DataInput in) throws IOException {
        DataBag bag = mBagFactory.newDefaultBag();
        bag.readFields(in);
        return bag;
    }
    
    public static Map<Object, Object> bytesToMap(DataInput in) throws IOException {
        int size = in.readInt();    
        Map<Object, Object> m = new HashMap<Object, Object>(size);
        for (int i = 0; i < size; i++) {
            Object key = readDatum(in);
            m.put(key, readDatum(in));
        }
        return m;    
    }
    
    public static String bytesToCharArray(DataInput in) throws IOException{
        int size = in.readUnsignedShort();
        byte[] ba = new byte[size];
        in.readFully(ba);
        return new String(ba, DataReaderWriter.UTF8);
    }

    public static String bytesToBigCharArray(DataInput in) throws IOException{
        int size = in.readInt();
        byte[] ba = new byte[size];
        in.readFully(ba);
        return new String(ba, DataReaderWriter.UTF8);
    }
    
    public static Object readDatum(DataInput in) throws IOException, ExecException {
        // Read the data type
        byte b = in.readByte();
        return readDatum(in, b);
    }
        
    public static Object readDatum(DataInput in, byte type) throws IOException, ExecException {
        switch (type) {
            case DataType.TUPLE: 
                return bytesToTuple(in);
            
            case DataType.BAG: 
                return bytesToBag(in);

            case DataType.MAP: 
                return bytesToMap(in);    

            case DataType.INTEGER:
                return new Integer(in.readInt());

            case DataType.LONG:
                return new Long(in.readLong());

            case DataType.FLOAT:
                return new Float(in.readFloat());

            case DataType.DOUBLE:
                return new Double(in.readDouble());

            case DataType.BOOLEAN:
                return new Boolean(in.readBoolean());

            case DataType.BYTE:
                return new Byte(in.readByte());

            case DataType.BYTEARRAY: {
                int size = in.readInt();
                byte[] ba = new byte[size];
                in.readFully(ba);
                return new DataByteArray(ba);
                                     }

            case DataType.BIGCHARARRAY: 
                return bytesToBigCharArray(in);
            

            case DataType.CHARARRAY: 
                return bytesToCharArray(in);
            
            case DataType.NULL:
                return null;

            default:
                throw new RuntimeException("Unexpected data type " + type +
                    " found in stream.");
        }
    }

    public static void writeDatum(
            DataOutput out,
            Object val) throws IOException {
        // Read the data type
        byte type = DataType.findType(val);
        switch (type) {
            case DataType.TUPLE:
                // Because tuples are written directly by hadoop, the
                // tuple's write method needs to write the indicator byte.
                // So don't write the indicator byte here as it is for
                // everyone else.
                ((Tuple)val).write(out);
                break;
                
            case DataType.BAG:
                out.writeByte(DataType.BAG);
                ((DataBag)val).write(out);
                break;

            case DataType.MAP: {
                out.writeByte(DataType.MAP);
                Map<Object, Object> m = (Map<Object, Object>)val;
                out.writeInt(m.size());
                Iterator<Map.Entry<Object, Object> > i =
                    m.entrySet().iterator();
                while (i.hasNext()) {
                    Map.Entry<Object, Object> entry = i.next();
                    writeDatum(out, entry.getKey());
                    writeDatum(out, entry.getValue());
                }
                break;
                               }

            case DataType.INTEGER:
                out.writeByte(DataType.INTEGER);
                out.writeInt((Integer)val);
                break;

            case DataType.LONG:
                out.writeByte(DataType.LONG);
                out.writeLong((Long)val);
                break;

            case DataType.FLOAT:
                out.writeByte(DataType.FLOAT);
                out.writeFloat((Float)val);
                break;

            case DataType.DOUBLE:
                out.writeByte(DataType.DOUBLE);
                out.writeDouble((Double)val);
                break;

            case DataType.BOOLEAN:
                out.writeByte(DataType.BOOLEAN);
                out.writeBoolean((Boolean)val);
                break;

            case DataType.BYTE:
                out.writeByte(DataType.BYTE);
                out.writeByte((Byte)val);
                break;

            case DataType.BYTEARRAY: {
                out.writeByte(DataType.BYTEARRAY);
                DataByteArray bytes = (DataByteArray)val;
                out.writeInt(bytes.size());
                out.write(bytes.mData);
                break;
                                     }

            case DataType.CHARARRAY: {
                String s = (String)val;
                byte[] utfBytes = s.getBytes(DataReaderWriter.UTF8);
                int length = utfBytes.length;
                
                if(length < DataReaderWriter.UNSIGNED_SHORT_MAX) {
                    out.writeByte(DataType.CHARARRAY);
                    out.writeShort(length);
                    out.write(utfBytes);
                } else {
                	out.writeByte(DataType.BIGCHARARRAY);
                	out.writeInt(length);
                	out.write(utfBytes);
                }
                break;
                                     }

            case DataType.NULL:
                out.writeByte(DataType.NULL);
                break;

            default:
                throw new RuntimeException("Unexpected data type " + type +
                    " found in stream.");
        }
    }
}

