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
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.Writable;

import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;
import org.apache.pig.backend.executionengine.ExecException;

/**
 * This class was used to handle reading and writing of intermediate
 *  results of data types. Now that functionality is in {@link BinInterSedes}
 *  This class could also be used for storing permanent results, it used 
 *  by BinStorage and Zebra through DefaultTuple class.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class DataReaderWriter {
    private static TupleFactory mTupleFactory = TupleFactory.getInstance();
    private static BagFactory mBagFactory = BagFactory.getInstance();
    static final int UNSIGNED_SHORT_MAX = 65535;
    public static final String UTF8 = "UTF-8";

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
        long size = in.readLong();
        
        for (long i = 0; i < size; i++) {
            try {
                Object o = readDatum(in);
                bag.add((Tuple)o);
            } catch (ExecException ee) {
                throw ee;
            }
        }
        return bag;
    }
    
    public static Map<String, Object> bytesToMap(DataInput in) throws IOException {
        int size = in.readInt();    
        Map<String, Object> m = new HashMap<String, Object>(size);
        for (int i = 0; i < size; i++) {
            String key = (String)readDatum(in);
            m.put(key, readDatum(in));
        }
        return m;    
    }

    public static InternalMap bytesToInternalMap(DataInput in) throws IOException {
        int size = in.readInt();    
        InternalMap m = new InternalMap(size);
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
    
    public static Writable bytesToWritable(DataInput in) throws IOException {
        String className = (String) readDatum(in);
        // create the writeable class . It needs to have a default constructor
        Class<?> objClass = null ;
        try {
            objClass = Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new IOException("Could not find class " + className + 
                    ", while attempting to de-serialize it ", e);
        }
        Writable writable = null;
        try {
            writable = (Writable) objClass.newInstance();
        } catch (Exception e) {
            String msg = "Could create instance of class " + className + 
            ", while attempting to de-serialize it. (no default constructor ?)";
            throw new IOException(msg, e);
        } 
        
        //read the fields of the object from DataInput
        writable.readFields(in);
        return writable;
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

            case DataType.INTERNALMAP: 
                return bytesToInternalMap(in);    

            case DataType.INTEGER:
                return Integer.valueOf(in.readInt());

            case DataType.LONG:
                return Long.valueOf(in.readLong());

            case DataType.FLOAT:
                return Float.valueOf(in.readFloat());

            case DataType.DOUBLE:
                return Double.valueOf(in.readDouble());

            case DataType.BOOLEAN:
                return Boolean.valueOf(in.readBoolean());

            case DataType.BYTE:
                return Byte.valueOf(in.readByte());

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
                
            case DataType.GENERIC_WRITABLECOMPARABLE :
                return bytesToWritable(in);
                
            case DataType.NULL:
                return null;

            default:
                throw new RuntimeException("Unexpected data type " + type +
                    " found in stream.");
        }
    }

	@SuppressWarnings("unchecked")
    public static void writeDatum(
            DataOutput out,
            Object val) throws IOException {
        // Read the data type
        byte type = DataType.findType(val);
        switch (type) {
            case DataType.TUPLE:
                Tuple t = (Tuple)val;
                out.writeByte(DataType.TUPLE);
                int sz = t.size();
                out.writeInt(sz);
                for (int i = 0; i < sz; i++) {
                    DataReaderWriter.writeDatum(out, t.get(i));
                }
                break;
                
            case DataType.BAG:
                DataBag bag = (DataBag)val;
                out.writeByte(DataType.BAG);
                out.writeLong(bag.size());
                Iterator<Tuple> it = bag.iterator();
                while (it.hasNext()) {
                    DataReaderWriter.writeDatum(out, it.next());
                }  
                break;

            case DataType.MAP: {
                out.writeByte(DataType.MAP);
                Map<String, Object> m = (Map<String, Object>)val;
                out.writeInt(m.size());
                Iterator<Map.Entry<String, Object> > i =
                    m.entrySet().iterator();
                while (i.hasNext()) {
                    Map.Entry<String, Object> entry = i.next();
                    writeDatum(out, entry.getKey());
                    writeDatum(out, entry.getValue());
                }
                break;
                               }
            
            case DataType.INTERNALMAP: {
                out.writeByte(DataType.INTERNALMAP);
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
            case DataType.GENERIC_WRITABLECOMPARABLE :
                out.writeByte(DataType.GENERIC_WRITABLECOMPARABLE);
                //store the class name, so we know the class to create on read
                writeDatum(out, val.getClass().getName());
                Writable writable = (Writable)val;
                writable.write(out);
                break;

            case DataType.NULL:
                out.writeByte(DataType.NULL);
                break;

            default:
                throw new RuntimeException("Unexpected data type " + type +
                    " found in stream.");
        }
    }
}

