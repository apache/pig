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

import org.apache.hadoop.io.Writable;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;

/**
 * A class to handle reading and writing of intermediate results of data
 * types. The serialization format used by this class more efficient than 
 * what was used in DataReaderWriter . 
 * The format used by the functions in this class is subject to change, so it
 * should be used ONLY to store intermediate results within a pig query.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class BinInterSedes implements InterSedes {

    public static final byte BOOLEAN_TRUE = 0;
    static final byte BOOLEAN_FALSE = 1;

    public static final byte BYTE = 2;

    public static final byte INTEGER = 3;
    // since boolean is not supported yet(v0.7) as external type,
    // lot of people use int instead
    // and some data with old schema is likely stay for some time.
    // so optimizing for that case as well
    public static final byte INTEGER_0 = 4; 
    public static final byte INTEGER_1 = 5;
    public static final byte INTEGER_INSHORT = 6;
    public static final byte INTEGER_INBYTE = 7;


    public static final byte LONG = 8;
    public static final byte FLOAT = 9;
    public static final byte DOUBLE = 10;

    public static final byte BYTEARRAY = 11;
    public static final byte SMALLBYTEARRAY = 12;
    public static final byte TINYBYTEARRAY = 13;

    public static final byte CHARARRAY = 14;
    public static final byte SMALLCHARARRAY = 15;

    public static final byte MAP = 16;
    public static final byte SMALLMAP = 17;
    public static final byte TINYMAP = 18;

    public static final byte TUPLE = 19;
    public static final byte SMALLTUPLE = 20;
    public static final byte TINYTUPLE = 21;

    public static final byte BAG = 22;
    public static final byte SMALLBAG = 23;
    public static final byte TINYBAG = 24;

    public static final byte GENERIC_WRITABLECOMPARABLE = 25;
    public static final byte INTERNALMAP = 26;

    public static final byte NULL = 27;

    private static TupleFactory mTupleFactory = TupleFactory.getInstance();
    private static BagFactory mBagFactory = BagFactory.getInstance();
    static final int UNSIGNED_SHORT_MAX = 65535;
    static final int UNSIGNED_BYTE_MAX = 255;
    static final String UTF8 = "UTF-8";

    
    
    private Tuple readTuple(DataInput in, byte type) throws IOException {
        // Read the size.
        int sz = getTupleSize(in, type);

        Tuple t = mTupleFactory.newTuple(sz);
        for (int i = 0; i < sz; i++) {
            t.set(i, readDatum(in));
        }
        return t;

    }
    

    
    private int getTupleSize(DataInput in, byte type) throws IOException {
        int sz ;
        switch(type){
        case TINYTUPLE:
            sz = in.readUnsignedByte();
            break;
        case SMALLTUPLE:
            sz = in.readUnsignedShort();
            break;
        case TUPLE:
            sz = in.readInt();
            break;
        default: {
            int errCode = 2112;
            String msg = "Unexpected datatype " + type + " while reading tuple" +
            "from binary file.";
            throw new ExecException(msg, errCode, PigException.BUG);
        }
        }
        // if sz == 0, we construct an "empty" tuple -
        // presumably the writer wrote an empty tuple!
        if (sz < 0) {
            throw new IOException("Invalid size " + sz + " for a tuple");
        }      
        return sz;
    }



    private DataBag readBag(DataInput in, byte type) throws IOException {
        DataBag bag = mBagFactory.newDefaultBag();
        long size;
        //determine size of bag
        switch(type){
        case TINYBAG:
            size = in.readUnsignedByte();
            break;
        case SMALLBAG:
            size = in.readUnsignedShort();
            break;
        case BAG:
            size = in.readLong();
            break;
        default:
            int errCode = 2219;
            String msg = "Unexpected data while reading bag " +
            "from binary file.";
            throw new ExecException(msg, errCode, PigException.BUG);            
        }

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
    
    
    private Map<String, Object> readMap(DataInput in, byte type) throws IOException {
        int size ;
        switch(type){
        case TINYMAP:
            size = in.readUnsignedByte();
            break;
        case SMALLMAP:
            size = in.readUnsignedShort();
            break;
        case MAP:
            size = in.readInt();
            break;
        default: {
            int errCode = 2220;
            String msg = "Unexpected data while reading map" +
            "from binary file.";
            throw new ExecException(msg, errCode, PigException.BUG);  
        }
        }
        Map<String, Object> m = new HashMap<String, Object>(size);
        for (int i = 0; i < size; i++) {
            String key = (String)readDatum(in);
            m.put(key, readDatum(in));
        }
        return m;    
    }

    private InternalMap readInternalMap(DataInput in) throws IOException {
        int size = in.readInt();    
        InternalMap m = new InternalMap(size);
        for (int i = 0; i < size; i++) {
            Object key = readDatum(in);
            m.put(key, readDatum(in));
        }
        return m;    
    }
    
    private static String readCharArray(DataInput in) throws IOException{
        return in.readUTF();
    }

    private static String readBigCharArray(DataInput in) throws IOException{
        int size = in.readInt();
        byte[] ba = new byte[size];
        in.readFully(ba);
        return new String(ba, UTF8);
    }
    
    private Writable readWritable(DataInput in) throws IOException {
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
    
    /* (non-Javadoc)
     * @see org.apache.pig.data.InterSedes#readDatum(java.io.DataInput)
     */
    public Object readDatum(DataInput in) throws IOException, ExecException {
        // Read the data type
        byte b = in.readByte();
        return readDatum(in, b);
    }
    
    private static Object readBytes(DataInput in, int size) throws IOException {
        byte[] ba = new byte[size];
        in.readFully(ba);
        return new DataByteArray(ba);
    }
        
    /* (non-Javadoc)
     * @see org.apache.pig.data.InterSedes#readDatum(java.io.DataInput, byte)
     */
    public Object readDatum(DataInput in, byte type) throws IOException, ExecException {
        switch (type) {
            case TUPLE: 
            case TINYTUPLE:
            case SMALLTUPLE:
                return readTuple(in, type);
            
            case BAG: 
            case TINYBAG:
            case SMALLBAG:
                return readBag(in, type);

            case MAP: 
            case TINYMAP:
            case SMALLMAP:
                return readMap(in, type);    

            case INTERNALMAP: 
                return readInternalMap(in);    

            case INTEGER_0:
                return Integer.valueOf(0);
            case INTEGER_1:
                return Integer.valueOf(1);
            case INTEGER_INBYTE:
                return Integer.valueOf(in.readByte());                
            case INTEGER_INSHORT:
                return Integer.valueOf(in.readShort());
            case INTEGER:
                return Integer.valueOf(in.readInt());

            case LONG:
                return Long.valueOf(in.readLong());

            case FLOAT:
                return Float.valueOf(in.readFloat());

            case DOUBLE:
                return Double.valueOf(in.readDouble());

            case BOOLEAN_TRUE:
                return Boolean.valueOf(true);
                
            case BOOLEAN_FALSE:
                return Boolean.valueOf(false);

            case BYTE:
                return Byte.valueOf(in.readByte());

            case TINYBYTEARRAY :{
                int size = in.readUnsignedByte();
                return readBytes(in, size);
            }
            
            case SMALLBYTEARRAY :{
                int size = in.readUnsignedShort();
                return readBytes(in, size);
            }
                
            case BYTEARRAY: {
                int size = in.readInt();
                return readBytes(in, size);
            }
            
            case CHARARRAY: 
                return readBigCharArray(in);

            case SMALLCHARARRAY: 
                return readCharArray(in);
                
            case GENERIC_WRITABLECOMPARABLE :
                return readWritable(in);
                
            case NULL:
                return null;

            default:
                throw new RuntimeException("Unexpected data type " + type +
                    " found in stream.");
        }
    }


    /* (non-Javadoc)
     * @see org.apache.pig.data.InterSedes#writeDatum(java.io.DataOutput, java.lang.Object)
     */
    @SuppressWarnings("unchecked")
    public void writeDatum(
            DataOutput out,
            Object val) throws IOException {
        // Read the data type
        byte type = DataType.findType(val);
        switch (type) {
            case DataType.TUPLE:
                writeTuple(out, (Tuple)val);
                break;
                
            case DataType.BAG:
                writeBag(out, (DataBag)val);
                break;

            case DataType.MAP: {
                writeMap(out, (Map<String, Object>)val);

                break;
                               }
            
            case DataType.INTERNALMAP: {
                out.writeByte(INTERNALMAP);
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
                int i = (Integer)val;
                if(i == 0){
                    out.writeByte(INTEGER_0);
                }else if(i == 1){
                    out.writeByte(INTEGER_1);
                }
                else if(Byte.MIN_VALUE <= i && i <= Byte.MAX_VALUE  ){
                    out.writeByte(INTEGER_INBYTE);
                    out.writeByte(i);
                }
                else if(Short.MIN_VALUE <= i && i <= Short.MAX_VALUE ){
                    out.writeByte(INTEGER_INSHORT);
                    out.writeShort(i);
                }
                else{
                    out.writeByte(INTEGER);
                    out.writeInt(i);
                }


                break;

            case DataType.LONG:
                out.writeByte(LONG);
                out.writeLong((Long)val);
                break;

            case DataType.FLOAT:
                out.writeByte(FLOAT);
                out.writeFloat((Float)val);
                break;

            case DataType.DOUBLE:
                out.writeByte(DOUBLE);
                out.writeDouble((Double)val);
                break;

            case DataType.BOOLEAN:
                if(((Boolean)val) == true)
                    out.writeByte(BOOLEAN_TRUE);
                else
                    out.writeByte(BOOLEAN_FALSE);
                break;

            case DataType.BYTE:
                out.writeByte(BYTE);
                out.writeByte((Byte)val);
                break;

            case DataType.BYTEARRAY: {
                DataByteArray bytes = (DataByteArray)val;
                final int sz = bytes.size();
                if(sz < UNSIGNED_BYTE_MAX){
                    out.writeByte(TINYBYTEARRAY);
                    out.writeByte(sz);
                }
                else if(sz < UNSIGNED_SHORT_MAX){
                    out.writeByte(SMALLBYTEARRAY);
                    out.writeShort(sz);
                }
                else {
                    out.writeByte(BYTEARRAY);
                    out.writeInt(sz);
                }
                out.write(bytes.mData);
                
                break;

            }

            case DataType.CHARARRAY: {
                String s = (String)val;
                // a char can take up to 3 bytes in the modified utf8 encoding
                // used by DataOutput.writeUTF, so use UNSIGNED_SHORT_MAX/3
                if(s.length() < UNSIGNED_SHORT_MAX/3) {
                    out.writeByte(SMALLCHARARRAY);
                    out.writeUTF(s);
                } else {
                    byte[] utfBytes = s.getBytes(UTF8);
                    int length = utfBytes.length;

                    out.writeByte(CHARARRAY);
                    out.writeInt(length);
                    out.write(utfBytes);
                }
                break;
            }
            case DataType.GENERIC_WRITABLECOMPARABLE :
                out.writeByte(GENERIC_WRITABLECOMPARABLE);
                //store the class name, so we know the class to create on read
                writeDatum(out, val.getClass().getName());
                Writable writable = (Writable)val;
                writable.write(out);
                break;

            case DataType.NULL:
                out.writeByte(NULL);
                break;

            default:
                throw new RuntimeException("Unexpected data type " + type +
                    " found in stream.");
        }
    }

    private void writeMap(DataOutput out, Map<String, Object> m)
    throws IOException {

        final int sz = m.size();
        if(sz < UNSIGNED_BYTE_MAX){
            out.writeByte(TINYMAP);
            out.writeByte(sz);
        }else if(sz < UNSIGNED_SHORT_MAX){
            out.writeByte(SMALLMAP);
            out.writeShort(sz);
        }else {
            out.writeByte(MAP);       
            out.writeInt(sz);
        }
        Iterator<Map.Entry<String, Object> > i =
            m.entrySet().iterator();
        while (i.hasNext()) {
            Map.Entry<String, Object> entry = i.next();
            writeDatum(out, entry.getKey());
            writeDatum(out, entry.getValue());
        }
    }



    private void writeBag(DataOutput out, DataBag bag)
    throws IOException {
        // We don't care whether this bag was sorted or distinct because
        // using the iterator to write it will guarantee those things come
        // correctly.  And on the other end there'll be no reason to waste
        // time re-sorting or re-applying distinct.
        final long sz = bag.size();
        if(sz < UNSIGNED_BYTE_MAX){
            out.writeByte(TINYBAG);
            out.writeByte((int)sz);
        }else if(sz < UNSIGNED_SHORT_MAX){
            out.writeByte(SMALLBAG);
            out.writeShort((int)sz);           
        }else {
            out.writeByte(BAG);
            out.writeLong(sz);
        }

        Iterator<Tuple> it = bag.iterator();
        while (it.hasNext()) {
            writeTuple(out, it.next());
        } 
        
    }

    private void writeTuple(DataOutput out, Tuple t) throws IOException {
        final int sz = t.size();
        if(sz < UNSIGNED_BYTE_MAX){
            out.writeByte(TINYTUPLE);
            out.writeByte(sz);
        }else if(sz < UNSIGNED_SHORT_MAX){
            out.writeByte(SMALLTUPLE);
            out.writeShort(sz);
        }else{
            out.writeByte(TUPLE);
            out.writeInt(sz);
        }

        for (int i = 0; i < sz; i++) {
            writeDatum(out, t.get(i));
        }
    }



    /* (non-Javadoc)
     * @see org.apache.pig.data.InterSedes#addColsToTuple(java.io.DataInput, org.apache.pig.data.Tuple)
     */
    @Override
    public void addColsToTuple(DataInput in, Tuple t)
    throws IOException {
        byte type = in.readByte();
        int sz = getTupleSize(in, type);
        for (int i = 0; i < sz; i++) {
            t.append(readDatum(in));
        }
    }


}

