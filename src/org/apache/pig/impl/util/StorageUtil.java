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
package org.apache.pig.impl.util;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.builtin.PigStreaming;

/**
 * This util class provides methods that are shared by storage class 
 * {@link PigStorage} and streaming class {@link PigStreaming} 
 * 
 */
public final class StorageUtil {

    private static final String UTF8 = "UTF-8";
    
    /**
     * Transform a <code>String</code> into a byte representing the
     * field delimiter. 
     * 
     * @param delimiter a string that may be in single-quoted form   
     * @return the field delimiter in byte form
     */
    public static byte parseFieldDel(String delimiter) {
        if (delimiter == null) {
            throw new IllegalArgumentException("Null delimiter");
        }
        
        delimiter = parseSingleQuotedString(delimiter);
        
        if (delimiter.length() > 1 && delimiter.charAt(0) != '\\') {
            throw new IllegalArgumentException("Delimeter must be a " +
            		"single character " + delimiter);
        }
        
        byte fieldDel = '\t';

        if (delimiter.length() == 1) {
            fieldDel = (byte)delimiter.charAt(0);
        } else if (delimiter.charAt(0) == '\\') {
            switch (delimiter.charAt(1)) {
            case 't':
                fieldDel = (byte)'\t';
                break;

            case 'x':
                fieldDel =
                    Integer.valueOf(delimiter.substring(2), 16).byteValue();
                break;
            case 'u':
                fieldDel =
                    Integer.valueOf(delimiter.substring(2)).byteValue();
                break;

            default:                
                throw new IllegalArgumentException("Unknown delimiter " + 
                        delimiter);
            }
        }
        
        return fieldDel;
    }
    
    /**
     * Serialize an object to an {@link OutputStream} in the 
     * field-delimited form.  
     * 
     * @param out an OutputStream object 
     * @param field an object to be serialized
     * @throws IOException if serialization fails.
     */
    @SuppressWarnings("unchecked")
    public static void putField(OutputStream out, Object field) 
    throws IOException {
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
            out.write(((Boolean)field).toString().getBytes());
            break;

        case DataType.INTEGER:
            out.write(((Integer)field).toString().getBytes());
            break;

        case DataType.LONG:
            out.write(((Long)field).toString().getBytes());
            break;

        case DataType.FLOAT:
            out.write(((Float)field).toString().getBytes());
            break;

        case DataType.DOUBLE:
            out.write(((Double)field).toString().getBytes());
            break;

        case DataType.BYTEARRAY: 
            byte[] b = ((DataByteArray)field).get();
            out.write(b, 0, b.length);
            break;

        case DataType.CHARARRAY:
            // oddly enough, writeBytes writes a string
            out.write(((String)field).getBytes(UTF8));
            break;

        case DataType.MAP:
            boolean mapHasNext = false;
            Map<String, Object> m = (Map<String, Object>)field;
            out.write(mapBeginDelim.getBytes(UTF8));
            for(Map.Entry<String, Object> e: m.entrySet()) {
                if(mapHasNext) {
                    out.write(fieldDelim.getBytes(UTF8));
                } else {
                    mapHasNext = true;
                }
                putField(out, e.getKey());
                out.write(mapKeyValueDelim.getBytes(UTF8));
                putField(out, e.getValue());
            }
            out.write(mapEndDelim.getBytes(UTF8));
            break;

        case DataType.TUPLE:
            boolean tupleHasNext = false;
            Tuple t = (Tuple)field;
            out.write(tupleBeginDelim.getBytes(UTF8));
            for(int i = 0; i < t.size(); ++i) {
                if(tupleHasNext) {
                    out.write(fieldDelim.getBytes(UTF8));
                } else {
                    tupleHasNext = true;
                }
                try {
                    putField(out, t.get(i));
                } catch (ExecException ee) {
                    throw ee;
                }
            }
            out.write(tupleEndDelim.getBytes(UTF8));
            break;

        case DataType.BAG:
            boolean bagHasNext = false;
            out.write(bagBeginDelim.getBytes(UTF8));
            Iterator<Tuple> tupleIter = ((DataBag)field).iterator();
            while(tupleIter.hasNext()) {
                if(bagHasNext) {
                    out.write(fieldDelim.getBytes(UTF8));
                } else {
                    bagHasNext = true;
                }
                putField(out, (Object)tupleIter.next());
            }
            out.write(bagEndDelim.getBytes(UTF8));
            break;
            
        default: {
            int errCode = 2108;
            String msg = "Could not determine data type of field: " + field;
            throw new ExecException(msg, errCode, PigException.BUG);
        }
        
        }
    }
    
    /**
     * Transform a line of <code>Text</code> to a <code>Tuple</code>
     * 
     * @param val a line of text
     * @param fieldDel the field delimiter
     * @return tuple constructed from the text
     */
    public static Tuple textToTuple(Text val, byte fieldDel) {
                                                                                  
        byte[] buf = val.getBytes();
        int len = val.getLength();
        int start = 0;
        
        ArrayList<Object> protoTuple = new ArrayList<Object>();
        
        for (int i = 0; i < len; i++) {
            if (buf[i] == fieldDel) {
                readField(protoTuple, buf, start, i);
                start = i + 1;
            }
        }
        
        // pick up the last field
        if (start <= len) {
            readField(protoTuple, buf, start, len);
        }
        
        return TupleFactory.getInstance().newTupleNoCopy(protoTuple);
    }
    
    //---------------------------------------------------------------
    // private methods 
    
    private static void readField(ArrayList<Object> protoTuple, 
                          byte[] buf, int start, int end) {
        if (start == end) {
            // NULL value
            protoTuple.add(null);
        } else {
            protoTuple.add(new DataByteArray(buf, start, end));
        }
    }
    
    private static String parseSingleQuotedString(String delimiter) {
        int startIndex = 0;
        int endIndex;
        while (startIndex < delimiter.length() 
                && delimiter.charAt(startIndex++) != '\'')
            ;
        endIndex = startIndex;
        while (endIndex < delimiter.length() 
                && delimiter.charAt(endIndex) != '\'') {
            if (delimiter.charAt(endIndex) == '\\') {
                endIndex++;
            }
            endIndex++;
        }
           
        return (endIndex < delimiter.length()) ?
                delimiter.substring(startIndex, endIndex) : delimiter;
    }
}
