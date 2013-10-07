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
package org.apache.pig.impl.streaming;

import org.apache.hadoop.io.WritableComparator;

public class StreamingDelimiters {
    //RECORD_END must be \n. This assumption is baked into our logic for reading in 
    //and parsing input.
    private static final byte RECORD_END = '\n'; 
    private static final byte PARAM_DELIM = '\t';
    private static final byte NULL_BYTE = '-';
    private static final byte TUPLE_BEGIN = '(';
    private static final byte TUPLE_END = ')';
    private static final byte BAG_BEGIN = '{';
    private static final byte BAG_END = '}';
    private static final byte MAP_BEGIN = '[';
    private static final byte MAP_END = ']';
    private static final byte FIELD_DELIM = ',';
    private static final byte MAP_KEY_VALUE_DELIM = '#'; //Not wrapped by wrapDelimField
    private byte preWrapDelim;
    private byte postWrapDelim;
    
    private byte[] tupleBegin;
    private byte[] tupleEnd;
    private byte[] bagBegin;
    private byte[] bagEnd;
    private byte[] mapBegin;
    private byte[] mapEnd;
    private byte[] fieldDelim;
    private byte[] nullByte;
    private byte[] paramDelim;
    private byte[] recordEnd;

    public StreamingDelimiters() {
        this((byte) 0, (byte) 0, true);
    }
    
    /**
     * 
     * @param preWrapDelim
     * @param postWrapDelim
     * @param useEmptyNull - In the past empty was used to serialize null.  But this can
     *      make it impossible to differentiate between an empty string and null.  Set
     *      to false if you want to use a special character to represent null.
     */
    public StreamingDelimiters(byte preWrapDelim, byte postWrapDelim, boolean useEmptyNull) {
        this.preWrapDelim = preWrapDelim;
        this.postWrapDelim = postWrapDelim;
        
        this.tupleBegin = getFullDelim(TUPLE_BEGIN);
        this.tupleEnd = getFullDelim(TUPLE_END);
        this.bagBegin = getFullDelim(BAG_BEGIN);
        this.bagEnd = getFullDelim(BAG_END);
        this.mapBegin = getFullDelim(MAP_BEGIN);
        this.mapEnd = getFullDelim(MAP_END);
        this.fieldDelim = getFullDelim(FIELD_DELIM);
        
        if (useEmptyNull) {
            this.nullByte = new byte[] {};
        } else {
            this.nullByte = getFullDelim(NULL_BYTE);
        }
        
        this.paramDelim = getFullDelim(PARAM_DELIM);
        //recordEnd has to end with the RECORD_END byte
        this.recordEnd = new byte[] {preWrapDelim, postWrapDelim, RECORD_END};
    }
    
    private byte[] getFullDelim(byte val) {
        if (preWrapDelim == 0)
            return new byte[] {val};
        else
            return new byte[] {preWrapDelim, val, postWrapDelim};
    }
    
    public byte[] getTupleBegin() {
        return tupleBegin;
    }
    
    public byte[] getTupleEnd() {
        return tupleEnd;
    }
    
    public byte[] getBagBegin() {
        return bagBegin;
    }
    
    public byte[] getBagEnd() {
        return bagEnd;
    }
    
    public byte[] getMapBegin() {
        return mapBegin;
    }
    
    public byte[] getMapEnd() {
        return mapEnd;
    }
    
    public byte[] getFieldDelim() {
        return fieldDelim;
    }
    
    public byte getMapKeyDelim() {
        return MAP_KEY_VALUE_DELIM;
    }
    
    public byte[] getNull() {
        return nullByte;
    }
    
    public byte[] getParamDelim() {
        return paramDelim;
    }
    
    public byte[] getRecordEnd() {
        return recordEnd;
    }

    /**
     * @return - The new depth.  Depth is increased if at the end of a byte sequence
     * that indicates the start of a bag, tuple, or map.  Depth is decreased if at the 
     * end of a byte sequence that indicates the end of a bug, tuple, or map.
     */
    public int updateDepth(byte[] buf, int currDepth, int index) {
        if (index < 2 || preWrapDelim == 0 || buf[index-2] != preWrapDelim || buf[index] != postWrapDelim) {
            return currDepth;
        }
        
        byte delimChar = preWrapDelim == 0 ? buf[index] : buf[index-1];
        if (delimChar == BAG_BEGIN || delimChar == TUPLE_BEGIN || delimChar == MAP_BEGIN) {
            return currDepth + 1;
        } else if (delimChar == BAG_END || delimChar == TUPLE_END || delimChar == MAP_END) {
            return currDepth - 1;
        } else {
            return currDepth;
        }
    }
    
    /**
     * 
     * @param delimiter
     * @param buf
     * @param index
     * @param depth
     * @param endIndex
     * @return - True iff the delimiter
     */
    public static boolean isDelimiter(byte[] delimiter, byte[] buf, int index, int depth, int endIndex) {
        return (depth == 0 && ( index == endIndex ||
                                ( index <= endIndex - 2 &&
                                  WritableComparator.compareBytes(
                                          buf, index, delimiter.length, 
                                          delimiter, 0, delimiter.length) == 0)));
    }

 }
