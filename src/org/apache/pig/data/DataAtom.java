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

/**
 * The basic data unit. For now, we represent all atomic data objects as strings
 */
final public class DataAtom extends Datum {
    String stringVal = null;
    Double doubleVal = null;
    public static String EMPTY = "";
    byte[] binaryVal = null;
    
    public DataAtom() {
        stringVal = EMPTY;
        doubleVal = Double.POSITIVE_INFINITY;
    }

    public DataAtom(String valIn) {
        setValue(valIn);
    }

    public DataAtom(int valIn) {
        setValue(valIn);
    }

    public DataAtom(long valIn) {
        setValue(valIn);
    }
    
    public DataAtom(byte[] valIn){
        setValue(valIn);
    }

    public DataAtom(double valIn) {
        setValue(valIn);
    }

    public void setValue(String valIn) {
        stringVal = valIn;
        doubleVal = Double.POSITIVE_INFINITY;
    }
    
    public void setValue(byte[] valIn) {
        binaryVal = valIn;
        stringVal = null;
        doubleVal = Double.POSITIVE_INFINITY;
    }

    public void setValue(int valIn) {
        // conversion is cheap, do it now
        doubleVal = new Double(valIn);
        stringVal = Integer.toString(valIn);
    }

    public void setValue(long valIn) {
        // conversion is cheap, do it now
        // doubleVal = new Double(valIn);
        stringVal = Long.toString(valIn);
        doubleVal = Double.POSITIVE_INFINITY;
    }

    public void setValue(double valIn) {
        // conversion is cheap, do it now
        doubleVal = new Double(valIn);
        stringVal = Double.toString(valIn);
    }

    public String strval() {
        return stringVal;
    }

    public Double numval() {
        // lazy parse and create the numeric member value
        if (doubleVal == Double.POSITIVE_INFINITY) {
            doubleVal = new Double(stringVal);
        }
        return doubleVal;
    }

    public long longVal() {
        Long ll = new Long(stringVal);
        return ll.longValue();
        //return Long.getLong(stringVal).longValue();
    }

    @Override
    public String toString() {
        return stringVal;
    }

    
    @Override
    public boolean equals(Object other) {
        
        return compareTo(other) == 0;
    }    
    
    public int compareTo(Object other) {
        if (!(other instanceof DataAtom))
            return -1;
        DataAtom dOther = (DataAtom) other;
        
        return stringVal.compareTo(dOther.stringVal);
            
    }

    @Override
    public void write(DataOutput out) throws IOException {
         out.write(ATOM);
         byte[] data;
         try {
             data = strval().getBytes("UTF-8");
         } catch (Exception e) {
             long size = strval().length();
             throw new RuntimeException("Error dealing with DataAtom of size " + size, e);
         }
         Tuple.encodeInt(out, data.length);
         out.write(data);    
    }
    
    static DataAtom read(DataInput in) throws IOException {
        int len = Tuple.decodeInt(in);
        DataAtom ret = new DataAtom();
        byte[] data = new byte[len];
        in.readFully(data);
        ret.setValue(new String(data, "UTF-8"));
        return ret;
    }

    
    @Override
    public int hashCode() {
        return stringVal.hashCode();
    }

    @Override
    public long getMemorySize() {
        long used = 0;
        if (stringVal != null) used += stringVal.length() * 2 + OBJECT_SIZE;
        if (doubleVal != null) used += 8 + OBJECT_SIZE;
        if (binaryVal != null) used += binaryVal.length + OBJECT_SIZE;
        used += OBJECT_SIZE + 3 * REF_SIZE;
        return used;
     }
}
