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

import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.lang.StringBuilder;
import java.util.ArrayList;
import java.util.Collection;

/**
 * An implementation of byte array.  This is done as an object because we
 * need to be able to implement compareTo, toString, hashCode, and some
 * other methods.
 */
public class DataByteArray implements Comparable, Serializable {
    byte[] mData = null;

    /**
     * Default constructor.  The data array will not be allocated when this
     * constructor is called.
     */
    public DataByteArray() {
    }

    /**
     * Construct a byte array using the provided bytes as the content.
     * @param b byte array to use as content.  A reference to the bytes
     * will be taken, the underlying bytes will not be copied.
     */
    public DataByteArray(byte[] b) {
        mData = b;
    }

    /**
     * Construct a byte array concatenating the two provided 
     * byte arrays as the content.
     * @param b the first byte array to use as content.
     * @param c the other byte array to use as content.
     * 
     */
    public DataByteArray(DataByteArray b, DataByteArray c) {
        byte[] ba = (b == null) ?  null : b.get();
        byte[] ca = (c == null) ?  null : c.get();
        int baLength = (ba == null) ? 0 : ba.length;
        int caLength = (ca == null) ? 0 : ca.length;
        
        int totalSize = baLength + caLength;
        if(totalSize == 0) {
            return;
        }
        mData = new byte[totalSize];
        int i = 0;
        for ( ;i < baLength; i++) {
            mData[i] = ba[i];
        }
        
        for (int j = 0; j < caLength; j++, i++) {
            mData[i] = ca[j];
        }
        
    }
    
    /**
     * Construct a byte array using a portion of the provided bytes as content.
     * @param b byte array to read from.  A copy of the underlying bytes will be
     * made.
     * @param start starting point to copy from
     * @param end ending point to copy to, exclusive.
     */
    public DataByteArray(byte[] b, int start, int end) {
        mData = new byte[end - start];
        for (int i = start; i < end; i++) {
            mData[i - start] = b[i];
        }
    }

    /**
     * Construct a byte array from a String.  The contents of the string
     * are copied.
     * @param s String to make a byte array out of.
     */
    public DataByteArray(String s) {
        try {
			mData = s.getBytes("UTF8");
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    /**
     * Find the size of the byte array.
     * @return number of bytes in the array.
     */
    public int size() {
        return mData.length;
    }

    /**
     * Get the underlying byte array.  This is the real thing, not a copy,
     * so don't mess with it!
     * @return underlying byte[]
     */
    public byte[] get() {
        return mData;
    }

    /**
     * Set the internal byte array.  This should not be called unless the
     * default constructor was used.
     * @param b byte array to store.  The contents of the byte array are
     * not copied.
     */
    public void set(byte[] b) {
        mData = b;
    }

    /**
     * Set the internal byte array.  This should not be called unless the
     * default constructor was used.
     * @param s String to copy.  The contents of the string are copied.
     */
    public void set(String s) {
        mData = s.getBytes();
    }

    @Override
    public String toString() {
        String r=null;
    	try {
			r = new String(mData, "UTF8");
		} catch (Exception e) {
			// TODO: handle exception
		}
		return r;
    }

    /**
     * Compare two byte arrays.  Comparison is done first using byte values
     * then length.  So "g" will be greater than "abcdefg", but "hello worlds"
     * is greater than "hello world".  If the other object is not a
     * DataByteArray, DataType.compare will be called.
     * @param other Other object to compare to.
     * @return -1 if less than, 1 if greater than, 0 if equal.
     */
    public int compareTo(Object other) {
        if (other instanceof DataByteArray) {
            DataByteArray dba = (DataByteArray)other;
            int mySz = mData.length;
            int tSz = dba.mData.length;
            int i;
            for (i = 0; i < mySz; i++) {
                // If the other has run out of characters, we're bigger.
                if (i >= tSz) return 1;
                if (mData[i] < dba.mData[i]) return -1;
                else if (mData[i] > dba.mData[i]) return 1;
            }
            // If the other still has characters left, it's greater
            if (i < tSz) return -1;
            return 0;
        } else {
            return DataType.compare(this, other);
        }
    }

    @Override
    public boolean equals(Object other) {
        return (compareTo(other) == 0);
    }

    @Override
    public int hashCode() {
        int hash = 1;
        for (int i = 0; i < mData.length; i++) {
            // 29 chosen because hash uses 31 and bag 37, and a I want a
            // prime.
            hash = 29 * hash + mData[i];
        }
        return hash;
    }

}
