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

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;

/**
 * An implementation of byte array.  This is done as an object because we
 * need to be able to implement compareTo, toString, hashCode, and some
 * other methods.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class DataByteArray implements Comparable, Serializable {

    private static final long serialVersionUID = 1L;
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
        System.arraycopy(ba, 0, mData = new byte[totalSize], 0, baLength);
        System.arraycopy(ca, 0, mData,baLength, caLength);
    }
    
    /**
     * Construct a byte array using a portion of the provided bytes as content.
     * @param b byte array to read from.  A copy of the underlying bytes will be
     * made.
     * @param start starting point to copy from
     * @param end ending point to copy to, exclusive.
     */
    public DataByteArray(byte[] b, int start, int end) {
    	
    System.arraycopy(b, start, mData = new byte[end - start], 0, end-start);
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

    /**
     * Append given byte array to the internal byte array.
     * @param b byte array who's contents to append.  The contents of the byte array are
     * copied.
     */
    public DataByteArray append(DataByteArray b) {

        byte[] ba = (b == null) ?  null : b.get();
        return append(ba, 0, ba == null ? 0 : ba.length);

    }

    public DataByteArray append(byte [] ba){
      return append(ba, 0, ba.length);
    }

    public DataByteArray append(byte [] ba, int start, int baLength){
        int mDataLength = (mData == null) ? 0 : mData.length;

        int totalSize = mDataLength + baLength;
        if(totalSize == 0) {
            return this;
        }
        byte[] oldData = mData == null ? new byte[0] : mData.clone();
        System.arraycopy(oldData, 0, mData = new byte[totalSize], 0, mDataLength);
        System.arraycopy(ba, start, mData, mDataLength, baLength);
        return this;
    }

    public DataByteArray append(String str){
      try {
        return append(str.getBytes("UTF8"));
      } catch (UnsupportedEncodingException e) {
        e.printStackTrace();
      }
      //TODO: better error here
      throw new RuntimeException("Unable to append str: " + str);
    }

    /**
     * Convert the byte array to a string.  UTF8 encoding will be assumed.
     */
    @Override
    public String toString() {
        String r="";
    	try {
			r = new String(mData, "UTF8");
		} catch (Exception e) {
			// TODO: handle exception
		}
		return r;
    }

    /**
     * Compare two byte arrays. Comparison is done first using byte values then
     * length. So "g" will be greater than "abcdefg", but "hello worlds" is
     * greater than "hello world". If the other object is not a DataByteArray,
     * {@link DataType#compare} will be called.
     * 
     * @param other Other object to compare to.
     * @return -1 if less than, 1 if greater than, 0 if equal.
     */
    public int compareTo(Object other) {
        if (other instanceof DataByteArray) {
            DataByteArray dba = (DataByteArray) other;
            return compare(mData, dba.mData);
        } else {
            return DataType.compare(this, other);
        }
    }

    public static int compare(byte[] b1, byte[] b2) {
        int i;
        for (i = 0; i < b1.length; i++) {
            // If the other has run out of characters, we're bigger.
            if (i >= b2.length)
                return 1;
            if (b1[i] < b2[i])
                return -1;
            else if (b1[i] > b2[i])
                return 1;
        }
        // If the other still has characters left, it's greater
        if (i < b2.length)
            return -1;
        return 0;
    }

    @Override
    public boolean equals(Object other) {
        return (compareTo(other) == 0);
    }

    @Override
    public int hashCode() {
        return hashCode(mData);
        }

    public static int hashCode(byte[] buf) {
        return Arrays.hashCode(buf);
    }

}
