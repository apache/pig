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
import java.lang.StringBuilder;
import java.util.ArrayList;
import java.util.Collection;

/**
 * An implementation of byte array.  This is done as an object because we
 * need to be able to implement compareTo, toString, hashCode, and some
 * other methods.
 */
public class DataByteArray implements Comparable {
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
     * Construct a byte array from a String.  The contents of the string
     * are copied.
     * @param s String to make a byte array out of.
     */
    public DataByteArray(String s) {
        mData = s.getBytes();
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
        return new String(mData);
    }

    public int compareTo(Object other) {
        if (other instanceof DataByteArray) {
            DataByteArray dba = (DataByteArray)other;
            int mySz = mData.length;
            int tSz = dba.mData.length;
            if (tSz < mySz) {
                return 1;
            } else if (tSz > mySz) {
                return -1;
            } else {
                for (int i = 0; i < mySz; i++) {
                    if (mData[i] < dba.mData[i]) return -1;
                    else if (mData[i] > dba.mData[i]) return 1;
                }
                return 0;
            }
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
