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
package org.apache.pig.impl.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

/**
 * A base class for all types that pig uses to move data between map and
 * reduce.  It implements WritableComparable so that compareTo etc. can be
 * called.  It also wraps a WritableComparable 'value'.  This is set by each
 * different type to be an object of its specific type.
 * It also provides a getIndex() and setIndex() calls that are used to get
 * and set the index.  These can be used by LocalRearrange, the partitioner,
 * and Package to determine the index.
 *
 * Index and the null indicator are packed into one byte to save space.
 */

//Put in to make the compiler not complain about WritableComparable
//being a generic type.
@SuppressWarnings("unchecked")
public abstract class PigNullableWritable implements WritableComparable {

    /**
     * indices in multiquery optimized maps
     * will have the Most Significant Bit set
     * This is a bitmask used in those cases.
     */
    public static final byte mqFlag = (byte)0x80;

    /**
     *  regular indices used in group and cogroup
     *  can only go from 0x00 to 0x7F
     */
    public static final byte idxSpace = (byte)0x7F;

    private boolean mNull;

    protected WritableComparable mValue;

    private byte mIndex;

    /**
     * Compare two nullable objects.  Step one is to check if either or both
     * are null.  If one is null and the other is not, then the one that is
     * null is declared to be less.  If both are null the indices are
     * compared.  If neither are null the indices are again compared.  If
     * these are equal, finally the values are compared.
     *
     * These comparators are used by hadoop as part of the post-map sort, when
     * the data is still in object format.
     */
    @Override
    public int compareTo(Object o) {
        PigNullableWritable w = (PigNullableWritable)o;

        if ((mIndex & mqFlag) != 0) { // this is a multi-query index

            if ((mIndex & idxSpace) < (w.mIndex & idxSpace)) return -1;
            else if ((mIndex & idxSpace) > (w.mIndex & idxSpace)) return 1;
        }

        if (!mNull && !w.mNull) {
            int result = mValue.compareTo(w.mValue);

            // If any of the field inside tuple is null, then we do not merge keys
            // See PIG-927
            if (result == 0 && mValue instanceof Tuple && w.mValue instanceof Tuple)
            {
                try {
                    for (int i=0;i<((Tuple)mValue).size();i++)
                        if (((Tuple)mValue).get(i)==null)
                            return mIndex - w.mIndex;
                } catch (ExecException e) {
                    throw new RuntimeException("Unable to access tuple field", e);
                }
            }
            return result;
        } else if (mNull && w.mNull) {
            // If they're both null, compare the indicies
            if ((mIndex & idxSpace) < (w.mIndex & idxSpace)) return -1;
            else if ((mIndex & idxSpace) > (w.mIndex & idxSpace)) return 1;
            else return 0;
        }
        else if (mNull) return -1;
        else return 1;
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.io.IntWritable#readFields(java.io.DataInput)
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        mNull = in.readBoolean();
        if (!mNull) mValue.readFields(in);
        mIndex = in.readByte();
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.io.IntWritable#write(java.io.DataOutput)
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeBoolean(mNull);
        if (!mNull) mValue.write(out);
        out.writeByte(mIndex);
    }

    /**
     * @return the isNull
     */
    public boolean isNull() {
        return mNull;
    }

    /**
     * @param isNull the isNull to set
     */
    public void setNull(boolean isNull) {
        mNull = isNull;
    }

    /**
     * @return the index for this value
     */
    public byte getIndex() {
        return mIndex;
    }

    /**
     * @param index for this value.
     */
    public void setIndex(byte index) {
        mIndex = index;
    }

    /**
     * @return The wrapped value as a pig type, not as a WritableComparable.
     */
    abstract public Object getValueAsPigType();

    @Override
    public int hashCode() {
        // For now, always give a null a hash code of 0.  It isn't clear this
        // is what we'll always want.  If nulls make a significant but
        // not overwhelming amount of the data we may want them to get their
        // own partition.  If they make up a big enough percentage of the
        // data we may want to split them across partitions (though that
        // would obviously limit how they could be dealt with afterwards).
        if (mNull) return 0;
        else return mValue.hashCode();
    }



    @Override
    public boolean equals(Object arg0) {
        return compareTo(arg0)==0;
    }

    @Override
    public String toString() {
        return "Null: " + mNull + " index: " + mIndex + " " + mValue.toString();
    }
}
