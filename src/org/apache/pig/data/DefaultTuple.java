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
import java.lang.StringBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.WritableComparable;

import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;

/**
 * A default implementation of Tuple.  This class will be created by the
 * DefaultTupleFactory.
 */
public class DefaultTuple implements Tuple {
    
    protected boolean isNull = false;
    private static final long serialVersionUID = 2L;
    protected List<Object> mFields;
    
    /**
     * Default constructor.  This constructor is public so that hadoop can call
     * it directly.  However, inside pig you should never be calling this
     * function.  Use TupleFactory instead.
     */
    public DefaultTuple() {
        mFields = new ArrayList<Object>();
    }

    /**
     * Construct a tuple with a known number of fields.  Package level so
     * that callers cannot directly invoke it.
     * @param size Number of fields to allocate in the tuple.
     */
    DefaultTuple(int size) {
        mFields = new ArrayList<Object>(size);
        for (int i = 0; i < size; i++) mFields.add(null);
    }

    /**
     * Construct a tuple from an existing list of objects.  Package
     * level so that callers cannot directly invoke it.
     * @param c List of objects to turn into a tuple.
     */
    DefaultTuple(List<Object> c) {
        mFields = new ArrayList<Object>(c.size());

        Iterator<Object> i = c.iterator();
        int field;
        for (field = 0; i.hasNext(); field++) mFields.add(field, i.next());
    }

    /**
     * Construct a tuple from an existing list of objects.  Package
     * level so that callers cannot directly invoke it.
     * @param c List of objects to turn into a tuple.  This list will be kept
     * as part of the tuple.
     * @param junk Just used to differentiate from the constructor above that
     * copies the list.
     */
    DefaultTuple(List<Object> c, int junk) {
        mFields = c;
    }


    /**
     * Make this tuple reference the contents of another.  This method does not copy
     * the underlying data.   It maintains references to the data from the original
     * tuple (and possibly even to the data structure holding the data).
     * @param t Tuple to reference.
     */
    public void reference(Tuple t) {
        mFields = t.getAll();
    }

    /**
     * Find the size of the tuple.  Used to be called arity().
     * @return number of fields in the tuple.
     */
    public int size() {
        return mFields.size();
    }

    /**
     * Find out if a given field is null.
     * @param fieldNum Number of field to check for null.
     * @return true if the field is null, false otherwise.
     * @throws ExecException if the field number given is greater
     * than or equal to the number of fields in the tuple.
     */
    public boolean isNull(int fieldNum) throws ExecException {
        checkBounds(fieldNum);
        return (mFields.get(fieldNum) == null);
    }

    /**
     * Find the type of a given field.
     * @param fieldNum Number of field to get the type for.
     * @return type, encoded as a byte value.  The values are taken from
     * the class DataType.  If the field is null, then DataType.UNKNOWN
     * will be returned.
     * @throws ExecException if the field number is greater than or equal to
     * the number of fields in the tuple.
     */
    public byte getType(int fieldNum) throws ExecException {
        checkBounds(fieldNum);
        return DataType.findType(mFields.get(fieldNum));
    }

    /**
     * Get the value in a given field.
     * @param fieldNum Number of the field to get the value for.
     * @return value, as an Object.
     * @throws ExecException if the field number is greater than or equal to
     * the number of fields in the tuple.
     */
    public Object get(int fieldNum) throws ExecException {
        checkBounds(fieldNum);
        return mFields.get(fieldNum);
    }

    /**
     * Get all of the fields in the tuple as a list.
     * @return List&lt;Object&gt; containing the fields of the tuple
     * in order.
     */
    public List<Object> getAll() {
        return mFields;
    }

    /**
     * Set the value in a given field.
     * @param fieldNum Number of the field to set the value for.
     * @param val Object to put in the indicated field.
     * @throws ExecException if the field number is greater than or equal to
     * the number of fields in the tuple.
     */
    public void set(int fieldNum, Object val) throws ExecException {
        checkBounds(fieldNum);
        mFields.set(fieldNum, val);
    }

    /**
     * Append a field to a tuple.  This method is not efficient as it may
     * force copying of existing data in order to grow the data structure.
     * Whenever possible you should construct your Tuple with the
     * newTuple(int) method and then fill in the values with set(), rather
     * than construct it with newTuple() and append values.
     * @param val Object to append to the tuple.
     */
    public void append(Object val) {
        mFields.add(val);
    }

    /**
     * Determine the size of tuple in memory.  This is used by data bags
     * to determine their memory size.  This need not be exact, but it
     * should be a decent estimation.
     * @return estimated memory size.
     */
    public long getMemorySize() {
        Iterator<Object> i = mFields.iterator();
        long sum = 0;
        while (i.hasNext()) {
            sum += getFieldMemorySize(i.next());
        }
        return sum;
    }

    /** 
     * Write a tuple of atomic values into a string.  All values in the
     * tuple must be atomic (no bags, tuples, or maps).
     * @param delim Delimiter to use in the string.
     * @return A string containing the tuple.
     * @throws ExecException if a non-atomic value is found.
     */
    public String toDelimitedString(String delim) throws ExecException {
        StringBuilder buf = new StringBuilder();
        for (Iterator<Object> it = mFields.iterator(); it.hasNext();) {
            Object field = it.next();
            buf.append(field == null ? "" : field.toString());
            if (it.hasNext())
                buf.append(delim);
        }
        return buf.toString();
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append('(');
        for (Iterator<Object> it = mFields.iterator(); it.hasNext();) {
            Object d = it.next();
            if(d != null) {
                if(d instanceof Map) {
                    sb.append(DataType.mapToString((Map<Object, Object>)d));
                } else {
                    sb.append(d.toString());
                    if(d instanceof Long) {
                        sb.append("L");
                    } else if(d instanceof Float) {
                        sb.append("F");
                    }
                }
            } else {
                sb.append("");
            }
            if (it.hasNext())
                sb.append(",");
        }
        sb.append(')');
        return sb.toString();
    }

    public int compareTo(Object other) {
        if (other instanceof Tuple) {
            Tuple t = (Tuple)other;
            int mySz = mFields.size();
            int tSz = t.size();
            if (tSz < mySz) {
                return 1;
            } else if (tSz > mySz) {
                return -1;
            } else {
                for (int i = 0; i < mySz; i++) {
                    try {
                        int c = DataType.compare(mFields.get(i), t.get(i));
                        if (c != 0) {
                            return c;
                        }
                    } catch (ExecException e) {
                        throw new RuntimeException("Unable to compare tuples", e);
                    }
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
        for (Iterator<Object> it = mFields.iterator(); it.hasNext();) {
            Object o = it.next();
            if (o != null) {
                hash = 31 * hash + o.hashCode();
            }
        }
        return hash;
    }

    public void write(DataOutput out) throws IOException {
        out.writeByte(DataType.TUPLE);
        int sz = size();
        out.writeInt(sz);
        for (int i = 0; i < sz; i++) {
            DataReaderWriter.writeDatum(out, mFields.get(i));
        }
    }

    public void readFields(DataInput in) throws IOException {
        // Clear our fields, in case we're being reused.
        mFields.clear();
    
        // Make sure it's a tuple.
        byte b = in.readByte();
        if (b != DataType.TUPLE) {
            int errCode = 2112;
            String msg = "Unexpected data while reading tuple " +
            "from binary file.";
            throw new ExecException(msg, errCode, PigException.BUG);
        }
        // Read the number of fields
        int sz = in.readInt();
        for (int i = 0; i < sz; i++) {
            try {
                append(DataReaderWriter.readDatum(in));
            } catch (ExecException ee) {
                throw ee;
            }
        }
    }

    private long getFieldMemorySize(Object o) {
        // 12 is added to each to account for the object overhead and the
        // pointer in the tuple.
        switch (DataType.findType(o)) {
            case DataType.BYTEARRAY: {
                byte[] bytes = ((DataByteArray)o).get();
                return bytes.length + 12;
            }

            case DataType.CHARARRAY: {
                String s = (String)o;
                return s.length() * 2 + 12;
            }

            case DataType.TUPLE: {
                Tuple t = (Tuple)o;
                return t.getMemorySize() + 12;
            }

            case DataType.BAG: {
                DataBag b = (DataBag)o;
                return b.getMemorySize() + 12;
            }

            case DataType.INTEGER:
                return 4 + 12;

            case DataType.LONG:
                return 8 + 12;

            case DataType.MAP: {
                Map<Object, Object> m = (Map<Object, Object>)o;
                Iterator<Map.Entry<Object, Object> > i =
                    m.entrySet().iterator();
                long sum = 0;
                while (i.hasNext()) {
                    Map.Entry<Object, Object> entry = i.next();
                    sum += getFieldMemorySize(entry.getKey());
                    sum += getFieldMemorySize(entry.getValue());
                }
                return sum + 12;
            }

            case DataType.FLOAT:
                return 8 + 12;

            case DataType.DOUBLE:
                return 16 + 12;

            case DataType.BOOLEAN:
                return 4 + 12;

            default:
                // ??
                return 12;
        }
    }

    private void checkBounds(int fieldNum) throws ExecException {
        if (fieldNum >= mFields.size()) {
            int errCode = 1072;
            String msg = "Out of bounds access: Request for field number " + fieldNum +
            " exceeds tuple size of " + mFields.size();
            throw new ExecException(msg, errCode, PigException.INPUT);
        }
    }
    
    /**
     * @return true if this Tuple is null
     */
    public boolean isNull() {
        return isNull;
    }

    /**
     * @param isNull boolean indicating whether this tuple is null
     */
    public void setNull(boolean isNull) {
        this.isNull = isNull;
    }

}
