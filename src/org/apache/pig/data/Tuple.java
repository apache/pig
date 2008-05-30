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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.WritableComparable;


/**
 * an ordered list of Datums
 */
public class Tuple extends Datum implements WritableComparable {
    
    private static final Log log = LogFactory.getLog(Tuple.class);
    private static int numFields = 5;
    
    protected ArrayList<Datum> fields;
    static String              defaultDelimiter = "[,\t]";
    static String              NULL = "__PIG_NULL__";

    public Tuple() {
        this(0);
    }

    public Tuple(int numFields) {
        fields = new ArrayList<Datum>(numFields);
        for (int i = 0; i < numFields; i++) {
            fields.add(null);
        }
    }

    public Tuple(List<Datum> fieldsIn) {
        fields = new ArrayList<Datum>(fieldsIn.size());
        fields.addAll(fieldsIn);
    }
    
    /**
     * shortcut, if tuple only has one field
     */
    public Tuple(Datum fieldIn) {
        fields = new ArrayList<Datum>(1);
        fields.add(fieldIn);
    }

    /**
     * Creates a tuple from a delimited line of text
     * 
     * @param textLine
     *            the line containing fields of data
     * @param delimiter 
     *              the delimiter (normal string, NO REGEX!!)
     */
    public Tuple(String textLine, String delimiter) {
        if (delimiter == null) {
            delimiter = defaultDelimiter;
        }
        
        fields = new ArrayList<Datum>(numFields) ;
        int delimSize = delimiter.length() ;
        boolean done = false ;
        
        int lastIdx = 0 ;
        
        while (!done) {
            int newIdx = textLine.indexOf(delimiter, lastIdx) ;
            if (newIdx != (-1)) {
                String token = textLine.substring(lastIdx, newIdx) ;
                fields.add(new DataAtom(token));
                lastIdx = newIdx + delimSize  ;
            }
            else {
                String token = textLine.substring(lastIdx) ;
                fields.add(new DataAtom(token));
                done = true ;
            }
        }

        numFields = fields.size();
    }

    /**
     * Creates a tuple from a delimited line of text. This will invoke Tuple(textLine, null)
     * 
     * @param textLine
     *            the line containing fields of data
     */
    public Tuple(String textLine) {
        this(textLine, defaultDelimiter);
    }

    public Tuple(Tuple[] otherTs) {
        fields = new ArrayList<Datum>(otherTs.length);
        for (int i = 0; i < otherTs.length; i++) {
                appendTuple(otherTs[i]);
        }
    }

    public void copyFrom(Tuple otherT) {
        this.fields = otherT.fields;
    }

    public int arity() {
        return fields.size();
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append('(');
        for (Iterator<Datum> it = fields.iterator(); it.hasNext();) {
            Datum d = it.next();
            if(d != null) {
                sb.append(d.toString());
            } else {
                sb.append(NULL);
            }
            if (it.hasNext())
                sb.append(", ");
        }
        sb.append(')');
        String s = sb.toString();
        return s;
    }

    public void setField(int i, Datum val) {
        getField(i); // throws exception if field doesn't exist

        fields.set(i, val);
    }

    public void setField(int i, int val) {
        setField(i, new DataAtom(val));
    }

    public void setField(int i, double val) {
        setField(i, new DataAtom(val));
    }

    public void setField(int i, String val) {
        setField(i, new DataAtom(val));
    }

    public Datum getField(int i) {
        if (fields.size() >= i + 1) {
            return fields.get(i);
        }

        StringBuilder sb = new StringBuilder();
        sb.append("Requested index ");
        sb.append(i);
        sb.append(" from tuple ");
        sb.append(toString());
        throw new IndexOutOfBoundsException(sb.toString());
    }

    // Get field i, if it is an Atom or can be coerced into an Atom
    public DataAtom getAtomField(int i) {
        Datum field = getField(i); // throws exception if field doesn't exist

        if (field instanceof DataAtom) {
            return (DataAtom) field;
        } else if (field instanceof Tuple) {
            Tuple t = (Tuple) field;
            if (t.arity() == 1) {
                log.warn("Requested for an atom field but found a tuple with one field.");
                return t.getAtomField(0);
            }
        } else if (field instanceof DataBag) {
            DataBag b = (DataBag) field;
            if (b.size() == 1) {
                Tuple t = b.iterator().next();
                if (t.arity() == 1) {
                    return t.getAtomField(0);
                }
            }
        }

        throw newTupleAccessException(field, "atom", i);
    }
    
    private RuntimeException newTupleAccessException(Datum field,
            String requestedFieldType, int index) {
        return new IllegalArgumentException("Requested " + requestedFieldType
                + " field at index " + index + " but was '"
                + field.getClass().getName() + "' in tuple: " + toString());
    }

    // Get field i, if it is a Tuple or can be coerced into a Tuple
    public Tuple getTupleField(int i) {
        Datum field = getField(i); // throws exception if field doesn't exist

        if (field instanceof Tuple) {
            return (Tuple) field;
        } else if (field instanceof DataBag) {
            DataBag b = (DataBag) field;
            if (b.size() == 1) {
                return b.iterator().next();
            }
        }

        throw newTupleAccessException(field, "tuple", i);
    }

    // Get field i, if it is a Bag or can be coerced into a Bag
    public DataBag getBagField(int i) {
        Datum field = getField(i); // throws exception if field doesn't exist

        if (field instanceof DataBag) {
            return (DataBag) field;
        }

        throw newTupleAccessException(field, "bag", i);
    }

    public void appendTuple(Tuple other){
        for (Iterator<Datum> it = other.fields.iterator(); it.hasNext();) {
            this.fields.add(it.next());
        }
    }

    public void appendField(Datum newField){
        this.fields.add(newField);
    }

    public String toDelimitedString(String delim) throws IOException {
        StringBuffer buf = new StringBuffer();
        for (Iterator<Datum> it = fields.iterator(); it.hasNext();) {
            Datum field = it.next();
            if (!(field instanceof DataAtom)) {
                throw new IOException("Unable to convert non-flat tuple to string.");
            }

            buf.append((DataAtom) field);
            if (it.hasNext())
                buf.append(delim);
        }
        return buf.toString();
    }

    public boolean lessThan(Tuple other) {
        return (this.compareTo(other) < 0);
    }

    public boolean greaterThan(Tuple other) {
        return (this.compareTo(other) > 0);
    }
    
    @Override
    public boolean equals(Object other){
            return compareTo(other)==0;
    }    
    
    public int compareTo(Tuple other) {
        if (other.fields.size() != this.fields.size())
            return other.fields.size() < this.fields.size() ? 1 : -1;

        for (int i = 0; i < this.fields.size(); i++) {
            int c = this.fields.get(i).compareTo(other.fields.get(i));
            if (c != 0)
                return c;
        }
        return 0;
    }

    public int compareTo(Object other) {
        if (other instanceof DataAtom)
            return +1;
        else if (other instanceof DataBag)
            return +1;
        else if (other instanceof Tuple)
            return compareTo((Tuple) other);
        else
            return -1;
    }

    @Override
    public int hashCode() {
        int hash = 1;
        for (Iterator<Datum> it = fields.iterator(); it.hasNext();) {
            hash = 31 * hash + it.next().hashCode();
        }
        return hash;
    }

    // WritableComparable methods:
   
    @Override
    public void write(DataOutput out) throws IOException {
        out.write(TUPLE);
        int n = arity();
        encodeInt(out, n);
        for (int i = 0; i < n; i++) {
            Datum d = getField(i);
            if (d!=null){
                d.write(out);
            }else{
                throw new RuntimeException("Null field in tuple");
            }
        }
    }

    //This method is invoked when the beginning 'TUPLE' is still on the stream
    public void readFields(DataInput in) throws IOException {
        byte[] b = new byte[1];
        in.readFully(b);
        if (b[0]!=TUPLE)
            throw new IOException("Unexpected data while reading tuple from binary file");
        Tuple t = read(in);
        fields = t.fields;
    }
    
    //This method is invoked when the beginning 'TUPLE' has been read off the stream
    public static Tuple read(DataInput in) throws IOException {
        // nuke the old contents of the tuple
        Tuple ret = new Tuple();
        ret.fields = new ArrayList<Datum>();

        int size = decodeInt(in);
        
        for (int i = 0; i < size; i++) {
            ret.appendField(readDatum(in));
        }
        
        return ret;

    }
    
    public static Datum readDatum(DataInput in) throws IOException{
        byte[] b = new byte[1];
        in.readFully(b);
        switch (b[0]) {
            case TUPLE:
                return Tuple.read(in);
            case BAG:
                return DataBag.read(in);
            case MAP:
                return DataMap.read(in);
            case ATOM:
                return DataAtom.read(in);
            default:
                throw new IOException("Invalid data while reading Datum from binary file");
        }
    }

    //  Encode the integer so that the high bit is set on the last
    // byte
    static void encodeInt(DataOutput os, int i) throws IOException {
        if (i >> 28 != 0)
            os.write((i >> 28) & 0x7f);
        if (i >> 21 != 0)
            os.write((i >> 21) & 0x7f);
        if (i >> 14 != 0)
            os.write((i >> 14) & 0x7f);
        if (i >> 7 != 0)
            os.write((i >> 7) & 0x7f);
        os.write((i & 0x7f) | (1 << 7));
    }

    static int decodeInt(DataInput is) throws IOException {
        int i = 0;
        int c;
        while (true) {
            c = is.readUnsignedByte();
            if (c == -1)
                break;
            i <<= 7;
            i += c & 0x7f;
            if ((c & 0x80) != 0)
                break;
        }
        return i;
    }

    @Override
    public long getMemorySize() {
        long used = 0;
        int sz = fields.size();
        for (int i = 0; i < sz; i++)
            used += getField(i).getMemorySize();
        used += 2 * OBJECT_SIZE + REF_SIZE;
        return used;
    }
}
