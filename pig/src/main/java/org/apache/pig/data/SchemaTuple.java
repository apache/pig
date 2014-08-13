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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;
import org.apache.pig.data.utils.MethodHelper;
import org.apache.pig.data.utils.MethodHelper.NotImplemented;
import org.apache.pig.data.utils.SedesHelper;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.ObjectSerializer;
import org.mortbay.log.Log;

import com.google.common.collect.Lists;

/**
 * A SchemaTuple is a type aware tuple that is much faster and more memory efficient.
 * In our implementation, given a Schema, code generation is used to extend this class.
 * This class provides a broad range of functionality that minimizes the complexity of the
 * code that must be generated. The odd looking generic signature allows for certain
 * optimizations, such as "setSpecific(T t)", which allows us to do much faster sets and
 * comparisons when types match (since the code is generated, there is no other way to know).
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public abstract class SchemaTuple<T extends SchemaTuple<T>> extends AbstractTuple implements TypeAwareTuple {

    private static final long serialVersionUID = 1L;
    private static final int ONE_MINUTE = 60000;
    private static final BinInterSedes bis = new BinInterSedes();

    @NotImplemented
    @Override
    public void append(Object val) {
        throw MethodHelper.methodNotImplemented();
    }

    /**
     * This only accounts for the size of members (adjusting for word alignment). It also includes
     * the size of the object itself, since this never affects word boundaries.
     */
    @Override
    public long getMemorySize() {
        return 16 //Object header
             + getGeneratedCodeMemorySize();
    }

    protected abstract long getGeneratedCodeMemorySize();

    /**
     * This method will return the identifier that the generated code
     * was generated with. This is useful because when the classes
     * are resolved generically, this let's us know the identifier, which
     * is used when serlializing and deserializing tuples.
     * @return the identifire as Int.
     */
    public abstract int getSchemaTupleIdentifier();
    protected abstract int schemaSize();

    public String getSchemaString() {
        return getSchema().toString();
    }

    protected SchemaTuple<T> set(SchemaTuple<?> t, boolean checkType) throws ExecException {
        return generatedCodeSet(t, checkType);
    }

    protected abstract SchemaTuple<T> generatedCodeSet(SchemaTuple<?> t, boolean checkType) throws ExecException;

    protected SchemaTuple<T> setSpecific(T t) {
        return generatedCodeSetSpecific(t);
    }

    protected abstract SchemaTuple<T> generatedCodeSetSpecific(T t);

    public SchemaTuple<T> set(Tuple t) throws ExecException {
        return set(t, true);
    }

    @SuppressWarnings("unchecked") //this is ok because we only cast to T after checking
    protected SchemaTuple<T> set(Tuple t, boolean checkType) throws ExecException {
        if (checkType) {
            if (isSpecificSchemaTuple(t)) {
                return setSpecific((T)t);
            }

            if (t instanceof SchemaTuple<?>) {
                return set((SchemaTuple<?>)t, false);
        }
        }

        return set(t.getAll());
    }

    public SchemaTuple<T> set(SchemaTuple<?> t) throws ExecException {
        return set(t, true);
    }

    public SchemaTuple<T> set(List<Object> l) throws ExecException {
        if (l.size() != schemaSize()) {
            throw new ExecException("Given list of objects has improper number of fields ("+l.size()+" vs "+schemaSize()+")");
        }

        generatedCodeSetIterator(l.iterator());

        return this;
    }

    protected abstract void generatedCodeSetIterator(Iterator<Object> l) throws ExecException;

    protected void write(DataOutput out, boolean writeIdentifiers) throws IOException {
        if (writeIdentifiers) {
            int id = getSchemaTupleIdentifier();
            if (id < BinInterSedes.UNSIGNED_BYTE_MAX) {
                out.writeByte(BinInterSedes.SCHEMA_TUPLE_BYTE_INDEX);
                out.writeByte(id);
            } else if (id < BinInterSedes.UNSIGNED_SHORT_MAX) {
                out.writeByte(BinInterSedes.SCHEMA_TUPLE_SHORT_INDEX);
                out.writeShort(id);
            } else {
                out.writeByte(BinInterSedes.SCHEMA_TUPLE);
                out.writeInt(id);
            }
        }
        writeElements(out);
    }

    protected static void write(DataOutput out, DataBag v) throws IOException {
        bis.writeDatum(out, v, DataType.BAG);
    }

    protected static void write(DataOutput out, Map<String, Object> v) throws IOException {
        bis.writeDatum(out, v, DataType.MAP);
    }

    protected static void write(DataOutput out, int v) throws IOException {
        SedesHelper.Varint.writeSignedVarInt(v, out);
    }

    protected static void write(DataOutput out, long v) throws IOException {
        SedesHelper.Varint.writeSignedVarLong(v, out);
    }

    protected static void write(DataOutput out, float v) throws IOException {
        out.writeFloat(v);
    }

    protected static void write(DataOutput out, double v) throws IOException {
        out.writeDouble(v);
    }

    protected static void write(DataOutput out, DateTime v) throws IOException {
        out.writeLong(v.getMillis());
        out.writeShort(v.getZone().getOffset(v) / ONE_MINUTE);
    }

    protected static void write(DataOutput out, BigDecimal v) throws IOException {
        bis.writeDatum(out, v, DataType.BIGDECIMAL);
    }

    protected static void write(DataOutput out, BigInteger v) throws IOException {
        bis.writeDatum(out, v, DataType.BIGINTEGER);
    }

    protected static void write(DataOutput out, byte[] v) throws IOException {
        SedesHelper.writeBytes(out, v);
    }

    protected static void write(DataOutput out, String v) throws IOException {
        SedesHelper.writeChararray(out, v);
    }

    protected static void write(DataOutput out, SchemaTuple<?> t) throws IOException {
        t.writeElements(out);
    }

    protected static DataBag read(DataInput in, DataBag v) throws IOException {
        return (DataBag) bis.readDatum(in);
    }

    @SuppressWarnings("unchecked")
    protected static Map<String, Object> read(DataInput in, Map<String, Object> v) throws IOException {
        return (Map<String, Object>) bis.readDatum(in);
    }

    protected static int read(DataInput in, int v) throws IOException {
        return SedesHelper.Varint.readSignedVarInt(in);
    }

    protected static long read(DataInput in, long v) throws IOException {
        return SedesHelper.Varint.readSignedVarLong(in);
    }

    protected static float read(DataInput in, float v) throws IOException {
        return in.readFloat();
    }

    protected static double read(DataInput in, double v) throws IOException {
        return in.readDouble();
    }

    protected static DateTime read(DataInput in, DateTime v) throws IOException {
        return new DateTime(in.readLong(), DateTimeZone.forOffsetMillis(in.readShort() * ONE_MINUTE));
    }

    protected static BigDecimal read(DataInput in, BigDecimal v) throws IOException {
        return (BigDecimal) bis.readDatum(in);
    }

    protected static BigInteger read(DataInput in, BigInteger v) throws IOException {
        return (BigInteger) bis.readDatum(in);
    }

    protected static String read(DataInput in, String v) throws IOException {
        return SedesHelper.readChararray(in, in.readByte());
    }

    protected static byte[] read(DataInput in, byte[] v) throws IOException {
        return SedesHelper.readBytes(in, in.readByte());
    }

    @Override
    public void write(DataOutput out) throws IOException {
       write(out, true);
    }

    @Override
    public void reference(Tuple t) {
        try {
            set(t);
        } catch (ExecException e) {
            throw new RuntimeException("Failure to set given tuple: " + t, e);
        }
    }

    //TODO could generate a faster getAll in the code
    @Override
    public List<Object> getAll() {
        List<Object> l = Lists.newArrayListWithCapacity(size());
        for (int i = 0; i < size(); i++) {
            try {
                l.add(get(i));
            } catch (ExecException e) {
                throw new RuntimeException("Error getting index " + i + " from SchemaTuple", e);
            }
        }
        return l;
    }

    public abstract boolean isSpecificSchemaTuple(Object o);

    //TODO also need to implement the raw comparator
    @SuppressWarnings("unchecked")
    @Override
    public int compareTo(Object other) {
        if (isSpecificSchemaTuple(other)) {
            return compareToSpecific((T)other);
        }

        if (other instanceof SchemaTuple<?>) {
            return compareTo((SchemaTuple<?>)other, false);
        }

        if (other instanceof Tuple) {
            return compareTo((Tuple)other, false);
        }

        return DataType.compare(this, other);
    }

    public int compareTo(Tuple t) {
         return compareTo(t, true);
    }

    @SuppressWarnings("unchecked")
    protected int compareTo(Tuple t, boolean checkType) {
        if (checkType) {
            if (isSpecificSchemaTuple(t)) {
                return compareToSpecific((T)t);
            }

            if (t instanceof SchemaTuple<?>) {
                return compareTo((SchemaTuple<?>)t, false);
            }
        }

        int mySz = size();
        int tSz = t.size();

        if (tSz < mySz) {
            return 1;
        }

        if (tSz > mySz) {
            return -1;
        }

        for (int i = 0; i < mySz; i++) {
            try {
                int c = DataType.compare(get(i), t.get(i));

                if (c != 0) {
                    return c;
                }

            } catch (ExecException e) {
                throw new RuntimeException("Unable to compare tuples, t1 class = "
                        + getClass() + ", t2 class = " + t.getClass(), e);
            }
        }

        return 0;
    }

    public int compareTo(SchemaTuple<?> t) {
        return compareTo(t, true);
    }

    @SuppressWarnings("unchecked")
    protected int compareTo(SchemaTuple<?> t, boolean checkType) {
        if (checkType && isSpecificSchemaTuple(t)) {
            return compareToSpecific((T)t);
        }

        int i = compareSize(t);
        if (i != 0) {
            return i;
        }
        return generatedCodeCompareTo(t, checkType);
    }

    protected abstract int generatedCodeCompareTo(SchemaTuple<?> t, boolean checkType);

    protected int compareToSpecific(T t) {
        return generatedCodeCompareToSpecific(t);
    }

    protected abstract int generatedCodeCompareToSpecific(T t);

    @Override
    public boolean equals(Object other) {
        return (compareTo(other) == 0);
    }

    protected DataBag unbox(Object v, DataBag t) {
        return unbox((DataBag) v);
    }

    protected Map<String, Object> unbox(Object v, Map<String, Object> t) {
        return unbox((Map<String, Object>) v);
    }

    protected byte[] unbox(Object v, byte[] t) {
        return unbox((DataByteArray)v);
    }

    protected int unbox(Object v, int t) {
        return unbox((Integer)v);
    }

    protected long unbox(Object v, long t) {
        return unbox((Long)v);
    }

    protected float unbox(Object v, float t) {
        return unbox((Float)v);
    }

    protected double unbox(Object v, double t) {
        return unbox((Double)v);
    }

    protected boolean unbox(Object v, boolean t) {
        return unbox((Boolean)v);
    }

    protected DateTime unbox(Object v, DateTime t) {
        return (DateTime)v;
    }

    protected BigDecimal unbox(Object v, BigDecimal t) {
        return (BigDecimal)v;
    }

    protected BigInteger unbox(Object v, BigInteger t) {
        return (BigInteger)v;
    }

    protected String unbox(Object v, String t) {
        return (String)v;
    }

    protected Tuple unbox(Object v, Tuple t) {
        return (Tuple)v;
    }

    protected DataBag unbox(DataBag v) {
        return v;
    }

    protected Map<String, Object> unbox(Map<String, Object> v) {
        return v;
    }

    protected byte[] unbox(DataByteArray v) {
        if (v == null) {
            return null;
        }
        return v.get();
    }

    protected int unbox(Integer v) {
        return v.intValue();
    }

    protected long unbox(Long v) {
        return v.longValue();
    }

    protected float unbox(Float v) {
        return v.floatValue();
    }

    protected double unbox(Double v) {
        return v.doubleValue();
    }

    protected boolean unbox(Boolean v) {
        return v.booleanValue();
    }

    protected DateTime unbox(DateTime v) {
        return v;
    }

    protected DataBag box(DataBag v) {
        return v;
    }

    protected Map<String, Object> box(Map<String, Object> v) {
        return v;
    }

    protected DataByteArray box(byte[] v) {
        if (v == null) {
            return null;
        }
        return new DataByteArray(v);
    }

    protected String box(String v) {
        return v;
    }

    protected Tuple box(Tuple t) {
        return t;
    }

    protected Integer box(int v) {
        return new Integer(v);
    }

    protected Long box(long v) {
        return new Long(v);
    }

    protected Float box(float v) {
        return new Float(v);
    }

    protected Double box(double v) {
        return new Double(v);
    }

    protected Boolean box(boolean v) {
        return new Boolean(v);
    }

    protected DateTime box(DateTime v) {
        return v;
    }

    protected BigDecimal box(BigDecimal v) {
        return v;
    }

    protected BigInteger box(BigInteger v) {
        return v;
    }

    protected int hashCodePiece(int hash, int v, boolean isNull) {
        return isNull ? hash : 31 * hash + v;
    }

    protected int hashCodePiece(int hash, long v, boolean isNull) {
        return isNull ? hash : 31 * hash + (int)(v^(v>>>32));
    }

    protected int hashCodePiece(int hash, float v, boolean isNull) {
        return isNull ? hash : 31 * hash + Float.floatToIntBits(v);
    }

    protected int hashCodePiece(int hash, double v, boolean isNull) {
        long v2 = Double.doubleToLongBits(v);
        return isNull ? hash : 31 * hash + (int)(v2^(v2>>>32));
    }

    protected int hashCodePiece(int hash, boolean v, boolean isNull) {
        return isNull ? hash : 31 * hash + (v ? 1231 : 1237);
    }

    protected int hashCodePiece(int hash, DateTime v, boolean isNull) {
        return isNull ? hash : 31 * hash + v.hashCode();
    }

    protected int hashCodePiece(int hash, BigDecimal v, boolean isNull) {
        return isNull ? hash : 31 * hash + v.hashCode();
    }

    protected int hashCodePiece(int hash, BigInteger v, boolean isNull) {
        return isNull ? hash : 31 * hash + v.hashCode();
    }

    protected int hashCodePiece(int hash, byte[] v, boolean isNull) {
        return isNull ? hash : 31 * hash + DataByteArray.hashCode(v);
    }

    protected int hashCodePiece(int hash, String v, boolean isNull) {
        return isNull ? hash : 31 * hash + v.hashCode();
    }

    protected int hashCodePiece(int hash, Tuple v, boolean isNull) {
        return isNull ? hash : 31 * hash + v.hashCode();
    }

    protected int hashCodePiece(int hash, DataBag v, boolean isNull) {
        return isNull ? hash : 31 * hash + v.hashCode();
    }

    protected int hashCodePiece(int hash, Map<String, Object> v, boolean isNull) {
        return isNull ? hash : 31 * hash + v.hashCode();
    }

    @Override
    public int hashCode() {
        return generatedCodeHashCode();
    }

    protected abstract int generatedCodeHashCode();

    @Override
    public void set(int fieldNum, Object val) throws ExecException {
        generatedCodeSetField(fieldNum, val);
    }

    public abstract void generatedCodeSetField(int fieldNum, Object val) throws ExecException;

    @Override
    public Object get(int fieldNum) throws ExecException {
        return generatedCodeGetField(fieldNum);
    }

    public abstract Object generatedCodeGetField(int fieldNum) throws ExecException;

    @Override
    public boolean isNull(int fieldNum) throws ExecException {
        return isGeneratedCodeFieldNull(fieldNum);
    }

    public abstract boolean isGeneratedCodeFieldNull(int fieldNum) throws ExecException;

    @Override
    public byte getType(int fieldNum) throws ExecException {
        return getGeneratedCodeFieldType(fieldNum);
    }

    public abstract byte getGeneratedCodeFieldType(int fieldNum) throws ExecException;

    protected void setTypeAwareBase(int fieldNum, Object val, String type) throws ExecException {
        throw new ExecException("Given field " + fieldNum + " not a " + type + " field!");
    }

    protected Object getTypeAwareBase(int fieldNum, String type) throws ExecException {
        throw new ExecException("Given field " + fieldNum + " not a " + type + " field!");
    }

    @Override
    public void setInt(int fieldNum, int val) throws ExecException {
        generatedCodeSetInt(fieldNum, val);
    }

    protected abstract void generatedCodeSetInt(int fieldNum, int val) throws ExecException;

    @Override
    public void setLong(int fieldNum, long val) throws ExecException {
        generatedCodeSetLong(fieldNum, val);
    }

    protected abstract void generatedCodeSetLong(int fieldNum, long val) throws ExecException;

    @Override
    public void setFloat(int fieldNum, float val) throws ExecException {
        generatedCodeSetFloat(fieldNum, val);
    }

    protected abstract void generatedCodeSetFloat(int fieldNum, float val) throws ExecException;

    @Override
    public void setDouble(int fieldNum, double val) throws ExecException {
        generatedCodeSetDouble(fieldNum, val);
    }

    protected abstract void generatedCodeSetDouble(int fieldNum, double val) throws ExecException;

    @Override
    public void setBoolean(int fieldNum, boolean val) throws ExecException {
        generatedCodeSetBoolean(fieldNum, val);
    }

    protected abstract void generatedCodeSetBoolean(int fieldNum, boolean val) throws ExecException;

    @Override
    public void setDateTime(int fieldNum, DateTime val) throws ExecException {
         generatedCodeSetDateTime(fieldNum, val);
    }

    protected abstract void generatedCodeSetDateTime(int fieldNum, DateTime val) throws ExecException;

    @Override
    public void setBigDecimal(int fieldNum, BigDecimal val) throws ExecException {
         generatedCodeSetBigDecimal(fieldNum, val);
    }

    protected abstract void generatedCodeSetBigDecimal(int fieldNum, BigDecimal val) throws ExecException;

    @Override
    public void setBigInteger(int fieldNum, BigInteger val) throws ExecException {
         generatedCodeSetBigInteger(fieldNum, val);
    }

    protected abstract void generatedCodeSetBigInteger(int fieldNum, BigInteger val) throws ExecException;

    @Override
    public void setString(int fieldNum, String val) throws ExecException {
         generatedCodeSetString(fieldNum, val);
    }

    protected abstract void generatedCodeSetString(int fieldNum, String val) throws ExecException;

    @Override
    public void setTuple(int fieldNum, Tuple val) throws ExecException {
         generatedCodeSetTuple(fieldNum, val);
    }

    protected abstract void generatedCodeSetTuple(int fieldNum, Tuple val) throws ExecException;

    @Override
    public void setBytes(int fieldNum, byte[] val) throws ExecException {
        generatedCodeSetBytes(fieldNum, val);
    }

    protected abstract void generatedCodeSetBytes(int fieldNum, byte[] val) throws ExecException;

    @Override
    public void setDataBag(int fieldNum, DataBag val) throws ExecException {
        generatedCodeSetDataBag(fieldNum, val);
    }

    protected abstract void generatedCodeSetDataBag(int fieldNum, DataBag val) throws ExecException;

    @Override
    public void setMap(int fieldNum, Map<String, Object> val) throws ExecException {
        generatedCodeSetMap(fieldNum, val);
    }

    protected abstract void generatedCodeSetMap(int fieldNum, Map<String, Object> val) throws ExecException;

    private void errorIfNull(boolean isNull, String type) throws FieldIsNullException {
        if (isNull) {
            throw new FieldIsNullException("Desired field of type ["+type+"] was null!");
        }
    }

    protected int returnUnlessNull(boolean isNull, int val) throws FieldIsNullException {
        errorIfNull(isNull, "int");
        return val;
    }

    protected long returnUnlessNull(boolean isNull, long val) throws FieldIsNullException {
        errorIfNull(isNull, "long");
        return val;
    }

    protected float returnUnlessNull(boolean isNull, float val) throws FieldIsNullException {
        errorIfNull(isNull, "float");
        return val;
    }

    protected double returnUnlessNull(boolean isNull, double val) throws FieldIsNullException {
        errorIfNull(isNull, "double");
        return val;
    }

    protected boolean returnUnlessNull(boolean isNull, boolean val) throws FieldIsNullException {
        errorIfNull(isNull, "boolean");
        return val;
    }

    protected DateTime returnUnlessNull(boolean isNull, DateTime val) throws FieldIsNullException {
        errorIfNull(isNull, "DateTime");
        return val;
    }

    protected BigDecimal returnUnlessNull(boolean isNull, BigDecimal val) throws FieldIsNullException {
        errorIfNull(isNull, "BigDecimal");
        return val;
    }

    protected BigInteger returnUnlessNull(boolean isNull, BigInteger val) throws FieldIsNullException {
        errorIfNull(isNull, "BigInteger");
        return val;
    }

    protected Tuple returnUnlessNull(boolean isNull, Tuple val) throws FieldIsNullException {
        errorIfNull(isNull, "Tuple");
        return val;
    }

    protected String returnUnlessNull(boolean isNull, String val) throws FieldIsNullException {
        errorIfNull(isNull, "String");
        return val;
    }

    protected byte[] returnUnlessNull(boolean isNull, byte[] val) throws FieldIsNullException {
        errorIfNull(isNull, "byte");
        return val;
    }

    protected DataBag returnUnlessNull(boolean isNull, DataBag val) throws FieldIsNullException {
        errorIfNull(isNull, "DataBag");
        return val;
    }

    protected Map<String, Object> returnUnlessNull(boolean isNull, Map<String, Object> val) throws FieldIsNullException {
        errorIfNull(isNull, "Map<String,Object>");
        return val;
    }

    @Override
    public int getInt(int fieldNum) throws ExecException {
        return generatedCodeGetInt(fieldNum);
    }

    protected abstract int generatedCodeGetInt(int fieldNum) throws ExecException;

    public int unboxInt(Object val) {
        return ((Number)val).intValue();
    }

    @Override
    public long getLong(int fieldNum) throws ExecException {
        return generatedCodeGetLong(fieldNum);
    }

    protected abstract long generatedCodeGetLong(int fieldNum) throws ExecException;

    public long unboxLong(Object val) {
        return ((Number)val).longValue();
    }

    @Override
    public float getFloat(int fieldNum) throws ExecException {
        return generatedCodeGetFloat(fieldNum);
    }

    protected abstract float generatedCodeGetFloat(int fieldNum) throws ExecException;

    public float unboxFloat(Object val) {
        return ((Number)val).floatValue();
    }

    @Override
    public double getDouble(int fieldNum) throws ExecException {
        return generatedCodeGetDouble(fieldNum);
    }

    protected abstract double generatedCodeGetDouble(int fieldNum) throws ExecException;

    public double unboxDouble(Object val) {
        return ((Number)val).doubleValue();
    }

    @Override
    public boolean getBoolean(int fieldNum) throws ExecException {
        return generatedCodeGetBoolean(fieldNum);
    }

    protected abstract boolean generatedCodeGetBoolean(int fieldNum) throws ExecException;

    public boolean unboxBoolean(Object val) {
        return ((Boolean)val).booleanValue();
    }    

    @Override
    public DateTime getDateTime(int fieldNum) throws ExecException {
        return generatedCodeGetDateTime(fieldNum);
    }

    protected abstract DateTime generatedCodeGetDateTime(int fieldNum) throws ExecException;

    public DateTime unboxDateTime(Object val) {
        return (DateTime)val;
    }

    @Override
    public String getString(int fieldNum) throws ExecException {
        return generatedCodeGetString(fieldNum);
    }

    protected abstract String generatedCodeGetString(int fieldNum) throws ExecException;

    public String unboxString(Object val) {
        return (String)val;
    }

    @Override
    public byte[] getBytes(int fieldNum) throws ExecException {
        return generatedCodeGetBytes(fieldNum);
    }

    public byte[] unboxBytes(Object val) {
        DataByteArray dba = (DataByteArray)val;
        return val == null ? null : dba.get();
    }

    protected abstract byte[] generatedCodeGetBytes(int fieldNum) throws ExecException;

    @Override
    public Tuple getTuple(int fieldNum) throws ExecException {
        return generatedCodeGetTuple(fieldNum);
    }

    protected abstract Tuple generatedCodeGetTuple(int fieldNum) throws ExecException;

    protected Tuple unboxTuple(Object val) {
        return (Tuple)val;
    }

    @Override
    public DataBag getDataBag(int fieldNum) throws ExecException {
        return generatedCodeGetDataBag(fieldNum);
    }

    protected abstract DataBag generatedCodeGetDataBag(int fieldNum) throws ExecException;

    protected DataBag unboxDataBag(Object val) {
        return (DataBag)val;
    }

    @Override
    public Map<String, Object> getMap(int fieldNum) throws ExecException {
        return generatedCodeGetMap(fieldNum);
    }

    protected abstract Map<String, Object> generatedCodeGetMap(int fieldNum) throws ExecException;

    @SuppressWarnings("unchecked")
    protected Map<String, Object> unboxMap(Object val) {
        return (Map<String, Object>)val;
    }

    @Override
    public BigDecimal getBigDecimal(int fieldNum) throws ExecException {
        return generatedCodeGetBigDecimal(fieldNum);
    }

    protected abstract BigDecimal generatedCodeGetBigDecimal(int fieldNum)
            throws ExecException;

    public BigDecimal unboxBigDecimal(Object val) {
        return (BigDecimal) val;
    }

    @Override
    public BigInteger getBigInteger(int fieldNum) throws ExecException {
        return generatedCodeGetBigInteger(fieldNum);
    }

    protected abstract BigInteger generatedCodeGetBigInteger(int fieldNum)
            throws ExecException;

    public BigInteger unboxBigInteger(Object val) {
        return (BigInteger) val;
    }

    protected static Schema staticSchemaGen(String s) {
        try {
            if (s.equals("")) {
                Log.warn("No Schema present in SchemaTuple generated class");
                return new Schema();
            }
            return (Schema) ObjectSerializer.deserialize(s);
        } catch (IOException e) {
            throw new RuntimeException("Unable to deserialize serialized Schema: " + s, e);
        }
    }

    public void setAndCatch(Tuple t) {
        try {
            set(t);
        } catch (ExecException e) {
            throw new RuntimeException("Unable to set position with Tuple: " + t, e);
        }
    }

    public void setAndCatch(SchemaTuple<?> t) {
        try {
            set(t);
        } catch (ExecException e) {
            throw new RuntimeException("Unable to set position with Tuple: " + t, e);
        }
    }

    /**
     * This method is responsible for writing everything contained by the Tuple.
     * Note that the base SchemaTuple class does not have an implementation (but is
     * not abstract) so that the generated code can call this method via super
     * without worrying about whether it is abstract or not, as there may be classes in
     * between in the inheritance tree (such as AppendableSchemaTuple).
     * @param out
     * @throws IOException
     */
    protected void writeElements(DataOutput out) throws IOException {
        boolean[] b = generatedCodeNullsArray();
        SedesHelper.writeBooleanArray(out, b);
        generatedCodeWriteElements(out);
    }

    protected abstract void generatedCodeWriteElements(DataOutput out) throws IOException;

    protected int compareSize(Tuple t) {
        return compare(size(), t.size());
    }

    protected int compareNull(boolean usNull, boolean themNull) {
        if (usNull && themNull) {
            return 2;
        } else if (themNull) {
            return 1;
        } else if (usNull) {
            return -1;
        }
        return 0;
    }

    protected int compareNull(boolean usNull, Tuple t, int pos) {
        boolean themNull;
        try {
            themNull = t.isNull(pos);
        } catch (ExecException e) {
            throw new RuntimeException("Unable to check if position " + pos + " is null in Tuple: " + t, e);
        }
        return compareNull(usNull, themNull);
    }

    protected int compareElementAtPos(int val, SchemaTuple<?> t, int pos) {
        int themVal;
        try {
            themVal = t.getInt(pos);
        } catch (ExecException e) {
            throw new RuntimeException("Unable to retrieve int field " + pos + " in given Tuple: " + t, e);
        }
        return compare(val, themVal);
    }

    protected int compare(boolean usNull, int usVal, boolean themNull, int themVal) {
        if (usNull && themNull) {
            return 0;
        } else if (themNull) {
            return 1;
        } else if (usNull) {
            return -1;
        }
        return compare(usVal, themVal);
    }

    protected int compare(int val, int themVal) {
        return val == themVal ? 0 : (val > themVal ? 1 : -1);
    }

    protected int compareWithElementAtPos(boolean isNull, int val, SchemaTuple<?> t, int pos) {
        int themVal;
        boolean themNull;
        try {
            themVal = t.getInt(pos);
            themNull = t.isNull(pos);
        } catch (ExecException e) {
            throw new RuntimeException("Unable to retrieve int field " + pos + " in given Tuple: " + t, e);
        }
        return compare(isNull, val, themNull, themVal);
    }

    protected int compare(boolean usNull, long usVal, boolean themNull, long themVal) {
        if (usNull && themNull) {
            return 0;
        } else if (themNull) {
            return 1;
        } else if (usNull) {
            return -1;
        }
        return compare(usVal, themVal);
    }

    protected int compare(long val, long themVal) {
        return val == themVal ? 0 : (val > themVal ? 1 : -1);
    }

    protected int compareWithElementAtPos(boolean isNull, long val, SchemaTuple<?> t, int pos) {
        long themVal;
        boolean themNull;
        try {
            themVal = t.getLong(pos);
            themNull = t.isNull(pos);
        } catch (ExecException e) {
            throw new RuntimeException("Unable to retrieve long field " + pos + " in given Tuple: " + t, e);
        }
        return compare(isNull, val, themNull, themVal);
    }

    protected int compare(boolean usNull, float usVal, boolean themNull, float themVal) {
        if (usNull && themNull) {
            return 0;
        } else if (themNull) {
            return 1;
        } else if (usNull) {
            return -1;
        }
        return compare(usVal, themVal);
    }

    public int compare(float val, float themVal) {
        return val == themVal ? 0 : (val > themVal ? 1 : -1);
    }

    protected int compareWithElementAtPos(boolean isNull, float val, SchemaTuple<?> t, int pos) {
        float themVal;
        boolean themNull;
        try {
            themVal = t.getFloat(pos);
            themNull = t.isNull(pos);
        } catch (ExecException e) {
            throw new RuntimeException("Unable to retrieve float field " + pos + " in given Tuple: " + t, e);
        }
        return compare(isNull, val, themNull, themVal);
    }

    protected int compare(boolean usNull, double usVal, boolean themNull, double themVal) {
        if (usNull && themNull) {
            return 0;
        } else if (themNull) {
            return 1;
        } else if (usNull) {
            return -1;
        }
        return compare(usVal, themVal);
    }

    protected int compare(double val, double themVal) {
        return val == themVal ? 0 : (val > themVal ? 1 : -1);
    }

    protected int compareWithElementAtPos(boolean isNull, double val, SchemaTuple<?> t, int pos) {
        double themVal;
        boolean themNull;
        try {
            themVal = t.getDouble(pos);
            themNull = t.isNull(pos);
        } catch (ExecException e) {
            throw new RuntimeException("Unable to retrieve double field " + pos + " in given Tuple: " + t, e);
        }
        return compare(isNull, val, themNull, themVal);
    }

    protected int compare(boolean usNull, boolean usVal, boolean themNull, boolean themVal) {
        if (usNull && themNull) {
            return 0;
        } else if (themNull) {
            return 1;
        } else if (usNull) {
            return -1;
        }
        return compare(usVal, themVal);
    }

    protected int compare(boolean val, boolean themVal) {
        if (val ^ themVal) {
            return val ? 1 : -1;
        }
        return 0;
    }

    protected int compareWithElementAtPos(boolean isNull, boolean val, SchemaTuple<?> t, int pos) {
        boolean themVal;
        boolean themNull;
        try {
            themVal = t.getBoolean(pos);
            themNull = t.isNull(pos);
        } catch (ExecException e) {
            throw new RuntimeException("Unable to retrieve boolean field " + pos + " in given Tuple: " + t, e);
        }
        return compare(isNull, val, themNull, themVal);
    }

    protected int compare(boolean usNull, byte[] usVal, boolean themNull, byte[] themVal) {
        if (usNull && themNull) {
            return 0;
        } else if (themNull) {
            return 1;
        } else if (usNull) {
            return -1;
        }
        return compare(usVal, themVal);
    }

    protected int compare(byte[] val, byte[] themVal) {
        return DataByteArray.compare(val, themVal);
    }

    protected int compareWithElementAtPos(boolean isNull, byte[] val, SchemaTuple<?> t, int pos) {
        byte[] themVal;
        boolean themNull;
        try {
            themVal = t.getBytes(pos);
            themNull = t.isNull(pos);
        } catch (ExecException e) {
            throw new RuntimeException("Unable to retrieve byte[] field " + pos + " in given Tuple: " + t, e);
        }
        return compare(isNull, val, themNull, themVal);
    }

    protected int compare(boolean usNull, DateTime usVal, boolean themNull, DateTime themVal) {
        if (usNull && themNull) {
            return 0;
        } else if (themNull) {
            return 1;
        } else if (usNull) {
            return -1;
        }
        return compare(usVal, themVal);
    }

    protected int compare(DateTime val, DateTime themVal) {
        return val.compareTo(themVal);
    }

    protected int compareWithElementAtPos(boolean isNull, DateTime val, SchemaTuple<?> t, int pos) {
        DateTime themVal;
        boolean themNull;
        try {
            themVal = t.getDateTime(pos);
            themNull = t.isNull(pos);
        } catch (ExecException e) {
            throw new RuntimeException("Unable to retrieve String field " + pos + " in given Tuple: " + t, e);
        }
        return compare(isNull, val, themNull, themVal);
    }

    protected int compare(boolean usNull, BigDecimal usVal, boolean themNull, BigDecimal themVal) {
        if (usNull && themNull) {
            return 0;
        } else if (themNull) {
            return 1;
        } else if (usNull) {
            return -1;
        }
        return compare(usVal, themVal);
    }

    protected int compare(BigDecimal val, BigDecimal themVal) {
        return val.compareTo(themVal);
    }

    protected int compareWithElementAtPos(boolean isNull, BigDecimal val, SchemaTuple<?> t, int pos) {
        BigDecimal themVal;
        boolean themNull;
        try {
            themVal = t.getBigDecimal(pos);
            themNull = t.isNull(pos);
        } catch (ExecException e) {
            throw new RuntimeException("Unable to retrieve String field " + pos + " in given Tuple: " + t, e);
        }
        return compare(isNull, val, themNull, themVal);
    }

    protected int compare(boolean usNull, BigInteger usVal, boolean themNull, BigInteger themVal) {
        if (usNull && themNull) {
            return 0;
        } else if (themNull) {
            return 1;
        } else if (usNull) {
            return -1;
        }
        return compare(usVal, themVal);
    }

    protected int compare(BigInteger val, BigInteger themVal) {
        return val.compareTo(themVal);
    }

    protected int compareWithElementAtPos(boolean isNull, BigInteger val, SchemaTuple<?> t, int pos) {
        BigInteger themVal;
        boolean themNull;
        try {
            themVal = t.getBigInteger(pos);
            themNull = t.isNull(pos);
        } catch (ExecException e) {
            throw new RuntimeException("Unable to retrieve String field " + pos + " in given Tuple: " + t, e);
        }
        return compare(isNull, val, themNull, themVal);
    }

    protected int compare(boolean usNull, String usVal, boolean themNull, String themVal) {
        if (usNull && themNull) {
            return 0;
        } else if (themNull) {
            return 1;
        } else if (usNull) {
            return -1;
        }
        return compare(usVal, themVal);
    }

    protected int compare(String val, String themVal) {
        return val.compareTo(themVal);
    }

    protected int compareWithElementAtPos(boolean isNull, String val, SchemaTuple<?> t, int pos) {
        String themVal;
        boolean themNull;
        try {
            themVal = t.getString(pos);
            themNull = t.isNull(pos);
        } catch (ExecException e) {
            throw new RuntimeException("Unable to retrieve String field " + pos + " in given Tuple: " + t, e);
        }
        return compare(isNull, val, themNull, themVal);
    }

    protected int compare(boolean usNull, DataBag usVal, boolean themNull, DataBag themVal) {
        if (usNull && themNull) {
            return 0;
        } else if (themNull) {
            return 1;
        } else if (usNull) {
            return -1;
        }
        return compare(usVal, themVal);
    }

    protected int compare(DataBag val, DataBag themVal) {
        return val.compareTo(themVal);
    }

    protected int compareWithElementAtPos(boolean isNull, DataBag val, SchemaTuple<?> t, int pos) {
        DataBag themVal;
        boolean themNull;
        try {
            themVal = t.getDataBag(pos);
            themNull = t.isNull(pos);
        } catch (ExecException e) {
            throw new RuntimeException("Unable to retrieve DataBag field " + pos + " in given Tuple: " + t, e);
        }
        return compare(isNull, val, themNull, themVal);
    }

    protected int compare(boolean usNull, Map<String, Object> usVal, boolean themNull, Map<String, Object> themVal) {
        if (usNull && themNull) {
            return 0;
        } else if (themNull) {
            return 1;
        } else if (usNull) {
            return -1;
        }
        return compare(usVal, themVal);
    }

    protected int compare(Map<String, Object> val, Map<String, Object> themVal) {
        return DataType.compare(val, themVal, DataType.MAP, DataType.MAP);
    }

    protected int compareWithElementAtPos(boolean isNull, Map<String, Object> val, SchemaTuple<?> t, int pos) {
        Map<String, Object> themVal;
        boolean themNull;
        try {
            themVal = t.getMap(pos);
            themNull = t.isNull(pos);
        } catch (ExecException e) {
            throw new RuntimeException("Unable to retrieve DataBag field " + pos + " in given Tuple: " + t, e);
        }
        return compare(isNull, val, themNull, themVal);
    }

    protected int compareWithElementAtPos(boolean isNull, SchemaTuple<?> val, SchemaTuple<?> t, int pos) {
        Object themVal;
        boolean themNull;
        try {
            themVal = t.get(pos);
            themNull = t.isNull(pos);
        } catch (ExecException e) {
            throw new RuntimeException("Unable to retrieve double field " + pos + " in given Tuple: " + t, e);
        }
        return compare(isNull, val, themNull, themVal);
    }

    protected int compare(boolean usNull, SchemaTuple<?> usVal, boolean themNull, Object themVal) {
        if (usNull && themNull) {
            return 0;
        } else if (themNull) {
            return 1;
        } else if (usNull) {
            return -1;
        }
        return usVal.compareTo(themVal);
    }

    /**
     * This is a mechanism which allows the SchemaTupleFactory to
     * get around having to use reflection. The generated code
     * will return a generator which will be created via reflection,
     * but after which can generate SchemaTuples at native speed.
     */
    public abstract SchemaTupleQuickGenerator<T> getQuickGenerator();

    public static abstract class SchemaTupleQuickGenerator<A> {
        public abstract A make();
    }

    public int size() {
        return generatedCodeSize();
    }

    protected abstract int generatedCodeSize();

    @Override
    public void readFields(DataInput in) throws IOException {
        boolean[] b = SedesHelper.readBooleanArray(in, schemaSize());
        generatedCodeReadFields(in, b);
    }

    protected abstract void generatedCodeReadFields(DataInput in, boolean[] nulls) throws IOException;

    protected abstract boolean[] generatedCodeNullsArray() throws IOException;
}
