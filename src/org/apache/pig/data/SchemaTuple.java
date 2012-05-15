package org.apache.pig.data;

import java.io.File;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.net.URI;
import java.net.MalformedURLException;
import java.util.Map;
import java.util.List;
import java.util.LinkedList;
import java.util.Queue;

import com.google.common.collect.Maps;
import com.google.common.collect.Lists;
import com.google.common.base.Joiner;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataType;
import org.apache.pig.data.utils.SedesHelper;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.PigContext;
import org.apache.pig.tools.pigstats.ScriptState;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.StandardLocation;

//ALSO: the memory estimate has to include all of these objects that come along with SchemaTuple...
//TODO: implement getField(String)

//TODO: implement hashCode

//the benefit of having the generic here is that in the case that we do ".set(t)" and t is the right type, it will be very fast
public abstract class SchemaTuple<T extends SchemaTuple> implements TypeAwareTuple {
    private Tuple append;

    private static final Log LOG = LogFactory.getLog(SchemaTuple.class);
    private static final TupleFactory mTupleFactory = TupleFactory.getInstance();
    private static final BinInterSedes pigSerializer = new BinInterSedes();

    private int largestSetValue = -1; //this exists so that append will have a reasonable semantic for an empty Schematuple

    public void updateLargestSetValue(int fieldNum) {
        largestSetValue = Math.max(fieldNum, largestSetValue);
    }

    public void append(Object val) {
        if (++largestSetValue < sizeNoAppend()) {
            try {
                set(largestSetValue, val);
            } catch (ExecException e) {
                throw new RuntimeException("Unable to append to position " + largestSetValue, e);
            }
            return;
        }

        if (append == null)
            append = mTupleFactory.newTuple();

        append.append(val);
        updateLargestSetValue(size());
    }

    public int appendSize() {
        return append == null ? 0 : append.size();
    }

    public boolean appendIsNull() {
        return appendSize() == 0;
    }

    public Object getAppend(int i) throws ExecException {
        return appendIsNull(i) ? null : append.get(i);
    }

    public boolean appendIsNull(int i) throws ExecException {
        return append == null || append.isNull() ? null : append.isNull(i);
    }

    public Tuple getAppend() {
        return append;
    }

    public void setAppend(Tuple t) {
        append = t;
        updateLargestSetValue(size());
    }

    public void appendReset() {
        append = null;
        largestSetValue = -1;
        for (int i = sizeNoAppend() - 1; i >= 0; i--) {
            try {
                if (!isNull(i)) {
                    largestSetValue = i;
                    return;
                }
            } catch (ExecException e) {
                throw new RuntimeException("Unable to check null status of field "+i, e);
            }
        }
    }

    public void setAppend(int fieldNum, Object val) throws ExecException {
        append.set(fieldNum, val);
        updateLargestSetValue(size());
    }

    //TODO this should account for all of the non-generated objects, and the general cost of being an object
    public long getMemorySize() {
        return 0;
    }

    public byte appendType(int i) throws ExecException {
        return append == null ? DataType.UNKNOWN : append.getType(i);
    }

    //need helpers that can be integrated with the generated code...
    //also need a helper to serialize and deserialize (yuck)
    //the first of the null bits can be dedicated to the append linkedlist... 1 if present,
    //0 otherwise. If it is present, will severely affect serialization

    public abstract int getSchemaTupleIdentifier();
    public abstract void writeElements(DataOutput out) throws IOException;
    public abstract String getSchemaString();
    public abstract int sizeNoAppend();

    //NOTE: this MUST be overriden by the generic code! This is simply to help out
    public SchemaTuple set(SchemaTuple t, boolean checkType) throws ExecException {
        appendReset();
        for (int j = sizeNoAppend(); j < t.size(); j++) {
            append(t.get(j));
        }
        updateLargestSetValue(size());
        return this;
    }

    @Override
    public int size() {
        return sizeNoAppend() + appendSize();
    }

    //NOTE: this MUST be overriden by the generic code! This is simply to help out
    public SchemaTuple setSpecific(T t) {
        appendReset();
        setAppend(t.getAppend());
        updateLargestSetValue(size());
        return this;
    }

    //TODO consider if there should be a "strict" method that won't let you append? I'm thinking no
    //TODO also consider: if set is called on a tuple that is too small, instead just null out the extra fields in the SchemaTuple?
    //TODO in all of the set and compareTo's, need to take into account the fact that the other one could be null

    public SchemaTuple set(Tuple t) throws ExecException {
        return set(t, true);
    }

    public SchemaTuple set(Tuple t, boolean checkType) throws ExecException {
        if (checkType) {
            if (t.getClass() == getClass())
                return setSpecific((T)t);

            if (t instanceof SchemaTuple)
                return set((SchemaTuple)t, false);
        }

        return set(t.getAll());
    }

    public SchemaTuple set(SchemaTuple t) throws ExecException {
        return set(t, true);
    }

    public SchemaTuple set(List<Object> l) throws ExecException {
        if (l.size() < sizeNoAppend())
            throw new ExecException("Given list of objects has too few fields ("+l.size()+" vs "+sizeNoAppend()+")");

        for (int i = 0; i < sizeNoAppend(); i++)
            set(i, l.get(i));

        appendReset();

        for (int i = sizeNoAppend(); i < l.size(); i++)
            append(l.get(i++));

        updateLargestSetValue(size());

        return this;
    }

    //this is the null value for the whole Tuple
    //-1 is unset, 0 means no, 1 means yes
    private int isNull = -1; //TODO make this a byte

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

    protected static void write(DataOutput out, int v) throws IOException {
        SedesHelper.Varint.writeSignedVarInt(v, out);
        //out.writeInt(v);
    }

    protected static void write(DataOutput out, long v) throws IOException {
        SedesHelper.Varint.writeSignedVarLong(v, out);
        //out.writeLong(v);
    }

    protected static void write(DataOutput out, float v) throws IOException {
        out.writeFloat(v);
    }

    protected static void write(DataOutput out, double v) throws IOException {
        out.writeDouble(v);
    }

    protected static void write(DataOutput out, byte[] v) throws IOException {
        SedesHelper.writeBytes(out, v);
    }

    protected static void write(DataOutput out, String v) throws IOException {
        SedesHelper.writeChararray(out, v);
    }

    protected static void write(DataOutput out, SchemaTuple t) throws IOException {
        t.writeElements(out);
    }

    public static int read(DataInput in, int v) throws IOException {
        return SedesHelper.Varint.readSignedVarInt(in);
        //return in.readInt();
    }

    public static long read(DataInput in, long v) throws IOException {
        return SedesHelper.Varint.readSignedVarLong(in);
        //return in.readLong();
    }

    public static float read(DataInput in, float v) throws IOException {
        return in.readFloat();
    }

    public static double read(DataInput in, double v) throws IOException {
        return in.readDouble();
    }

    public static String read(DataInput in, String v) throws IOException {
        return SedesHelper.readChararray(in, in.readByte());
    }

    public static byte[] read(DataInput in, byte[] v) throws IOException {
        return SedesHelper.readBytes(in, in.readByte());
    }

    @Override
    public void write(DataOutput out) throws IOException {
       write(out, true);
    }

    //returns true only if every element is null
    @Override
    public boolean isNull() {
        if (isNull > -1)
            return isNull == 1;

        for (int i = 0; i < size(); i++) {
            try {
                if (!isNull(i)) {
                    isNull = 0;
                    return false;
                 }
            } catch (ExecException e) {
                throw new RuntimeException("Unable to check if value null for index: " + i, e);
            }
        }
        return true;
    }

    @Override
    public void setNull(boolean isNull) {
        this.isNull = isNull ? 1 : 0;
    }

    @Override
    public void reference(Tuple t) {
        try {
            set(t);
        } catch (ExecException e) {
            throw new RuntimeException("Failure to set given tuple: " + t, e);
        }
    }

    @Override
    public String toDelimitedString(String delimiter) throws ExecException {
        return Joiner.on(delimiter).useForNull("").join(getAll());
    }

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

    //TODO use something more optimized and generated? this is just lifted from PrimitiveTuple
    //TODO also need to implement the raw comparator
    @Override
    public int compareTo(Object other) {
        if (getClass() == other.getClass())
            return compareToSpecific((T)other);

        if (other instanceof SchemaTuple)
            return compareTo((SchemaTuple)other, false);

        if (other instanceof Tuple)
            compareTo((Tuple)other, false);

         return DataType.compare(this, other);
    }

    public int compareTo(Tuple t) {
         return compareTo(t, true);
    }

    //TODO override this in the generated code as well
    public int compareTo(Tuple t, boolean checkType) {
        if (checkType) {
            if (getClass() == t.getClass())
                return compareToSpecific((T)t);

            if (t instanceof SchemaTuple)
                return compareTo((SchemaTuple)t, false);
        }

        int mySz = size();
        int tSz = t.size();

        if (tSz < mySz)
            return 1;

        if (tSz > mySz)
            return -1;

        for (int i = 0; i < mySz; i++) {
            try {
                int c = DataType.compare(get(i), t.get(i));

                if (c != 0)
                    return c;

            } catch (ExecException e) {
                throw new RuntimeException("Unable to compare tuples", e);
            }
        }

        return 0;
    }

    public int compareTo(SchemaTuple t) {
        if (getClass() == t.getClass())
            return compareToSpecific((T)t);

        return compareTo(t, false);
    }

    public abstract int compareTo(SchemaTuple t, boolean checkType);

    public abstract int compareToSpecific(T other);

    @Override
    public boolean equals(Object other) {
        return (compareTo(other) == 0);
    }

    public static byte[] unbox(Object v, byte[] t) {
        return unbox((DataByteArray)v);
    }

    public static int unbox(Object v, int t) {
        return unbox((Integer)v);
    }

    public static long unbox(Object v, long t) {
        return unbox((Long)v);
    }

    public static float unbox(Object v, float t) {
        return unbox((Float)v);
    }

    public static double unbox(Object v, double t) {
        return unbox((Double)v);
    }

    public static boolean unbox(Object v, boolean t) {
        return unbox((Boolean)v);
    }

    public static String unbox(Object v, String t) {
        return (String)v;
    }

    public static byte[] unbox(DataByteArray v) {
        if (v == null)
            return null;
        return v.get();
    }

    public static int unbox(Integer v) {
        return v.intValue();
    }

    public static long unbox(Long v) {
        return v.longValue();
    }

    public static float unbox(Float v) {
        return v.floatValue();
    }

    public static double unbox(Double v) {
        return v.doubleValue();
    }

    public static boolean unbox(Boolean v) {
        return v.booleanValue();
    }

    public static DataByteArray box(byte[] v) {
        if (v == null)
            return null;
        return new DataByteArray(v);
    }

    public static String box(String v) {
        return v;
    }

    public static Tuple box(Tuple t) {
        return t;
    }

    public static Integer box(int v) {
        return new Integer(v);
    }

    public static Long box(long v) {
        return new Long(v);
    }

    public static Float box(float v) {
        return new Float(v);
    }

    public static Double box(double v) {
        return new Double(v);
    }

    public static Boolean box(boolean v) {
        return new Boolean(v);
    }

    public static int hashCodePiece(int hash, int v, boolean isNull) {
        return isNull ? 0 : 31 * hash + v;
    }

    public static int hashCodePiece(int hash, long v, boolean isNull) {
        return isNull ? 0 : 31 * hash + (int)(v^(v>>>32));
    }

    public static int hashCodePiece(int hash, float v, boolean isNull) {
        return isNull ? 0 : 31 * hash + Float.floatToIntBits(v);
    }

    public static int hashCodePiece(int hash, double v, boolean isNull) {
        long v2 = Double.doubleToLongBits(v);
        return isNull ? 0 : 31 * hash + (int)(v2^(v2>>>32));
    }

    public static int hashCodePiece(int hash, boolean v, boolean isNull) {
        return isNull ? 0 : 31 * hash + (v ? 1231 : 1237);
    }

    public static int hashCodePiece(int hash, byte[] v, boolean isNull) {
        return isNull ? 0 : 31 * hash + DataByteArray.hashCode(v);
    }

    public static int hashCodePiece(int hash, String v, boolean isNull) {
        return isNull ? 0 : 31 * hash + v.hashCode();
    }

    public static int hashCodePiece(int hash, Tuple v, boolean isNull) {
        return isNull ? 0 : 31 * hash + v.hashCode();
    }

    @Override
    public int hashCode() {
        throw new RuntimeException("IMPLEMENT HASHCODE");
    }

    //this is intended to be overriden by the generated code
    public void set(int fieldNum, Object val) throws ExecException {
        int diff = fieldNum - sizeNoAppend();
        if (diff < appendSize()) {
            setAppend(diff, val);
            return;
        }
        throw new ExecException("Invalid index " + fieldNum + " given");
    }

    //this is intended to be overriden by the generated code
    public Object get(int fieldNum) throws ExecException {
        int diff = fieldNum - sizeNoAppend();
        if (diff < appendSize())
            return getAppend(diff);
        throw new ExecException("Invalid index " + fieldNum + " given");
    }

    //this is intended to be overriden by the generated code
    public boolean isNull(int fieldNum) throws ExecException {
        int diff = fieldNum - sizeNoAppend();
        if (diff < appendSize())
            return appendIsNull(diff);
        throw new ExecException("Invalid index " + fieldNum + " given");
    }

    //this is intended to be overriden by the generated code
    public void setNull(int fieldNum) throws ExecException {
        int diff = fieldNum - sizeNoAppend();
        if (diff < appendSize()) {
            setAppend(diff, null);
        } else {
            throw new ExecException("Invalid index " + fieldNum + " given");
        }
    }

    //this is intended to be overriden by the generated code
    public byte getType(int fieldNum) throws ExecException {
        int diff = fieldNum - sizeNoAppend();
        if (diff < appendSize())
            return appendType(diff);
        throw new ExecException("Invalid index " + fieldNum + " given");
    }

    private void setPrimitiveBase(int fieldNum, Object val, String type) throws ExecException {
        int diff = fieldNum - sizeNoAppend();
        if (diff < appendSize()) {
            setAppend(diff, val);
        }
        throw new ExecException("Given field " + fieldNum + " not a " + type + " field!");
    }

    private Object getPrimitiveBase(int fieldNum, String type) throws ExecException {
        int diff = fieldNum - sizeNoAppend();
        if (diff < appendSize()) {
            return getAppend(diff);
        }
        throw new ExecException("Given field " + fieldNum + " not a " + type + " field!");
    }

    public void setInt(int fieldNum, int val) throws ExecException {
        setPrimitiveBase(fieldNum, val, "int");
    }

    public void setLong(int fieldNum, long val) throws ExecException {
        setPrimitiveBase(fieldNum, val, "long");
    }

    public void setFloat(int fieldNum, float val) throws ExecException {
        setPrimitiveBase(fieldNum, val, "float");
    }

    public void setDouble(int fieldNum, double val) throws ExecException {
        setPrimitiveBase(fieldNum, val, "double");
    }

    public void setBoolean(int fieldNum, boolean val) throws ExecException {
        setPrimitiveBase(fieldNum, val, "boolean");
    }

    public void setString(int fieldNum, String val) throws ExecException {
        setPrimitiveBase(fieldNum, val, "String");
    }

    public void setBytes(int fieldNum, byte[] val) throws ExecException {
        setPrimitiveBase(fieldNum, val, "byte[]");
    }

    public int getInt(int fieldNum) throws ExecException {
        return ((Number)getPrimitiveBase(fieldNum, "int")).intValue();
    }

    public long getLong(int fieldNum) throws ExecException {
        return ((Number)getPrimitiveBase(fieldNum, "long")).longValue();
    }

    public float getFloat(int fieldNum) throws ExecException {
        return ((Number)getPrimitiveBase(fieldNum, "float")).floatValue();
    }

    public double getDouble(int fieldNum) throws ExecException {
        return ((Number)getPrimitiveBase(fieldNum, "double")).doubleValue();
    }

    public boolean getBoolean(int fieldNum) throws ExecException {
        return (Boolean)getPrimitiveBase(fieldNum, "boolean");
    }

    public String getString(int fieldNum) throws ExecException {
        return (String)getPrimitiveBase(fieldNum, "String");
    }

    public byte[] getBytes(int fieldNum) throws ExecException {
        return ((DataByteArray)getPrimitiveBase(fieldNum, "byte[]")).get();
    }

    protected static Schema staticSchemaGen(String s) {
        try {
            return Utils.getSchemaFromString(s);
        } catch (FrontendException e) {
            throw new RuntimeException("Unable to make Schema for String: " + s);
        }
    }

    public void setAndCatch(Tuple t) {
        try {
            set(t, false);
        } catch (ExecException e) {
            throw new RuntimeException("Unable to set position 6 with Tuple: " + t, e);
        }
    }

    public void setAndCatch(SchemaTuple t) {
        try {
            set(t, false);
        } catch (ExecException e) {
            throw new RuntimeException("Unable to set position 6 with Tuple: " + t, e);
        }
    }
}
