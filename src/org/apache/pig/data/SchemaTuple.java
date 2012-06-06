package org.apache.pig.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;
import org.apache.pig.data.utils.SedesHelper;
import org.apache.pig.data.utils.HierarchyHelper.MustOverride;
import org.apache.pig.data.utils.MethodHelper.NotImplemented;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;

import com.google.common.collect.Lists;

//ALSO: the memory estimate has to include all of these objects that come along with SchemaTuple...
//TODO: implement getField(String)

//TODO need a better benchmark of varint and varlong performance
//TODO custom iterator?

//the benefit of having the generic here is that in the case that we do ".set(t)" and t is the right type, it will be very fast
@InterfaceAudience.Public
@InterfaceStability.Unstable
public abstract class SchemaTuple<T extends SchemaTuple> extends AbstractTuple implements TypeAwareTuple {
    private static final Log LOG = LogFactory.getLog(SchemaTuple.class);
    private static final TupleFactory mTupleFactory = TupleFactory.getInstance();
    private static final BinInterSedes pigSerializer = new BinInterSedes();

    @NotImplemented
    @Override
    public void append(Object val) {
        throw SchemaTupleFactory.methodNotImplemented();
    }

    //TODO this should account for all of the non-generated objects, and the general cost of being an object
    @MustOverride
    public long getMemorySize() {
        return 0;
    }

    //need helpers that can be integrated with the generated code...
    //also need a helper to serialize and deserialize (yuck)
    //the first of the null bits can be dedicated to the append linkedlist... 1 if present,
    //0 otherwise. If it is present, will severely affect serialization

    public abstract int getSchemaTupleIdentifier();
    public abstract String getSchemaString();
    protected abstract int sizeNoAppend();

    @MustOverride
    protected SchemaTuple set(SchemaTuple t, boolean checkType) throws ExecException {
        return this;
    }

    @MustOverride
    protected SchemaTuple setSpecific(T t) {
        return this;
    }

    //TODO consider if there should be a "strict" method that won't let you append? I'm thinking no
    //TODO also consider: if set is called on a tuple that is too small, instead just null out the extra fields in the SchemaTuple?
    //TODO in all of the set and compareTo's, need to take into account the fact that the other one could be null

    public SchemaTuple set(Tuple t) throws ExecException {
        return set(t, true);
    }

    protected SchemaTuple set(Tuple t, boolean checkType) throws ExecException {
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

    protected static int read(DataInput in, int v) throws IOException {
        //return SedesHelper.Varint.readSignedVarInt(in);
        return in.readInt();
    }

    protected static long read(DataInput in, long v) throws IOException {
        //return SedesHelper.Varint.readSignedVarLong(in);
        return in.readLong();
    }

    protected static float read(DataInput in, float v) throws IOException {
        return in.readFloat();
    }

    protected static double read(DataInput in, double v) throws IOException {
        return in.readDouble();
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

    //TODO use something more optimized and generated? this is just lifted from PrimitiveTuple
    //TODO also need to implement the raw comparator
    @Override
    public int compareTo(Object other) {
        if (getClass() == other.getClass()) {
            return compareToSpecific((T)other);
        }

        if (other instanceof SchemaTuple) {
            return compareTo((SchemaTuple)other, false);
        }

        if (other instanceof Tuple) {
            compareTo((Tuple)other, false);
        }

        return DataType.compare(this, other);
    }

    public int compareTo(Tuple t) {
         return compareTo(t, true);
    }

    protected int compareTo(Tuple t, boolean checkType) {
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

    @MustOverride
    protected int compareTo(SchemaTuple t, boolean checkType) {
        return 0;
    }

    @MustOverride
    protected int compareToSpecific(T t) {
        return 0;
    }

    @Override
    public boolean equals(Object other) {
        return (compareTo(other) == 0);
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

    protected String unbox(Object v, String t) {
        return (String)v;
    }

    protected Tuple unbox(Object v, Tuple t) {
        return (Tuple)v;
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

    protected int hashCodePiece(int hash, int v, boolean isNull) {
        return isNull ? 0 : 31 * hash + v;
    }

    protected int hashCodePiece(int hash, long v, boolean isNull) {
        return isNull ? 0 : 31 * hash + (int)(v^(v>>>32));
    }

    protected int hashCodePiece(int hash, float v, boolean isNull) {
        return isNull ? 0 : 31 * hash + Float.floatToIntBits(v);
    }

    protected int hashCodePiece(int hash, double v, boolean isNull) {
        long v2 = Double.doubleToLongBits(v);
        return isNull ? 0 : 31 * hash + (int)(v2^(v2>>>32));
    }

    protected int hashCodePiece(int hash, boolean v, boolean isNull) {
        return isNull ? 0 : 31 * hash + (v ? 1231 : 1237);
    }

    protected int hashCodePiece(int hash, byte[] v, boolean isNull) {
        return isNull ? 0 : 31 * hash + DataByteArray.hashCode(v);
    }

    protected int hashCodePiece(int hash, String v, boolean isNull) {
        return isNull ? 0 : 31 * hash + v.hashCode();
    }

    protected int hashCodePiece(int hash, Tuple v, boolean isNull) {
        return isNull ? 0 : 31 * hash + v.hashCode();
    }

    @MustOverride
    public int hashCode() {
        return 0;
    }

    @MustOverride
    public void set(int fieldNum, Object val) throws ExecException {
        throw new ExecException("Invalid index " + fieldNum + " given");
    }

    @MustOverride
    public Object get(int fieldNum) throws ExecException {
        throw new ExecException("Invalid index " + fieldNum + " given");
    }

    @MustOverride
    public boolean isNull(int fieldNum) throws ExecException {
        throw new ExecException("Invalid index " + fieldNum + " given");
    }

    //TODO: do we even need this?
    @MustOverride
    public void setNull(int fieldNum) throws ExecException {
        throw new ExecException("Invalid index " + fieldNum + " given");
    }

    @MustOverride
    public byte getType(int fieldNum) throws ExecException {
        throw new ExecException("Invalid index " + fieldNum + " given");
    }

    protected void setPrimitiveBase(int fieldNum, Object val, String type) throws ExecException {
        throw new ExecException("Given field " + fieldNum + " not a " + type + " field!");
    }

    protected Object getPrimitiveBase(int fieldNum, String type) throws ExecException {
        throw new ExecException("Given field " + fieldNum + " not a " + type + " field!");
    }

    @MustOverride
    public void setInt(int fieldNum, int val) throws ExecException {
        setPrimitiveBase(fieldNum, val, "int");
    }

    @MustOverride
    public void setLong(int fieldNum, long val) throws ExecException {
        setPrimitiveBase(fieldNum, val, "long");
    }

    @MustOverride
    public void setFloat(int fieldNum, float val) throws ExecException {
        setPrimitiveBase(fieldNum, val, "float");
    }

    @MustOverride
    public void setDouble(int fieldNum, double val) throws ExecException {
        setPrimitiveBase(fieldNum, val, "double");
    }

    @MustOverride
    public void setBoolean(int fieldNum, boolean val) throws ExecException {
        setPrimitiveBase(fieldNum, val, "boolean");
    }

    @MustOverride
    public void setString(int fieldNum, String val) throws ExecException {
        setPrimitiveBase(fieldNum, val, "String");
    }

    @MustOverride
    public void setBytes(int fieldNum, byte[] val) throws ExecException {
        setPrimitiveBase(fieldNum, val, "byte[]");
    }

    @MustOverride
    public int getInt(int fieldNum) throws ExecException {
        return ((Number)getPrimitiveBase(fieldNum, "int")).intValue();
    }

    @MustOverride
    public long getLong(int fieldNum) throws ExecException {
        return ((Number)getPrimitiveBase(fieldNum, "long")).longValue();
    }

    @MustOverride
    public float getFloat(int fieldNum) throws ExecException {
        return ((Number)getPrimitiveBase(fieldNum, "float")).floatValue();
    }

    @MustOverride
    public double getDouble(int fieldNum) throws ExecException {
        return ((Number)getPrimitiveBase(fieldNum, "double")).doubleValue();
    }

    @MustOverride
    public boolean getBoolean(int fieldNum) throws ExecException {
        return (Boolean)getPrimitiveBase(fieldNum, "boolean");
    }

    @MustOverride
    public String getString(int fieldNum) throws ExecException {
        return (String)getPrimitiveBase(fieldNum, "String");
    }

    @MustOverride
    public byte[] getBytes(int fieldNum) throws ExecException {
        return ((DataByteArray)getPrimitiveBase(fieldNum, "byte[]")).get();
    }

    protected static Schema staticSchemaGen(String s) {
        try {
            if (s.equals("")) {
                return new Schema();
            }
            return Utils.getSchemaFromString(s);
        } catch (FrontendException e) {
            throw new RuntimeException("Unable to make Schema for String: " + s);
        }
    }

    protected void setAndCatch(Tuple t) {
        try {
            set(t);
        } catch (ExecException e) {
            throw new RuntimeException("Unable to set position 6 with Tuple: " + t, e);
        }
    }

    protected void setAndCatch(SchemaTuple t) {
        try {
            set(t);
        } catch (ExecException e) {
            throw new RuntimeException("Unable to set position 6 with Tuple: " + t, e);
        }
    }

    @MustOverride
    protected void writeElements(DataOutput out) throws IOException {
    }

    protected int compareSize(Tuple t) {
        int mySz = size();
        int tSz = t.size();
        if (mySz == tSz) {
            return 0;
        } else {
            return mySz > tSz ? 1 : -1;
        }
    }

    protected int compareNull(boolean usNull, boolean themNull) {
        if (usNull && themNull) {
            return 2;
        }
        if (themNull) {
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

    protected int compare(int val, SchemaTuple t, int pos) {
        int themVal;
        try {
            themVal = t.getInt(pos);
        } catch (ExecException e) {
            throw new RuntimeException("Unable to retrieve int field " + pos + " in given Tuple: " + t, e);
        }
        return compare(val, themVal);
    }

    protected int compare(int val, int themVal) {
        if (val != themVal) {
            return val > themVal ? 1 : -1;
        }
        return 0;
    }

    protected int compare(long val, SchemaTuple t, int pos) {
        long themVal;
        try {
            themVal = t.getLong(pos);
        } catch (ExecException e) {
            throw new RuntimeException("Unable to retrieve long field " + pos + " in given Tuple: " + t, e);
        }
        return compare(val, themVal);
    }

    protected int compare(long val, long themVal) {
        if (val != themVal) {
            return val > themVal ? 1 : -1;
        }
        return 0;
    }

    protected int compare(float val, SchemaTuple t, int pos) {
        float themVal;
        try {
            themVal = t.getFloat(pos);
        } catch (ExecException e) {
            throw new RuntimeException("Unable to retrieve float field " + pos + " in given Tuple: " + t, e);
        }
        return compare(val, themVal);
    }

    public int compare(float val, float themVal) {
        if (val != themVal) {
            return val > themVal ? 1 : -1;
        }
        return 0;
    }

    protected int compare(double val, SchemaTuple t, int pos) {
        double themVal;
        try {
            themVal = t.getDouble(pos);
        } catch (ExecException e) {
            throw new RuntimeException("Unable to retrieve double field " + pos + " in given Tuple: " + t, e);
        }
        return compare(val, themVal);
    }

    protected int compare(double val, double themVal) {
        if (val != themVal) {
            return val > themVal ? 1 : -1;
        }
        return 0;
    }

    protected int compare(boolean val, SchemaTuple t, int pos) {
        boolean themVal;
        try {
            themVal = t.getBoolean(pos);
        } catch (ExecException e) {
            throw new RuntimeException("Unable to retrieve boolean field " + pos + " in given Tuple: " + t, e);
        }
        return compare(val, themVal);
    }

    protected int compare(boolean val, boolean themVal) {
        if (val ^ themVal) {
            return val ? 1 : -1;
        }
        return 0;
    }

    protected int compare(byte[] val, SchemaTuple t, int pos) {
        try {
            return compare(val, t.getBytes(pos));
        } catch (ExecException e) {
            throw new RuntimeException("Unable to retrieve boolean field " + pos + " in given Tuple: " + t, e);
        }
    }

    protected int compare(byte[] val, byte[] themVal) {
        return DataByteArray.compare(val, themVal);
    }

    protected int compare(String val, SchemaTuple t, int pos) {
        try {
            return compare(val, t.getString(pos));
        } catch (ExecException e) {
            throw new RuntimeException("Unable to retrieve boolean field " + pos + " in given Tuple: " + t, e);
        }
    }

    protected int compare(String val, String themVal) {
        return val.compareTo(themVal);
    }

    protected int compare(SchemaTuple val, SchemaTuple t, int pos) {
        try {
            return compare(val, t.get(pos));
        } catch (ExecException e) {
            throw new RuntimeException("Unable to retrieve boolean field " + pos + " in given Tuple: " + t, e);
        }
    }

    protected int compare(SchemaTuple val, Object themVal) {
        return val.compareTo(themVal);
    }

    /**
     * This is slightly annoying, but this will allow SchemaTupleFactories to
     * avoid having to use reflection to instantiate a new SchemaTuple
     */
    public abstract SchemaTupleQuickGenerator<T> getQuickGenerator();

    public static abstract class SchemaTupleQuickGenerator<A> {
        public abstract A make();
    }
}
