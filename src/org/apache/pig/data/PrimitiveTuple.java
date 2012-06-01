package org.apache.pig.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Iterator;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BinInterSedes;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import com.google.common.collect.Lists;

/**
 * Keeps all data serialized in a byte array, and deserializes on demand.
 * Useful for reducing the memory usage. Only holds primitive data types
 * (integers, longs, floats, doubles, and booleans).<br>
 * <p>
 * A rough-cuts test using Google's Caliper shows results as follows:
 * <pre>
numFields           benchmark    ns linear runtime
        1       ObjListCreate 102.2 ====
        1        ObjReadField 111.6 ==== [9.4]
        1 PrimitiveListCreate  53.7 ==
        1  PrimitiveReadField 105.4 ==== [51.7]
        5       ObjListCreate 322.3 ==============
        5        ObjReadField 351.5 =============== [29.2]
        5 PrimitiveListCreate 118.3 =====
        5  PrimitiveReadField 164.8 ======= [46.5]
       10       ObjListCreate 596.3 ==========================
       10        ObjReadField 685.1 ============================== [89]
       10 PrimitiveListCreate 274.8 ============
       10  PrimitiveReadField 344.8 =============== [70]
    </pre>
 * (PrimitiveReadField creates a tuple of numFields *and* reads
 * 2 fields out of it, so the cost of reading is approximately
 * xReadField - xListCreate; I indicate it here in brackets.)
 * <p>
 * This appears to indicate that read costs are a bit higher, but reasonable,
 * while creation cost is low.<br>
 * The memory overhead is significantly smaller, so for most cases where it matters,
 * we will easily make up what we lose on deserialization by saving allocation and GC costs
 * (in my experience, it's very rare that we read the same bag multiple times).
 * <p>
 * For single-field Tuples, PrimitiveFieldTuple is a better class.
 * BetterTupleFactory will automatically create the appropriate tuple for a given schema.
 * When we use PrimitiveFieldTuples, the benchmark looks like this (!):
<pre>
numFields                       benchmark    ns linear runtime
        1       ObjListCreate 111.4 ====
        1        ObjReadField 115.3 ====
        1 PrimitiveListCreate  13.5 =
        1  PrimitiveReadField  31.2 = [17.7]
</pre>
 *
 */
public class PrimitiveTuple extends AbstractTuple implements TypeAwareTuple {
    private static final long serialVersionUID = 1L;
    private ByteBuffer buffer;
    private boolean[] nulls;
    private byte[] types;
    private static final InterSedes sedes = InterSedesFactory.getInterSedesInstance();

    /**
     * Constructs an empty PrimitiveTuple. This is generally a bad idea.
     * You really want to avoid using the {@link PrimitiveTuple#append(Object)} method,
     * and rely on constructing a PrimitiveTuple with the appropriate schema from
     * the get-go.
     * <p>
     * If you do use this constructor, the {@link PrimitiveTuple#reset(byte...)} method
     * is an efficient way to build these if you can find out the schema before needing to add
     * actual values to the tuple.
     */
    public PrimitiveTuple() {
        buffer = ByteBuffer.allocate(0);
        nulls = new boolean[0];
        types = new byte[0];
    }

    public PrimitiveTuple(Schema schema) {
        nulls = new boolean[schema.size()];
        types = new byte[schema.size()];
        int slabsize = 0;
        List<FieldSchema> fieldSchemas = schema.getFields();
        for (int i = 0; i < fieldSchemas.size(); i++) {
            FieldSchema fs = fieldSchemas.get(i);
            slabsize += sizeOf(fs.type);
            types[i] = fs.type;
            if (sizeOf(fs.type) == 0) {
                throw new RuntimeException("Type "
                        + getNameForType(fs.type)
                        + " is not supported by PrimitiveTuple");
            }
        }
        buffer = ByteBuffer.allocate(slabsize);
    }

    public PrimitiveTuple(byte... types) {
        this.nulls = new boolean[types.length];
        this.types = types;
        int slabsize = 0;
        for (int i = 0; i < types.length; i++) {
            slabsize += sizeOf(types[i]);
        }
        buffer = ByteBuffer.allocate(slabsize);
    }

    /**
     * Clear out the contents of this tuple, if any, and set it to look
     * as if it was constructed for the "types" schema
     * @param types A list of bytes from DataType representing the expected schema
     */
    public void reset(byte... types) {
        this.nulls = new boolean[types.length];
        this.types = types;
        int slabsize = 0;
        for (int i = 0; i < types.length; i++) {
            slabsize += sizeOf(types[i]);
        }
        buffer = ByteBuffer.allocate(slabsize);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int numFields = readPrimitiveTupleSize(in);
        byte[] schema = new byte[numFields];
        byte[] nulls = new byte[numFields];
        in.readFully(schema);
        in.readFully(nulls);
        reset(schema);

        for (int i = 0; i < numFields; i++) {
            if (nulls[i] == 1) {
                set(i, null, DataType.NULL);
            } else {
                switch(schema[i]) {
                case DataType.INTEGER: setInt(i, in.readInt()); break;
                case DataType.FLOAT: setFloat(i, in.readFloat()); break;
                case DataType.LONG: setLong(i, in.readLong()); break;
                case DataType.DOUBLE: setDouble(i, in.readDouble()); break;
                case DataType.BOOLEAN: setBoolean(i, in.readBoolean()); break;
                default: throw new IOException("Unexpected type in serialized tuple schema: " + schema[i]);
                }
            }
        }
    }

    private int readPrimitiveTupleSize(DataInput in) throws IOException {
        byte byteVal = in.readByte();
        int size = 0;
        while (byteVal == Byte.MIN_VALUE) {
            size += Byte.MAX_VALUE;
            byteVal = in.readByte();
        }
        return size + byteVal;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        int size = size();
        out.writeByte(BinInterSedes.PRIMITIVE_TUPLE);
        while (size > Byte.MAX_VALUE) {
            out.write(Byte.MIN_VALUE);
            size -= Byte.MAX_VALUE;
        }
        out.write((byte) size);

        for (int i = 0; i < size; i++) {
            out.writeByte(getType(i));
        }
        for (int i = 0; i < size; i++) {
            out.writeByte(isNull(i) ? 1 : 0);
        }

        for (int i = 0; i < size; i++) {
            switch (getType(i)) {
            case DataType.INTEGER: out.writeInt(getInteger(i)); break;
            case DataType.FLOAT: out.writeFloat(getFloat(i)); break;
            case DataType.LONG: out.writeLong(getLong(i)); break;
            case DataType.DOUBLE: out.writeDouble(getDouble(i)); break;
            case DataType.BOOLEAN: out.writeBoolean(getBoolean(i)); break;
            }
        }
    }

    //TODO add optimizations from BinSedes?
    private int sizeOf(byte type) {
        switch (type) {
        case DataType.INTEGER: return 4;
        case DataType.DOUBLE: return 8;
        case DataType.FLOAT: return 4;
        case DataType.LONG: return 8;
        case DataType.BOOLEAN: return 1;
        default: return 0;
        }
    }

    private byte dataTypeToBinInterSedesType(byte type) {
        switch (type) {
        case DataType.INTEGER: return BinInterSedes.INTEGER;
        case DataType.DOUBLE: return BinInterSedes.DOUBLE;
        case DataType.FLOAT: return BinInterSedes.FLOAT;
        case DataType.LONG: return BinInterSedes.LONG;
        default: throw new RuntimeException("Unknown DataType " + type);
        }
    }

    private byte binInterSedesToDataType(byte type) {
        switch (type) {
        case BinInterSedes.INTEGER: return DataType.INTEGER;
        case BinInterSedes.DOUBLE: return DataType.DOUBLE;
        case BinInterSedes.FLOAT: return DataType.FLOAT;
        case BinInterSedes.LONG: return DataType.LONG;
        case BinInterSedes.BOOLEAN_FALSE: return DataType.BOOLEAN;
        case BinInterSedes.BOOLEAN_TRUE: return DataType.BOOLEAN;
        default: throw new RuntimeException("Unknown BinInterSedes type: " + type);
        }
    }

    //TODO use binsedes comparator?
    @Override
    public int compareTo(Object other) {
        if (other instanceof Tuple) {
            Tuple t = (Tuple) other;
            int mySz = types.length;
            int tSz = t.size();
            if (tSz < mySz) {
                return 1;
            } else if (tSz > mySz) {
                return -1;
            } else {
                for (int i = 0; i < mySz; i++) {
                    try {
                        int c = DataType.compare(get(i), t.get(i));
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
    public void append(Object obj) {
        byte type = DataType.findType(obj);
        int idx = types.length;
        types = Arrays.copyOf(types, types.length + 1);
        nulls = Arrays.copyOf(nulls, nulls.length + 1);

        buffer = ByteBuffer.wrap(Arrays.copyOf(buffer.array(), buffer.capacity() + sizeOf(type)));
        try {
            set(idx, obj, type);
        } catch (ExecException e) {
            // Set throws an EE and append does not. Riddle me that.
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object get(int idx) throws ExecException {
        try {
            if (nulls[idx]) return null;
            byte type = types[idx];
            int length = sizeOf(types[idx]);
            int offset = findByteOffset(idx);
            return readField(offset, length, type);
        } catch (IndexOutOfBoundsException e) {
            throw new ExecException("Attempt to access non-existent field in PrimitiveTuple", e);
        }
    }

    private int findByteOffset(int idx) {
        int offset = 0;
        for (int i = 0; i < idx; i++) {
            offset += sizeOf(types[i]);
        }
        return offset;
    }

    private Object readField(int offset, int length, byte type) {
        switch (type) {
        case DataType.INTEGER:
            return buffer.getInt(offset);
        case DataType.LONG:
            return buffer.getLong(offset);
        case DataType.FLOAT:
            return buffer.getFloat(offset);
        case DataType.DOUBLE:
            return buffer.getDouble(offset);
        case DataType.BOOLEAN:
            return buffer.get(offset) == 1;
        default:
            throw new RuntimeException("Can't interpret type " + getNameForType(type));
        }
    }

    @Override
    public List<Object> getAll() {
        List<Object> list = Lists.newArrayListWithExpectedSize(types.length);
        int offset = 0;
        for (byte type : types) {
            list.add(readField(offset, sizeOf(type), type));
            offset += sizeOf(type);
        }
        return list;
    }

    @Override
    public long getMemorySize() {
        // add seven, divide by 8, multiply by 8 in order to
        // round up to nearest multiple of 8.
        return 8 * ((
                8 // object header
                + (8 * (nulls.length + 7) / 8)
                + 1 // isNull
                + types.length
                + buffer.capacity() + 7) / 8);
    }

    @Override
    public byte getType(int idx) throws ExecException {
        if (idx >= types.length) throw new ExecException("Out-of-bounds access for idx " + idx);
        return types[idx];
    }

    @Override
    public boolean isNull(int idx) throws ExecException {
        if (idx >= types.length) throw new ExecException("Out-of-bounds access for idx " + idx);
        return nulls[idx];
    }

    /**
     * NOT IMPLEMENTED
     */
    @Override
    public void reference(Tuple t) {
        // This method is not used anywhere in the pig codebase.
        // We should deprecate it.
        // Not implemented.
        throw new RuntimeException("This method is not implemented for PrimitiveTuple");
    }

    @Override
    public void set(int idx, Object obj) throws ExecException {
        byte type = DataType.findType(obj);
        set(idx, obj, type);
    }

    @Override
    public void setInt(int idx, int val) throws ExecException {
        int offset = findByteOffset(idx);
        buffer.putInt(offset, val);
    }

    @Override
    public void setFloat(int idx, float val) throws ExecException {
        int offset = findByteOffset(idx);
        buffer.putFloat(offset, val);
    }

    @Override
    public void setLong(int idx, long val) throws ExecException {
        int offset = findByteOffset(idx);
        buffer.putLong(offset, val);
    }

    @Override
    public void setDouble(int idx, double val) throws ExecException {
        int offset = findByteOffset(idx);
        buffer.putDouble(offset, val);
    }

    @Override
    public void setBoolean(int idx, boolean val) throws ExecException {
        int offset = findByteOffset(idx);
        buffer.put(offset, (byte) ((val) ? 1 : 0));
    }

    public void set(int idx, Object obj, byte type) throws ExecException {
        try {
            if (type == DataType.NULL) {
                nulls[idx] = true;
                return;
            }
            if (type != types[idx])
                throw new ExecException("Expected type " +
                        DataType.genTypeToNameMap().get(types[idx]) +
                        ", got " + DataType.genTypeToNameMap().get(type));
            int offset = findByteOffset(idx);
            switch(type) {
            case DataType.INTEGER:
                buffer.putInt(offset, (Integer) obj); break;
            case DataType.LONG:
                buffer.putLong(offset, (Long) obj); break;
            case DataType.FLOAT:
                buffer.putFloat(offset, (Float) obj); break;
            case DataType.DOUBLE:
                buffer.putDouble(offset, (Double) obj); break;
            case DataType.BOOLEAN:
                buffer.put(offset, (byte) (((Boolean) obj) ? 1 : 0)); break;
            }
        } catch (IndexOutOfBoundsException e) {
            throw new ExecException("Can't set field " + idx + " in a tuple of " + types.length + " fields.", e);
        }
    }

    @Override
    public int size() {
        return types.length;
    }

    /**
     * NOT IMPLEMENTED
     */
    @Override
    public void setString(int idx, String val) throws ExecException {
        throw new ExecException("PrimitiveTuple does not support Strings.");
    }

    private String getNameForType(byte type) {
        return DataType.genTypeToNameMap().get(type);
    }

    @Override
    public Integer getInteger(int idx) throws ExecException {
        return buffer.getInt(findByteOffset(idx));
    }

    @Override
    public Float getFloat(int idx) throws ExecException {
        return buffer.getFloat(findByteOffset(idx));
    }

    @Override
    public Double getDouble(int idx) throws ExecException {
        return buffer.getDouble(findByteOffset(idx));
    }

    @Override
    public Long getLong(int idx) throws ExecException {
        return buffer.getLong(findByteOffset(idx));
    }

    @Override
    public Boolean getBoolean(int idx) throws ExecException {
        return buffer.get(findByteOffset(idx)) == 1;
    }

    /**
     * Always throws ExecException since Strings are not supported
     */
    @Override
    public String getString(int idx) throws ExecException {
        throw new ExecException("PrimitiveTuple does not support Strings");
    }
}