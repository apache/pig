package org.apache.pig.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BinInterSedes.BinInterSedesTupleRawComparator;

@SuppressWarnings("serial")
public abstract class PrimitiveFieldTuple implements TypeAwareTuple {
    protected boolean isSet = false;

    @Override
    public void readFields(DataInput in) throws IOException {
        isSet = false;
        byte type = this.getType(0);
        // Only bother reading more if the isSet bit is on.
        if (in.readBoolean()) {
            switch (type) {
            case DataType.INTEGER:
                set(0, in.readInt());
                break;
            case DataType.FLOAT:
                set(0, in.readFloat());
                break;
            case DataType.LONG:
                set(0, in.readLong());
                break;
            case DataType.DOUBLE:
                set(0, in.readDouble());
                break;
            case DataType.CHARARRAY:
                set(0, in.readUTF());
                break;
            case DataType.BOOLEAN:
                set(0, in.readBoolean());
                break;
            default:
                throw new IOException("Unrecognized tuple type " + type);
            }
        }
    }

    /**
     * Serializes the tuple.
     * <b>Note</b> the write and readFields methods are not symmetric!
     * write prepends the serialized tuple with a type byte from {@link BinInterSedes}.
     * {@link BinInterSedes#readDatum(DataInput)} will use that byte to know to call
     * {@link PrimitiveFieldTuple#readFields(DataInput)}, which expects just the raw
     * serialized data.
     */
    @Override
    public void write(DataOutput out) throws IOException {
        byte type = this.getType(0);
        switch (type) {
        case DataType.INTEGER:
            out.writeByte(BinInterSedes.PINT_TUPLE);
            out.writeBoolean(isSet);
            if (isSet) {
                out.writeInt(getInteger(0));
            }
            break;
        case DataType.FLOAT:
            out.writeByte(BinInterSedes.PFLOAT_TUPLE);
            out.writeBoolean(isSet);
            if (isSet) {
                out.writeFloat(getFloat(0));
            }
            break;
        case DataType.LONG:
            out.writeByte(BinInterSedes.PLONG_TUPLE);
            out.writeBoolean(isSet);
            if (isSet) {
                out.writeLong(getLong(0));
            }
            break;
        case DataType.DOUBLE:
            out.writeByte(BinInterSedes.PDOUBLE_TUPLE);
            out.writeBoolean(isSet);
            if (isSet) {
                out.writeDouble(getDouble(0));
            }
            break;
        case DataType.BOOLEAN:
            out.writeByte(BinInterSedes.PBOOL_TUPLE);
            out.writeBoolean(isSet);
            if (isSet) {
                out.writeBoolean(getBoolean(0));
            }
            break;
        case DataType.CHARARRAY:
            out.writeByte(BinInterSedes.PSTRING_TUPLE);
            out.writeBoolean(isSet);
            if (isSet) {
                out.writeUTF(getString(0));
            }
            break;
        default:
            throw new IOException("Unrecognized tuple type " + type);
        }
    }

    @Override
    public int compareTo(Object other) {
        if (other instanceof Tuple) {
            Tuple t = (Tuple) other;
            int mySz = isSet ? 1 : 0;
            int tSz = t.size();
            if (tSz < mySz) {
                return 1;
            } else if (tSz > mySz) {
                return -1;
            } else {
                try {
                    int c = DataType.compare(get(0), t.get(0));
                    if (c != 0) {
                        return c;
                    }
                } catch (ExecException e) {
                    throw new RuntimeException("Unable to compare tuples", e);
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
        try {
            return isSet ? this.get(0).hashCode() : 13;
        } catch (ExecException e) {
            // Not going to happen.
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object get(int pos) throws ExecException {
        if (!isSet) return null;
        if (pos != 0) throw new ExecException("Attempt to access field " + pos + " of a primitive tuple");
        return get();
    }

    @Override
    public int size() {
        return isSet ? 1 : 0;
    }

    @Override
    public boolean isNull(int idx) throws ExecException {
        if (idx != 0) throw new ExecException("Only 1 field in primitive tuples.");
        return isSet;
    }

    /**
     * Since our serialization matches BinInterSedes, we can use the same comparator.
     * @return
     */
    public static Class<? extends TupleRawComparator> getComparatorClass() {
        return BinInterSedesTupleRawComparator.class;
    }

    /**
     * UNSUPPORTED OPERATION
     */
    @Override
    public void reference(Tuple inTup) {
        throw new RuntimeException("Unsupported Operation");
    }

    @Override
    public long getMemorySize() {
        return 8 /* tuple object header */
                + 8 /* isNull - but rounded to 8 bytes as total obj size needs to be multiple of 8 */
                + objectBytesSize();
    }

    @Override
    public String toDelimitedString(String arg0) throws ExecException {
        return isSet ? String.valueOf(get()) : "";
    }

    protected abstract int objectBytesSize();

    protected abstract Object get();

    /**
     * These methods blow up by default. Individual implementations, such as
     * {@link PIntTuple} should override as appropriate.
     */
    @Override
    public void setInt(int idx, int val) throws ExecException {
        throw new ExecException("This operation is not supported.");
    }

    /**
     * These methods blow up by default. Individual implementations, such as
     * {@link PIntTuple} should override as appropriate.
     */
    @Override
    public void setFloat(int idx, float val) throws ExecException {
        throw new ExecException("This operation is not supported.");
    }

    /**
     * These methods blow up by default. Individual implementations, such as
     * {@link PFloatTuple} should override as appropriate.
     */
    @Override
    public void setDouble(int idx, double val) throws ExecException {
        throw new ExecException("This operation is not supported.");
    }

    /**
     * These methods blow up by default. Individual implementations, such as
     * {@link PLongTuple} should override as appropriate.
     */
    @Override
    public void setLong(int idx, long val) throws ExecException {
        throw new ExecException("This operation is not supported.");
    }

    /**
     * These methods blow up by default. Individual implementations, such as
     * {@link PStringTuple} should override as appropriate.
     */
    @Override
    public void setString(int idx, String val) throws ExecException {
        throw new ExecException("This operation is not supported.");
    }

    /**
     * These methods blow up by default. Individual implementations, such as
     * {@link PStringTuple} should override as appropriate.
     */
    @Override
    public void setBoolean(int idx, boolean val) throws ExecException {
        throw new ExecException("This operation is not supported.");
    }

    /**
     * These methods blow up by default. Individual implementations, such as
     * {@link PStringTuple} should override as appropriate.
     */
    @Override
    public Integer getInteger(int idx) throws ExecException {
        throw new ExecException("This operation is not supported.");
    }

    /**
     * These methods blow up by default. Individual implementations, such as
     * {@link PStringTuple} should override as appropriate.
     */
    @Override
    public Long getLong(int idx) throws ExecException {
        throw new ExecException("This operation is not supported.");
    }

    /**
     * These methods blow up by default. Individual implementations, such as
     * {@link PStringTuple} should override as appropriate.
     */
    @Override
    public Float getFloat(int idx) throws ExecException {
        throw new ExecException("This operation is not supported.");
    }

    /**
     * These methods blow up by default. Individual implementations, such as
     * {@link PStringTuple} should override as appropriate.
     */
    @Override
    public Double getDouble(int idx) throws ExecException {
        throw new ExecException("This operation is not supported.");
    }

    /**
     * These methods blow up by default. Individual implementations, such as
     * {@link PStringTuple} should override as appropriate.
     */
    @Override
    public String getString(int idx) throws ExecException {
        throw new ExecException("This operation is not supported.");
    }

    /**
     * These methods blow up by default. Individual implementations, such as
     * {@link PStringTuple} should override as appropriate.
     */
    @Override
    public Boolean getBoolean(int idx) throws ExecException {
        throw new ExecException("This operation is not supported.");
    }

    @Override
    public String toString() {
        return "" + get();
    }
}
