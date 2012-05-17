package org.apache.pig.data;

import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.google.common.collect.Lists;

@SuppressWarnings("serial")
public class PBooleanTuple extends PrimitiveFieldTuple {

    private boolean val;

    public PBooleanTuple() {}

    public PBooleanTuple(Boolean v) {
        val = v;
        isSet = true;
    }

    private static Schema s = new Schema(new Schema.FieldSchema(null, DataType.BOOLEAN));

    public Schema getSchema() {
        return s;
    }

    @Override
    public void append(Object o) {
        if (isSet) {
            throw new RuntimeException("Unable to append to a Primitive Tuple");
        } else {
            val = (Boolean) o;
        }
        isSet = true;
    }

    @Override
    public Object get() {
        return val;
    }

    @Override
    public List<Object> getAll() {
        Boolean f = isSet ? val : null;
        List<Object> l = Lists.newArrayListWithExpectedSize(1);
        l.add(f);
        return l;
    }

    @Override
    public byte getType(int pos) throws ExecException {
        if (pos != 0) throw new ExecException("Only 1 field in primitive tuples.");
        return DataType.CHARARRAY;
    }

    @Override
    public void set(int pos, Object o) throws ExecException {
        if (o == null) {
            isSet = false;
            return;
        }
        val = (Boolean) o;
        isSet = true;
    }

    @Override
    public void setBoolean(int pos, boolean v) throws ExecException {
        if (pos != 0) throw new ExecException("Only 1 field in primitive tuples.");
        val = v;
        isSet = true;
    }

    @Override
    public boolean getBoolean(int pos) throws ExecException {
        if (pos != 0) throw new ExecException("Only 1 field in primitive tuples.");
        return isSet ? val : null;
    }

    @Override
    protected int objectBytesSize() {
            return isSet ? 0 : 1;
    }
}
