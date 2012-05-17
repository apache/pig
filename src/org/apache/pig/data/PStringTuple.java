package org.apache.pig.data;

import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.google.common.collect.Lists;

@SuppressWarnings("serial")
public class PStringTuple extends PrimitiveFieldTuple {

    private String val;

    public PStringTuple() {}

    public PStringTuple(String i) {
        val = i;
        isSet = true;
    }

    private static Schema s = new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY));

    public Schema getSchema() {
        return s;
    }

    @Override
    public void append(Object o) {
        if (isSet) {
            throw new RuntimeException("Unable to append to a Primitive Tuple");
        } else {
            if (o instanceof Number) {
                val = ((Number) o).toString();
            } else if (o instanceof String) {
                val = (String) o;
            } else {
                throw new RuntimeException("Unable to convert " + o + " to String.");
            }
        }
        isSet = true;
    }

    @Override
    public Object get() {
        return val;
    }

    @Override
    public List<Object> getAll() {
        String f = isSet ? val : null;
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
        if (pos != 0) throw new ExecException("Only 1 field in primitive tuples.");
        if (o == null) {
            isSet = false;
            return;
        }
        if (o instanceof Number) {
            val = ((Number) o).toString();
        } else if (o instanceof String) {
            val = (String) o;
        } else {
            throw new RuntimeException("Unable to convert " + o + " to string.");
        }
        isSet = true;
    }

    @Override
    public void setString(int pos, String s) throws ExecException {
        if (pos != 0) throw new ExecException("Only 1 field in primitive tuples.");
        val = s;
        isSet = true;
    }

    @Override
    public String getString(int pos) throws ExecException {
        if (pos != 0) throw new ExecException("Only 1 field in primitive tuples.");
        return isSet ? val : null;
    }

    @Override
    protected int objectBytesSize() {
        try {
            return isSet ? 0 : val.getBytes("UTF-8").length;
        } catch (UnsupportedEncodingException e) {
            // UTF-8 is hardcoded, not going to happen.
            throw new RuntimeException("WTF, UTF-8 is not supported?", e);
        }
    }

}
