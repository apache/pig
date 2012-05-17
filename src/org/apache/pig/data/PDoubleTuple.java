package org.apache.pig.data;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.PigWarning;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.util.LogUtils;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.google.common.collect.Lists;

@SuppressWarnings("serial")
public class PDoubleTuple extends PrimitiveFieldTuple {
    private static final Log logger = LogFactory.getLog(PDoubleTuple.class);

    private double val;

    public PDoubleTuple() {}

    public PDoubleTuple(double i) {
        val = i;
        isSet = true;
    }

    private static Schema s = new Schema(new Schema.FieldSchema(null, DataType.DOUBLE));

    public Schema getSchema() {
        return s;
    }

    @Override
    public void append(Object o) {
        if (isSet) {
            throw new RuntimeException("Unable to append to a Primitive Tuple");
        } else {
            if (o instanceof Number) {
                val = ((Number) o).doubleValue();
            } else if (o instanceof String) {
                val = Double.valueOf((String) o);
            } else {
                throw new RuntimeException("Unable to convert " + o + " to int.");
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
        Double d = isSet ? val : null;
        List<Object> l = Lists.newArrayListWithExpectedSize(1);
        l.add(d);
        return l;
    }

    @Override
    public byte getType(int pos) throws ExecException {
        if (pos != 0) throw new ExecException("Only 1 field in primitive tuples.");
        return DataType.DOUBLE;
    }

    @Override
    public void set(int pos, Object o) throws ExecException {
        if (pos != 0) throw new ExecException("Only 1 field in primitive tuples.");
        if (o == null) {
            isSet = false;
            return;
        }
        if (o instanceof Double) {
            val = (Double) o;
        } else if (o instanceof Number) {
            LogUtils.warn(this, "Coercing object to Double", PigWarning.IMPLICIT_CAST_TO_DOUBLE, logger);
            val = ((Number) o).doubleValue();
        } else if (o instanceof String) {
            LogUtils.warn(this, "Coercing object to Double", PigWarning.IMPLICIT_CAST_TO_DOUBLE, logger);
            val = Double.valueOf((String) o);
        } else {
            throw new RuntimeException("Unable to convert " + o + " to double.");
        }
        isSet = true;
    }

    @Override
    public void setDouble(int pos, double d) throws ExecException {
        if (pos != 0) throw new ExecException("Only 1 field in primitive tuples.");
        val = d;
        isSet = true;
    }

    @Override
    public double getDouble(int pos) throws ExecException {
        if (pos != 0) throw new ExecException("Only 1 field in primitive tuples.");
        return isSet ? val : null;
    }

    @Override
    protected int objectBytesSize() {
        return 8;
    }

}
