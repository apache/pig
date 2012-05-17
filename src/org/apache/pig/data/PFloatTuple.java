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
public class PFloatTuple extends PrimitiveFieldTuple {
    private static final Log logger = LogFactory.getLog(PFloatTuple.class);
    private float val;

    public PFloatTuple() {}

    public PFloatTuple(float i) {
        val = i;
        isSet = true;
    }

    private static Schema s = new Schema(new Schema.FieldSchema(null, DataType.FLOAT));

    public Schema getSchema() {
        return s;
    }

    @Override
    public void append(Object o) {
        if (isSet) {
            throw new RuntimeException("Unable to append to a Primitive Tuple");
        } else {
            if (o instanceof Number) {
                val = ((Number) o).floatValue();
            } else if (o instanceof String) {
                val = Float.valueOf((String) o);
            } else {
                throw new RuntimeException("Unable to convert " + o + " to float.");
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
        Float f = isSet ? val : null;
        List<Object> l = Lists.newArrayListWithExpectedSize(1);
        l.add(f);
        return l;
    }

    @Override
    public byte getType(int pos) throws ExecException {
        if (pos != 0) throw new ExecException("Only 1 field in primitive tuples.");
        return DataType.FLOAT;
    }

    @Override
    public void set(int pos, Object o) throws ExecException {
        if (pos != 0) throw new ExecException("Only 1 field in primitive tuples.");
        if (o == null) {
            isSet = false;
            return;
        }
        if (o instanceof Float) {
            val = (Float) o;
        } else if (o instanceof Number) {
            LogUtils.warn(this, "Coercing object to Float", PigWarning.IMPLICIT_CAST_TO_FLOAT, logger);
            val = ((Number) o).floatValue();
        } else if (o instanceof String) {
            LogUtils.warn(this, "Coercing object to Float", PigWarning.IMPLICIT_CAST_TO_FLOAT, logger);
            val = Float.valueOf((String) o);
        } else {
            throw new RuntimeException("Unable to convert " + o + " to float.");
        }
        isSet = true;
    }

    @Override
    public void setFloat(int pos, float f) throws ExecException {
        if (pos != 0) throw new ExecException("Only 1 field in primitive tuples.");
        val = f;
        isSet = true;
    }

    @Override
    public float getFloat(int pos) throws ExecException {
        if (pos != 0) throw new ExecException("Only 1 field in primitive tuples.");
        return isSet ? val : null;
    }

    @Override
    protected int objectBytesSize() {
        return 4;
    }

}
