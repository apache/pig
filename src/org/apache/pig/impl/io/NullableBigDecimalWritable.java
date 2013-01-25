package org.apache.pig.impl.io;

import java.math.BigDecimal;

import org.apache.pig.backend.hadoop.BigDecimalWritable;

public class NullableBigDecimalWritable extends PigNullableWritable {

    public NullableBigDecimalWritable() {
        mValue = new BigDecimalWritable();
    }

    public NullableBigDecimalWritable(BigDecimal value) {
        mValue = new BigDecimalWritable(value);
    }

    @Override
    public Object getValueAsPigType() {
        return isNull() ? null : ((BigDecimalWritable)mValue).get();
    }

}
