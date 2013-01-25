package org.apache.pig.impl.io;

import java.math.BigInteger;

import org.apache.pig.backend.hadoop.BigIntegerWritable;

public class NullableBigIntegerWritable extends PigNullableWritable {

    public NullableBigIntegerWritable() {
        mValue = new BigIntegerWritable();
    }

    public NullableBigIntegerWritable(BigInteger value) {
        mValue = new BigIntegerWritable(value);
    }

    @Override
    public Object getValueAsPigType() {
        return isNull() ? null : ((BigIntegerWritable)mValue).get();
    }

}
