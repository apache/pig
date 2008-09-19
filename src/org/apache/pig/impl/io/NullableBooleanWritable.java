/**
 * 
 */
package org.apache.pig.impl.io;

import org.apache.hadoop.io.BooleanWritable;

/**
 *
 */
public class NullableBooleanWritable extends PigNullableWritable {

    public NullableBooleanWritable() {
        mValue = new BooleanWritable();
    }

    /**
     * @param value
     */
    public NullableBooleanWritable(boolean value) {
        mValue = new BooleanWritable(value);
    }

    public Object getValueAsPigType() {
        return isNull() ? null : ((BooleanWritable)mValue).get();
    }
}
