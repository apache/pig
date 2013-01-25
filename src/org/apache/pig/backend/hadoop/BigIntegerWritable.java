package org.apache.pig.backend.hadoop;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigInteger;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.pig.data.BinInterSedes;
import org.apache.pig.data.DataType;

public class BigIntegerWritable implements WritableComparable<BigIntegerWritable> {

    private static final BinInterSedes bis = new BinInterSedes();
    private BigInteger value;

    public BigIntegerWritable() {
        value = BigInteger.ZERO;
    }

    public BigIntegerWritable(BigInteger bi) {
        value = bi;
    }

    public void set(BigInteger value) {
        this.value = value;
    }

    public BigInteger get() {
        return value;
    }

    @Override
    public int compareTo(BigIntegerWritable o) {
        return value.compareTo(o.get());
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof BigIntegerWritable)) {
            return false;
        }
        return compareTo((BigIntegerWritable)o) == 0;
    }

    @Override
    public String toString() {
        return value.toString();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        value = (BigInteger)bis.readDatum(in, DataType.BIGINTEGER);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        bis.writeDatum(out, value, DataType.BIGINTEGER);
    }

    /** A Comparator optimized for BigIntegerWritable. */
    //TODO consider trying to do something a big more optimizied
    public static class Comparator extends WritableComparator {
        private BigIntegerWritable thisValue = new BigIntegerWritable();
        private BigIntegerWritable thatValue = new BigIntegerWritable();

        public Comparator() {
            super(BigIntegerWritable.class);
        }

        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {
            try {
                thisValue.readFields(new DataInputStream(new ByteArrayInputStream(b1)));
                thatValue.readFields(new DataInputStream(new ByteArrayInputStream(b2)));
            } catch (IOException e) {
                throw new RuntimeException("Unable to read field from byte array: " + e);
            }
            return thisValue.compareTo(thatValue);
        }
    }

    // register this comparator
    static {
        WritableComparator.define(BigIntegerWritable.class, new Comparator());
    }

}
