package org.apache.pig.backend.hadoop;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.pig.data.BinInterSedes;
import org.apache.pig.data.DataType;

public class BigDecimalWritable implements WritableComparable<BigDecimalWritable> {

    private static final BinInterSedes bis = new BinInterSedes();
    private BigDecimal value;

    public BigDecimalWritable() {
        value = BigDecimal.ZERO;
    }

    public BigDecimalWritable(BigDecimal bi) {
        value = bi;
    }

    public void set(BigDecimal value) {
        this.value = value;
    }

    public BigDecimal get() {
        return value;
    }

    @Override
    public int compareTo(BigDecimalWritable o) {
        return value.compareTo(o.get());
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof BigDecimalWritable)) {
            return false;
        }
        return compareTo((BigDecimalWritable)o) == 0;
    }

    @Override
    public String toString() {
        return value.toString();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        value = (BigDecimal)bis.readDatum(in, DataType.BIGDECIMAL);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        bis.writeDatum(out, value, DataType.BIGDECIMAL);
    }

    /** A Comparator optimized for BigDecimalWritable. */
    //TODO consider trying to do something a big more optimizied
    public static class Comparator extends WritableComparator {
        private BigDecimalWritable thisValue = new BigDecimalWritable();
        private BigDecimalWritable thatValue = new BigDecimalWritable();

        public Comparator() {
            super(BigDecimalWritable.class);
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
        WritableComparator.define(BigDecimalWritable.class, new Comparator());
    }
}