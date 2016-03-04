/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer;

import org.apache.hadoop.io.WritableComparator;
import org.apache.pig.impl.io.NullablePartitionWritable;

public class PigWritableComparators {

    // Byte only raw comparators for faster comparison for non-orderby jobs. Not re-using
    // JobControlCompiler.Pig<DataType>WritableComparator which extend PigWritableComparator.
    // Those use PigNullablePartitionWritable.compareTo which is not that efficient in cases like
    // tuple where tuple is iterated for null checking instead of taking advantage of
    // TupleRawComparator.hasComparedTupleNull(). Also skips multi-query index checking

    public static class PigBooleanRawBytesComparator extends PigBooleanRawComparator {

        public PigBooleanRawBytesComparator() {
            super();
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return WritableComparator.compareBytes(b1, s1, l1, b2, s2, l2);
        }
    }

    public static class PigIntRawBytesComparator extends PigIntRawComparator {

        public PigIntRawBytesComparator() {
            super();
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return WritableComparator.compareBytes(b1, s1, l1, b2, s2, l2);
        }
    }

    public static class PigBigIntegerRawBytesComparator extends PigBigIntegerRawComparator {

        public PigBigIntegerRawBytesComparator() {
            super();
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return WritableComparator.compareBytes(b1, s1, l1, b2, s2, l2);
        }
    }

    public static class PigBigDecimalRawBytesComparator extends PigBigDecimalRawComparator {

        public PigBigDecimalRawBytesComparator() {
            super();
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return WritableComparator.compareBytes(b1, s1, l1, b2, s2, l2);
        }
    }

    public static class PigLongRawBytesComparator extends PigLongRawComparator {

        public PigLongRawBytesComparator() {
            super();
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return WritableComparator.compareBytes(b1, s1, l1, b2, s2, l2);
        }
    }

    public static class PigFloatRawBytesComparator extends PigFloatRawComparator {

        public PigFloatRawBytesComparator() {
            super();
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return WritableComparator.compareBytes(b1, s1, l1, b2, s2, l2);
        }
    }

    public static class PigDoubleRawBytesComparator extends PigDoubleRawComparator {

        public PigDoubleRawBytesComparator() {
            super();
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return WritableComparator.compareBytes(b1, s1, l1, b2, s2, l2);
        }
    }

    public static class PigDateTimeRawBytesComparator extends PigDateTimeRawComparator {

        public PigDateTimeRawBytesComparator() {
            super();
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return WritableComparator.compareBytes(b1, s1, l1, b2, s2, l2);
        }
    }

    public static class PigTextRawBytesComparator extends PigTextRawComparator {

        public PigTextRawBytesComparator() {
            super();
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return WritableComparator.compareBytes(b1, s1, l1, b2, s2, l2);
        }
    }

    public static class PigBytesRawBytesComparator extends PigBytesRawComparator {

        public PigBytesRawBytesComparator() {
            super();
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return WritableComparator.compareBytes(b1, s1, l1, b2, s2, l2);
        }
    }

    public static class PigTupleSortBytesComparator extends PigTupleSortComparator {

        public PigTupleSortBytesComparator() {
            super();
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return WritableComparator.compareBytes(b1, s1, l1, b2, s2, l2);
        }
    }

    //
    // Byte only raw comparators for faster comparison for Skewed Join.
    //
    public static class PigBooleanRawBytesPartitionComparator extends PigBooleanRawComparator {

        public PigBooleanRawBytesPartitionComparator() {
            super();
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return WritableComparator.compareBytes(b1, s1, l1, b2, s2, l2);
        }

        @Override
        public int compare(Object o1, Object o2) {
            return super.compare(((NullablePartitionWritable)o1).getKey(), ((NullablePartitionWritable)o2).getKey());
        }
    }

    public static class PigIntRawBytesPartitionComparator extends PigIntRawComparator {

        public PigIntRawBytesPartitionComparator() {
            super();
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return WritableComparator.compareBytes(b1, s1, l1, b2, s2, l2);
        }

        @Override
        public int compare(Object o1, Object o2) {
            return super.compare(((NullablePartitionWritable)o1).getKey(), ((NullablePartitionWritable)o2).getKey());
        }
    }

    public static class PigBigIntegerRawBytesPartitionComparator extends PigBigIntegerRawComparator {

        public PigBigIntegerRawBytesPartitionComparator() {
            super();
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return WritableComparator.compareBytes(b1, s1, l1, b2, s2, l2);
        }

        @Override
        public int compare(Object o1, Object o2) {
            return super.compare(((NullablePartitionWritable)o1).getKey(), ((NullablePartitionWritable)o2).getKey());
        }
    }

    public static class PigBigDecimalRawBytesPartitionComparator extends PigBigDecimalRawComparator {

        public PigBigDecimalRawBytesPartitionComparator() {
            super();
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return WritableComparator.compareBytes(b1, s1, l1, b2, s2, l2);
        }

        @Override
        public int compare(Object o1, Object o2) {
            return super.compare(((NullablePartitionWritable)o1).getKey(), ((NullablePartitionWritable)o2).getKey());
        }
    }

    public static class PigLongRawBytesPartitionComparator extends PigLongRawComparator {

        public PigLongRawBytesPartitionComparator() {
            super();
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return WritableComparator.compareBytes(b1, s1, l1, b2, s2, l2);
        }

        @Override
        public int compare(Object o1, Object o2) {
            return super.compare(((NullablePartitionWritable)o1).getKey(), ((NullablePartitionWritable)o2).getKey());
        }
    }

    public static class PigFloatRawBytesPartitionComparator extends PigFloatRawComparator {

        public PigFloatRawBytesPartitionComparator() {
            super();
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return WritableComparator.compareBytes(b1, s1, l1, b2, s2, l2);
        }

        @Override
        public int compare(Object o1, Object o2) {
            return super.compare(((NullablePartitionWritable)o1).getKey(), ((NullablePartitionWritable)o2).getKey());
        }
    }

    public static class PigDoubleRawBytesPartitionComparator extends PigDoubleRawComparator {

        public PigDoubleRawBytesPartitionComparator() {
            super();
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return WritableComparator.compareBytes(b1, s1, l1, b2, s2, l2);
        }

        @Override
        public int compare(Object o1, Object o2) {
            return super.compare(((NullablePartitionWritable)o1).getKey(), ((NullablePartitionWritable)o2).getKey());
        }
    }

    public static class PigDateTimeRawBytesPartitionComparator extends PigDateTimeRawComparator {

        public PigDateTimeRawBytesPartitionComparator() {
            super();
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return WritableComparator.compareBytes(b1, s1, l1, b2, s2, l2);
        }

        @Override
        public int compare(Object o1, Object o2) {
            return super.compare(((NullablePartitionWritable)o1).getKey(), ((NullablePartitionWritable)o2).getKey());
        }
    }

    public static class PigTextRawBytesPartitionComparator extends PigTextRawComparator {

        public PigTextRawBytesPartitionComparator() {
            super();
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return WritableComparator.compareBytes(b1, s1, l1, b2, s2, l2);
        }

        @Override
        public int compare(Object o1, Object o2) {
            return super.compare(((NullablePartitionWritable)o1).getKey(), ((NullablePartitionWritable)o2).getKey());
        }
    }

    public static class PigBytesRawBytesPartitionComparator extends PigBytesRawComparator {

        public PigBytesRawBytesPartitionComparator() {
            super();
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return WritableComparator.compareBytes(b1, s1, l1, b2, s2, l2);
        }

        @Override
        public int compare(Object o1, Object o2) {
            return super.compare(((NullablePartitionWritable)o1).getKey(), ((NullablePartitionWritable)o2).getKey());
        }
    }

    public static class PigTupleSortBytesPartitionComparator extends PigTupleSortComparator {

        public PigTupleSortBytesPartitionComparator() {
            super();
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return WritableComparator.compareBytes(b1, s1, l1, b2, s2, l2);
        }

        @Override
        public int compare(Object o1, Object o2) {
            return super.compare(((NullablePartitionWritable)o1).getKey(), ((NullablePartitionWritable)o2).getKey());
        }
    }

    //
    //  Raw Comparators for Skewed Join
    //
    public static class PigBooleanRawPartitionComparator extends PigBooleanRawComparator {

        public PigBooleanRawPartitionComparator() {
            super();
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            // Skip the first byte which is the type of the key
            return super.compare(b1, s1 + 1, l1, b2, s2 + 1, l2);
        }

        @Override
        public int compare(Object o1, Object o2) {
            return super.compare(((NullablePartitionWritable)o1).getKey(), ((NullablePartitionWritable)o2).getKey());
        }
    }

    public static class PigIntRawPartitionComparator extends PigIntRawComparator {

        public PigIntRawPartitionComparator() {
            super();
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return super.compare(b1, s1 + 1, l1, b2, s2 + 1, l2);
        }

        @Override
        public int compare(Object o1, Object o2) {
            return super.compare(((NullablePartitionWritable)o1).getKey(), ((NullablePartitionWritable)o2).getKey());
        }
    }

    public static class PigBigIntegerRawPartitionComparator extends PigBigIntegerRawComparator {

        public PigBigIntegerRawPartitionComparator() {
            super();
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return super.compare(b1, s1 + 1, l1, b2, s2 + 1, l2);
        }

        @Override
        public int compare(Object o1, Object o2) {
            return super.compare(((NullablePartitionWritable)o1).getKey(), ((NullablePartitionWritable)o2).getKey());
        }
    }

    public static class PigBigDecimalRawPartitionComparator extends PigBigDecimalRawComparator {

        public PigBigDecimalRawPartitionComparator() {
            super();
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return super.compare(b1, s1 + 1, l1, b2, s2 + 1, l2);
        }

        @Override
        public int compare(Object o1, Object o2) {
            return super.compare(((NullablePartitionWritable)o1).getKey(), ((NullablePartitionWritable)o2).getKey());
        }
    }

    public static class PigLongRawPartitionComparator extends PigLongRawComparator {

        public PigLongRawPartitionComparator() {
            super();
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return super.compare(b1, s1 + 1, l1, b2, s2 + 1, l2);
        }

        @Override
        public int compare(Object o1, Object o2) {
            return super.compare(((NullablePartitionWritable)o1).getKey(), ((NullablePartitionWritable)o2).getKey());
        }
    }

    public static class PigFloatRawPartitionComparator extends PigFloatRawComparator {

        public PigFloatRawPartitionComparator() {
            super();
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return super.compare(b1, s1 + 1, l1, b2, s2 + 1, l2);
        }

        @Override
        public int compare(Object o1, Object o2) {
            return super.compare(((NullablePartitionWritable)o1).getKey(), ((NullablePartitionWritable)o2).getKey());
        }
    }

    public static class PigDoubleRawPartitionComparator extends PigDoubleRawComparator {

        public PigDoubleRawPartitionComparator() {
            super();
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return super.compare(b1, s1 + 1, l1, b2, s2 + 1, l2);
        }

        @Override
        public int compare(Object o1, Object o2) {
            return super.compare(((NullablePartitionWritable)o1).getKey(), ((NullablePartitionWritable)o2).getKey());
        }
    }

    public static class PigDateTimeRawPartitionComparator extends PigDateTimeRawComparator {

        public PigDateTimeRawPartitionComparator() {
            super();
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return super.compare(b1, s1 + 1, l1, b2, s2 + 1, l2);
        }

        @Override
        public int compare(Object o1, Object o2) {
            return super.compare(((NullablePartitionWritable)o1).getKey(), ((NullablePartitionWritable)o2).getKey());
        }
    }

    public static class PigTextRawPartitionComparator extends PigTextRawComparator {

        public PigTextRawPartitionComparator() {
            super();
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return super.compare(b1, s1 + 1, l1, b2, s2 + 1, l2);
        }

        @Override
        public int compare(Object o1, Object o2) {
            return super.compare(((NullablePartitionWritable)o1).getKey(), ((NullablePartitionWritable)o2).getKey());
        }
    }

    public static class PigBytesRawPartitionComparator extends PigBytesRawComparator {

        public PigBytesRawPartitionComparator() {
            super();
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return super.compare(b1, s1 + 1, l1, b2, s2 + 1, l2);
        }

        @Override
        public int compare(Object o1, Object o2) {
            return super.compare(((NullablePartitionWritable)o1).getKey(), ((NullablePartitionWritable)o2).getKey());
        }
    }

    public static class PigTupleSortPartitionComparator extends PigTupleSortComparator {

        public PigTupleSortPartitionComparator() {
            super();
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return super.compare(b1, s1 + 1, l1, b2, s2 + 1, l2);
        }

        @Override
        public int compare(Object o1, Object o2) {
            return super.compare(((NullablePartitionWritable)o1).getKey(), ((NullablePartitionWritable)o2).getKey());
        }
    }

}
