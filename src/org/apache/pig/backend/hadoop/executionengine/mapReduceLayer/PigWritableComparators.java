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

import org.apache.pig.impl.io.NullablePartitionWritable;

public class PigWritableComparators {

    //
    // Raw Comparators for Skewed Join
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
