/*
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
package org.apache.pig.data.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.data.BinInterSedes;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

@InterfaceAudience.Private
public class SedesHelper {
    private static final BinInterSedes pigSerializer = new BinInterSedes();
    private static final TupleFactory mTupleFactory = TupleFactory.getInstance();

    public static void writeBytes(DataOutput out, byte[] buf) throws IOException {
        int sz = buf.length;
        if (sz < BinInterSedes.UNSIGNED_BYTE_MAX) {
            out.writeByte(BinInterSedes.TINYBYTEARRAY);
            out.writeByte(sz);
        } else if (sz < BinInterSedes.UNSIGNED_SHORT_MAX) {
            out.writeByte(BinInterSedes.SMALLBYTEARRAY);
            out.writeShort(sz);
        } else {
            out.writeByte(BinInterSedes.BYTEARRAY);
            out.writeInt(sz);
        }
        out.write(buf);
    }

    public static byte[] readBytes(DataInput in, byte type) throws IOException {
       int sz = 0;
       switch(type) {
       case(BinInterSedes.TINYBYTEARRAY): sz = in.readUnsignedByte(); break;
       case(BinInterSedes.SMALLBYTEARRAY): sz = in.readUnsignedShort(); break;
       case(BinInterSedes.BYTEARRAY): sz = in.readInt(); break;
       }
       byte[] buf = new byte[sz];
       in.readFully(buf);
       return buf;
    }

    public static void writeChararray(DataOutput out, String s) throws IOException {
        // a char can take up to 3 bytes in the modified utf8 encoding
        // used by DataOutput.writeUTF, so use UNSIGNED_SHORT_MAX/3
        if (s.length() < BinInterSedes.UNSIGNED_SHORT_MAX / 3) {
            out.writeByte(BinInterSedes.SMALLCHARARRAY);
            out.writeUTF(s);
        } else {
            byte[] utfBytes = s.getBytes(BinInterSedes.UTF8);
            int length = utfBytes.length;

            out.writeByte(BinInterSedes.CHARARRAY);
            out.writeInt(length);
            out.write(utfBytes);
        }
    }

    public static String readChararray(DataInput in, byte type) throws IOException {
        if (type == BinInterSedes.SMALLCHARARRAY) {
            return in.readUTF();
        }

        int size = in.readInt();
        byte[] buf = new byte[size];
        in.readFully(buf);
        return new String(buf, BinInterSedes.UTF8);
    }

    public static void writeGenericTuple(DataOutput out, Tuple t) throws IOException {
        int sz = t.size();
        switch (sz) {
        case 0:
            out.writeByte(BinInterSedes.TUPLE_0);
            break;
        case 1:
            out.writeByte(BinInterSedes.TUPLE_1);
            break;
        case 2:
            out.writeByte(BinInterSedes.TUPLE_2);
            break;
        case 3:
            out.writeByte(BinInterSedes.TUPLE_3);
            break;
        case 4:
            out.writeByte(BinInterSedes.TUPLE_4);
            break;
        case 5:
            out.writeByte(BinInterSedes.TUPLE_5);
            break;
        case 6:
            out.writeByte(BinInterSedes.TUPLE_6);
            break;
        case 7:
            out.writeByte(BinInterSedes.TUPLE_7);
            break;
        case 8:
            out.writeByte(BinInterSedes.TUPLE_8);
            break;
        case 9:
            out.writeByte(BinInterSedes.TUPLE_9);
            break;
        default:
        if (sz < BinInterSedes.UNSIGNED_BYTE_MAX) {
            out.writeByte(BinInterSedes.TINYTUPLE);
            out.writeByte(sz);
        } else if (sz < BinInterSedes.UNSIGNED_SHORT_MAX) {
            out.writeByte(BinInterSedes.SMALLTUPLE);
            out.writeShort(sz);
        } else {
            out.writeByte(BinInterSedes.TUPLE);
            out.writeInt(sz);
        }
        }

        for (int i = 0; i < sz; i++) {
            pigSerializer.writeDatum(out, t.get(i));
        }
    }

    public static Tuple readGenericTuple(DataInput in, byte type) throws IOException {
        int sz = pigSerializer.getTupleSize(in, type);

        Tuple t = mTupleFactory.newTuple(sz);
        for (int i = 0; i < sz; i++) {
            t.set(i, pigSerializer.readDatum(in));
        }
        return t;
    }

    public static void writeBooleanArray(DataOutput out, boolean[] v, boolean extra) throws IOException {
        int len = v.length + 1;
        for (int chunk = 0; chunk < len; chunk += 8) {
            byte encoding = 0;
            for (int i = chunk; i < len && i < chunk + 8; i++) {
                encoding <<= 1;
                if (i == v.length) {
                    encoding += extra ? 1 : 0; //v[len] is the extra piece
                } else {
                    encoding += v[i] ? 1 : 0;
                }
            }
            out.writeByte(encoding);
       }
    }

    public static void writeBooleanArray(DataOutput out, boolean[] v) throws IOException {
        for (int chunk = 0; chunk < v.length; chunk += 8) {
            byte encoding = 0;
            for (int i = chunk; i < v.length && i < chunk + 8; i++) {
                encoding <<= 1;
                encoding += v[i] ? 1 : 0;
            }
            out.writeByte(encoding);
       }
    }

    public static boolean[] readBooleanArray(DataInput in, int size) throws IOException {
        boolean[] v = new boolean[size];
        for (int chunk = 0; chunk < size; chunk += 8) {
            byte decoding = in.readByte();
            for (int i = chunk + Math.min(7, size - chunk - 1); i >= 0; i--) {
               v[i] = decoding % 2 == 1;
               decoding >>= 1;
            }
        }
        return v;
    }

    /**
     * <p>Encodes signed and unsigned values using a common variable-length
     * scheme, found for example in
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html">
     * Google's Protocol Buffers</a>. It uses fewer bytes to encode smaller values,
     * but will use slightly more bytes to encode large values.</p>
     *
     * <p>Signed values are further encoded using so-called zig-zag encoding
     * in order to make them "compatible" with variable-length encoding.</p>
     *
     * <p>This is taken from mahout-core, and is included to avoid having to pull
     * in the entirety of Mahout.</p>
     */
    public static class Varint {

        private Varint() {
        }

        /**
         * Encodes a value using the variable-length encoding from
         * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html">
         * Google Protocol Buffers</a>. It uses zig-zag encoding to efficiently
         * encode signed values. If values are known to be nonnegative,
         * {@link #writeUnsignedVarLong(long, DataOutput)} should be used.
         *
         * @param value value to encode
         * @param out to write bytes to
         * @throws IOException if {@link DataOutput} throws {@link IOException}
         */
        public static void writeSignedVarLong(long value, DataOutput out) throws IOException {
            // Great trick from http://code.google.com/apis/protocolbuffers/docs/encoding.html#types
            writeUnsignedVarLong((value << 1) ^ (value >> 63), out);
        }

        /**
         * Encodes a value using the variable-length encoding from
         * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html">
         * Google Protocol Buffers</a>. Zig-zag is not used, so input must not be negative.
         * If values can be negative, use {@link #writeSignedVarLong(long, DataOutput)}
         * instead. This method treats negative input as like a large unsigned value.
         *
         * @param value value to encode
         * @param out to write bytes to
         * @throws IOException if {@link DataOutput} throws {@link IOException}
         */
        public static void writeUnsignedVarLong(long value, DataOutput out) throws IOException {
            while ((value & 0xFFFFFFFFFFFFFF80L) != 0L) {
                out.writeByte(((int) value & 0x7F) | 0x80);
                value >>>= 7;
            }
            out.writeByte((int) value & 0x7F);
        }

        /**
         * @see #writeSignedVarLong(long, DataOutput)
         */
        public static void writeSignedVarInt(int value, DataOutput out) throws IOException {
            // Great trick from http://code.google.com/apis/protocolbuffers/docs/encoding.html#types
            writeUnsignedVarInt((value << 1) ^ (value >> 31), out);
        }

        /**
         * @see #writeUnsignedVarLong(long, DataOutput)
         */
        public static void writeUnsignedVarInt(int value, DataOutput out) throws IOException {
            while ((value & 0xFFFFFF80) != 0L) {
                out.writeByte((value & 0x7F) | 0x80);
                value >>>= 7;
            }
            out.writeByte(value & 0x7F);
        }

        /**
         * @param in to read bytes from
         * @return decode value
         * @throws IOException if {@link DataInput} throws {@link IOException}
         * @throws IllegalArgumentException if variable-length value does not terminate
         *  after 9 bytes have been read
         * @see #writeSignedVarLong(long, DataOutput)
         */
        public static long readSignedVarLong(DataInput in) throws IOException {
            long raw = readUnsignedVarLong(in);
            // This undoes the trick in writeSignedVarLong()
            long temp = (((raw << 63) >> 63) ^ raw) >> 1;
            // This extra step lets us deal with the largest signed values by treating
            // negative results from read unsigned methods as like unsigned values
            // Must re-flip the top bit if the original read value had it set.
            return temp ^ (raw & (1L << 63));
        }

        /**
         * @param in to read bytes from
         * @return decode value
         * @throws IOException if {@link DataInput} throws {@link IOException}
         * @throws IllegalArgumentException if variable-length value does not terminate
         *  after 9 bytes have been read
         * @see #writeUnsignedVarLong(long, DataOutput)
         */
        public static long readUnsignedVarLong(DataInput in) throws IOException {
            long value = 0L;
            int i = 0;
            long b;
            while (((b = in.readByte()) & 0x80L) != 0) {
                value |= (b & 0x7F) << i;
                i += 7;
                if (i > 63) {
                    throw new RuntimeException("Variable length quantity is too long");
                }
            }
            return value | (b << i);
        }

        /**
         * @throws IllegalArgumentException if variable-length value does not terminate
         *  after 5 bytes have been read
         * @throws IOException if {@link DataInput} throws {@link IOException}
         * @see #readSignedVarLong(DataInput)
         */
        public static int readSignedVarInt(DataInput in) throws IOException {
            int raw = readUnsignedVarInt(in);
            // This undoes the trick in writeSignedVarInt()
            int temp = (((raw << 31) >> 31) ^ raw) >> 1;
            // This extra step lets us deal with the largest signed values by treating
            // negative results from read unsigned methods as like unsigned values.
            // Must re-flip the top bit if the original read value had it set.
            return temp ^ (raw & (1 << 31));
        }

        /**
         * @throws IllegalArgumentException if variable-length value does not terminate
         *  after 5 bytes have been read
         * @throws IOException if {@link DataInput} throws {@link IOException}
         * @see #readUnsignedVarLong(DataInput)
         */
        public static int readUnsignedVarInt(DataInput in) throws IOException {
            int value = 0;
            int i = 0;
            int b;
            while (((b = in.readByte()) & 0x80) != 0) {
                value |= (b & 0x7F) << i;
                i += 7;
                if (i > 35) {
                    throw new RuntimeException("Variable length quantity is too long");
                }
            }
            return value | (b << i);
        }
    }
}
