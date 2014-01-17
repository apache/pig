/* Licensed to the Apache Software Foundation (ASF) under one
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

package org.apache.pig.backend.hadoop.accumulo;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;

import org.apache.accumulo.core.Constants;
import org.apache.pig.LoadStoreCaster;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.joda.time.DateTime;
import org.python.google.common.base.Preconditions;

/**
 * A LoadStoreCaster implementation which stores most type implementations as
 * bytes generated from the toString representation with a UTF8 Charset. Pulled
 * some implementations from the Accumulo Lexicoder implementations in 1.6.0.
 */
public class AccumuloBinaryConverter implements LoadStoreCaster {
    private static final int SIZE_OF_INT = Integer.SIZE / Byte.SIZE;
    private static final int SIZE_OF_LONG = Long.SIZE / Byte.SIZE;

    /**
     * NOT IMPLEMENTED
     */
    @Override
    public DataBag bytesToBag(byte[] b, ResourceFieldSchema fieldSchema)
            throws IOException {
        throw new ExecException("Can't generate DataBags from byte[]");
    }

    @Override
    public BigDecimal bytesToBigDecimal(byte[] b) throws IOException {
        throw new ExecException("Can't generate a BigInteger from byte[]");
    }

    @Override
    public BigInteger bytesToBigInteger(byte[] b) throws IOException {
        // Taken from Accumulo's BigIntegerLexicoder in 1.6.0
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(b));
        int len = dis.readInt();
        len = len ^ 0x80000000;
        len = Math.abs(len);

        byte[] bytes = new byte[len];
        dis.readFully(bytes);

        bytes[0] = (byte) (0x80 ^ bytes[0]);

        return new BigInteger(bytes);
    }

    @Override
    public Boolean bytesToBoolean(byte[] b) throws IOException {
        Preconditions.checkArgument(1 == b.length);
        return b[0] == (byte) 1;
    }

    @Override
    public String bytesToCharArray(byte[] b) throws IOException {
        return new String(b, Constants.UTF8);
    }

    /**
     * NOT IMPLEMENTED
     */
    @Override
    public DateTime bytesToDateTime(byte[] b) throws IOException {
        String s = new String(b, Constants.UTF8);
        return DateTime.parse(s);
    }

    @Override
    public Double bytesToDouble(byte[] b) throws IOException {
        return Double.longBitsToDouble(bytesToLong(b));
    }

    @Override
    public Float bytesToFloat(byte[] b) throws IOException {
        return Float.intBitsToFloat(bytesToInteger(b));
    }

    @Override
    public Integer bytesToInteger(byte[] b) throws IOException {
        Preconditions.checkArgument(b.length == SIZE_OF_INT);
        int n = 0;
        for (int i = 0; i < b.length; i++) {
            n <<= 8;
            n ^= b[i] & 0xFF;
        }
        return n;
    }

    @Override
    public Long bytesToLong(byte[] b) throws IOException {
        Preconditions.checkArgument(b.length == SIZE_OF_LONG);
        long l = 0;
        for (int i = 0; i < b.length; i++) {
            l <<= 8;
            l ^= b[i] & 0xFF;
        }
        return l;
    }

    /**
     * NOT IMPLEMENTED
     */
    @Override
    public Map<String, Object> bytesToMap(byte[] b,
            ResourceFieldSchema fieldSchema) throws IOException {
        throw new ExecException("Can't generate Map from byte[]");
    }

    /**
     * NOT IMPLEMENTED
     */
    @Override
    public Tuple bytesToTuple(byte[] b, ResourceFieldSchema fieldSchema)
            throws IOException {
        throw new ExecException("Can't generate a Tuple from byte[]");
    }

    /**
     * Not implemented!
     */
    @Override
    public byte[] toBytes(BigDecimal bd) throws IOException {
        throw new IOException("Can't generate bytes from BigDecimal");
    }

    @Override
    public byte[] toBytes(BigInteger bi) throws IOException {
        // Taken from Accumulo's BigIntegerLexicoder in 1.6.0
        byte[] bytes = bi.toByteArray();

        byte[] ret = new byte[4 + bytes.length];

        DataOutputStream dos = new DataOutputStream(
                new FixedByteArrayOutputStream(ret));

        // flip the sign bit
        bytes[0] = (byte) (0x80 ^ bytes[0]);

        int len = bytes.length;
        if (bi.signum() < 0)
            len = -len;

        len = len ^ 0x80000000;

        dos.writeInt(len);
        dos.write(bytes);
        dos.close();

        return ret;
    }

    @Override
    public byte[] toBytes(Boolean b) throws IOException {
        return new byte[] { b ? (byte) 1 : (byte) 0 };
    }

    /**
     * NOT IMPLEMENTED
     */
    @Override
    public byte[] toBytes(DataBag bag) throws IOException {
        throw new ExecException("Cant' generate bytes from DataBag");
    }

    @Override
    public byte[] toBytes(DataByteArray a) throws IOException {
        return a.get();
    }

    @Override
    public byte[] toBytes(DateTime dt) throws IOException {
        return dt.toString().getBytes(Constants.UTF8);
    }

    @Override
    public byte[] toBytes(Double d) throws IOException {
        return toBytes(Double.doubleToRawLongBits(d));
    }

    @Override
    public byte[] toBytes(Float f) throws IOException {
        return toBytes(Float.floatToRawIntBits(f));
    }

    @Override
    public byte[] toBytes(Integer val) throws IOException {
        int intVal = val.intValue();
        byte[] b = new byte[4];
        for (int i = 3; i > 0; i--) {
            b[i] = (byte) intVal;
            intVal >>>= 8;
        }
        b[0] = (byte) intVal;
        return b;
    }

    @Override
    public byte[] toBytes(Long val) throws IOException {
        long longVal = val.longValue();
        byte[] b = new byte[8];
        for (int i = 7; i > 0; i--) {
            b[i] = (byte) longVal;
            longVal >>>= 8;
        }
        b[0] = (byte) longVal;
        return b;
    }

    /**
     * NOT IMPLEMENTED
     */
    @Override
    public byte[] toBytes(Map<String, Object> m) throws IOException {
        throw new IOException("Can't generate bytes from Map");
    }

    @Override
    public byte[] toBytes(String s) throws IOException {
        return s.getBytes(Constants.UTF8);
    }

    /**
     * NOT IMPLEMENTED
     */
    @Override
    public byte[] toBytes(Tuple t) throws IOException {
        throw new IOException("Can't generate bytes from Tuple");
    }
}
