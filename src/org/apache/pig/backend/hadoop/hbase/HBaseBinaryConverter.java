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

package org.apache.pig.backend.hadoop.hbase;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.pig.LoadStoreCaster;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.joda.time.DateTime;

public class HBaseBinaryConverter implements LoadStoreCaster {

    @Override
    public String bytesToCharArray(byte[] b) throws IOException {
        return Bytes.toString(b);
    }

    @Override
    public Double bytesToDouble(byte[] b) throws IOException {
        if (Bytes.SIZEOF_DOUBLE > b.length){
            return Bytes.toDouble(Bytes.padHead(b, Bytes.SIZEOF_DOUBLE - b.length));
        } else {
            return Bytes.toDouble(Bytes.head(b, Bytes.SIZEOF_DOUBLE));
        }
    }

    @Override
    public Float bytesToFloat(byte[] b) throws IOException {
        if (Bytes.SIZEOF_FLOAT > b.length){
            return Bytes.toFloat(Bytes.padHead(b, Bytes.SIZEOF_FLOAT - b.length));
        } else {
            return Bytes.toFloat(Bytes.head(b, Bytes.SIZEOF_FLOAT));
        }
    }

    @Override
    public Integer bytesToInteger(byte[] b) throws IOException {
        if (Bytes.SIZEOF_INT > b.length){
            return Bytes.toInt(Bytes.padHead(b, Bytes.SIZEOF_INT - b.length));
        } else {
            return Bytes.toInt(Bytes.head(b, Bytes.SIZEOF_INT));
        }
    }

    @Override
    public Long bytesToLong(byte[] b) throws IOException {
        if (Bytes.SIZEOF_LONG > b.length){
            return Bytes.toLong(Bytes.padHead(b, Bytes.SIZEOF_LONG - b.length));
        } else {
            return Bytes.toLong(Bytes.head(b, Bytes.SIZEOF_LONG));
        }
    }

    @Override
    public Boolean bytesToBoolean(byte[] b) throws IOException {
        if (Bytes.SIZEOF_BOOLEAN > b.length) {
            return Bytes.toBoolean(Bytes.padHead(b, Bytes.SIZEOF_BOOLEAN - b.length));
        } else {
            return Bytes.toBoolean(Bytes.head(b, Bytes.SIZEOF_BOOLEAN));
        }
    }

    /**
     * NOT IMPLEMENTED
     */
    @Override
    public DateTime bytesToDateTime(byte[] b) throws IOException {
        throw new ExecException("Can't generate a DateTime from byte[]");
    }

    @Override
    public Map<String, Object> bytesToMap(byte[] b, ResourceFieldSchema fieldSchema) throws IOException {
        return bytesToMap(b, fieldSchema);
    }

    /**
     * NOT IMPLEMENTED
     */
    @Override
    public Tuple bytesToTuple(byte[] b, ResourceFieldSchema fieldSchema) throws IOException {
        throw new ExecException("Can't generate a Tuple from byte[]");
    }

    /**
     * NOT IMPLEMENTED
     */
    @Override
    public DataBag bytesToBag(byte[] b, ResourceFieldSchema fieldSchema) throws IOException {
        throw new ExecException("Can't generate DataBags from byte[]");
    }

    /**
     * NOT IMPLEMENTED
     */
    @Override
    public byte[] toBytes(DataBag bag) throws IOException {
        throw new ExecException("Cant' generate bytes from DataBag");
    }

    @Override
    public byte[] toBytes(String s) throws IOException {
        return Bytes.toBytes(s);
    }

    @Override
    public byte[] toBytes(Double d) throws IOException {
        return Bytes.toBytes(d);
    }

    @Override
    public byte[] toBytes(Float f) throws IOException {
        return Bytes.toBytes(f);
    }

    @Override
    public byte[] toBytes(Integer i) throws IOException {
        return Bytes.toBytes(i);
    }

    @Override
    public byte[] toBytes(Long l) throws IOException {
        return Bytes.toBytes(l);
    }

    @Override
    public byte[] toBytes(Boolean b) throws IOException {
        return Bytes.toBytes(b);
    }

    /**
     * NOT IMPLEMENTED
     */
    @Override
    public byte[] toBytes(DateTime dt) throws IOException {
        throw new IOException("Can't generate bytes from DateTime");
    }

    /**
     * NOT IMPLEMENTED
     */
    @Override
    public byte[] toBytes(Map<String, Object> m) throws IOException {
        throw new IOException("Can't generate bytes from Map");
    }

    /**
     * NOT IMPLEMENTED
     */
    @Override
    public byte[] toBytes(Tuple t) throws IOException {
       throw new IOException("Can't generate bytes from Tuple");
    }

    @Override
    public byte[] toBytes(DataByteArray a) throws IOException {
        return a.get();
    }

    /**
     * Not implemented!
     */
    @Override
    public BigInteger bytesToBigInteger(byte[] b) throws IOException {
        throw new ExecException("Can't generate a BigInteger from byte[]");
    }

    /**
     * Not implemented!
     */
    @Override
    public BigDecimal bytesToBigDecimal(byte[] b) throws IOException {
        throw new ExecException("Can't generate a BigInteger from byte[]");
    }

    /**
     * Not implemented!
     */
    @Override
    public byte[] toBytes(BigInteger bi) throws IOException {
        throw new IOException("Can't generate bytes from BigInteger");
    }

    /**
     * Not implemented!
     */
    @Override
    public byte[] toBytes(BigDecimal bd) throws IOException {
        throw new IOException("Can't generate bytes from BigDecimal");
    }
}
