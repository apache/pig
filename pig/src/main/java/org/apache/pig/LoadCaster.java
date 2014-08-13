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
package org.apache.pig;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;

import org.joda.time.DateTime;

import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

/**
 * An interface that provides cast implementations for load functions.  For casts between
 * bytearray objects and internal types, Pig relies on the load function that loaded the data to
 * provide the cast.  This is because Pig does not understand the binary representation of the
 * data and thus cannot cast it.  This interface provides functions to cast from bytearray to each
 * of Pig's internal types.
 * @since Pig 0.7
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving // Because we still don't have the map casts quite right
public interface LoadCaster {

    /**
     * Cast data from bytearray to boolean value.
     * @param b bytearray to be cast.
     * @return Boolean value.
     * @throws IOException if the value cannot be cast.
     */
    public Boolean bytesToBoolean(byte[] b) throws IOException;

    /**
     * Cast data from bytearray to long value.
     * @param b bytearray to be cast.
     * @return Long value.
     * @throws IOException if the value cannot be cast.
     */
    public Long bytesToLong(byte[] b) throws IOException;

    /**
     * Cast data from bytearray to float value.
     * @param b bytearray to be cast.
     * @return Float value.
     * @throws IOException if the value cannot be cast.
     */
    public Float bytesToFloat(byte[] b) throws IOException;

    /**
     * Cast data from bytearray to double value.
     * @param b bytearray to be cast.
     * @return Double value.
     * @throws IOException if the value cannot be cast.
     */
    public Double bytesToDouble(byte[] b) throws IOException;

    /**
     * Cast data from bytearray to datetime value.
     * @param b bytearray to be cast.
     * @return datetime value.
     * @throws IOException if the value cannot be cast.
     */
    public DateTime bytesToDateTime(byte[] b) throws IOException;

    /**
     * Cast data from bytearray to integer value.
     * @param b bytearray to be cast.
     * @return Double value.
     * @throws IOException if the value cannot be cast.
     */
    public Integer bytesToInteger(byte[] b) throws IOException;

    /**
     * Cast data from bytearray to chararray value.
     * @param b bytearray to be cast.
     * @return String value.
     * @throws IOException if the value cannot be cast.
     */
    public String bytesToCharArray(byte[] b) throws IOException;

    /**
     * Cast data from bytearray to map value.
     * @param b bytearray to be cast.
     * @param fieldSchema field schema for the output map
     * @return Map value.
     * @throws IOException if the value cannot be cast.
     */
    public Map<String, Object> bytesToMap(byte[] b, ResourceFieldSchema fieldSchema) throws IOException;

    /**
     * Cast data from bytearray to tuple value.
     * @param b bytearray to be cast.
     * @param fieldSchema field schema for the output tuple
     * @return Tuple value.
     * @throws IOException if the value cannot be cast.
     */
    public Tuple bytesToTuple(byte[] b, ResourceFieldSchema fieldSchema) throws IOException;

    /**
     * Cast data from bytearray to bag value.
     * @param b bytearray to be cast.
     * @param fieldSchema field schema for the output bag
     * @return Bag value.
     * @throws IOException if the value cannot be cast.
     */
    public DataBag bytesToBag(byte[] b, ResourceFieldSchema fieldSchema) throws IOException;

    /**
     * Cast data from bytearray to BigInteger value.
     * @param b bytearray to be cast.
     * @param fieldSchema field schema for the output bag
     * @return BigInteger value.
     * @throws IOException if the value cannot be cast.
     */
    public BigInteger bytesToBigInteger(byte[] b) throws IOException;

    /**
     * Cast data from bytearray to BigDecimal value.
     * @param b bytearray to be cast.
     * @param fieldSchema field schema for the output bag
     * @return BigInteger value.
     * @throws IOException if the value cannot be cast.
     */
    public BigDecimal bytesToBigDecimal(byte[] b) throws IOException;

}
