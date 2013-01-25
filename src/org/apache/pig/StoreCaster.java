/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.pig;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;

import org.joda.time.DateTime;

import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

/**
 * An interface that provides methods for converting Pig internal types to byte[].
 * It is intended to be used by StoreFunc implementations.
 * @since Pig 0.8
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving // Because we still don't have the map casts quite right
public interface StoreCaster extends LoadCaster {
    public byte[] toBytes(DataBag bag) throws IOException;

    public byte[] toBytes(String s) throws IOException;

    public byte[] toBytes(Double d) throws IOException;

    public byte[] toBytes(Float f) throws IOException;

    public byte[] toBytes(Integer i) throws IOException;

    public byte[] toBytes(Boolean b) throws IOException;

    public byte[] toBytes(Long l) throws IOException;

    public byte[] toBytes(DateTime dt) throws IOException;

    public byte[] toBytes(Map<String, Object> m) throws IOException;

    public byte[] toBytes(Tuple t) throws IOException;

    public byte[] toBytes(DataByteArray a) throws IOException;

    public byte[] toBytes(BigInteger bi) throws IOException;

    public byte[] toBytes(BigDecimal bd) throws IOException;
}
