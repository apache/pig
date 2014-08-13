/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.io.IOException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestAccumuloBinaryConverter {

    AccumuloBinaryConverter converter;

    @Before
    public void setup() {
        converter = new AccumuloBinaryConverter();
    }

    @Test
    public void testInts() throws IOException {
        byte[] b = converter.toBytes(Integer.MAX_VALUE);
        Assert.assertEquals(Integer.MAX_VALUE, converter.bytesToInteger(b)
                .intValue());
    }

    @Test
    public void testDoubles() throws IOException {
        byte[] b = converter.toBytes(Double.MAX_VALUE);
        Assert.assertEquals(Double.MAX_VALUE, converter.bytesToDouble(b)
                .doubleValue(), 0);
    }

    @Test
    public void testLongs() throws IOException {
        byte[] b = converter.toBytes(Long.MAX_VALUE);
        Assert.assertEquals(Long.MAX_VALUE, converter.bytesToLong(b)
                .longValue());
    }

    @Test
    public void testFloats() throws IOException {
        byte[] b = converter.toBytes(Float.MAX_VALUE);
        Assert.assertEquals(Float.MAX_VALUE, converter.bytesToFloat(b)
                .floatValue(), 0f);
    }

    @Test
    public void testBoolean() throws IOException {
        Assert.assertTrue(converter.bytesToBoolean(converter.toBytes(true)));
        Assert.assertFalse(converter.bytesToBoolean(converter.toBytes(false)));
    }

}
