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
package org.apache.pig.builtin;

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.util.Utils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

public class TestUtf8StorageConverter {

    @Test
    /* Test that the simple data types convert properly in a tuple context */
    public void testSimpleTypes() throws Exception {
        Utf8StorageConverter converter = new Utf8StorageConverter();
        String schemaString = "a:int, b:long, c:float, d:double, e:chararray, f:bytearray, g:boolean, h:biginteger, i:bigdecimal, j:datetime";
        String dataString = "(1,2,3.0,4.0,five,6,true,12345678901234567890,1234567890.0987654321,2007-04-05T14:30Z)";

        ResourceSchema.ResourceFieldSchema rfs = new ResourceFieldSchema(new FieldSchema("schema", Utils.getSchemaFromString(schemaString)));
        Tuple result = converter.bytesToTuple(dataString.getBytes(), rfs);
        assertEquals(10, result.size());
        assertEquals(new Integer(1), result.get(0));
        assertEquals(new Long(2L), result.get(1));
        assertEquals(new Float(3.0f), result.get(2));
        assertEquals(new Double(4.0), result.get(3));
        assertEquals("five", result.get(4));
        assertEquals(new DataByteArray(new byte[] { (byte) '6' }), result.get(5));
        assertEquals(new Boolean(true), result.get(6));
        assertEquals(new BigInteger("12345678901234567890"), result.get(7));
        assertEquals(new BigDecimal("1234567890.0987654321"), result.get(8));
        assertEquals(new DateTime("2007-04-05T14:30Z", DateTimeZone.UTC), result.get(9));
    }
}
