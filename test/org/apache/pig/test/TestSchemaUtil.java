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

package org.apache.pig.test;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.SchemaUtil;
import org.junit.Test;

public class TestSchemaUtil {

    @Test
    public void testTupleSchema() throws Exception {
        String tupleName = "mytuple";
        String[] fieldNames = new String[] { "field_0", "field_1" };
        Byte[] dataTypes = new Byte[] { DataType.LONG, DataType.CHARARRAY };

        String expected = "{mytuple: (field_0: long,field_1: chararray)}";
        Schema tupleSchema = SchemaUtil.newTupleSchema(tupleName,
                fieldNames, dataTypes);
        assertEquals(expected, tupleSchema.toString());

        tupleSchema = SchemaUtil.newTupleSchema(tupleName, Arrays
                .asList(fieldNames), Arrays.asList(dataTypes));
        assertEquals(expected, tupleSchema.toString());

        expected = "{t: (field_0: long,field_1: chararray)}";
        tupleSchema = SchemaUtil.newTupleSchema(fieldNames, dataTypes);
        assertEquals(expected, tupleSchema.toString());

        tupleSchema = SchemaUtil.newTupleSchema(Arrays.asList(fieldNames),
                Arrays.asList(dataTypes));
        assertEquals(expected, tupleSchema.toString());

        expected = "{t: (f0: long,f1: chararray)}";
        tupleSchema = SchemaUtil.newTupleSchema(dataTypes);
        assertEquals(expected, tupleSchema.toString());

        tupleSchema = SchemaUtil.newTupleSchema(Arrays.asList(dataTypes));
        assertEquals(expected, tupleSchema.toString());
    }

    @Test
    public void testBagSchema() throws Exception {
        String bagName="mybag";
        String tupleName = "mytuple";
        String[] fieldNames = new String[] { "field_0", "field_1" };
        Byte[] dataTypes = new Byte[] { DataType.LONG, DataType.CHARARRAY };

        String expected = "{mybag: {mytuple: (field_0: long,field_1: chararray)}}";
        Schema bagSchema = SchemaUtil.newBagSchema(bagName,tupleName,
                fieldNames, dataTypes);
        assertEquals(expected, bagSchema.toString());

        bagSchema = SchemaUtil.newBagSchema(bagName,tupleName, Arrays
                .asList(fieldNames), Arrays.asList(dataTypes));
        assertEquals(expected, bagSchema.toString());

        expected = "{b: {t: (field_0: long,field_1: chararray)}}";
        bagSchema = SchemaUtil.newBagSchema(fieldNames, dataTypes);
        assertEquals(expected, bagSchema.toString());

        bagSchema = SchemaUtil.newBagSchema(Arrays.asList(fieldNames),
                Arrays.asList(dataTypes));
        assertEquals(expected, bagSchema.toString());

        expected = "{b: {t: (f0: long,f1: chararray)}}";
        bagSchema = SchemaUtil.newBagSchema(dataTypes);
        assertEquals(expected, bagSchema.toString());

        bagSchema = SchemaUtil.newBagSchema(Arrays.asList(dataTypes));
        assertEquals(expected, bagSchema.toString());
    }
}
