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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.apache.pig.PigConfiguration;
import org.apache.pig.PigServer;
import org.apache.pig.ResourceSchema;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.logicalLayer.schema.SchemaMergeException;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.newplan.logical.relational.LogicalSchema.MergeMode;
import org.apache.pig.parser.ParserException;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestSchema {

    private static MiniGenericCluster cluster;
    private static PigServer pigServer;

    @BeforeClass
    public static void setupTestCluster() throws Exception {
        cluster = MiniGenericCluster.buildCluster();
        pigServer = new PigServer(cluster.getExecType(), cluster.getProperties());
    }

    @AfterClass
    public static void tearDownTestCluster() throws Exception {
        cluster.shutDown();
    }

    @Test
    public void testSchemaEqual1() {

        List<FieldSchema> innerList1 = new ArrayList<FieldSchema>();
        innerList1.add(new FieldSchema("11a", DataType.INTEGER));
        innerList1.add(new FieldSchema("11b", DataType.LONG));

        List<FieldSchema> innerList2 = new ArrayList<FieldSchema>();
        innerList2.add(new FieldSchema("11a", DataType.INTEGER));
        innerList2.add(new FieldSchema("11b", DataType.LONG));

        Schema innerSchema1 = new Schema(innerList1);
        Schema innerSchema2 = new Schema(innerList2);

        List<FieldSchema> list1 = new ArrayList<FieldSchema>();
        list1.add(new FieldSchema("1a", DataType.BYTEARRAY));
        list1.add(new FieldSchema("1b", innerSchema1));
        list1.add(new FieldSchema("1c", DataType.INTEGER));

        List<FieldSchema> list2 = new ArrayList<FieldSchema>();
        list2.add(new FieldSchema("1a", DataType.BYTEARRAY));
        list2.add(new FieldSchema("1b", innerSchema2));
        list2.add(new FieldSchema("1c", DataType.INTEGER));

        Schema schema1 = new Schema(list1);
        Schema schema2 = new Schema(list2);

        assertTrue(Schema.equals(schema1, schema2, false, false));

        innerList2.get(1).alias = "pi";

        assertFalse(Schema.equals(schema1, schema2, false, false));
        assertTrue(Schema.equals(schema1, schema2, false, true));

        innerList2.get(1).alias = "11b";
        innerList2.get(1).type = DataType.BYTEARRAY;

        assertFalse(Schema.equals(schema1, schema2, false, false));
        assertTrue(Schema.equals(schema1, schema2, true, false));

        innerList2.get(1).type = DataType.LONG;

        assertTrue(Schema.equals(schema1, schema2, false, false));

        list2.get(0).type = DataType.CHARARRAY;
        assertFalse(Schema.equals(schema1, schema2, false, false));
    }

    // Positive test
    @Test
    public void testSchemaEqualWithNullSchema1() {

        List<FieldSchema> list1 = new ArrayList<FieldSchema>();
        list1.add(new FieldSchema("1a", DataType.BYTEARRAY));
        list1.add(new FieldSchema("1b", null));
        list1.add(new FieldSchema("1c", DataType.INTEGER));

        List<FieldSchema> list2 = new ArrayList<FieldSchema>();
        list2.add(new FieldSchema("1a", DataType.BYTEARRAY));
        list2.add(new FieldSchema("1b", null));
        list2.add(new FieldSchema("1c", DataType.INTEGER));

        Schema schema1 = new Schema(list1);
        Schema schema2 = new Schema(list2);

        // First check
        assertTrue(Schema.equals(schema1, schema2, false, false));

        // Manipulate
        List<FieldSchema> dummyList = new ArrayList<FieldSchema>();
        Schema dummySchema = new Schema(dummyList);
        list2.get(1).schema = dummySchema;

        // And check again
        assertFalse(Schema.equals(schema1, schema2, false, false));
    }

    @Test
    public void testParsingMapSchemasFromString() throws ParserException {
        assertNotNull(Utils.getSchemaFromString("b:[(a:int)]"));
        assertNotNull(Utils.getSchemaFromString("b:[someAlias: (b:int)]"));
        assertNotNull(Utils.getSchemaFromString("a:map[{bag: (a:int)}]"));
        assertNotNull(Utils.getSchemaFromString("a:map[someAlias: {bag: (a:int)}]"));
        assertNotNull(Utils.getSchemaFromString("a:map[chararray]"));
        assertNotNull(Utils.getSchemaFromString("a:map[someAlias: chararray]"));
        assertNotNull(Utils.getSchemaFromString("a:map[someAlias: (bar: {bag: (a:int)})]"));
    }

    @Test
    public void testMapWithoutAlias() throws FrontendException {
        List<FieldSchema> innerFields = new ArrayList<FieldSchema>();
        innerFields.add(new FieldSchema(null, DataType.LONG));
        List<FieldSchema> fields = new ArrayList<FieldSchema>();
        fields.add(new FieldSchema("mapAlias", new Schema(innerFields), DataType.MAP));

        Schema inputSchema = new Schema(fields);
        Schema fromString = Utils.getSchemaFromBagSchemaString(inputSchema.toString());
        assertTrue(Schema.equals(inputSchema, fromString, false, false));
    }

    @Test
    public void testMapWithAlias() throws FrontendException {
        List<FieldSchema> innerFields = new ArrayList<FieldSchema>();
        innerFields.add(new FieldSchema("valueAlias", DataType.LONG));
        List<FieldSchema> fields = new ArrayList<FieldSchema>();
        fields.add(new FieldSchema("mapAlias", new Schema(innerFields), DataType.MAP));

        Schema inputSchema = new Schema(fields);
        Schema fromString = Utils.getSchemaFromBagSchemaString(inputSchema.toString());
        assertTrue(Schema.equals(inputSchema, fromString, false, false));
    }

    @Test
    public void testNormalNestedMerge1() {

        // Generate two schemas
        List<FieldSchema> innerList1 = new ArrayList<FieldSchema>();
        innerList1.add(new FieldSchema("11a", DataType.INTEGER));
        innerList1.add(new FieldSchema("11b", DataType.FLOAT));

        List<FieldSchema> innerList2 = new ArrayList<FieldSchema>();
        innerList2.add(new FieldSchema("22a", DataType.DOUBLE));
        innerList2.add(new FieldSchema(null, DataType.LONG));

        Schema innerSchema1 = new Schema(innerList1);
        Schema innerSchema2 = new Schema(innerList2);

        List<FieldSchema> list1 = new ArrayList<FieldSchema>();
        list1.add(new FieldSchema("1a", DataType.BYTEARRAY));
        list1.add(new FieldSchema("1b", innerSchema1));
        list1.add(new FieldSchema("1c", DataType.LONG));

        List<FieldSchema> list2 = new ArrayList<FieldSchema>();
        list2.add(new FieldSchema("2a", DataType.BYTEARRAY));
        list2.add(new FieldSchema("2b", innerSchema2));
        list2.add(new FieldSchema("2c", DataType.INTEGER));

        Schema schema1 = new Schema(list1);
        Schema schema2 = new Schema(list2);

        // Merge
        Schema mergedSchema = schema1.merge(schema2, true);


        // Generate expected schema
        List<FieldSchema> expectedInnerList = new ArrayList<FieldSchema>();
        expectedInnerList.add(new FieldSchema("22a", DataType.DOUBLE));
        expectedInnerList.add(new FieldSchema("11b", DataType.FLOAT));

        Schema expectedInner = new Schema(expectedInnerList);

        List<FieldSchema> expectedList = new ArrayList<FieldSchema>();
        expectedList.add(new FieldSchema("2a", DataType.BYTEARRAY));
        expectedList.add(new FieldSchema("2b", expectedInner));
        expectedList.add(new FieldSchema("2c", DataType.LONG));

        Schema expected = new Schema(expectedList);

        // Compare
        assertTrue(Schema.equals(mergedSchema, expected, false, false));
    }

    @Test
    public void testMergeNullSchemas1() throws Throwable {

        List<FieldSchema> innerList2 = new ArrayList<FieldSchema>();
        innerList2.add(new FieldSchema("22a", DataType.DOUBLE));
        innerList2.add(new FieldSchema(null, DataType.LONG));

        Schema innerSchema2 = new Schema(innerList2);

        List<FieldSchema> list1 = new ArrayList<FieldSchema>();
        list1.add(new FieldSchema("1a", DataType.BYTEARRAY));
        list1.add(new FieldSchema("1b", null));
        list1.add(new FieldSchema("1c", DataType.LONG));

        List<FieldSchema> list2 = new ArrayList<FieldSchema>();
        list2.add(new FieldSchema("2a", DataType.BYTEARRAY));
        list2.add(new FieldSchema("2b", innerSchema2));
        list2.add(new FieldSchema("2c", DataType.INTEGER));

        Schema schema1 = new Schema(list1);
        Schema schema2 = new Schema(list2);

        // Merge
        Schema mergedSchema = Schema.mergeSchema(schema1,
                                                 schema2,
                                                 true,
                                                 false,
                                                 true);


        // Generate expected schema

        List<FieldSchema> expectedList = new ArrayList<FieldSchema>();
        expectedList.add(new FieldSchema("2a", DataType.BYTEARRAY));
        expectedList.add(new FieldSchema("2b", null));
        expectedList.add(new FieldSchema("2c", DataType.LONG));

        Schema expected = new Schema(expectedList);

        // Compare
        assertTrue(Schema.equals(mergedSchema, expected, false, false));
    }

    @Test
    public void testMergeNullSchemas2() throws Throwable {
        // Inner of inner schema
        Schema innerInner = new Schema(new ArrayList<FieldSchema>());

        // Generate two schemas
        List<FieldSchema> innerList1 = new ArrayList<FieldSchema>();
        innerList1.add(new FieldSchema("11a", innerInner));
        innerList1.add(new FieldSchema("11b", DataType.FLOAT));

        List<FieldSchema> innerList2 = new ArrayList<FieldSchema>();
        innerList2.add(new FieldSchema("22a", null));
        innerList2.add(new FieldSchema(null, DataType.LONG));

        Schema innerSchema1 = new Schema(innerList1);
        Schema innerSchema2 = new Schema(innerList2);

        List<FieldSchema> list1 = new ArrayList<FieldSchema>();
        list1.add(new FieldSchema("1a", DataType.BYTEARRAY));
        list1.add(new FieldSchema("1b", innerSchema1));
        list1.add(new FieldSchema("1c", DataType.LONG));

        List<FieldSchema> list2 = new ArrayList<FieldSchema>();
        list2.add(new FieldSchema("2a", DataType.BYTEARRAY));
        list2.add(new FieldSchema("2b", innerSchema2));
        list2.add(new FieldSchema("2c", DataType.INTEGER));

        Schema schema1 = new Schema(list1);
        Schema schema2 = new Schema(list2);

        // Merge
        Schema mergedSchema = Schema.mergeSchema(schema1,
                                                 schema2,
                                                 true,
                                                 false,
                                                 true);


        // Generate expected schema
        List<FieldSchema> expectedInnerList = new ArrayList<FieldSchema>();
        expectedInnerList.add(new FieldSchema("22a", null));
        expectedInnerList.add(new FieldSchema("11b", DataType.FLOAT));

        Schema expectedInner = new Schema(expectedInnerList);

        List<FieldSchema> expectedList = new ArrayList<FieldSchema>();
        expectedList.add(new FieldSchema("2a", DataType.BYTEARRAY));
        expectedList.add(new FieldSchema("2b", expectedInner));
        expectedList.add(new FieldSchema("2c", DataType.LONG));

        Schema expected = new Schema(expectedList);

        // Compare
        assertTrue(Schema.equals(mergedSchema, expected, false, false));
    }

    @Test
    public void testMergeDifferentSize1() throws Throwable {

        // Generate two schemas
        List<FieldSchema> innerList1 = new ArrayList<FieldSchema>();
        innerList1.add(new FieldSchema("11a", DataType.INTEGER));
        innerList1.add(new FieldSchema("11b", DataType.FLOAT));
        innerList1.add(new FieldSchema("11c", DataType.CHARARRAY));

        List<FieldSchema> innerList2 = new ArrayList<FieldSchema>();
        innerList2.add(new FieldSchema("22a", DataType.DOUBLE));
        innerList2.add(new FieldSchema(null, DataType.LONG));

        Schema innerSchema1 = new Schema(innerList1);
        Schema innerSchema2 = new Schema(innerList2);

        List<FieldSchema> list1 = new ArrayList<FieldSchema>();
        list1.add(new FieldSchema("1a", DataType.BYTEARRAY));
        list1.add(new FieldSchema("1b", innerSchema1));
        list1.add(new FieldSchema("1c", DataType.LONG));

        List<FieldSchema> list2 = new ArrayList<FieldSchema>();
        list2.add(new FieldSchema("2a", DataType.BYTEARRAY));
        list2.add(new FieldSchema("2b", innerSchema2));
        list2.add(new FieldSchema("2c", DataType.INTEGER));
        list2.add(new FieldSchema("2d", DataType.MAP));

        Schema schema1 = new Schema(list1);
        Schema schema2 = new Schema(list2);

        // Merge
        Schema mergedSchema = Schema.mergeSchema(schema1,
                                                 schema2,
                                                 true,
                                                 true,
                                                 false);


        // Generate expected schema
        List<FieldSchema> expectedInnerList = new ArrayList<FieldSchema>();
        expectedInnerList.add(new FieldSchema("22a", DataType.DOUBLE));
        expectedInnerList.add(new FieldSchema("11b", DataType.FLOAT));
        expectedInnerList.add(new FieldSchema("11c", DataType.CHARARRAY));

        Schema expectedInner = new Schema(expectedInnerList);

        List<FieldSchema> expectedList = new ArrayList<FieldSchema>();
        expectedList.add(new FieldSchema("2a", DataType.BYTEARRAY));
        expectedList.add(new FieldSchema("2b", expectedInner));
        expectedList.add(new FieldSchema("2c", DataType.LONG));
        expectedList.add(new FieldSchema("2d", DataType.MAP));

        Schema expected = new Schema(expectedList);

        // Compare
        assertTrue(Schema.equals(mergedSchema, expected, false, false));
    }

    // negative test
    @Test(expected = SchemaMergeException.class)
    public void testMergeDifferentSize2() throws Throwable {
        List<FieldSchema> list1 = new ArrayList<FieldSchema>();
        list1.add(new FieldSchema("1a", DataType.BYTEARRAY));
        list1.add(new FieldSchema("1b", DataType.BYTEARRAY));
        list1.add(new FieldSchema("1c", DataType.LONG));

        List<FieldSchema> list2 = new ArrayList<FieldSchema>();
        list2.add(new FieldSchema("2a", DataType.BYTEARRAY));
        list2.add(new FieldSchema("2b", DataType.BYTEARRAY));
        list2.add(new FieldSchema("2c", DataType.INTEGER));
        list2.add(new FieldSchema("2d", DataType.MAP));

        Schema schema1 = new Schema(list1);
        Schema schema2 = new Schema(list2);

        // Merge
        Schema mergedSchema = Schema.mergeSchema(schema1,
                                             schema2,
                                             true,
                                             false,
                                             false);
    }


    @Test
    public void testMergeMismatchType1() throws Throwable {

        // Generate two schemas
        List<FieldSchema> innerList1 = new ArrayList<FieldSchema>();
        innerList1.add(new FieldSchema("11a", DataType.CHARARRAY));
        innerList1.add(new FieldSchema("11b", DataType.FLOAT));

        List<FieldSchema> innerList2 = new ArrayList<FieldSchema>();
        innerList2.add(new FieldSchema("22a", DataType.DOUBLE));
        innerList2.add(new FieldSchema(null, DataType.LONG));

        Schema innerSchema1 = new Schema(innerList1);
        Schema innerSchema2 = new Schema(innerList2);

        List<FieldSchema> list1 = new ArrayList<FieldSchema>();
        list1.add(new FieldSchema("1a", DataType.BYTEARRAY));
        list1.add(new FieldSchema("1b", innerSchema1));
        list1.add(new FieldSchema("1c", DataType.MAP));

        List<FieldSchema> list2 = new ArrayList<FieldSchema>();
        list2.add(new FieldSchema("2a", DataType.BYTEARRAY));
        list2.add(new FieldSchema("2b", innerSchema2));
        list2.add(new FieldSchema("2c", DataType.INTEGER));

        Schema schema1 = new Schema(list1);
        Schema schema2 = new Schema(list2);

        // Merge
        Schema mergedSchema = Schema.mergeSchema(schema1,
                                                 schema2,
                                                 true,
                                                 false,
                                                 true);


        // Generate expected schema
        List<FieldSchema> expectedInnerList = new ArrayList<FieldSchema>();
        expectedInnerList.add(new FieldSchema("22a", DataType.BYTEARRAY));
        expectedInnerList.add(new FieldSchema("11b", DataType.FLOAT));

        Schema expectedInner = new Schema(expectedInnerList);

        List<FieldSchema> expectedList = new ArrayList<FieldSchema>();
        expectedList.add(new FieldSchema("2a", DataType.BYTEARRAY));
        expectedList.add(new FieldSchema("2b", expectedInner));
        expectedList.add(new FieldSchema("2c", DataType.BYTEARRAY));

        Schema expected = new Schema(expectedList);

        // Compare
        assertTrue(Schema.equals(mergedSchema, expected, false, false));
    }


    @Test
    public void testMergeMismatchType2() throws Throwable {

        // Generate two schemas
        List<FieldSchema> innerList1 = new ArrayList<FieldSchema>();
        innerList1.add(new FieldSchema("11a", DataType.CHARARRAY));
        innerList1.add(new FieldSchema("11b", DataType.FLOAT));

        Schema innerSchema1 = new Schema(innerList1);

        List<FieldSchema> list1 = new ArrayList<FieldSchema>();
        list1.add(new FieldSchema("1a", DataType.BYTEARRAY));
        list1.add(new FieldSchema("1b", innerSchema1));
        list1.add(new FieldSchema("1c", DataType.MAP));

        List<FieldSchema> list2 = new ArrayList<FieldSchema>();
        list2.add(new FieldSchema("2a", DataType.BYTEARRAY));
        list2.add(new FieldSchema("2b", DataType.BYTEARRAY));
        list2.add(new FieldSchema("2c", DataType.INTEGER));

        Schema schema1 = new Schema(list1);
        Schema schema2 = new Schema(list2);

        // Merge
        Schema mergedSchema = Schema.mergeSchema(schema1,
                                                 schema2,
                                                 true,
                                                 false,
                                                 true);


        // Generate expected schema
        List<FieldSchema> expectedList = new ArrayList<FieldSchema>();
        expectedList.add(new FieldSchema("2a", DataType.BYTEARRAY));
        expectedList.add(new FieldSchema("2b", DataType.TUPLE));
        expectedList.add(new FieldSchema("2c", DataType.BYTEARRAY));

        Schema expected = new Schema(expectedList);

        // Compare
        assertTrue(Schema.equals(mergedSchema, expected, false, false));
    }

    // Negative test
    @Test
    public void testMergeMismatchType3()  {

      // Generate two schemas
        List<FieldSchema> innerList1 = new ArrayList<FieldSchema>();
        innerList1.add(new FieldSchema("11a", DataType.CHARARRAY));
        innerList1.add(new FieldSchema("11b", DataType.FLOAT));

        List<FieldSchema> innerList2 = new ArrayList<FieldSchema>();
        innerList2.add(new FieldSchema("22a", DataType.DOUBLE));
        innerList2.add(new FieldSchema(null, DataType.LONG));

        Schema innerSchema1 = new Schema(innerList1);
        Schema innerSchema2 = new Schema(innerList2);

        List<FieldSchema> list1 = new ArrayList<FieldSchema>();
        list1.add(new FieldSchema("1a", DataType.BYTEARRAY));
        list1.add(new FieldSchema("1b", innerSchema1));
        list1.add(new FieldSchema("1c", DataType.MAP));

        List<FieldSchema> list2 = new ArrayList<FieldSchema>();
        list2.add(new FieldSchema("2a", DataType.BYTEARRAY));
        list2.add(new FieldSchema("2b", innerSchema2));
        list2.add(new FieldSchema("2c", DataType.MAP));

        Schema schema1 = new Schema(list1);
        Schema schema2 = new Schema(list2);

        // Merge

        try {
            Schema mergedSchema = Schema.mergeSchema(schema1,
                                                     schema2,
                                                     true,
                                                     false,
                                                     false);
            fail("Expect error here!");
        } catch (SchemaMergeException e) {
            // good
        }
    }

    @Test
    public void testMergeDifferentSizeAndTypeMismatch1() throws Throwable {

        // Generate two schemas
        List<FieldSchema> innerList1 = new ArrayList<FieldSchema>();
        innerList1.add(new FieldSchema("11a", DataType.INTEGER));
        innerList1.add(new FieldSchema("11b", DataType.FLOAT));
        innerList1.add(new FieldSchema("11c", DataType.CHARARRAY));

        List<FieldSchema> innerList2 = new ArrayList<FieldSchema>();
        innerList2.add(new FieldSchema("22a", DataType.CHARARRAY));
        innerList2.add(new FieldSchema(null, DataType.LONG));

        Schema innerSchema1 = new Schema(innerList1);
        Schema innerSchema2 = new Schema(innerList2);

        List<FieldSchema> list1 = new ArrayList<FieldSchema>();
        list1.add(new FieldSchema("1a", DataType.INTEGER));
        list1.add(new FieldSchema("1b", innerSchema1));
        list1.add(new FieldSchema("1c", DataType.LONG));

        List<FieldSchema> list2 = new ArrayList<FieldSchema>();
        list2.add(new FieldSchema("2a", DataType.CHARARRAY));
        list2.add(new FieldSchema("2b", innerSchema2));
        list2.add(new FieldSchema("2c", DataType.INTEGER));
        list2.add(new FieldSchema("2d", DataType.MAP));

        Schema schema1 = new Schema(list1);
        Schema schema2 = new Schema(list2);

        // Merge
        Schema mergedSchema = Schema.mergeSchema(schema1,
                                                 schema2,
                                                 true,
                                                 true,
                                                 true);


        // Generate expected schema
        List<FieldSchema> expectedInnerList = new ArrayList<FieldSchema>();
        expectedInnerList.add(new FieldSchema("22a", DataType.BYTEARRAY));
        expectedInnerList.add(new FieldSchema("11b", DataType.FLOAT));
        expectedInnerList.add(new FieldSchema("11c", DataType.CHARARRAY));

        Schema expectedInner = new Schema(expectedInnerList);

        List<FieldSchema> expectedList = new ArrayList<FieldSchema>();
        expectedList.add(new FieldSchema("2a", DataType.BYTEARRAY));
        expectedList.add(new FieldSchema("2b", expectedInner));
        expectedList.add(new FieldSchema("2c", DataType.LONG));
        expectedList.add(new FieldSchema("2d", DataType.MAP));

        Schema expected = new Schema(expectedList);

        // Compare
        assertTrue(Schema.equals(mergedSchema, expected, false, false));
    }

    @Test
    public void testSchemaEqualTwoLevelAccess() throws Exception {

        List<FieldSchema> innerList1 = new ArrayList<FieldSchema>();
        innerList1.add(new FieldSchema("11a", DataType.INTEGER));
        innerList1.add(new FieldSchema("11b", DataType.LONG));

        List<FieldSchema> innerList2 = new ArrayList<FieldSchema>();
        innerList2.add(new FieldSchema("11a", DataType.INTEGER));
        innerList2.add(new FieldSchema("11b", DataType.LONG));

        Schema innerSchema1 = new Schema(innerList1);
        Schema innerSchema2 = new Schema(innerList2);

        List<FieldSchema> list1 = new ArrayList<FieldSchema>();
        list1.add(new FieldSchema("1a", DataType.BYTEARRAY));
        list1.add(new FieldSchema("1b", innerSchema1));
        list1.add(new FieldSchema("1c", DataType.INTEGER));

        List<FieldSchema> list2 = new ArrayList<FieldSchema>();
        list2.add(new FieldSchema("1a", DataType.BYTEARRAY));
        list2.add(new FieldSchema("1b", innerSchema2));
        list2.add(new FieldSchema("1c", DataType.INTEGER));

        Schema schema1 = new Schema(list1);
        Schema schema2 = new Schema(list2);

        Schema.FieldSchema bagFs1 = new Schema.FieldSchema("b", schema1, DataType.BAG);
        Schema bagSchema1 = new Schema(bagFs1);

        Schema.FieldSchema tupleFs = new Schema.FieldSchema("t", schema2, DataType.TUPLE);
        Schema bagSchema = new Schema(tupleFs);
        bagSchema.setTwoLevelAccessRequired(true);
        Schema.FieldSchema bagFs2 = new Schema.FieldSchema("b", bagSchema, DataType.BAG);
        Schema bagSchema2 = new Schema(bagFs2);


        assertTrue(Schema.equals(bagSchema1, bagSchema2, false, false));

        innerList2.get(1).alias = "pi";

        assertFalse(Schema.equals(bagSchema1, bagSchema2, false, false));
        assertTrue(Schema.equals(bagSchema1, bagSchema2, false, true));

        innerList2.get(1).alias = "11b";
        innerList2.get(1).type = DataType.BYTEARRAY;

        assertFalse(Schema.equals(bagSchema1, bagSchema2, false, false));
        assertTrue(Schema.equals(bagSchema1, bagSchema2, true, false));

        innerList2.get(1).type = DataType.LONG;

        assertTrue(Schema.equals(bagSchema1, bagSchema2, false, false));

        list2.get(0).type = DataType.CHARARRAY;
        assertFalse(Schema.equals(bagSchema1, bagSchema2, false, false));
    }

    @Test
    public void testCharArray2Numeric(){
    	byte[] numbericTypes=new byte[]{DataType.DOUBLE,DataType.FLOAT,DataType.LONG,DataType.INTEGER};
    	Schema.FieldSchema inputFieldSchema=new Schema.FieldSchema("",DataType.CHARARRAY);
    	for (byte type:numbericTypes){
    		Schema.FieldSchema castFieldSchema=new Schema.FieldSchema("",type);
    		assertTrue(Schema.FieldSchema.castable(castFieldSchema, inputFieldSchema));
    	}
    }

    @Test
    public void testSchemaSerialization() throws IOException {
        String inputFileName = "testSchemaSerialization-input.txt";
        String[] inputData = new String[] { "foo\t1", "hello\t2" };
        Util.createInputFile(cluster, inputFileName, inputData);
        String script = "a = load '"+ inputFileName +"' as (f1:chararray, f2:int);" +
        		" b = group a all; c = foreach b generate org.apache.pig.test.InputSchemaUDF(a);";
        Util.registerMultiLineQuery(pigServer, script);
        Iterator<Tuple> it = pigServer.openIterator("c");
        while(it.hasNext()) {
            Tuple t = it.next();
            assertEquals("{a: {(f1: chararray,f2: int)}}", t.get(0));
        }
    }

    @Test
    // See PIG-730
    public void testMergeSchemaWithTwoLevelAccess1() throws Exception {
        // Generate two schemas
        Schema s1 = Utils.getSchemaFromString("a:{t:(a0:int, a1:int)}");
        Schema s2 = Utils.getSchemaFromString("b:{t:(b0:int, b1:int)}");
        s1.getField(0).schema.setTwoLevelAccessRequired(true);
        s2.getField(0).schema.setTwoLevelAccessRequired(true);
        Schema s3 = Schema.mergeSchema(s1, s2, true);
        assertTrue(s3.getField(0).schema.isTwoLevelAccessRequired());
    }

    @Test
    // See PIG-730
    public void testMergeSchemaWithTwoLevelAccess() throws Exception {
        // Generate two schemas
        Schema s1 = Utils.getSchemaFromString("a:{t:(a0:int, a1:int)}");
        Schema s2 = Utils.getSchemaFromString("b:{t:(b0:int, b1:int)}");
        s1.getField(0).schema.setTwoLevelAccessRequired(true);
        s1.getField(0).schema.setTwoLevelAccessRequired(false);
        Schema s3 = Schema.mergeSchema(s1, s2, true);
        assertEquals(s3, s2);
    }

    @Test
    // See PIG-730
    public void testMergeSchemaWithTwoLevelAccess3() throws Exception {
        // Generate two schemas
        LogicalSchema ls1 = Utils.parseSchema("a:{t:(a0:int, a1:int)}");
        LogicalSchema ls2 = Utils.parseSchema("b:{t:(b0:int, b1:int)}");
        LogicalSchema ls3 = LogicalSchema.merge(ls1, ls2, MergeMode.LoadForEach);
        assertEquals("{a: {t: (a0: int,a1: int)}}", org.apache.pig.newplan.logical.Util.translateSchema(ls3).toString());
    }

    @Test
    public void testNewNormalNestedMerge1() throws Exception {
        LogicalSchema a = org.apache.pig.newplan.logical.Util.translateSchema(Utils.getSchemaFromString(
            "a1:bytearray, b1:(b11:int, b12:float), c1:long"));
        LogicalSchema b = org.apache.pig.newplan.logical.Util.translateSchema(Utils.getSchemaFromString(
            "a2:bytearray, b2:(b21:double, b22:long), c2:int"));

        LogicalSchema mergedSchema = LogicalSchema.merge(a, b, LogicalSchema.MergeMode.Union);
        LogicalSchema expected = org.apache.pig.newplan.logical.Util.translateSchema(Utils.getSchemaFromString(
            "a1:bytearray, b1:(), c1:long"));
        expected.getField(1).schema = new LogicalSchema();
        assertTrue(LogicalSchema.equals(mergedSchema, expected, false, false));

        mergedSchema = LogicalSchema.merge(a, b, LogicalSchema.MergeMode.LoadForEach);
        expected = org.apache.pig.newplan.logical.Util.translateSchema(Utils.getSchemaFromString(
            "a1:bytearray, b1:(b11:int, b12:float), c1:long"));
        assertTrue(LogicalSchema.equals(mergedSchema, expected, false, false));

        mergedSchema = LogicalSchema.merge(b, a, LogicalSchema.MergeMode.LoadForEach);
        expected = org.apache.pig.newplan.logical.Util.translateSchema(Utils.getSchemaFromString(
            "a2:bytearray, b2:(b21:double, b22:long), c2:int"));
        assertTrue(LogicalSchema.equals(mergedSchema, expected, false, false));
    }



    @Test
    public void testNewNormalNestedMerge2() throws Exception {
        LogicalSchema a = org.apache.pig.newplan.logical.Util.translateSchema(Utils.getSchemaFromString(
            "a1:(a11:chararray, a12:float), b1:(b11:chararray, b12:float), c1:long"));
        LogicalSchema b = org.apache.pig.newplan.logical.Util.translateSchema(Utils.getSchemaFromString(
            "a2:bytearray, b2:(b21:double, b22:long), c2:chararray"));

        LogicalSchema mergedSchema = LogicalSchema.merge(a, b, LogicalSchema.MergeMode.Union);
        LogicalSchema expected = org.apache.pig.newplan.logical.Util.translateSchema(Utils.getSchemaFromString(
            "a1:(a11:chararray, a12:float), b1:(), c1:bytearray"));
        expected.getField(1).schema = new LogicalSchema();
        assertTrue(LogicalSchema.equals(mergedSchema, expected, false, false));

        mergedSchema = LogicalSchema.merge(a, b, LogicalSchema.MergeMode.LoadForEach);
        expected = org.apache.pig.newplan.logical.Util.translateSchema(Utils.getSchemaFromString(
            "a1:(a11:chararray, a12:float), b1:(b11:chararray, b12:float), c1:long"));
        assertTrue(LogicalSchema.equals(mergedSchema, expected, false, false));

        mergedSchema = LogicalSchema.merge(b, a, LogicalSchema.MergeMode.LoadForEach);
        expected = org.apache.pig.newplan.logical.Util.translateSchema(Utils.getSchemaFromString(
                "a2:(a11:chararray, a12:float), b2:(b21:double, b22:long), c2:chararray"));
        assertTrue(LogicalSchema.equals(mergedSchema, expected, false, false));
    }

    @Test
    public void testNewMergeNullSchemas() throws Throwable {
        LogicalSchema a = Utils.parseSchema( "a1:bytearray, b1:(b11:int, b12:float), c1:long" );
        LogicalSchema b = Utils.parseSchema( "a2:bytearray, b2:(), c2:int" );

        LogicalSchema mergedSchema = LogicalSchema.merge(a, b, LogicalSchema.MergeMode.Union);
        LogicalSchema expected = Utils.parseSchema( "a1:bytearray, b1:(), c1:long" );
        assertTrue(LogicalSchema.equals(mergedSchema, expected, false, false));

        mergedSchema = LogicalSchema.merge(a, b, LogicalSchema.MergeMode.LoadForEach);
        expected = Utils.parseSchema( "a1:bytearray, b1:(b11:int, b12:float), c1:long" );
        assertTrue(LogicalSchema.equals(mergedSchema, expected, false, false));

        mergedSchema = LogicalSchema.merge(b, a, LogicalSchema.MergeMode.LoadForEach);
        expected = Utils.parseSchema( "a2:bytearray, b2:(b11:int,b12:float), c2:int" );
        assertTrue(LogicalSchema.equals(mergedSchema, expected, false, false));
    }

    @Test(expected = FrontendException.class)
    public void testNewMergeDifferentSize1() throws Throwable {
        LogicalSchema a = Utils.parseSchema( "a1:bytearray, b1:long, c1:long" );
        LogicalSchema b = Utils.parseSchema( "a2:bytearray, b2:long" );

        LogicalSchema mergedSchema = LogicalSchema.merge(a, b, LogicalSchema.MergeMode.Union);
        assertNull(mergedSchema);

        try {
            LogicalSchema.merge(a, b, LogicalSchema.MergeMode.LoadForEach);
        } catch (FrontendException e) {
            assertEquals(1031, e.getErrorCode());
            throw e;
        }
    }

    @Test(expected = FrontendException.class)
    public void testNewMergeDifferentSize2() throws Throwable {
        LogicalSchema a = org.apache.pig.newplan.logical.Util.translateSchema(Utils.getSchemaFromString(
            "a1:bytearray, b1:(b11:int, b12:float, b13:float), c1:long"));
        LogicalSchema b = org.apache.pig.newplan.logical.Util.translateSchema(Utils.getSchemaFromString(
            "a2:bytearray, b2:(b21:double, b22:long), c2:int"));

        LogicalSchema mergedSchema = LogicalSchema.merge(a, b, LogicalSchema.MergeMode.Union);
        LogicalSchema expected = org.apache.pig.newplan.logical.Util.translateSchema(Utils.getSchemaFromString(
            "a1:bytearray, b1:(), c1:long"));
        expected.getField(1).schema = new LogicalSchema();
        assertTrue(LogicalSchema.equals(mergedSchema, expected, false, false));

        try {
            LogicalSchema.merge(a, b, LogicalSchema.MergeMode.LoadForEach);
        } catch (FrontendException e) {
            assertEquals(1031, e.getErrorCode());
            throw e;
        }
    }


    @Test(expected = FrontendException.class)
    public void testNewMergeMismatchType1() throws Throwable {
        LogicalSchema a = org.apache.pig.newplan.logical.Util.translateSchema(Utils.getSchemaFromString(
            "a1:chararray, b1:long, c1:long"));
        LogicalSchema b = org.apache.pig.newplan.logical.Util.translateSchema(Utils.getSchemaFromString(
            "a2:bytearray, b2:(b21:double, b22:long), c2:int"));

        LogicalSchema mergedSchema = LogicalSchema.merge(a, b, LogicalSchema.MergeMode.Union);
        LogicalSchema expected = org.apache.pig.newplan.logical.Util.translateSchema(Utils.getSchemaFromString(
            "a1:chararray, b1:bytearray, c1:long"));
        assertTrue(LogicalSchema.equals(mergedSchema, expected, false, false));

        try {
            LogicalSchema.merge(a, b, LogicalSchema.MergeMode.LoadForEach);
            fail();
        } catch (FrontendException e) {
            assertEquals(1031, e.getErrorCode());
        }

        try {
            LogicalSchema.merge(b, a, LogicalSchema.MergeMode.LoadForEach);
        } catch (FrontendException e) {
            assertEquals(1031, e.getErrorCode());
            throw e;
        }
    }


    @Test(expected = FrontendException.class)
    public void testNewMergeMismatchType2() throws Throwable {
        LogicalSchema a = org.apache.pig.newplan.logical.Util.translateSchema(Utils.getSchemaFromString(
            "a1:chararray, b1:(b11:double, b12:(b121:int)), c1:long"));
        LogicalSchema b = org.apache.pig.newplan.logical.Util.translateSchema(Utils.getSchemaFromString(
            "a2:bytearray, b2:(b21:double, b22:long), c2:int"));

        LogicalSchema mergedSchema = LogicalSchema.merge(a, b, LogicalSchema.MergeMode.Union);
        LogicalSchema expected = org.apache.pig.newplan.logical.Util.translateSchema(Utils.getSchemaFromString(
            "a1:chararray, b1:(), c1:long"));
        expected.getField(1).schema = new LogicalSchema();
        assertTrue(LogicalSchema.equals(mergedSchema, expected, false, false));

        try {
            LogicalSchema.merge(a, b, LogicalSchema.MergeMode.LoadForEach);
            fail();
        } catch (FrontendException e) {
            assertEquals(1031, e.getErrorCode());
        }

        try {
            LogicalSchema.merge(b, a, LogicalSchema.MergeMode.LoadForEach);
        } catch (FrontendException e) {
            assertEquals(1031, e.getErrorCode());
            throw e;
        }
    }

    @Test
    public void testResourceSchemaToSchema() throws ParserException,FrontendException{
        Schema s1 = Utils.getSchemaFromString("b:bag{t:tuple(name:chararray,age:int)}");
        Schema s2 = Schema.getPigSchema(new ResourceSchema(s1));
        assertEquals(s1, s2);
    }

    @Test
    public void testGetStringFromSchema() throws ParserException {
        String[] schemaStrings = {
            "a:int",
            "a:long",
            "a:chararray",
            "a:double",
            "a:float",
            "a:bytearray",
            "b:bag{tuple(x:int,y:int,z:int)}",
            "b:bag{t:tuple(x:int,y:int,z:int)}",
            "a:int,b:chararray,c:Map[int]",
            "a:double,b:float,t:tuple(x:int,y:double,z:bytearray)",
            "a:double,b:float,t:tuple(x:int,b:bag{t:tuple(a:int,b:float,c:double,x:tuple(z:bag{r:tuple(z:bytearray)}))},z:bytearray)",
            "a,b,t:tuple(x,b:bag{t:tuple(a,b,c,x:tuple(z:bag{r:tuple(z)}))},z)",
            "a:bag{t:tuple(a:bag{t:tuple(a:bag{t:tuple(a:bag{t:tuple(a:bag{t:tuple(a:bag{t:tuple(a:int,b:float)})})})})})}",
            "a:bag{}",
            "b:{null:(a:int)}",
            "int,int,int,int,int,int,int,int,int,int",
            "long,long,long,long,long,long,long,long,long,long",
            "float,float,float,float,float,float,float,float,float,float",
            "double,double,double,double,double,double,double,double,double,double",
            "boolean,boolean,boolean,boolean,boolean,boolean,boolean,boolean,boolean,boolean",
            "datetime,datetime,datetime,datetime,datetime,datetime,datetime,datetime,datetime,datetime",
            "{},{},{},{},{},{},{},{},{},{}",
            "map[],map[],map[],map[],map[],map[],map[],map[],map[],map[]",
            "int,int,long,long,float,float,double,double,boolean,boolean,datetime,datetime,(int,long,float,double,boolean,datetime),{(int,long,float,double,boolean,datetime)},map[(int,long,float,double,boolean,datetime)]"
        };
        for (String schemaString : schemaStrings) {
            Schema s1 = Utils.getSchemaFromString(schemaString);
            String s=s1.toString();
            Schema s2 = Utils.getSchemaFromBagSchemaString(s); // removes outer curly-braces added by Schema#toString
            assertTrue(Schema.equals(s1,s2,false,true));
        }
    }

    @Test(expected = ParserException.class)
    public void testGetStringFromSchemaNegative() throws Exception {
        String schemaString = "a:int b:long"; // A comma is missing between fields
        Utils.getSchemaFromString(schemaString);
        fail("The schema string is invalid, so parsing should fail!");
    }
    
    @Test
    public void testGetInitialSchemaStringFromSchema() throws ParserException {
        String[] schemaStrings = {
                "my_list:{array:(array_element:(num1:int,num2:int))}",
                "my_list:{array:(array_element:(num1:int,num2:int),c:chararray)}",
                "bag:{mytuple3:(mytuple2:(mytuple:(f1:int)))}",
                "bag:{mytuple:(f1:int)}",
                "{((num1:int,num2:int))}"
        };
        for (String schemaString : schemaStrings) {
            String s1 = Utils.getSchemaFromString(schemaString).toString();
            //check if we get back the initial schema string
            String s2 = s1.substring(1, s1.length() - 1).replaceAll("\\s|bag_0:|tuple_0:", "");
            assertTrue(schemaString.equals(s2));
        }
    }

    @Test
    public void testDisabledDisambiguationContainsNoColons() throws IOException {
        resetDisambiguationTestPropertyOverride();

        String inputFileName = "testPrepend-input.txt";
        String[] inputData = new String[]{"apple\t1\tred", "orange\t2\torange", "kiwi\t3\tgreen", "orange\t4\torange"};
        Util.createInputFile(cluster, inputFileName, inputData);

        String script = "A = LOAD '" + inputFileName + "' AS (fruit:chararray, foo:int, color: chararray);" +
                "B = LOAD '" + inputFileName + "' AS (id:chararray, bar:int);" +
                "C = GROUP A BY (fruit,color);" +
                "D = FOREACH C GENERATE FLATTEN(group), AVG(A.foo);" +
                "D2 = FOREACH C GENERATE FLATTEN(group), AVG(A.foo) as avgFoo;" +
                "E = JOIN B BY id, D BY group::fruit;" +
                "F = UNION ONSCHEMA B, D2;" +
                "G = CROSS B, D2;";

        Util.registerMultiLineQuery(pigServer, script);

        //Prepending should happen with default settings
        assertEquals("{B::id: chararray,B::bar: int,D::group::fruit: chararray,D::group::color: chararray,double}", pigServer.dumpSchema("E").toString());

        //Override prepend property setting (check for flatten, join)
        pigServer.getPigContext().getProperties().setProperty(PigConfiguration.PIG_STORE_SCHEMA_DISAMBIGUATE, "false");
        assertEquals("{id: chararray,bar: int,fruit: chararray,color: chararray,double}", pigServer.dumpSchema("E").toString());
        assertTrue(pigServer.openIterator("E").hasNext());

        //Check for union and cross
        assertEquals("{id: chararray,bar: int,fruit: chararray,color: chararray,avgFoo: double}", pigServer.dumpSchema("F").toString());
        assertEquals("{id: chararray,bar: int,fruit: chararray,color: chararray,avgFoo: double}", pigServer.dumpSchema("G").toString());

    }

    @Test
    public void testEnabledDisambiguationPassesForDupeAliases() throws IOException {
        resetDisambiguationTestPropertyOverride();

        checkForDupeAliases();

        //Should pass with default settings
        assertEquals("{A::id: chararray,A::val: int,B::id: chararray,B::val: int}", pigServer.dumpSchema("C").toString());
        assertTrue(pigServer.openIterator("C").hasNext());
    }

    @Test
    public void testDisabledDisambiguationFailsForDupeAliases() throws IOException {
        resetDisambiguationTestPropertyOverride();

        try {
            checkForDupeAliases();
            //Should fail with prepending disabled
            pigServer.getPigContext().getProperties().setProperty(PigConfiguration.PIG_STORE_SCHEMA_DISAMBIGUATE, "false");
            pigServer.dumpSchema("C");
        } catch (FrontendException e){
            Assert.assertEquals("Duplicate schema alias: id in \"fake\"",e.getCause().getMessage());
        }
    }

    private static void checkForDupeAliases() throws IOException {
        String inputFileName = "testPrependFail-input" + UUID.randomUUID().toString() + ".txt";
        String[] inputData = new String[]{"foo\t1", "bar\t2"};
        Util.createInputFile(cluster, inputFileName, inputData);

        String script = "A = LOAD '" + inputFileName + "' AS (id:chararray, val:int);" +
                "B = LOAD '" + inputFileName + "' AS (id:chararray, val:int);" +
                "C = JOIN A by id, B by id;";

        Util.registerMultiLineQuery(pigServer, script);
    }

    private static void resetDisambiguationTestPropertyOverride() {
        //Reset possible overrides
        pigServer.getPigContext().getProperties().remove(PigConfiguration.PIG_STORE_SCHEMA_DISAMBIGUATE);
    }
}
