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

import java.util.* ;

import org.apache.pig.data.* ;
import org.apache.pig.impl.logicalLayer.schema.* ;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import org.junit.* ;

import junit.framework.Assert;
import junit.framework.TestCase ;

public class TestSchema extends TestCase {
    
    @Test
    public void testSchemaEqual1() {
        
        List<FieldSchema> innerList1 = new ArrayList<FieldSchema>() ;
        innerList1.add(new FieldSchema("11a", DataType.INTEGER)) ;
        innerList1.add(new FieldSchema("11b", DataType.LONG)) ;
        
        List<FieldSchema> innerList2 = new ArrayList<FieldSchema>() ;
        innerList2.add(new FieldSchema("11a", DataType.INTEGER)) ;
        innerList2.add(new FieldSchema("11b", DataType.LONG)) ;
        
        Schema innerSchema1 = new Schema(innerList1) ;
        Schema innerSchema2 = new Schema(innerList2) ;
                
        List<FieldSchema> list1 = new ArrayList<FieldSchema>() ;
        list1.add(new FieldSchema("1a", DataType.BYTEARRAY)) ;
        list1.add(new FieldSchema("1b", innerSchema1)) ;
        list1.add(new FieldSchema("1c", DataType.INTEGER)) ;
        
        List<FieldSchema> list2 = new ArrayList<FieldSchema>() ;
        list2.add(new FieldSchema("1a", DataType.BYTEARRAY)) ;
        list2.add(new FieldSchema("1b", innerSchema2)) ;
        list2.add(new FieldSchema("1c", DataType.INTEGER)) ;
        
        Schema schema1 = new Schema(list1) ;
        Schema schema2 = new Schema(list2) ;
        
        Assert.assertTrue(Schema.equals(schema1, schema2, false, false)) ;

        innerList2.get(1).alias = "pi" ;

        Assert.assertFalse(Schema.equals(schema1, schema2, false, false)) ;
        Assert.assertTrue(Schema.equals(schema1, schema2, false, true)) ;
        
        innerList2.get(1).alias = "11b" ;
        innerList2.get(1).type = DataType.BYTEARRAY ;
        
        Assert.assertFalse(Schema.equals(schema1, schema2, false, false)) ;
        Assert.assertTrue(Schema.equals(schema1, schema2, true, false)) ;
        
        innerList2.get(1).type = DataType.LONG ;
        
        Assert.assertTrue(Schema.equals(schema1, schema2, false, false)) ;
        
        list2.get(0).type = DataType.CHARARRAY ;
        Assert.assertFalse(Schema.equals(schema1, schema2, false, false)) ;
    }

    // Positive test
    @Test
    public void testSchemaEqualWithNullSchema1() {

        List<FieldSchema> list1 = new ArrayList<FieldSchema>() ;
        list1.add(new FieldSchema("1a", DataType.BYTEARRAY)) ;
        list1.add(new FieldSchema("1b", null)) ;
        list1.add(new FieldSchema("1c", DataType.INTEGER)) ;

        List<FieldSchema> list2 = new ArrayList<FieldSchema>() ;
        list2.add(new FieldSchema("1a", DataType.BYTEARRAY)) ;
        list2.add(new FieldSchema("1b", null)) ;
        list2.add(new FieldSchema("1c", DataType.INTEGER)) ;

        Schema schema1 = new Schema(list1) ;
        Schema schema2 = new Schema(list2) ;

        // First check
        Assert.assertTrue(Schema.equals(schema1, schema2, false, false)) ;

        // Manipulate
        List<FieldSchema> dummyList = new ArrayList<FieldSchema>() ;
        Schema dummySchema = new Schema(dummyList) ;
        list2.get(1).schema = dummySchema ;

        // And check again
        Assert.assertFalse(Schema.equals(schema1, schema2, false, false)) ;
    }
    
    @Test
    public void testNormalNestedMerge1() {
        
        // Generate two schemas
        List<FieldSchema> innerList1 = new ArrayList<FieldSchema>() ;
        innerList1.add(new FieldSchema("11a", DataType.INTEGER)) ; 
        innerList1.add(new FieldSchema("11b", DataType.FLOAT)) ;
        
        List<FieldSchema> innerList2 = new ArrayList<FieldSchema>() ;
        innerList2.add(new FieldSchema("22a", DataType.DOUBLE)) ;
        innerList2.add(new FieldSchema(null, DataType.LONG)) ;
        
        Schema innerSchema1 = new Schema(innerList1) ;
        Schema innerSchema2 = new Schema(innerList2) ;
                
        List<FieldSchema> list1 = new ArrayList<FieldSchema>() ;
        list1.add(new FieldSchema("1a", DataType.BYTEARRAY)) ;
        list1.add(new FieldSchema("1b", innerSchema1)) ;
        list1.add(new FieldSchema("1c", DataType.LONG)) ;
        
        List<FieldSchema> list2 = new ArrayList<FieldSchema>() ;
        list2.add(new FieldSchema("2a", DataType.BYTEARRAY)) ;
        list2.add(new FieldSchema("2b", innerSchema2)) ;
        list2.add(new FieldSchema("2c", DataType.INTEGER)) ;
        
        Schema schema1 = new Schema(list1) ;
        Schema schema2 = new Schema(list2) ;
        
        // Merge
        Schema mergedSchema = schema1.merge(schema2, true) ;
        
        
        // Generate expected schema
        List<FieldSchema> expectedInnerList = new ArrayList<FieldSchema>() ;
        expectedInnerList.add(new FieldSchema("22a", DataType.DOUBLE)) ;
        expectedInnerList.add(new FieldSchema("11b", DataType.FLOAT)) ;
        
        Schema expectedInner = new Schema(expectedInnerList) ;
        
        List<FieldSchema> expectedList = new ArrayList<FieldSchema>() ;
        expectedList.add(new FieldSchema("2a", DataType.BYTEARRAY)) ;
        expectedList.add(new FieldSchema("2b", expectedInner)) ;
        expectedList.add(new FieldSchema("2c", DataType.LONG)) ;
        
        Schema expected = new Schema(expectedList) ;
        
        // Compare
        Assert.assertTrue(Schema.equals(mergedSchema, expected, false, false)) ;
    }

    @Test
    public void testMergeNullSchemas1() throws Throwable {

        List<FieldSchema> innerList2 = new ArrayList<FieldSchema>() ;
        innerList2.add(new FieldSchema("22a", DataType.DOUBLE)) ;
        innerList2.add(new FieldSchema(null, DataType.LONG)) ;

        Schema innerSchema2 = new Schema(innerList2) ;

        List<FieldSchema> list1 = new ArrayList<FieldSchema>() ;
        list1.add(new FieldSchema("1a", DataType.BYTEARRAY)) ;
        list1.add(new FieldSchema("1b", null)) ;
        list1.add(new FieldSchema("1c", DataType.LONG)) ;

        List<FieldSchema> list2 = new ArrayList<FieldSchema>() ;
        list2.add(new FieldSchema("2a", DataType.BYTEARRAY)) ;
        list2.add(new FieldSchema("2b", innerSchema2)) ;
        list2.add(new FieldSchema("2c", DataType.INTEGER)) ;

        Schema schema1 = new Schema(list1) ;
        Schema schema2 = new Schema(list2) ;

        // Merge
        Schema mergedSchema = Schema.mergeSchema(schema1,
                                                 schema2,
                                                 true,
                                                 false,
                                                 true) ;


        // Generate expected schema

        List<FieldSchema> expectedList = new ArrayList<FieldSchema>() ;
        expectedList.add(new FieldSchema("2a", DataType.BYTEARRAY)) ;
        expectedList.add(new FieldSchema("2b", null)) ;
        expectedList.add(new FieldSchema("2c", DataType.LONG)) ;

        Schema expected = new Schema(expectedList) ;

        // Compare
        Assert.assertTrue(Schema.equals(mergedSchema, expected, false, false)) ;
    }

    @Test
    public void testMergeNullSchemas2() throws Throwable {
        // Inner of inner schema
        Schema innerInner = new Schema(new ArrayList<FieldSchema>()) ;

        // Generate two schemas
        List<FieldSchema> innerList1 = new ArrayList<FieldSchema>() ;
        innerList1.add(new FieldSchema("11a", innerInner)) ;
        innerList1.add(new FieldSchema("11b", DataType.FLOAT)) ;

        List<FieldSchema> innerList2 = new ArrayList<FieldSchema>() ;
        innerList2.add(new FieldSchema("22a", null)) ;
        innerList2.add(new FieldSchema(null, DataType.LONG)) ;

        Schema innerSchema1 = new Schema(innerList1) ;
        Schema innerSchema2 = new Schema(innerList2) ;

        List<FieldSchema> list1 = new ArrayList<FieldSchema>() ;
        list1.add(new FieldSchema("1a", DataType.BYTEARRAY)) ;
        list1.add(new FieldSchema("1b", innerSchema1)) ;
        list1.add(new FieldSchema("1c", DataType.LONG)) ;

        List<FieldSchema> list2 = new ArrayList<FieldSchema>() ;
        list2.add(new FieldSchema("2a", DataType.BYTEARRAY)) ;
        list2.add(new FieldSchema("2b", innerSchema2)) ;
        list2.add(new FieldSchema("2c", DataType.INTEGER)) ;

        Schema schema1 = new Schema(list1) ;
        Schema schema2 = new Schema(list2) ;

        // Merge
        Schema mergedSchema = Schema.mergeSchema(schema1,
                                                 schema2,
                                                 true,
                                                 false,
                                                 true) ;


        // Generate expected schema
        List<FieldSchema> expectedInnerList = new ArrayList<FieldSchema>() ;
        expectedInnerList.add(new FieldSchema("22a", null)) ;
        expectedInnerList.add(new FieldSchema("11b", DataType.FLOAT)) ;

        Schema expectedInner = new Schema(expectedInnerList) ;

        List<FieldSchema> expectedList = new ArrayList<FieldSchema>() ;
        expectedList.add(new FieldSchema("2a", DataType.BYTEARRAY)) ;
        expectedList.add(new FieldSchema("2b", expectedInner)) ;
        expectedList.add(new FieldSchema("2c", DataType.LONG)) ;

        Schema expected = new Schema(expectedList) ;

        // Compare
        Assert.assertTrue(Schema.equals(mergedSchema, expected, false, false)) ;
    }

    @Test
    public void testMergeDifferentSize1() throws Throwable {

        // Generate two schemas
        List<FieldSchema> innerList1 = new ArrayList<FieldSchema>() ;
        innerList1.add(new FieldSchema("11a", DataType.INTEGER)) ;
        innerList1.add(new FieldSchema("11b", DataType.FLOAT)) ;
        innerList1.add(new FieldSchema("11c", DataType.CHARARRAY)) ;

        List<FieldSchema> innerList2 = new ArrayList<FieldSchema>() ;
        innerList2.add(new FieldSchema("22a", DataType.DOUBLE)) ;
        innerList2.add(new FieldSchema(null, DataType.LONG)) ;

        Schema innerSchema1 = new Schema(innerList1) ;
        Schema innerSchema2 = new Schema(innerList2) ;

        List<FieldSchema> list1 = new ArrayList<FieldSchema>() ;
        list1.add(new FieldSchema("1a", DataType.BYTEARRAY)) ;
        list1.add(new FieldSchema("1b", innerSchema1)) ;
        list1.add(new FieldSchema("1c", DataType.LONG)) ;

        List<FieldSchema> list2 = new ArrayList<FieldSchema>() ;
        list2.add(new FieldSchema("2a", DataType.BYTEARRAY)) ;
        list2.add(new FieldSchema("2b", innerSchema2)) ;
        list2.add(new FieldSchema("2c", DataType.INTEGER)) ;
        list2.add(new FieldSchema("2d", DataType.MAP)) ;

        Schema schema1 = new Schema(list1) ;
        Schema schema2 = new Schema(list2) ;

        // Merge
        Schema mergedSchema = Schema.mergeSchema(schema1,
                                                 schema2,
                                                 true,
                                                 true,
                                                 false) ;


        // Generate expected schema
        List<FieldSchema> expectedInnerList = new ArrayList<FieldSchema>() ;
        expectedInnerList.add(new FieldSchema("22a", DataType.DOUBLE)) ;
        expectedInnerList.add(new FieldSchema("11b", DataType.FLOAT)) ;
        expectedInnerList.add(new FieldSchema("11c", DataType.CHARARRAY)) ;

        Schema expectedInner = new Schema(expectedInnerList) ;

        List<FieldSchema> expectedList = new ArrayList<FieldSchema>() ;
        expectedList.add(new FieldSchema("2a", DataType.BYTEARRAY)) ;
        expectedList.add(new FieldSchema("2b", expectedInner)) ;
        expectedList.add(new FieldSchema("2c", DataType.LONG)) ;
        expectedList.add(new FieldSchema("2d", DataType.MAP)) ;

        Schema expected = new Schema(expectedList) ;

        // Compare
        Assert.assertTrue(Schema.equals(mergedSchema, expected, false, false)) ;
    }

    // negative test
    @Test
    public void testMergeDifferentSize2() throws Throwable {


        List<FieldSchema> list1 = new ArrayList<FieldSchema>() ;
        list1.add(new FieldSchema("1a", DataType.BYTEARRAY)) ;
        list1.add(new FieldSchema("1b", DataType.BYTEARRAY)) ;
        list1.add(new FieldSchema("1c", DataType.LONG)) ;

        List<FieldSchema> list2 = new ArrayList<FieldSchema>() ;
        list2.add(new FieldSchema("2a", DataType.BYTEARRAY)) ;
        list2.add(new FieldSchema("2b", DataType.BYTEARRAY)) ;
        list2.add(new FieldSchema("2c", DataType.INTEGER)) ;
        list2.add(new FieldSchema("2d", DataType.MAP)) ;

        Schema schema1 = new Schema(list1) ;
        Schema schema2 = new Schema(list2) ;

        // Merge
        try {
            Schema mergedSchema = Schema.mergeSchema(schema1,
                                                 schema2,
                                                 true,
                                                 false,
                                                 false) ;
            fail("Expect Error Here!") ;
        }
        catch (SchemaMergeException sme) {
            // good
        }


    }


    @Test
    public void testMergeMismatchType1() throws Throwable {

        // Generate two schemas
        List<FieldSchema> innerList1 = new ArrayList<FieldSchema>() ;
        innerList1.add(new FieldSchema("11a", DataType.CHARARRAY)) ;
        innerList1.add(new FieldSchema("11b", DataType.FLOAT)) ;

        List<FieldSchema> innerList2 = new ArrayList<FieldSchema>() ;
        innerList2.add(new FieldSchema("22a", DataType.DOUBLE)) ;
        innerList2.add(new FieldSchema(null, DataType.LONG)) ;

        Schema innerSchema1 = new Schema(innerList1) ;
        Schema innerSchema2 = new Schema(innerList2) ;

        List<FieldSchema> list1 = new ArrayList<FieldSchema>() ;
        list1.add(new FieldSchema("1a", DataType.BYTEARRAY)) ;
        list1.add(new FieldSchema("1b", innerSchema1)) ;
        list1.add(new FieldSchema("1c", DataType.MAP)) ;

        List<FieldSchema> list2 = new ArrayList<FieldSchema>() ;
        list2.add(new FieldSchema("2a", DataType.BYTEARRAY)) ;
        list2.add(new FieldSchema("2b", innerSchema2)) ;
        list2.add(new FieldSchema("2c", DataType.INTEGER)) ;

        Schema schema1 = new Schema(list1) ;
        Schema schema2 = new Schema(list2) ;

        // Merge
        Schema mergedSchema = Schema.mergeSchema(schema1,
                                                 schema2,
                                                 true,
                                                 false,
                                                 true) ;


        // Generate expected schema
        List<FieldSchema> expectedInnerList = new ArrayList<FieldSchema>() ;
        expectedInnerList.add(new FieldSchema("22a", DataType.BYTEARRAY)) ;
        expectedInnerList.add(new FieldSchema("11b", DataType.FLOAT)) ;

        Schema expectedInner = new Schema(expectedInnerList) ;

        List<FieldSchema> expectedList = new ArrayList<FieldSchema>() ;
        expectedList.add(new FieldSchema("2a", DataType.BYTEARRAY)) ;
        expectedList.add(new FieldSchema("2b", expectedInner)) ;
        expectedList.add(new FieldSchema("2c", DataType.BYTEARRAY)) ;

        Schema expected = new Schema(expectedList) ;

        // Compare
        Assert.assertTrue(Schema.equals(mergedSchema, expected, false, false)) ;
    }


    @Test
    public void testMergeMismatchType2() throws Throwable {

        // Generate two schemas
        List<FieldSchema> innerList1 = new ArrayList<FieldSchema>() ;
        innerList1.add(new FieldSchema("11a", DataType.CHARARRAY)) ;
        innerList1.add(new FieldSchema("11b", DataType.FLOAT)) ;

        Schema innerSchema1 = new Schema(innerList1) ;

        List<FieldSchema> list1 = new ArrayList<FieldSchema>() ;
        list1.add(new FieldSchema("1a", DataType.BYTEARRAY)) ;
        list1.add(new FieldSchema("1b", innerSchema1)) ;
        list1.add(new FieldSchema("1c", DataType.MAP)) ;

        List<FieldSchema> list2 = new ArrayList<FieldSchema>() ;
        list2.add(new FieldSchema("2a", DataType.BYTEARRAY)) ;
        list2.add(new FieldSchema("2b", DataType.BYTEARRAY)) ;
        list2.add(new FieldSchema("2c", DataType.INTEGER)) ;

        Schema schema1 = new Schema(list1) ;
        Schema schema2 = new Schema(list2) ;

        // Merge
        Schema mergedSchema = Schema.mergeSchema(schema1,
                                                 schema2,
                                                 true,
                                                 false,
                                                 true) ;


        // Generate expected schema
        List<FieldSchema> expectedList = new ArrayList<FieldSchema>() ;
        expectedList.add(new FieldSchema("2a", DataType.BYTEARRAY)) ;
        expectedList.add(new FieldSchema("2b", DataType.TUPLE)) ;
        expectedList.add(new FieldSchema("2c", DataType.BYTEARRAY)) ;

        Schema expected = new Schema(expectedList) ;

        // Compare
        Assert.assertTrue(Schema.equals(mergedSchema, expected, false, false)) ;
    }

    // Negative test
    @Test
    public void testMergeMismatchType3()  {

      // Generate two schemas
        List<FieldSchema> innerList1 = new ArrayList<FieldSchema>() ;
        innerList1.add(new FieldSchema("11a", DataType.CHARARRAY)) ;
        innerList1.add(new FieldSchema("11b", DataType.FLOAT)) ;

        List<FieldSchema> innerList2 = new ArrayList<FieldSchema>() ;
        innerList2.add(new FieldSchema("22a", DataType.DOUBLE)) ;
        innerList2.add(new FieldSchema(null, DataType.LONG)) ;

        Schema innerSchema1 = new Schema(innerList1) ;
        Schema innerSchema2 = new Schema(innerList2) ;

        List<FieldSchema> list1 = new ArrayList<FieldSchema>() ;
        list1.add(new FieldSchema("1a", DataType.BYTEARRAY)) ;
        list1.add(new FieldSchema("1b", innerSchema1)) ;
        list1.add(new FieldSchema("1c", DataType.MAP)) ;

        List<FieldSchema> list2 = new ArrayList<FieldSchema>() ;
        list2.add(new FieldSchema("2a", DataType.BYTEARRAY)) ;
        list2.add(new FieldSchema("2b", innerSchema2)) ;
        list2.add(new FieldSchema("2c", DataType.MAP)) ;

        Schema schema1 = new Schema(list1) ;
        Schema schema2 = new Schema(list2) ;

        // Merge

        try {
            Schema mergedSchema = Schema.mergeSchema(schema1,
                                                     schema2,
                                                     true,
                                                     false,
                                                     false) ;
            fail("Expect error here!") ;
        } catch (SchemaMergeException e) {
            // good
        }
    }

    @Test
    public void testMergeDifferentSizeAndTypeMismatch1() throws Throwable {

        // Generate two schemas
        List<FieldSchema> innerList1 = new ArrayList<FieldSchema>() ;
        innerList1.add(new FieldSchema("11a", DataType.INTEGER)) ;
        innerList1.add(new FieldSchema("11b", DataType.FLOAT)) ;
        innerList1.add(new FieldSchema("11c", DataType.CHARARRAY)) ;

        List<FieldSchema> innerList2 = new ArrayList<FieldSchema>() ;
        innerList2.add(new FieldSchema("22a", DataType.CHARARRAY)) ;
        innerList2.add(new FieldSchema(null, DataType.LONG)) ;

        Schema innerSchema1 = new Schema(innerList1) ;
        Schema innerSchema2 = new Schema(innerList2) ;

        List<FieldSchema> list1 = new ArrayList<FieldSchema>() ;
        list1.add(new FieldSchema("1a", DataType.INTEGER)) ;
        list1.add(new FieldSchema("1b", innerSchema1)) ;
        list1.add(new FieldSchema("1c", DataType.LONG)) ;

        List<FieldSchema> list2 = new ArrayList<FieldSchema>() ;
        list2.add(new FieldSchema("2a", DataType.CHARARRAY)) ;
        list2.add(new FieldSchema("2b", innerSchema2)) ;
        list2.add(new FieldSchema("2c", DataType.INTEGER)) ;
        list2.add(new FieldSchema("2d", DataType.MAP)) ;

        Schema schema1 = new Schema(list1) ;
        Schema schema2 = new Schema(list2) ;

        // Merge
        Schema mergedSchema = Schema.mergeSchema(schema1,
                                                 schema2,
                                                 true,
                                                 true,
                                                 true) ;


        // Generate expected schema
        List<FieldSchema> expectedInnerList = new ArrayList<FieldSchema>() ;
        expectedInnerList.add(new FieldSchema("22a", DataType.BYTEARRAY)) ;
        expectedInnerList.add(new FieldSchema("11b", DataType.FLOAT)) ;
        expectedInnerList.add(new FieldSchema("11c", DataType.CHARARRAY)) ;

        Schema expectedInner = new Schema(expectedInnerList) ;

        List<FieldSchema> expectedList = new ArrayList<FieldSchema>() ;
        expectedList.add(new FieldSchema("2a", DataType.BYTEARRAY)) ;
        expectedList.add(new FieldSchema("2b", expectedInner)) ;
        expectedList.add(new FieldSchema("2c", DataType.LONG)) ;
        expectedList.add(new FieldSchema("2d", DataType.MAP)) ;

        Schema expected = new Schema(expectedList) ;

        // Compare
        Assert.assertTrue(Schema.equals(mergedSchema, expected, false, false)) ;
    }
    
    @Test
    public void testSchemaEqualTwoLevelAccess() throws Exception {
        
        List<FieldSchema> innerList1 = new ArrayList<FieldSchema>() ;
        innerList1.add(new FieldSchema("11a", DataType.INTEGER)) ;
        innerList1.add(new FieldSchema("11b", DataType.LONG)) ;
        
        List<FieldSchema> innerList2 = new ArrayList<FieldSchema>() ;
        innerList2.add(new FieldSchema("11a", DataType.INTEGER)) ;
        innerList2.add(new FieldSchema("11b", DataType.LONG)) ;
        
        Schema innerSchema1 = new Schema(innerList1) ;
        Schema innerSchema2 = new Schema(innerList2) ;
                
        List<FieldSchema> list1 = new ArrayList<FieldSchema>() ;
        list1.add(new FieldSchema("1a", DataType.BYTEARRAY)) ;
        list1.add(new FieldSchema("1b", innerSchema1)) ;
        list1.add(new FieldSchema("1c", DataType.INTEGER)) ;
        
        List<FieldSchema> list2 = new ArrayList<FieldSchema>() ;
        list2.add(new FieldSchema("1a", DataType.BYTEARRAY)) ;
        list2.add(new FieldSchema("1b", innerSchema2)) ;
        list2.add(new FieldSchema("1c", DataType.INTEGER)) ;
        
        Schema schema1 = new Schema(list1) ;
        Schema schema2 = new Schema(list2) ;        
      
        Schema.FieldSchema bagFs1 = new Schema.FieldSchema("b", schema1, DataType.BAG);
        Schema bagSchema1 = new Schema(bagFs1);
        
        Schema.FieldSchema tupleFs = new Schema.FieldSchema("t", schema2, DataType.TUPLE);
        Schema bagSchema = new Schema(tupleFs);
        bagSchema.setTwoLevelAccessRequired(true);
        Schema.FieldSchema bagFs2 = new Schema.FieldSchema("b", bagSchema, DataType.BAG);
        Schema bagSchema2 = new Schema(bagFs2);

        
        Assert.assertTrue(Schema.equals(bagSchema1, bagSchema2, false, false)) ;

        innerList2.get(1).alias = "pi" ;

        Assert.assertFalse(Schema.equals(bagSchema1, bagSchema2, false, false)) ;
        Assert.assertTrue(Schema.equals(bagSchema1, bagSchema2, false, true)) ;
        
        innerList2.get(1).alias = "11b" ;
        innerList2.get(1).type = DataType.BYTEARRAY ;
        
        Assert.assertFalse(Schema.equals(bagSchema1, bagSchema2, false, false)) ;
        Assert.assertTrue(Schema.equals(bagSchema1, bagSchema2, true, false)) ;
        
        innerList2.get(1).type = DataType.LONG ;
        
        Assert.assertTrue(Schema.equals(bagSchema1, bagSchema2, false, false)) ;
        
        list2.get(0).type = DataType.CHARARRAY ;
        Assert.assertFalse(Schema.equals(bagSchema1, bagSchema2, false, false)) ;
    }

}
