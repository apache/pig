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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.ResourceSchema;
import org.apache.pig.SortColInfo;
import org.apache.pig.SortInfo;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.SchemaMergeException;
import org.apache.pig.test.utils.TypeCheckingTestUtil;
import org.junit.Test;

public class TestResourceSchema {

    /**
     * Test that ResourceSchema is correctly created given a
     * pig.Schema and vice versa 
     */
    @Test
    public void testResourceFlatSchemaCreation() 
    throws ExecException, SchemaMergeException, FrontendException {
        String [] aliases ={"f1", "f2"};
        byte[] types = {DataType.CHARARRAY, DataType.INTEGER};
        Schema origSchema = TypeCheckingTestUtil.genFlatSchema(
                aliases,types);
        ResourceSchema rsSchema = new ResourceSchema(origSchema);
        assertEquals("num fields", aliases.length, rsSchema.getFields().length);
        ResourceSchema.ResourceFieldSchema[] fields = rsSchema.getFields();
        for (int i=0; i<fields.length; i++) {
            assertEquals(fields[i].getName(), aliases[i]);
            assertEquals(fields[i].getType(), types[i]);
        }
        Schema genSchema = Schema.getPigSchema(rsSchema);
        assertTrue("generated schema equals original", 
                Schema.equals(genSchema, origSchema, true, false));
    }
    
    /**
     * Test that ResourceSchema is correctly created given a
     * pig.Schema and vice versa 
     */
    @Test
    public void testResourceFlatSchemaCreation2() 
    throws ExecException, SchemaMergeException, FrontendException {
        String [] aliases ={"f1", "f2"};
        byte[] types = {DataType.CHARARRAY, DataType.INTEGER};
        
        Schema origSchema = new Schema(
                new Schema.FieldSchema("t1", 
                        new Schema(
                                new Schema.FieldSchema("t0", 
                                        TypeCheckingTestUtil.genFlatSchema(
                                                aliases,types), 
                                                DataType.TUPLE)), DataType.BAG));
                        
        ResourceSchema rsSchema = new ResourceSchema(origSchema);

        Schema genSchema = Schema.getPigSchema(rsSchema);
        assertTrue("generated schema equals original", 
                Schema.equals(genSchema, origSchema, true, false));
    }
    
    /**
     * Test that ResourceSchema is correctly with SortInfo
     */
    @Test
    public void testResourceFlatSchemaCreationWithSortInfo() 
    throws ExecException, SchemaMergeException, FrontendException {
        String [] aliases ={"f1", "f2"};
        byte[] types = {DataType.CHARARRAY, DataType.INTEGER};
        
        Schema origSchema = new Schema(
                new Schema.FieldSchema("t1", 
                        new Schema(
                                new Schema.FieldSchema("t0", 
                                        TypeCheckingTestUtil.genFlatSchema(
                                                aliases,types), 
                                                DataType.TUPLE)), DataType.BAG));
        List<SortColInfo> colList = new ArrayList<SortColInfo>();
        SortColInfo col1 = new SortColInfo("f1", 0, SortColInfo.Order.ASCENDING);
        SortColInfo col2 = new SortColInfo("f1", 1, SortColInfo.Order.DESCENDING);
        colList.add(col1);
        colList.add(col2);
        SortInfo sortInfo = new SortInfo(colList);
                        
        ResourceSchema rsSchema = new ResourceSchema(origSchema, sortInfo);

        Schema genSchema = Schema.getPigSchema(rsSchema);
        assertTrue("generated schema equals original", 
                Schema.equals(genSchema, origSchema, true, false));
        assertTrue(rsSchema.getSortKeys()[0]==0);
        assertTrue(rsSchema.getSortKeys()[1]==1);
        assertTrue(rsSchema.getSortKeyOrders()[0]==ResourceSchema.Order.ASCENDING);
        assertTrue(rsSchema.getSortKeyOrders()[1]==ResourceSchema.Order.DESCENDING);
    }
    
    /**
     * Test that Pig Schema is correctly created given a
     * ResourceSchema and vice versa. Test also that 
     * TwoLevelAccess flag is set for Pig Schema when needed.
     * @throws IOException 
     */
    @Test
    public void testToPigSchemaWithTwoLevelAccess() throws IOException {
        ResourceFieldSchema[] level0 = 
            new ResourceFieldSchema[] {
                new ResourceFieldSchema()
                    .setName("fld0").setType(DataType.CHARARRAY),
                new ResourceFieldSchema()
                    .setName("fld1").setType(DataType.DOUBLE),
                new ResourceFieldSchema()
                    .setName("fld2").setType(DataType.INTEGER)
        };
               
        ResourceSchema rSchema0 = new ResourceSchema()
            .setFields(level0);
        
        ResourceFieldSchema[] level1 = 
            new ResourceFieldSchema[] {
                new ResourceFieldSchema()
                    .setName("t1").setType(DataType.TUPLE)
                    .setSchema(rSchema0)
        };
        
        ResourceSchema rSchema1 = new ResourceSchema()
            .setFields(level1);
        
        ResourceFieldSchema[] level2 = 
            new ResourceFieldSchema[] {
                new ResourceFieldSchema()
                    .setName("t2").setType(DataType.BAG)
                    .setSchema(rSchema1)
        };
        
        ResourceSchema origSchema = new ResourceSchema()
            .setFields(level2);        
        
        Schema pSchema = Schema.getPigSchema(origSchema);
                
        assertTrue(CheckTwoLevelAccess(pSchema));
                
        assertTrue(ResourceSchema.equals(origSchema, new ResourceSchema(pSchema)));
    }
    
    private boolean CheckTwoLevelAccess(Schema s) {
        if (s == null) return false;
        for (Schema.FieldSchema fs : s.getFields()) {
            if (fs.type == DataType.BAG 
                    && fs.schema != null
                    && fs.schema.isTwoLevelAccessRequired()) {
                return true;
            }
            if (CheckTwoLevelAccess(fs.schema)) return true;
        }            
        return false;        
    }
    
    /**
     * Test invalid Resource Schema: multiple fields for a bag
     * @throws IOException 
     */
    @Test(expected=FrontendException.class) 
    public void testToPigSchemaWithInvalidSchema() throws IOException {
        ResourceFieldSchema[] level0 = new ResourceFieldSchema[] {
                new ResourceFieldSchema()
                    .setName("fld0").setType(DataType.CHARARRAY),
                new ResourceFieldSchema()
                    .setName("fld1").setType(DataType.DOUBLE),        
                new ResourceFieldSchema()
                    .setName("fld2").setType(DataType.INTEGER)
        };
        
        ResourceSchema rSchema0 = new ResourceSchema()
            .setFields(level0);
        
        ResourceFieldSchema[] level2 = new ResourceFieldSchema[] {
                new ResourceFieldSchema()
                    .setName("t2").setType(DataType.BAG).setSchema(rSchema0)
        };
    }

    /**
     * Test invalid Resource Schema: bag without tuple field
     * @throws IOException 
     */
    @Test(expected=FrontendException.class) 
    public void testToPigSchemaWithInvalidSchema2() throws IOException {
        ResourceFieldSchema[] level0 = new ResourceFieldSchema[] {
                new ResourceFieldSchema()
                    .setName("fld0").setType(DataType.CHARARRAY)
        };
        
        ResourceSchema rSchema0 = new ResourceSchema()
            .setFields(level0);
        
        ResourceFieldSchema[] level2 = new ResourceFieldSchema[] {
                new ResourceFieldSchema()
                    .setName("t2").setType(DataType.BAG).setSchema(rSchema0)
        };
         
    }
    
    /**
     * Test one-level Pig Schema: multiple fields for a bag
     */
    @Test
    public void testResourceSchemaWithInvalidPigSchema() 
    throws FrontendException {
        String [] aliases ={"f1", "f2"};
        byte[] types = {DataType.CHARARRAY, DataType.INTEGER};
        Schema level0 = TypeCheckingTestUtil.genFlatSchema(
                aliases,types);
        Schema.FieldSchema fld0 = 
            new Schema.FieldSchema("f0", level0, DataType.BAG);
        Schema level1 = new Schema(fld0);
        Schema genSchema = Schema.getPigSchema(new ResourceSchema(level1));
        assertTrue(CheckTwoLevelAccess(genSchema));
    }
    
    /**
     * Test one-level Pig Schema: bag without tuple field
     */
    @Test
    public void testResourceSchemaWithInvalidPigSchema2() 
    throws FrontendException {
        String [] aliases ={"f1"};
        byte[] types = {DataType.INTEGER};
        Schema level0 = TypeCheckingTestUtil.genFlatSchema(
                aliases,types);
        Schema.FieldSchema fld0 = 
            new Schema.FieldSchema("f0", level0, DataType.BAG);
        Schema level1 = new Schema(fld0);
        Schema genSchema = Schema.getPigSchema(new ResourceSchema(level1));
        assertTrue(CheckTwoLevelAccess(genSchema));
    }
}
