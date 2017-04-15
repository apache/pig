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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;

import org.apache.pig.ResourceSchema;
import org.apache.pig.SortColInfo;
import org.apache.pig.SortInfo;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.SchemaMergeException;
import org.apache.pig.impl.util.Utils;
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
                
        assertTrue(!CheckTwoLevelAccess(pSchema));
                
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
        try {
            Schema.getPigSchema(new ResourceSchema(level1));
            Assert.fail();
        } catch(FrontendException e) {
            assertTrue(e.getErrorCode()==2218);
        }
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
        try {
            Schema.getPigSchema(new ResourceSchema(level1));
            Assert.fail();
        } catch (FrontendException e) {
            assertTrue(e.getErrorCode()==2218);
        }
    }

    /**
     * Test that we can turn a schema into a string and then parse it
     * back into a schema.
     */
    @Test
    public void testToStringAndParse() throws Exception {
        ResourceSchema rs = new ResourceSchema();
        ResourceFieldSchema[] fields = new ResourceFieldSchema[13];

        byte[] types = {DataType.INTEGER, DataType.LONG, DataType.FLOAT,
            DataType.DOUBLE, DataType.BYTEARRAY, DataType.CHARARRAY,
            DataType.MAP, DataType.TUPLE, DataType.TUPLE, DataType.BAG,
            DataType.BAG, DataType.BOOLEAN, DataType.DATETIME};
        String[] names = {"i", "l", "f",
            "d", "b", "s",
            "m", "tschema", "tnull", "bschema",
            "bnull", "bb", "dt"};
        
        for (int i = 0; i < fields.length; i++) {
            fields[i] = new ResourceFieldSchema();
            fields[i].setName(names[i]);
            fields[i].setType(types[i]);
        }

        // Add in the schemas for the tuple and the bag
        ResourceSchema tschema = new ResourceSchema();
        ResourceFieldSchema[] tfields = new ResourceFieldSchema[3];
        for (int i = 0; i < 3; i++) {
            tfields[i] = new ResourceFieldSchema();
            tfields[i].setName(names[i]);
            tfields[i].setType(types[i]);
        }
        tschema.setFields(tfields);
        fields[7].setSchema(tschema);

        ResourceSchema bschema = new ResourceSchema();
        ResourceFieldSchema[] bfields = new ResourceFieldSchema[1];
        bfields[0] = new ResourceFieldSchema();
        bfields[0].setName("t");
        bfields[0].setType(DataType.TUPLE);

        ResourceSchema tbschema = new ResourceSchema();
        ResourceFieldSchema[] tbfields = new ResourceFieldSchema[3];
        for (int i = 3; i < 6; i++) {
            tbfields[i-3] = new ResourceFieldSchema();
            tbfields[i-3].setName(names[i]);
            tbfields[i-3].setType(types[i]);
        }
        tbschema.setFields(tbfields);

        bfields[0].setSchema(tbschema);
        bschema.setFields(bfields);

        fields[9].setSchema(bschema);

        rs.setFields(fields);

        String strSchema = rs.toString();

        assertEquals("i:int,l:long,f:float,d:double,b:bytearray,s:" +
            "chararray,m:[],tschema:(i:int,l:long,f:float)," +
            "tnull:(),bschema:{t:(d:double,b:bytearray,s:chararray)},bnull:{},bb:boolean,dt:datetime",
            strSchema);

        ResourceSchema after =
            new ResourceSchema(Utils.getSchemaFromString(strSchema));
        ResourceFieldSchema[] afterFields = after.getFields();

        assertEquals(13, afterFields.length);
        assertEquals("i", afterFields[0].getName());
        assertEquals(DataType.INTEGER, afterFields[0].getType());
        assertEquals("l", afterFields[1].getName());
        assertEquals(DataType.LONG, afterFields[1].getType());
        assertEquals("f", afterFields[2].getName());
        assertEquals(DataType.FLOAT, afterFields[2].getType());
        assertEquals("d", afterFields[3].getName());
        assertEquals(DataType.DOUBLE, afterFields[3].getType());
        assertEquals("b", afterFields[4].getName());
        assertEquals(DataType.BYTEARRAY, afterFields[4].getType());
        assertEquals("s", afterFields[5].getName());
        assertEquals(DataType.CHARARRAY, afterFields[5].getType());
        assertEquals("m", afterFields[6].getName());
        assertEquals(DataType.MAP, afterFields[6].getType());
        assertEquals("tschema", afterFields[7].getName());
        assertEquals(DataType.TUPLE, afterFields[7].getType());
        assertEquals("tnull", afterFields[8].getName());
        assertEquals(DataType.TUPLE, afterFields[8].getType());
        assertEquals("bschema", afterFields[9].getName());
        assertEquals(DataType.BAG, afterFields[9].getType());
        assertEquals("bnull", afterFields[10].getName());
        assertEquals(DataType.BAG, afterFields[10].getType());
        assertEquals("bb", afterFields[11].getName());
        assertEquals(DataType.BOOLEAN, afterFields[11].getType());
        assertEquals("dt", afterFields[12].getName());
        assertEquals(DataType.DATETIME, afterFields[12].getType());
        

        assertNotNull(afterFields[7].getSchema());
        ResourceFieldSchema[] tAfterFields =
            afterFields[7].getSchema().getFields();
        assertEquals(3, tAfterFields.length);
        assertEquals("i", tAfterFields[0].getName());
        assertEquals(DataType.INTEGER, tAfterFields[0].getType());
        assertEquals("l", tAfterFields[1].getName());
        assertEquals(DataType.LONG, tAfterFields[1].getType());
        assertEquals("f", tAfterFields[2].getName());
        assertEquals(DataType.FLOAT, tAfterFields[2].getType());

        assertNotNull(afterFields[9].getSchema());
        ResourceFieldSchema[] bAfterFields =
            afterFields[9].getSchema().getFields();
        assertEquals(1, bAfterFields.length);
        assertNotNull(bAfterFields[0].getSchema());

        ResourceFieldSchema[] tbAfterFields =
            bAfterFields[0].getSchema().getFields();
        assertEquals(3, tbAfterFields.length);
        assertEquals("d", tbAfterFields[0].getName());
        assertEquals(DataType.DOUBLE, tbAfterFields[0].getType());
        assertEquals("b", tbAfterFields[1].getName());
        assertEquals(DataType.BYTEARRAY, tbAfterFields[1].getType());
        assertEquals("s", tbAfterFields[2].getName());
        assertEquals(DataType.CHARARRAY, tbAfterFields[2].getType());
    }
}
