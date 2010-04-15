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
package org.apache.hadoop.owl.driver;

import java.util.List;

import org.apache.hadoop.owl.OwlTestCase;
import org.apache.hadoop.owl.common.ErrorType;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.common.OwlUtil;
import org.apache.hadoop.owl.common.TestOwlUtil;
import org.apache.hadoop.owl.protocol.ColumnType;
import org.apache.hadoop.owl.protocol.OwlColumnSchema;
import org.apache.hadoop.owl.protocol.OwlDatabase;
import org.apache.hadoop.owl.protocol.OwlSchema;
import org.apache.hadoop.owl.protocol.OwlTable;
import org.apache.hadoop.owl.protocol.OwlTableName;
import org.apache.hadoop.owl.protocol.OwlKey.DataType;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestOwlTable {

    private static String dbname = "testdowltabledb";
    private static String tableName = "testdowltabletab";
    private static OwlTableName name = new OwlTableName(dbname, tableName);
    private static OwlTable table = null;

    private static OwlDriver driver;

    public TestOwlTable() {
    }

    @BeforeClass
    public static void setUp() throws OwlException {
        driver = new OwlDriver(OwlTestCase.getUri());
        DriverVerificationUtil.dropOwlTableIfExists(driver, name);
        DriverVerificationUtil.dropOwlDatabaseIfExists(driver, dbname);
        driver.createOwlDatabase(new OwlDatabase(dbname, "hdfs://localhost:9000/data/testdowltabledb"));

        OwlSchema subSubSchema = new OwlSchema();
        subSubSchema.addColumnSchema(new OwlColumnSchema("i1", ColumnType.INT));
        OwlSchema subSchema = new OwlSchema();
        subSchema.addColumnSchema(new OwlColumnSchema(ColumnType.RECORD, subSubSchema));
        OwlSchema schema = new OwlSchema();
        schema.addColumnSchema(new OwlColumnSchema("c1", ColumnType.COLLECTION, subSchema));


        table = new OwlTableBuilder().
        setName(name).
        addPartition("part1", DataType.INT).
        setSchema(schema).
        build();
        System.out.println("Table level schema is ["+schema.getSchemaString()+"]");
        driver.createOwlTable(table);
    }

    @Test
    public void testOwltableName() throws OwlException {
        // negative test to validate the length of owltable name is not longer than 64 characters
        String owltablename_negativecase = TestOwlUtil.generateLongString(OwlUtil.IDENTIFIER_LIMIT +1);
        OwlTableName name_negativecase = new OwlTableName(dbname, owltablename_negativecase);
        OwlSchema schema = new OwlSchema();
        schema.addColumnSchema(new OwlColumnSchema("i1", ColumnType.STRING));

        table = new OwlTableBuilder().
        setName(name_negativecase).
        addPartition("part1", DataType.INT).
        setSchema(schema).
        build();

        try{
            driver.createOwlTable(table);
            Assert.fail("There is no OwlException thrown.  We are expecting owltable name variable length validation fails");
        }catch(OwlException e){
            Assert.assertEquals(e.getErrorType(), ErrorType.ERROR_IDENTIFIER_LENGTH_VALIDATION_FAILED);
        }
    }

    @Test
    public void testPartitionKeyName() throws OwlException {
        // negative test to validate the length of partition key name is not longer than 64 characters
        String partitionkeyname_negativecase = TestOwlUtil.generateLongString(OwlUtil.IDENTIFIER_LIMIT +1);
        OwlTableName name = new OwlTableName(dbname, "testpartitionkeyname_owltable");
        OwlSchema schema = new OwlSchema();
        schema.addColumnSchema(new OwlColumnSchema("i1", ColumnType.INT));

        table = new OwlTableBuilder().
        setName(name).
        addPartition(partitionkeyname_negativecase, DataType.INT).
        setSchema(schema).
        build();

        try{
            driver.createOwlTable(table);
            Assert.fail("There is no OwlException thrown. We are expecting partition key length validation fails");
        }catch(OwlException e){
            Assert.assertEquals(e.getErrorType(), ErrorType.ERROR_IDENTIFIER_LENGTH_VALIDATION_FAILED);
        }
    }

    @Test
    public void testPropertyKeyName() throws OwlException {
        // negative test to validate the length of property key name is not longer than its limit
        String propertykeyname_negativecase = TestOwlUtil.generateLongString(OwlUtil.IDENTIFIER_LIMIT +1);
        OwlTableName name = new OwlTableName(dbname, "testpropertykeyname_owltable");
        OwlSchema schema = new OwlSchema();
        schema.addColumnSchema(new OwlColumnSchema("i1", ColumnType.INT));

        table = new OwlTableBuilder().
        setName(name).
        addPartition("part1", DataType.INT).
        addProperty(propertykeyname_negativecase, DataType.STRING).
        setSchema(schema).
        build();

        try{
            driver.createOwlTable(table);
            Assert.fail("There is no OwlException thrown.  We are expecting property key length validation fails");
        }catch(OwlException e){
            Assert.assertEquals(e.getErrorType(), ErrorType.ERROR_IDENTIFIER_LENGTH_VALIDATION_FAILED);
        }
    }

    @Test
    public void testDescribe() throws OwlException {
        OwlTable tab = driver.getOwlTable(name);
        Assert.assertEquals(name, tab.getName());
    }

    @Test
    public void testSelect() throws OwlException {
        List<OwlTableName> allTables = driver.showOwlTables(dbname);
        Assert.assertTrue(allTables.contains(name));    
    }

    @SuppressWarnings("null")
    @Test
    public void testInvalidCreate() throws OwlException {
        OwlException exc = null;
        try {
            driver.createOwlTable(null);
        } catch(OwlException e) {
            exc = e;
        }

        Assert.assertNotNull(exc);
        Assert.assertEquals(ErrorType.INVALID_FIELD_VALUE, exc.getErrorType());
    }


    @SuppressWarnings("null")
    @Test
    public void testInvalidGet1() throws OwlException {
        OwlException exc = null;
        try {
            OwlTable tab = driver.getOwlTable(null);
        } catch(OwlException e) {
            exc = e;
        }

        Assert.assertNotNull(exc);
        Assert.assertEquals(ErrorType.INVALID_FIELD_VALUE, exc.getErrorType());
    }

    @SuppressWarnings("null")
    @Test
    public void testInvalidGet2() throws OwlException {
        OwlException exc = null;
        try {
            OwlTable tab = driver.getOwlTable(new OwlTableName(null, null));
        } catch(OwlException e) {
            exc = e;
        }

        Assert.assertNotNull(exc);
        Assert.assertEquals(ErrorType.INVALID_FIELD_VALUE, exc.getErrorType());
    }

    @AfterClass
    public static void tearDown() throws OwlException {
        OwlTable tab = driver.getOwlTable(name);

        driver.dropOwlTable(tab);
        List<OwlTableName> allTables = driver.showOwlTables(dbname);
        Assert.assertFalse(allTables.contains(name));  

        OwlDatabase db = driver.getOwlDatabase(dbname);
        driver.dropOwlDatabase(db);
    }

}
