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

import org.apache.hadoop.owl.OwlTestCase;
import org.apache.hadoop.owl.common.ErrorType;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.protocol.ColumnType;
import org.apache.hadoop.owl.protocol.OwlColumnSchema;
import org.apache.hadoop.owl.protocol.OwlDatabase;
import org.apache.hadoop.owl.protocol.OwlSchema;
import org.apache.hadoop.owl.protocol.OwlTable;
import org.apache.hadoop.owl.protocol.OwlTableName;
import org.apache.hadoop.owl.protocol.OwlKey.DataType;
import org.apache.hadoop.owl.testdriver.CompareOwlObjects;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestNestedSchema {

    private static String dbname = "testDriverNestedSchemaDB";
    private static String tableName = "testDriverNestedSchemaTab";

    private static OwlTableName name = new OwlTableName(dbname, tableName);
    private static OwlDriver driver;

    //MAP types cannot be used in this test since server currently adds a default schema for MAP causing compare to fail

    private static void runSchemaTest(OwlSchema inputSchema) throws Exception {

        OwlTable table = new OwlTableBuilder().
        setName(name).
        addPartition("part1", DataType.INT).
        setSchema(inputSchema).
        build();

        driver.createOwlTable(table);

        OwlTable outputTable = driver.getOwlTable(name);
        OwlSchema outputSchema = outputTable.getSchema();

        CompareOwlObjects.compareObjects(outputSchema, inputSchema);

        driver.dropOwlTable(outputTable);
    }

    @BeforeClass
    public static void setUp() throws OwlException {
        driver = new OwlDriver(OwlTestCase.getUri());

        DriverVerificationUtil.dropOwlTableIfExists(driver,name);
        DriverVerificationUtil.dropOwlDatabaseIfExists(driver,dbname);

        driver.createOwlDatabase(new OwlDatabase(dbname, "hdfs://localhost:9000/data/testdatabase"));
    }

    @Test
    public void testNestedScehma1() throws Exception {
        OwlSchema schema = new OwlSchema();
        schema.addColumnSchema(new OwlColumnSchema("part1", ColumnType.INT));
        schema.addColumnSchema(new OwlColumnSchema("c1", ColumnType.INT));

        runSchemaTest(schema);
    }

    @Test
    public void testNestedScehma2() throws Exception {
        OwlSchema schema = new OwlSchema();
        schema.addColumnSchema(new OwlColumnSchema("part1", ColumnType.INT));
        schema.addColumnSchema(new OwlColumnSchema("c1", ColumnType.INT));
        schema.addColumnSchema(new OwlColumnSchema("c2", ColumnType.BYTES));

        runSchemaTest(schema);
    }

    @Test
    public void testNestedScehma3() throws Exception {
        OwlSchema subSubSchema = new OwlSchema();
        subSubSchema.addColumnSchema(new OwlColumnSchema("c11", ColumnType.INT));

        OwlSchema subSchema = new OwlSchema();
        subSchema.addColumnSchema(new OwlColumnSchema("r1", ColumnType.RECORD, subSubSchema));

        OwlSchema schema = new OwlSchema();
        schema.addColumnSchema(new OwlColumnSchema("part1", ColumnType.INT));
        schema.addColumnSchema(new OwlColumnSchema("c1", ColumnType.COLLECTION, subSchema));

        runSchemaTest(schema);
    }

    @Test
    public void testNestedScehma4() throws Exception {
        OwlSchema subSchema1 = new OwlSchema();
        subSchema1.addColumnSchema(new OwlColumnSchema("c111", ColumnType.BYTES));

        OwlSchema subSchema2 = new OwlSchema();
        subSchema2.addColumnSchema(new OwlColumnSchema("c11", ColumnType.RECORD, subSchema1));

        OwlSchema schema = new OwlSchema();
        schema.addColumnSchema(new OwlColumnSchema("part1", ColumnType.INT));
        schema.addColumnSchema(new OwlColumnSchema("c1", ColumnType.COLLECTION, subSchema2));
        schema.addColumnSchema(new OwlColumnSchema("c2", ColumnType.INT));

        runSchemaTest(schema);
    }

    @Test
    public void testRecordError() throws Exception {
        OwlSchema schema = new OwlSchema();
        schema.addColumnSchema(new OwlColumnSchema("c1", ColumnType.RECORD));

        OwlException exc = null;
        try {
            runSchemaTest(schema);
        }catch(OwlException e) {
            exc = e;
        }

        Assert.assertNotNull(exc);
        Assert.assertEquals(ErrorType.ERROR_MISSING_SUBSCHEMA, exc.getErrorType());
    }

    @Test
    public void testCollectionError() throws Exception {
        OwlSchema schema = new OwlSchema();
        schema.addColumnSchema(new OwlColumnSchema("c1", ColumnType.COLLECTION));

        OwlException exc = null;
        try {
            runSchemaTest(schema);
        }catch(OwlException e) {
            exc = e;
        }

        Assert.assertNotNull(exc);
        Assert.assertEquals(ErrorType.ERROR_MISSING_SUBSCHEMA, exc.getErrorType());
    }

    @Test
    public void testSchemaAlter() throws Exception {

        OwlSchema schema = new OwlSchema();
        schema.addColumnSchema(new OwlColumnSchema("part1", ColumnType.INT));
        schema.addColumnSchema(new OwlColumnSchema("c1", ColumnType.INT));

        OwlTable table = new OwlTableBuilder().
        setName(name).
        addPartition("part1", DataType.INT).
        setSchema(schema).
        build();

        driver.createOwlTable(table);

        OwlSchema newSchema = new OwlSchema();
        newSchema.addColumnSchema(new OwlColumnSchema("t1", ColumnType.INT));
        newSchema.addColumnSchema(new OwlColumnSchema("part1", ColumnType.INT));

        driver.alterOwlTable(AlterOwlTableCommand.createForSetSchema(name, newSchema));

        OwlTable outputTable = driver.getOwlTable(name);
        OwlSchema outputSchema = outputTable.getSchema();
        CompareOwlObjects.compareObjects(outputSchema, newSchema);

        driver.dropOwlTable(outputTable);
    }

    @AfterClass
    public static void tearDown() throws OwlException {
        driver.dropOwlDatabase(new OwlDatabase(dbname, null));
    }
}
