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


import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.owl.OwlTestCase;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.protocol.ColumnType;
import org.apache.hadoop.owl.protocol.OwlColumnSchema;
import org.apache.hadoop.owl.protocol.OwlDatabase;
import org.apache.hadoop.owl.protocol.OwlKeyValue;
import org.apache.hadoop.owl.protocol.OwlLoaderInfo;
import org.apache.hadoop.owl.protocol.OwlPartition;
import org.apache.hadoop.owl.protocol.OwlSchema;
import org.apache.hadoop.owl.protocol.OwlTable;
import org.apache.hadoop.owl.protocol.OwlTableName;
import org.apache.hadoop.owl.protocol.OwlKey.DataType;
import org.apache.hadoop.owl.schema.ParseException;
import org.apache.hadoop.owl.schema.Schema;
import org.junit.Test;

public class TestMultiplePartitions extends OwlTestCase{

    private static String dbname = "testmultiplepartitiondb";
    private static String tableName = "testmultiplepartitiontable";

    private static OwlTableName name = new OwlTableName(dbname, tableName);
    private static OwlDriver driver;

    static List<OwlKeyValue> keyValues;
    static OwlKeyValue kvps1, kvps2, kvps3;
    static int count = 1;

    static OwlSchema tableSchema, partSchemaP1, partSchemaP2, partSchemaP3;

    private static boolean initialized = false;
    private static int numTestsRemaining = 2;

    public TestMultiplePartitions() {
    }

    private static OwlLoaderInfo _instantiateOwlLoaderInfo(int i) throws OwlException{
        if ((i % 2)==0){
            return new OwlLoaderInfo("test");
        }else{
            return new OwlLoaderInfo("test",Integer.toString(i));
        }
    }

    @SuppressWarnings("boxing")
    static void publish(int v1, String v2, int v3, OwlSchema inputSchema) throws OwlException {
        kvps1.setIntValue(v1);
        kvps2.setStringValue(v2);
        kvps3.setIntValue(v3);

        driver.publish(name, keyValues, null, "hdfs://dummytestlocation/1234" + count, inputSchema, _instantiateOwlLoaderInfo(v1));
        count++;
    }

    public static void initialize() throws OwlException {
        if (!initialized){
            driver = new OwlDriver(getUri());

            DriverVerificationUtil.dropOwlTableIfExists(driver, name);
            DriverVerificationUtil.dropOwlDatabaseIfExists(driver, dbname);

            driver.createOwlDatabase(new OwlDatabase(dbname, "hdfs://localhost:9000/data/testdatabase"));

            // table schema
            OwlSchema tableSchema = new OwlSchema();
            tableSchema.addColumnSchema(new OwlColumnSchema("NAME", ColumnType.STRING));
            tableSchema.addColumnSchema(new OwlColumnSchema("SEX", ColumnType.STRING));
            tableSchema.addColumnSchema(new OwlColumnSchema("AGE", ColumnType.INT));

            // create owl table
            OwlTable table = new OwlTableBuilder()
            .setName(name)
            .addPartition("part1", DataType.INT)
            .addPartition("part2", DataType.STRING)
            .addPartition("part3", DataType.INT)
            .setSchema(tableSchema)
            .build();

            driver.createOwlTable(table);


            // partitionSchema for part1
            partSchemaP1 = new OwlSchema();
            partSchemaP1.addColumnSchema(new OwlColumnSchema("NAME", ColumnType.STRING));
            partSchemaP1.addColumnSchema(new OwlColumnSchema("SEX", ColumnType.STRING));
            partSchemaP1.addColumnSchema(new OwlColumnSchema("AGE", ColumnType.INT));
            partSchemaP1.addColumnSchema(new OwlColumnSchema("SCORE", ColumnType.FLOAT));

            // subSubschemas
            OwlSchema subSubSchema = new OwlSchema();
            subSubSchema.addColumnSchema(new OwlColumnSchema("f1", ColumnType.INT));
            subSubSchema.addColumnSchema(new OwlColumnSchema("f2", ColumnType.STRING));

            // subSchema
            OwlSchema subSchema = new OwlSchema();
            subSchema.addColumnSchema(new OwlColumnSchema("r1", ColumnType.RECORD,subSubSchema));

            // partitionSchema for part2
            partSchemaP2 = new OwlSchema();
            partSchemaP2.addColumnSchema(new OwlColumnSchema("c10", ColumnType.COLLECTION, subSchema));
            partSchemaP2.addColumnSchema(new OwlColumnSchema("c1", ColumnType.INT));

            // partitionSchema for part3
            partSchemaP3 = new OwlSchema();
            partSchemaP3.addColumnSchema(new OwlColumnSchema("c3", ColumnType.INT));
            partSchemaP3.addColumnSchema(new OwlColumnSchema("c2", ColumnType.STRING));
            partSchemaP3.addColumnSchema(new OwlColumnSchema("c1", ColumnType.INT));


            // publish partitions
            // set the key for partition key value pairs here
            keyValues = new ArrayList<OwlKeyValue>();
            kvps1 = new OwlKeyValue("part1", 1);
            kvps2 = new OwlKeyValue("part2", "aaa");
            kvps3 = new OwlKeyValue("part3", 3);

            keyValues.add(kvps1);
            keyValues.add(kvps2);
            keyValues.add(kvps3);

            publish(1, "aaa", 3, partSchemaP1);
            publish(2, "aaa", 5, partSchemaP2);
            publish(3, "bbb", 4, partSchemaP3);
            initialized = true;
        }
    }

    @Test
    public static void testRunSelectOwlTable() throws OwlException {
        initialize();
        OwlTable owlTable = driver.getOwlTable(name);
        OwlSchema schema = owlTable.getSchema();
        System.out.println("owltable Schema is " + schema.getSchemaString());

        Assert.assertTrue
        (
                "part1:int,part2:string,part3:int,name:string,sex:string,age:int,score:float,c10:collection(r1:record(f1:int,f2:string)),c1:int,c3:int,c2:string".equalsIgnoreCase( 
                        schema.getSchemaString() )
        );
        cleanup();
    }

    public static void runSelectLeafPartition(String filter, int expectedCount, String expectedSchema) throws OwlException {
        System.out.println("Filter : " + filter + " ExpectedCount : " + expectedCount);

        List<OwlPartition> partitions = driver.getPartitions(name, filter);
        assertNotNull(partitions);
        assertEquals(expectedCount, partitions.size());

        for (OwlPartition ptn : partitions){
            OwlLoaderInfo loaderInfo = ptn.getLoader();

            // make sure the partition is a leaf level one
            Assert.assertTrue (ptn.isLeaf());
            // make sure the schemaString is not null
            Assert.assertTrue(ptn.getSchema().getSchemaString() != null);

            System.out.println("[orange]partition level schema is " + ptn.getSchema().getSchemaString() );
            Assert.assertEquals(expectedSchema, ptn.getSchema().getSchemaString());

            // verify loader info.
            assertNotNull(loaderInfo);
            for (OwlKeyValue kv:ptn.getKeyValues()){
                if (kv.getKeyName().equalsIgnoreCase("part1")){
                    _verifyPartitionOwlLoaderInfo(loaderInfo, kv);
                }
            }
        }
    }

    private static void _verifyPartitionOwlLoaderInfo(OwlLoaderInfo loaderInfo,
            OwlKeyValue kv) {
        int i = kv.getIntValue().intValue();
        if ((i % 2) == 0){
            assertNull(loaderInfo.getInputDriverArgs());
        }else {
            assertNotNull(loaderInfo.getInputDriverArgs());
            assertEquals(kv.getIntValue().toString(),loaderInfo.getInputDriverArgs());
        }
    }

    @Test
    public static void testSelectLeafPartitionLevel() throws OwlException {
        initialize();
        runSelectLeafPartition("part1 = 1", 1, partSchemaP1.getSchemaString());
        runSelectLeafPartition("part1 = 2", 1, partSchemaP2.getSchemaString());
        runSelectLeafPartition("part1 = 3", 1, partSchemaP3.getSchemaString());
        cleanup();
    }

    public static void cleanup() throws OwlException {
        numTestsRemaining--;
        if (numTestsRemaining  == 0){
            initialize();
            driver.dropOwlTable(new OwlTable(name));
            driver.dropOwlDatabase(new OwlDatabase(dbname, null));
        }
    }
}
