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

import org.apache.hadoop.owl.OwlTestCase;
import org.apache.hadoop.owl.common.ErrorType;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.common.OwlUtil;
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
import org.junit.Test;

public class TestOwlSchema extends OwlTestCase{

    private static String dbname = "testowlschemadb";
    private static String tableName = "testowlschematab";

    private static OwlTableName name = new OwlTableName(dbname, tableName);
    private static OwlDriver driver;

    static List<OwlKeyValue> keyValues;
    static OwlKeyValue k1, k2, k3;
    static int count = 1;

    static OwlSchema partitionSchema1 = new OwlSchema();
    static OwlSchema partitionSchema2 = new OwlSchema();
    static OwlSchema partitionSchema3 = new OwlSchema();

    private static boolean initialized = false;
    private static int numTestsRemaining = 1;

    public TestOwlSchema() {
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
        k1.setIntValue(v1);
        k2.setStringValue(v2);
        k3.setIntValue(v3);
        // TODO if we change the deLocation to "testselectloc" + count, data nucleus would throw an exception, 
        // "JDOQL Single-String query has a subquery clause without a closing parenthesis". -- we think this is a bug in data nucleus
        driver.publish(name, keyValues, null, "hdfs://dummylocation/1234" + count, inputSchema, _instantiateOwlLoaderInfo(v1));
        count++;
    }

    public static void initialize() throws OwlException {
        if (!initialized){
            driver = new OwlDriver(getUri());

            DriverVerificationUtil.dropOwlTableIfExists(driver, name);
            DriverVerificationUtil.dropOwlDatabaseIfExists(driver, dbname);

            driver.createOwlDatabase(new OwlDatabase(dbname, "hdfs://localhost:9000/data/testdatabase"));

            OwlSchema tableSchema = new OwlSchema();
            tableSchema.addColumnSchema(new OwlColumnSchema("date", ColumnType.LONG));

            OwlTable table = new OwlTableBuilder().
            setName(name).
            addPartition("part1", DataType.INT).
            addPartition("part2", DataType.STRING).
            addPartition("part3", DataType.INT).
            setSchema(tableSchema).
            build();

            driver.createOwlTable(table);

            keyValues = new ArrayList<OwlKeyValue>();
            k1 = new OwlKeyValue("part1", 1);
            k2 = new OwlKeyValue("part2", "aaa");
            k3 = new OwlKeyValue("part3", 3);
            keyValues.add(k1);
            keyValues.add(k2);
            keyValues.add(k3);

            // test schema -- c1:collection(r1:record(i1:int))
            OwlSchema subSubSchema = new OwlSchema();
            subSubSchema.addColumnSchema(new OwlColumnSchema("i1", ColumnType.INT));
            OwlSchema subSchema = new OwlSchema();
            subSchema.addColumnSchema(new OwlColumnSchema("r1", ColumnType.RECORD, subSubSchema));
            partitionSchema1.addColumnSchema(new OwlColumnSchema("c1", ColumnType.COLLECTION, subSchema));

            publish(1, "aaa", 3, partitionSchema1);

            // test schema -- c2:collection(record(i1:long)) consolidation with partitionSchema1 should succeed
            OwlSchema subSubSchema2 = new OwlSchema();
            subSubSchema2.addColumnSchema(new OwlColumnSchema("i1", ColumnType.LONG));
            OwlSchema subSchema2 = new OwlSchema();
            subSchema2.addColumnSchema(new OwlColumnSchema(ColumnType.RECORD, subSubSchema2));
            partitionSchema2.addColumnSchema(new OwlColumnSchema("c2", ColumnType.COLLECTION, subSchema2));
            publish(1, "bbb", 3, partitionSchema2);

            // test schema -- c1:collection(record(i1:int )) consolidation with partitionSchema1 consolidation with partitionSchema1 should *fail*
            OwlSchema subSubSchema3 = new OwlSchema();
            subSubSchema3.addColumnSchema(new OwlColumnSchema("i1", ColumnType.INT));
            OwlSchema subSchema3 = new OwlSchema();
            subSchema3.addColumnSchema(new OwlColumnSchema(ColumnType.RECORD, subSubSchema3));
            partitionSchema3.addColumnSchema(new OwlColumnSchema("c1", ColumnType.COLLECTION, subSchema3));

            try{
                publish(1, "bbb", 4, partitionSchema3);
            }catch(OwlException e){
                if (e.getErrorCode() == ErrorType.ZEBRA_SCHEMA_EXCEPTION.getErrorCode()){
                    // good. this is a negative test case
                    System.out.println("ErrorType is : " + e.getErrorCode());
                }else{
                    throw e;
                }
            }
            //            publish(1, "bbb", 4);
            //            publish(2, "aaa", 5);
            //            publish(2, "bbb", 4);

            initialized = true;
        }
    }

    public static void runSelect(String filter, String partitionKeyName, OwlSchema expectedSchema) throws OwlException {
        System.out.println("Filter : " + filter + " Key : " + partitionKeyName + " ExpectedSchema : " + expectedSchema.getSchemaString());

        List<OwlPartition> partitions = driver.getPartitions(name, filter, partitionKeyName);
        assertNotNull(partitions);

        for (OwlPartition ptn : partitions){
            if (ptn.isLeaf()){
                // only leaf level partition has data schema
                assertEquals(expectedSchema.getSchemaString(), (ptn.getSchema()).getSchemaString());
            }else{
                assertFalse("selected schema is not leaf.  only leaf schema can have data schema", false);
            }
        }
    }

    @Test
    public static void testSelectLeaf() throws OwlException {

        initialize();
        runSelect("(part1 = 1) and (part2=\"aaa\") and (part3 = 3)", null, partitionSchema1);
        runSelect("(part1 = 1) and (part2=\"bbb\") and (part3 = 3)", null, partitionSchema2);

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
