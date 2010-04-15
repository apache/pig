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

public class TestSelectPartition extends OwlTestCase{

    private static String dbname = "testpartitiondb";
    private static String tableName = "testpartitiontab";

    private static OwlTableName name = new OwlTableName(dbname, tableName);
    private static OwlDriver driver;

    static List<OwlKeyValue> keyValues;
    static OwlKeyValue k1, k2, k3;
    static int count = 1;

    private static boolean initialized = false;
    private static int numTestsRemaining = 6;

    public TestSelectPartition() {
    }

    private static OwlLoaderInfo _instantiateOwlLoaderInfo(int i) throws OwlException{
        if ((i % 2)==0){
            return new OwlLoaderInfo("test");
        }else{
            return new OwlLoaderInfo("test",Integer.toString(i));
        }
    }

    @SuppressWarnings("boxing")
    static void publish(int v1, String v2, int v3) throws OwlException {
        String deLocation = "hdfs://localhost/data/testdatabase/1234";
        k1.setIntValue(v1);
        k2.setStringValue(v2);
        k3.setIntValue(v3);

        OwlSchema schema1 = new OwlSchema();
        schema1.addColumnSchema(new OwlColumnSchema("c1", ColumnType.INT));
        schema1.addColumnSchema(new OwlColumnSchema("c2", ColumnType.STRING));
        driver.publish(name, keyValues, null, deLocation +  count, schema1, _instantiateOwlLoaderInfo(v1));
        count++;
    }

    public static void initialize() throws OwlException {
        if (!initialized){
            driver = new OwlDriver(getUri());

            DriverVerificationUtil.dropOwlTableIfExists(driver, name);
            DriverVerificationUtil.dropOwlDatabaseIfExists(driver, dbname);

            driver.createOwlDatabase(new OwlDatabase(dbname, "hdfs://localhost:9000/data/testdatabase"));

            OwlSchema schema = new OwlSchema();
            schema.addColumnSchema(new OwlColumnSchema("date", ColumnType.LONG));

            OwlTable table = new OwlTableBuilder().
            setName(name).
            addPartition("part1", DataType.INT).
            addPartition("part2", DataType.STRING).
            addPartition("part3", DataType.INT).
            setSchema(schema).
            build();

            driver.createOwlTable(table);

            keyValues = new ArrayList<OwlKeyValue>();
            k1 = new OwlKeyValue("part1", 1);
            k2 = new OwlKeyValue("part2", "aaa");
            k3 = new OwlKeyValue("part3", 3);
            keyValues.add(k1);
            keyValues.add(k2);
            keyValues.add(k3);

            publish(1, "aaa", 3);
            publish(1, "bbb", 3);
            publish(1, "bbb", 4);
            publish(2, "aaa", 5);
            publish(2, "bbb", 4);

            initialized = true;
        }
    }

    public static void runSelect(String filter, String partitionKeyName, int expectedCount) throws OwlException {
        System.out.println("Filter : " + filter + " Key : " + partitionKeyName + " ExpectedCount : " + expectedCount);

        List<OwlPartition> partitions = driver.getPartitions(name, filter, partitionKeyName);
        assertNotNull(partitions);
        assertEquals(expectedCount, partitions.size());

        for (OwlPartition ptn : partitions){
            OwlLoaderInfo loaderInfo = ptn.getLoader();
            if (ptn.isLeaf()){
                assertNotNull(loaderInfo);
                for (OwlKeyValue kv:ptn.getKeyValues()){
                    if (kv.getKeyName().equalsIgnoreCase("part1")){
                        _verifyPartitionOwlLoaderInfo(loaderInfo, kv);
                    }
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
    public static void testSelectAll() throws OwlException {
        initialize();
        runSelect(null, null, 5);
        runSelect("", null, 5);
        cleanup();
    }

    @Test
    public static void testSelectPartitionLevel1() throws OwlException {
        initialize();
        runSelect(null, "part1", 2);
        runSelect("part1 = 1", "part1", 1);
        cleanup();
    }

    @Test
    public static void testSelectPartitionLevel2() throws OwlException {
        initialize();
        runSelect(null, "part2", 4);
        runSelect("part1 = 1", "part2", 2);
        cleanup();
    }

    @Test
    public static void testSelectPartitionLevel3() throws OwlException {
        initialize();
        runSelect(null, "part3", 5);
        runSelect("part1 = 1", "part3", 3);
        cleanup();
    }

    @Test
    public static void testSelectLeaf() throws OwlException {
        initialize();
        runSelect("(part1 = 1) and (part2=\"aaa\")", null, 1);
        runSelect("(part1 = 1 or part1 = 2) and part2=\"aaa\"", null, 2);
        runSelect("(part1 = 1 and (part2=\"bbb\" or part3 = 4)) or (part1=2 and (part2 = \"bbb\" or part3 = 4))", null, 3);
        runSelect("(part1 < 2) and (part2 >= \"aaa\")", null, 3);
        runSelect("part1 = 1", null, 3);
        cleanup();
    }

    @Test
    public static void testSelectNotEquals() throws OwlException {
        initialize();
        runSelect("part1 != 1", null, 2);
        runSelect("part1 != 1", "part2", 2);
        runSelect("(part1 != 1) or (part2 != \"bbb\")", "part2", 3);
        runSelect("(part1 != 1) and (part2 != \"aaa\")", null, 1);
    }

    @Test
    public static void testDropPartition() throws OwlException {
        initialize();
        driver.dropPartition(name, keyValues); //drop the last inserted partition

        //Only four partitions remain after drop
        runSelect(null, null, 4);
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
