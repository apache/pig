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

import java.util.Arrays;

import org.apache.hadoop.owl.OwlTestCase;
import org.apache.hadoop.owl.client.OwlClient;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.protocol.ColumnType;
import org.apache.hadoop.owl.protocol.OwlColumnSchema;
import org.apache.hadoop.owl.protocol.OwlSchema;
import org.apache.hadoop.owl.protocol.OwlTable;
import org.apache.hadoop.owl.protocol.OwlTableName;
import org.apache.hadoop.owl.protocol.OwlKey.DataType;
import org.apache.hadoop.owl.protocol.OwlPartitionKey.IntervalFrequencyUnit;
import org.apache.hadoop.owl.testdriver.CompareOwlObjects;
import org.junit.Test;

public class TestOwlTableBuilder extends OwlTestCase {

    private static OwlClient client;
    private static OwlDriver driver;

    private static boolean initialized = false;

    public TestOwlTableBuilder() {
    }

    public static void initialize() throws OwlException {
        if (!initialized){
            client = new OwlClient(getUri());
            driver = new OwlDriver(getUri());

            DriverVerificationUtil.dropOwlDatabaseIfExists(client, "testdriverdatabase");

            client.execute("create owldatabase testdriverdatabase identified by storage directory \"testdriverdatabase\"");
            initialized = true;
        }
    }

    public static void owlTableCreateTest(String tableName) throws Exception {
        initialize();

        DriverVerificationUtil.dropOwlTableIfExists(client, tableName, "testdriverdatabase");
        try {

            OwlSchema schema = new OwlSchema();
            schema.addColumnSchema(new OwlColumnSchema("date", ColumnType.LONG));
            schema.addColumnSchema(new OwlColumnSchema("region", ColumnType.STRING));
            schema.addColumnSchema(new OwlColumnSchema("zip", ColumnType.INT));

            OwlTable table = new OwlTableBuilder().
            setName(new OwlTableName("testdriverdatabase", tableName)).
            addProperty("size", DataType.INT).
            addIntervalPartition("date", "2009-10-09 18:00:00 PDT", 15, IntervalFrequencyUnit.MINUTES).
            addProperty("timezone", DataType.STRING).
            addPartition("region", DataType.STRING, Arrays.asList(new String[] { "us", "uk" })).
            addProperty("continent", DataType.STRING).
            addPartition("zip", DataType.INT).
            setSchema(schema).
            build();

            driver.createOwlTable(table);

            OwlTable driverTable = driver.getOwlTable(new OwlTableName("testdriverdatabase", tableName) ); 

            CompareOwlObjects.compareObjects(table, driverTable);

            assertEquals(driverTable.getPartitionKeys().size(), 3);
            assertEquals(driverTable.getPropertyKeys().size(), 3);
        } finally {
            client.execute("drop owltable " + tableName + " within owldatabase testdriverdatabase");
        }
    }

    @Test
    public static void testBoundedList() throws Exception {
        initialize();
        owlTableCreateTest("testtable");
    }

}
