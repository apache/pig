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
import org.apache.hadoop.owl.protocol.ColumnType;
import org.apache.hadoop.owl.protocol.OwlColumnSchema;
import org.apache.hadoop.owl.protocol.OwlDatabase;
import org.apache.hadoop.owl.protocol.OwlLoaderInfo;
import org.apache.hadoop.owl.protocol.OwlPartition;
import org.apache.hadoop.owl.protocol.OwlSchema;
import org.apache.hadoop.owl.protocol.OwlTable;
import org.apache.hadoop.owl.protocol.OwlTableName;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestNonPartitionedTable {

    private static String dbname = "testDnonpartitiondb";
    private static String tableName = "testDnonpartitiontab";

    private static OwlTableName name = new OwlTableName(dbname, tableName);
    private static OwlDriver driver;

    private static OwlSchema schema = null;

    @BeforeClass
    public static void setUp() throws OwlException {
        driver = new OwlDriver(OwlTestCase.getUri());

        DriverVerificationUtil.dropOwlTableIfExists(driver,name);
        DriverVerificationUtil.dropOwlDatabaseIfExists(driver,dbname);

        driver.createOwlDatabase(new OwlDatabase(dbname, "hdfs://localhost:9000/data/testNPdatabase"));

        schema = new OwlSchema();
        schema.addColumnSchema(new OwlColumnSchema("c1", ColumnType.INT));

        OwlTable table = new OwlTableBuilder().
        setName(name).
        setSchema(schema).
        build();

        driver.createOwlTable(table);

        driver.publish(name, null, null, "partNPloc", schema, new OwlLoaderInfo("test"));
    }

    @AfterClass
    public static void tearDown() throws OwlException {
        driver.dropOwlTable(new OwlTable(name));
        driver.dropOwlDatabase(new OwlDatabase(dbname, null));
    }

    @Test
    public void testSelectPartition() throws Exception {
        List<OwlPartition> pList = driver.getPartitions(name, null);
        Assert.assertEquals(1, pList.size());
        Assert.assertEquals(pList.get(0).getStorageLocation(), "partNPloc");
    }

    @Test
    public void testDropPartition() throws Exception {
        driver.dropPartition(name, null);

        List<OwlPartition> pList = driver.getPartitions(name, null);
        Assert.assertEquals(0, pList.size());

        driver.publish(name, null, null, "partNPloc", schema, new OwlLoaderInfo("test"));
    }

    @Test
    public void testDuplicatePublish() throws Exception {
        OwlException exc = null;
        try {
            OwlSchema schema = new OwlSchema();
            schema.addColumnSchema(new OwlColumnSchema("c1", ColumnType.INT));

            driver.publish(name, null, null, "partNPloc2", schema, new OwlLoaderInfo("test"));
        } catch(OwlException e ) {
            exc = e;
        }

        Assert.assertNotNull(exc);
        Assert.assertEquals(ErrorType.ERROR_DUPLICATE_PUBLISH, exc.getErrorType());
    }

    @SuppressWarnings("boxing")
    @Test
    public void testDescribe() throws OwlException {
        OwlTable table = driver.getOwlTable(name);
        Assert.assertEquals(name, table.getName());
        Assert.assertEquals(0, table.getPartitionKeys().size());
    }
}
