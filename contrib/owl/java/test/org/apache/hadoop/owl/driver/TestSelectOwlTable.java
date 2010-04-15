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
import org.apache.hadoop.owl.common.OwlException;
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

public class TestSelectOwlTable {

    private static String dbname = "tsoDB";
    private static String tableName = "tsoOwltable";
    private static OwlTableName name = new OwlTableName(dbname, tableName);

    private static OwlDriver driver;
    private static OwlTable table;

    public TestSelectOwlTable() {
    }

    @BeforeClass
    public static void setUp() throws OwlException {
        driver = new OwlDriver(OwlTestCase.getUri());
        DriverVerificationUtil.dropOwlTableIfExists(driver, name);
        DriverVerificationUtil.dropOwlDatabaseIfExists(driver, dbname);
        String databaseLocation = "hdfs://localhost:9000/data/dummy";
        driver.createOwlDatabase(new OwlDatabase(dbname, databaseLocation));
    }


    @Test
    public void testSelectEmptyOwlTable() throws OwlException {
        // select a owltable that does exist
        List<OwlTableName> allTables = driver.showOwlTables(dbname, "likestring");
        Assert.assertNotNull(allTables); 
        Assert.assertEquals(0, allTables.size());
    }

    @AfterClass
    public static void tearDown() throws OwlException {

        DriverVerificationUtil.dropOwlTableIfExists(driver, name);
        DriverVerificationUtil.dropOwlDatabaseIfExists(driver, dbname);
    }

}
