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
import org.junit.Test;

public class TestDriverSelectOwlTableNameLike extends OwlTestCase{

    private static String dbname = "testdriverlikedb";
    private static String tbname = "testdriverliketb";
    private static String liketbstr = "%liketb";
    private static OwlTableName owltbname = new OwlTableName(dbname, tbname);
    private static OwlDriver driver;

    private static boolean initialized = false;
    private static int numTestsRemaining = 3;

    public TestDriverSelectOwlTableNameLike() {
    }

    public static void initialize() throws OwlException {
        if (!initialized){
            driver = new OwlDriver(getUri());

            DriverVerificationUtil.dropOwlTableIfExists(driver, owltbname);
            DriverVerificationUtil.dropOwlDatabaseIfExists(driver, dbname);

            OwlSchema schema = new OwlSchema();
            schema.addColumnSchema(new OwlColumnSchema("c1", ColumnType.INT));

            driver.createOwlDatabase(new OwlDatabase(dbname, "hdfs://localhost:9000/data/testdriverdblikedir"));
            OwlTable table = new OwlTableBuilder().
            setName(owltbname).
            addPartition("part1", DataType.INT).
            setSchema(schema).
            build();

            driver.createOwlTable(table);
            initialized = true;
        }
    }

    @Test
    public static void testfetch() throws OwlException {
        initialize();
        List<OwlTableName> tables = driver.showOwlTables(dbname);
        assertTrue(tables.contains(owltbname));
        cleanup();
    }

    @Test
    public static void testfetchLike() throws OwlException {
        initialize();
        List<OwlTableName> likeTables = driver.showOwlTables(dbname,liketbstr );
        assertTrue(likeTables.contains(owltbname));
        cleanup();
    }

    @Test
    public static void testfetchLikeWithoutdb() throws OwlException {
        initialize();
        List<OwlTableName> likeTables = driver.showOwlTables(null,liketbstr );
        assertTrue(likeTables.contains(owltbname));
        cleanup();
    }

    public static void cleanup() throws OwlException {
        numTestsRemaining --;
        if (numTestsRemaining == 0){
            driver.dropOwlTable(new OwlTable(owltbname));
            driver.dropOwlDatabase(new OwlDatabase(dbname, null));
        }
    }
}
