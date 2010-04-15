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
import org.apache.hadoop.owl.client.OwlClient;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.protocol.OwlResultObject;
import org.apache.hadoop.owl.protocol.OwlTable;
import org.apache.hadoop.owl.protocol.OwlTableName;
import org.apache.hadoop.owl.testdriver.CompareOwlObjects;
import org.junit.Test;

public class TestDriverOwlTable extends OwlTestCase {


    private static OwlClient client;
    private static OwlDriver driver;

    private static boolean initialized = false;

    public TestDriverOwlTable() {
    }

    public static void initialize() throws OwlException {
        if (!initialized){
            System.out.println("GREG " + new Exception().getStackTrace()[0].getMethodName());
            client = new OwlClient(getUri());
            driver = new OwlDriver(getUri());

            DriverVerificationUtil.dropOwlTableIfExists(client, "testbounds", "testdriverdatabase");
            DriverVerificationUtil.dropOwlTableIfExists(client, "testinterval", "testdriverdatabase");
            DriverVerificationUtil.dropOwlTableIfExists(client, "testpropkey", "testdriverdatabase");
            DriverVerificationUtil.dropOwlDatabaseIfExists(client, "testdriverdatabase");

            client.execute("create owldatabase testdriverdatabase identified by storage directory \"testdriverdatabase\"");
            initialized = true;
        }
    }

    public static void owlTableCreateTest(String tableName, String createCommand) throws Exception {
        initialize();
        DriverVerificationUtil.dropOwlTableIfExists(client, tableName, "testdriverdatabase");

        //Create the table using client API.
        //Fetch the created OwlTable. Validate the string definition against original input
        //Create and drop owltable using driver interface

        try {
            client.execute(createCommand);

            OwlResultObject result = client.execute("describe owltable " + tableName + " within owldatabase testdriverdatabase");

            assertEquals(1, result.getOutput().size());

            OwlTable table = (OwlTable) result.getOutput().get(0);

            String definition = OwlTableHandler.getTableDefinition(table);

            System.out.println("Command    " + createCommand);
            System.out.println("Definition " + definition);

            //For interval partitioning, the original input timezone is lost in translation (since everything is saved as GMT),
            //So only GMT timezone can be used in these tests 
            assertTrue(createCommand.equalsIgnoreCase( definition ));

            driver.dropOwlTable(table);
            driver.createOwlTable(table);

            OwlTable driverTable = driver.getOwlTable(new OwlTableName("testdriverdatabase", tableName) ); 

            CompareOwlObjects.compareObjects(table, driverTable);
        } finally {
            client.execute("drop owltable " + tableName + " within owldatabase testdriverdatabase");
        }
    }

    @Test
    public static void testBoundedList() throws Exception {
        initialize();
        owlTableCreateTest("testbounds", "create owltable type basic testbounds " +
                "within owldatabase testdriverdatabase partitioned by LIST (\"us\", \"uk\") with partition key country : STRING " +
                "partitioned by LIST (\"CA\", \"AZ\") with partition key state : STRING " +
                "partitioned by LIST (94000, 94001) with partition key zip : INTEGER " +
        "define property key prk1 : INTEGER define property key continent : STRING schema \"country:string,state:string,zip:int\"");
    }

    @Test
    public static void testInterval() throws Exception {
        initialize();
        owlTableCreateTest("testinterval", "create owltable type basic testinterval " +
                "within owldatabase testdriverdatabase " +
                "partitioned by INTERVAL (\"2009-10-09 18:00:00 GMT\", 15 MINUTES) " +
        "with partition key datest partitioned by LIST with partition key part2 : INTEGER schema \"datest:long,part2:int\"");
    }

    @Test
    public static void testPropKey() throws Exception {
        initialize();
        owlTableCreateTest("testpropkey", "create owltable type basic testpropkey within owldatabase testdriverdatabase " +
                "partitioned by LIST with partition key part1 : INTEGER " + 
                "partitioned by LIST with partition key part2 : INTEGER " +
                "partitioned by LIST with partition key part3 : INTEGER " +
                "define property key prk3 : INTEGER " +
                "define property key prk1 : INTEGER " +
                "define property key prk11 : INTEGER " +
                "define property key prkot : INTEGER " +
        "schema \"part1:int,part2:int,part3:int\"");
    }   
}
