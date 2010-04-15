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

package org.apache.hadoop.owl.client;

import java.util.List;

import org.apache.hadoop.owl.OwlTestCase;
import org.apache.hadoop.owl.client.OwlClient;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.protocol.OwlObject;
import org.apache.hadoop.owl.protocol.OwlResultObject;
import org.apache.hadoop.owl.protocol.OwlTable;
import org.junit.Test;

public class PartitionedOwlTableTest extends OwlTestCase {

    private static OwlClient client;
    private static String dbLocation = "\"http://dblocationpartionedowltabletest\"";

    public PartitionedOwlTableTest() {
    }

    @Test
    public static void testInitialize() {
        client = new OwlClient(getUri());
        try {
            try {
                client.execute("drop owldatabase testdatabase1");
            } catch (Exception e) {
            } // cleanup of database
            client
            .execute("create owldatabase testdatabase1 identified by storage directory " + dbLocation);
        } catch (OwlException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /*
     * create owltable type basic foods within owldatabase fooca define property key
     * feed : INTEGER partitioned by LIST with partition key country : STRING
     * define property key color : STRING shape : STRING partitioned by INTERVAL
     * with partition key datestamp : STRING
     */

    public static void createPartitionedOwlTable(String owlTableName,
            String databaseName, String propertyKeyName, String type,
            String partitionKey, String partitionKeyType,
            String partPropertyKey1, String partPropertyKey1Type,
            String partPropertyKey2, String partPropertyKey2Type)
    throws OwlException {
        try {
            // System.out.println("Owl Table Name " + owlTableName +
            // "database name " + databaseName + " property key name " +
            // propertyKeyName + " Type " + type );
            System.out.println("Owl Table name " + owlTableName
                    + " within owldatabase testdatabase1");

            client.execute("create owltable type basic " + owlTableName
                    + " within owldatabase testdatabase1 " + " define property key "
                    + propertyKeyName + " : " + type
                    + " partitioned by LIST with partition key " + partitionKey
                    + " : " + partitionKeyType + " define property key "
                    + partPropertyKey1 + " : " + partPropertyKey1Type + " , "
                    + partPropertyKey2 + " : " + partPropertyKey2Type
                    + " schema \"c1:collection(r1:record(f1:int, f2:int))\"");

            OwlResultObject result = client
            .execute("select OwlTable objects where owldatabase in ("
                    + "testdatabase1" + ")");

            assertNotNull(result);
            List<? extends OwlObject> output = result.getOutput();
            OwlTable owlTable = (OwlTable) output.get(0);
            assertNotNull(output);

            assertEquals(1, output.size());
            assertTrue(owlTableName.equalsIgnoreCase( owlTable.getName().getTableName()));
            assertTrue(databaseName.equalsIgnoreCase(owlTable.getName().getDatabaseName()));
            assertEquals(3, owlTable.getPropertyKeys().size());

            //List <OwlPartitionKey> partitionKeys = owlTable.getPartitionKeys();
            //OwlPartitionKey partitionKeyName =  partitionKeys.get(0);
            //String partKeyName = partitionKeyName.getName();
            //assertEquals("country", partKeyName);
            // Creating a list for property keys of owl table. This doesnt work
            // as of now.
            //List<OwlPropertyKey> owlPropertyKeys = owlTable.getPropertyKeys();
            //OwlPropertyKey key = (OwlPropertyKey) owlPropertyKeys.get(0);
            //String keyName = key.getName();
            //assertEquals(propertyKeyName, keyName);

        }

        finally {
            client.execute("drop owlTable " + owlTableName
                    + " within owldatabase testdatabase1");
        }

    }

    @Test
    public static void testCreateOwlTable() throws OwlException {
        createPartitionedOwlTable("testOwlTable", "testdatabase1", "feed",
                "INTEGER", "country", "STRING", "color", "STRING", "shape",
        "STRING");
    }

    @Test
    public static void testCleanup() throws OwlException {
        client.execute("drop owldatabase testdatabase1");

    }
}
