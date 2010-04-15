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
import org.apache.hadoop.owl.common.OwlUtil;
import org.apache.hadoop.owl.protocol.OwlDatabase;
import org.apache.hadoop.owl.protocol.OwlKey;
import org.apache.hadoop.owl.protocol.OwlKeyValue;
import org.apache.hadoop.owl.protocol.OwlKey.DataType;
import org.apache.hadoop.owl.protocol.OwlObject;
import org.apache.hadoop.owl.protocol.OwlPropertyKey;
import org.apache.hadoop.owl.protocol.OwlResultObject;
import org.apache.hadoop.owl.protocol.OwlTable;
import org.junit.Test;

public class OwlTableTest extends OwlTestCase {

    private static OwlClient client;
    private static String dbLocation = "\"http://owltabletestlocation\"";

    public OwlTableTest() {
    }

    @Test
    public static void testInitialize() {
        client = new OwlClient(getUri());
        try {
            try {
                client.execute("drop owldatabase dbowltableclienttest");
            } catch (Exception e) {
            } // cleanup of database
            client
            .execute("create owldatabase dbowltableclienttest identified by storage directory " + dbLocation);
        } catch (OwlException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static void createOwlTable(String owlTableName, String databaseName,
            String propertyKeyName, String type) throws OwlException {
        try {
            // System.out.println("Owl Table Name " + owlTableName +
            // "database name " + databaseName + " property key name " +
            // propertyKeyName + " Type " + type );
            System.out.println("Owl Table name " + owlTableName
                    + " within owldatabase dbowltableclienttest");

            client.execute("create owltable type basic " + owlTableName
                    + " within owldatabase dbowltableclienttest " + " define property key "
                    + propertyKeyName + " : " + type + " schema \"c1:collection(r1:record(f1:int, f2:int))\"");

            OwlResultObject result = client
            .execute("select OwlTable objects where owldatabase in ("
                    + "dbowltableclienttest" + ")");
            assertNotNull(result);
            @SuppressWarnings("unchecked") List<OwlTable> output = (List<OwlTable>) result.getOutput();
            OwlTable owlTable = output.get(0);
            System.out.println(OwlUtil.getJSONFromObject(owlTable));
            OwlResultObject descOwlTable = client.execute("Describe OWLTABLE " + owlTableName + " WITHIN OWLDATABASE " + databaseName);
            System.out.println(OwlUtil.getJSONFromObject(descOwlTable));
            assertNotNull(output);

            assertEquals(1, output.size());
            assertTrue(owlTableName.equalsIgnoreCase( owlTable.getName().getTableName() ));
            assertTrue(databaseName.equalsIgnoreCase( owlTable.getName().getDatabaseName() ));
            assertEquals(1, owlTable.getPropertyKeys().size());

            // select every database in the system
            OwlResultObject result2 = client
            .execute("select OwlTable objects ");
            assertNotNull(result2);
            @SuppressWarnings("unchecked") List<OwlTable> output2 = (List<OwlTable>) result.getOutput();
            OwlTable owlTable2 = output2.get(0);
            System.out.println(OwlUtil.getJSONFromObject(owlTable2));

            assertEquals(1, output2.size());
            assertTrue(owlTableName.equalsIgnoreCase( owlTable2.getName().getTableName() ));
            assertTrue(databaseName.equalsIgnoreCase( owlTable2.getName().getDatabaseName() ));
            assertEquals(1, owlTable2.getPropertyKeys().size());

            // Creating a list for property keys of owl table. This doesnt work
            // as of now.
            /*
                    List<OwlPropertyKey> owlPropertyKeys = owlTable.getPropertyKeys();
                    OwlPropertyKey key = owlPropertyKeys.get(0);
                    String keyName = key.getName();

                    assertEquals(propertyKeyName, keyName);
             */
        }

        finally {
            client.execute("drop owlTable " + owlTableName
                    + " within owldatabase dbowltableclienttest");
        }

    }

    @Test
    public static void testCreateOwlTable() throws OwlException {
        createOwlTable("OwlTableClientTest", "dbowltableclienttest", "feed", "INTEGER");
    }

    @Test
    public static void testCleanup() throws OwlException {
        client.execute("drop owldatabase dbowltableclienttest");

    }
}
