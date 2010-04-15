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
import org.apache.hadoop.owl.common.ErrorType;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.protocol.OwlObject;
import org.apache.hadoop.owl.protocol.OwlResultObject;
import org.apache.hadoop.owl.protocol.OwlDatabase;
import org.junit.Before;
import org.junit.Test;

public class NegativeCreateDatabaseScenariosTest extends OwlTestCase {

    private static OwlClient client;

    public NegativeCreateDatabaseScenariosTest() {
    }

    @Before
    public static void testInitialize() {
        client = new OwlClient(getUri());
        try {
            client.execute("drop owldatabase cat1");
        } catch (Exception e) {
        }
    }

    /*
     * i. Create a Database without specifying the value for name ii. Create a
     * Database without specifying the name key iii. Create a Database without
     * providing the HDFS Directory value (value for the Storage Dir Key) iv.
     * Create a Database without specifying the STORAGE DIR key
     */

    public static void createDatabaseWithoutDatabaseValue(String databaseName,
            String location) throws OwlException {
        OwlException oe = null;
        try {
            System.out
            .println("Database name without giving value of the database name key");
            System.out.println("database name " + databaseName + " location "
                    + location);
            try {
                client
                .execute("create owldatabase identified by storage directory \""
                        + location + "\"");
            } catch (OwlException e) {
                oe = e;
            }
            assertNotNull(oe);
            assertEquals(ErrorType.PARSE_EXCEPTION.getErrorCode(), oe
                    .getErrorCode());
            assertNotNull(oe.getMessage());
            assertTrue(oe.getMessage().indexOf(
            "org.apache.hadoop.owl.common.OwlException") != -1);
        } finally {
        }
    }

    public static void verifyCreateDatabaseWithoutDatabaseValue(
            String databaseName, String location) throws OwlException {

        OwlException oe = null;

        try {

            OwlResultObject result = client
            .execute("select owldatabase objects where owldatabase in ("
                    + databaseName + ")");
            assertNotNull(result);

            List<? extends OwlObject> output = result.getOutput();
        } catch (OwlException e) {
            oe = e;
        }

        assertNotNull(oe);
        assertEquals(oe.getErrorCode(), ErrorType.ERROR_UNKNOWN_DATABASE
                .getErrorCode());
        assertNotNull(oe.getMessage());
        assertTrue(oe.getMessage().indexOf("Unable to map given database name") != -1);

    }

    public static void createDatabaseWithNullDatabaseValue(String databaseName,
            String location) throws OwlException {
        OwlException oe = null;
        try {
            System.out
            .println("Database name without giving value of the database name key");
            System.out.println("database name " + databaseName + " location "
                    + location);

            client.execute("create owldatabase " + databaseName
                    + " identified by storage directory \"" + location + "\"");

        } catch (OwlException e) {
            oe = e;
        }

        finally {
        }
        assertNotNull(oe);
        assertEquals(oe.getErrorCode(), ErrorType.PARSE_EXCEPTION
                .getErrorCode());
        assertNotNull(oe.getMessage());
        assertTrue(oe.getMessage().indexOf(
        "org.apache.hadoop.owl.common.OwlException") != -1);

    }

    public static void verifyCreateDatabaseWithNullDatabaseValue(
            String databaseName, String location) throws OwlException {
        OwlException oe = null;

        try {
            OwlResultObject result = client
            .execute("select owldatabase objects where owldatabase in ("
                    + databaseName + ")");
            assertNotNull(result);

            List<? extends OwlObject> output = result.getOutput();
            assertNotNull(output);
            assertEquals(0, output.size());

        } catch (OwlException e) {
            oe = e;
        }

        finally {
        }

    }

    public static void createDatabaseWithoutDatabaseKey(String databaseName,
            String location) throws OwlException {
        OwlException oe = null;
        try {
            System.out
            .println("Database name without giving value of the database name key");
            System.out.println("database name " + databaseName + " location "
                    + location);

            client.execute("create " + databaseName
                    + " identified by storage directory \"" + location + "\"");
        } catch (OwlException e) {
            oe = e;
        } finally {
        }

        assertEquals(oe.getErrorCode(), ErrorType.PARSE_EXCEPTION
                .getErrorCode());
        assertNotNull(oe.getMessage());
        assertTrue(oe.getMessage().indexOf(
        "org.apache.hadoop.owl.common.OwlException") != -1);

    }

    public static void verifyCreateDatabaseWithoutDatabaseKey(String databaseName,
            String location) throws OwlException {
        OwlException oe = null;
        try {
            OwlResultObject result = client
            .execute("select owldatabase objects where owldatabase in ("
                    + databaseName + ")");
            assertNotNull(result);

            List<? extends OwlObject> output = result.getOutput();
            assertNotNull(output);

            assertEquals(0, output.size());

        } catch (OwlException e) {
            oe = e;

        }

        assertNotNull(oe);
        assertEquals(oe.getErrorCode(), ErrorType.ERROR_UNKNOWN_DATABASE
                .getErrorCode());
        assertNotNull(oe.getMessage());

    }

    @Test
    public static void testCreateDatabaseWithoutValue() throws OwlException {
        createDatabaseWithoutDatabaseValue("cat1", "testloc");

    }

    @Test
    public static void testVerifyCreateDatabaseWithoutValue()
    throws OwlException {
        verifyCreateDatabaseWithoutDatabaseValue("cat1", "testloc");
    }

    @Test
    public static void testCreateDatabaseWithNullDatabaseValue()
    throws OwlException {

        createDatabaseWithNullDatabaseValue("", "testloc");
    }

    @Test
    public static void testVerifyDatabaseWithNullDatabaseValue()
    throws OwlException {
        verifyCreateDatabaseWithNullDatabaseValue("", "testloc");

    }

    @Test
    public static void testCreateDatabaseWithoutDatabaseKey() throws OwlException {
        createDatabaseWithoutDatabaseKey("cat1", "testloc");
    }

    @Test
    public static void testVerifyDatabaseWithoutDatabaseKey() throws OwlException {
        verifyCreateDatabaseWithoutDatabaseKey("cat1", "testloc");
    }
}
