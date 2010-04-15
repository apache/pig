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
import org.junit.Test;


public class DatabaseTest extends OwlTestCase {

    public static final int CONNECTION_RETRY_COUNT = 5;
    public static final long RETRY_SLEEP_MILLIS = 2000;

    private static OwlClient client;

    public DatabaseTest() {
    }

    public static void testInitialize() throws OwlException {
        OwlException connectException = null;

        //This is the first test which connects to the web server. The server might not be ready. Retry for 5 times 
        //if there is a connection error.
        for(int i = 0; i < CONNECTION_RETRY_COUNT;i++) {
            try {
                client = new OwlClient(getUri());
                client.execute("select owldatabase objects");
                connectException = null;
                break;
            } catch(OwlException e) {
                if( e.getErrorType() != ErrorType.ERROR_SERVER_CONNECTION && e.getErrorType() != ErrorType.ERROR_DB_INIT ) {
                    throw e;
                } else {
                    connectException = e;
                    long retryTime = RETRY_SLEEP_MILLIS * (i + 1);
                    System.out.println("Connection failed " + e.toString() + ". Retrying after " + retryTime + " ms");
                    try { Thread.sleep(retryTime) ; } catch(Exception e2) {}
                }
            }
        }

        if( connectException != null ) {
            throw connectException;
        }
    }


    public static void createDatabase(String databaseName, String location) throws OwlException {
        try {

            try { 
                client.execute("drop owldatabase " + databaseName);
            } catch(Exception e) {}

            System.out.println("database name " + databaseName + " location " + location );

            client.execute("create owldatabase " + databaseName + " identified by storage directory \"" + location + "\"" );

            OwlResultObject result = client.execute("select owldatabase objects where owldatabase in (" + databaseName + ")");
            assertNotNull(result);

            List<? extends OwlObject> output = result.getOutput();
            assertNotNull(output);
            assertEquals(1, output.size());
            OwlDatabase database = (OwlDatabase) output.get(0);

            assertTrue(databaseName.equalsIgnoreCase( database.getName() ));
            assertEquals(location, database.getStorageLocation());

            // select all the databases in the system
            OwlResultObject result2 = client.execute("select owldatabase objects ");
            assertNotNull(result2);

            List<? extends OwlObject> output2 = result.getOutput();
            assertNotNull(output2);
            assertEquals(1, output2.size());
            OwlDatabase database2 = (OwlDatabase) output2.get(0);

            assertTrue(databaseName.equalsIgnoreCase( database2.getName() ));
            assertEquals(location, database2.getStorageLocation());

        } finally {
            client.execute("drop owldatabase " + databaseName);

        }
    }

    @Test
    public static void testCreateDatabase() throws OwlException {
        testInitialize();
        createDatabase("ctestcat1", "testloc");
        createDatabase("ctestcat2", "test loc"); //fails currently
        //createDatabase("'ctestcat3 name'", "testloc");
        testCleanup();
    }

    public static void testCleanup() {
        try {
            client.execute("drop owldatabase ctestcat1");
        } catch(Exception e) {}

        try { 
            client.execute("drop owldatabase ctestcat2");
        } catch(Exception e){}
    }

}
