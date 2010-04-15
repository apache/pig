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

import junit.framework.Assert;

import org.apache.hadoop.owl.OwlTestCase;
import org.apache.hadoop.owl.client.OwlClient;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.protocol.OwlGlobalKey;
import org.apache.hadoop.owl.protocol.OwlKey.DataType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * 
 * Test read access to global keys.
 *
 */
public class TestGlobalKey {
    private static String serverUrl = OwlTestCase.getUri();
    //    private static String serverUrl = "http://localhost:8080/owl/rest";

    private static String globalKey1 = "globalKey1_string";
    private static String globalKey2 = "globalKey2_int";

    private  static OwlClient client = null;
    private static OwlDriver driver = null;

    public TestGlobalKey() {
    }

    private static void dropGlobalKey(String keyName) throws OwlException { 
        client.execute( "drop globalkey " + keyName );
    }

    private static void createGlobalKeys() throws OwlException {
        client.execute( "create globalkey " + globalKey1 + ":string" );
        client.execute( "create globalkey " + globalKey2 + ":int" );
    }

    @BeforeClass
    public static void setUp() throws OwlException {
        client = new OwlClient( serverUrl );
        driver = new OwlDriver( serverUrl );

        try {
            dropGlobalKey( globalKey1 );
            dropGlobalKey( globalKey2 );
        } catch(OwlException ex) {}

        createGlobalKeys();
    }

    @AfterClass
    public static void tearDown() throws OwlException {
        dropGlobalKey( globalKey1 );
        dropGlobalKey( globalKey2 );
    }


    @Test
    public void testFetchGlobalKeys() throws OwlException {
        List<String> keyNames = driver.showGlobalKeys();
        int count = 0;
        for( String keyName : keyNames ) {
            if( keyName.equalsIgnoreCase( globalKey1 ) || keyName.equalsIgnoreCase(globalKey2) ) {
                count++;
            }
        }
        Assert.assertEquals( 2, count );
    }

    @Test
    public void testFetchGlobalKey() throws OwlException {
        OwlGlobalKey gKey = driver.getGlobalKey(globalKey1);
        Assert.assertTrue( gKey.getName().equalsIgnoreCase( globalKey1 ) );
        Assert.assertTrue( gKey.getDataType() == DataType.STRING );
    }

}
