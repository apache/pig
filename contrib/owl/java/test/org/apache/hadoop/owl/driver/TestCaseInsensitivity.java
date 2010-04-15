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

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.owl.OwlTestCase;
import org.apache.hadoop.owl.client.OwlClient;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.protocol.OwlDatabase;
import org.apache.hadoop.owl.protocol.OwlPartition;
import org.apache.hadoop.owl.protocol.OwlPartitionKey;
import org.apache.hadoop.owl.protocol.OwlPropertyKey;
import org.apache.hadoop.owl.protocol.OwlSchema;
import org.apache.hadoop.owl.protocol.OwlTable;
import org.apache.hadoop.owl.protocol.OwlTableName;
import org.apache.hadoop.owl.protocol.OwlKey.DataType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * 
 * Test add/modify/drop property keys.
 *
 */
public class TestCaseInsensitivity {
    private static String serverUrl = OwlTestCase.getUri();
    //private static String serverUrl = "http://localhost:8080/owl/rest";
    private static String dbName = "tEstDB";
    private static String tableName = "TesTTable";

    private static String partKey1 = "partitionKey1_STR";
    private static String partKey2 = "PartiTionKey2_inT";
    private static String partKey1Value = "us";
    private static int partKey2Value = 2;

    private static String propKey1 = "propertyKey1_str";
    private static String propKey2 = "propertyKey2_InT";

    private static String colName1 = "fIEld1";
    private static String colName2 = "FielD2";

    private  static OwlClient client = null;
    private static OwlDriver driver = null;

    public TestCaseInsensitivity() {
    }

    static void dropDatabase() throws OwlException {
        client.execute( "drop owldatabase " + dbName );
    }

    static void dropTable() throws OwlException {
        client.execute( "drop owltable " + tableName + " within owldatabase " + dbName  );
    }

    static void createDatabase() throws OwlException {
        client.execute( "create owldatabase " + dbName +
        " identified by storage directory \"dir\"" );
    }

    static void createTable() throws OwlException {
        String cmd = "create owltable type basic " + tableName + " within owldatabase " + dbName +
        " partitioned by LIST with partition key " + partKey1 + ":string " +
        " partitioned by LIST with partition key " + partKey2 + ":int" +
        " schema \""+ colName1 + ":float, " + colName2 + ":string\" loader \"abcloader\"";
        System.out.println( "cmd> " + cmd );
        client.execute( cmd );
    }

    static void publishData() throws OwlException {
        String cmd = "publish dataelement to owltable " + tableName + " within owldatabase " +
        dbName + " partition ( " + partKey1 + " = \"" + partKey1Value + "\", " + partKey2 + " = " + partKey2Value + " ) " + 
        " schema \"" + colName1 + ":float, " + colName2 + ":string\" delocation \"location\"" +
        " loader \"abcloader\"";
        System.out.println( "cmd> " + cmd );
        client.execute( cmd );
    }

    @BeforeClass
    public static void setUp() throws OwlException {
        client = new OwlClient( serverUrl );
        driver = new OwlDriver( serverUrl );
        try {
            dropTable();
        } catch(OwlException ex) {}

        try {
            dropDatabase();
        } catch(OwlException ex) {}

        // create db and table, and add some property keys
        createDatabase();
        createTable();
        addPropertyKeys();
        publishData();
    }

    @AfterClass
    public static void tearDown() throws OwlException {
        dropTable();
        dropDatabase();
    }

    private static void addPropertyKeys() throws OwlException {
        OwlTableName tn = new OwlTableName( dbName, tableName );
        List<OwlPropertyKey> pks = new ArrayList<OwlPropertyKey>(2);
        OwlPropertyKey pk = new OwlPropertyKey();
        pk.setName( propKey1 );
        pk.setDataType( DataType.STRING );
        pks.add( pk );
        pk = new OwlPropertyKey();
        pk.setName( propKey2 );
        pk.setDataType( DataType.INT );
        pks.add( pk );
        AlterOwlTableCommand cmd = AlterOwlTableCommand.createForAddPropertyKey( tn, pks );
        String commandStr = cmd.buildCommand();
        System.out.println( "cmd> " + commandStr );       
        client.execute( commandStr );
    }


    @Test
    public void test() {
        try {
            OwlDatabase odb = driver.getOwlDatabase( dbName );
            // Datatbase name should be in lower case.
            Assert.assertTrue( odb.getName().equalsIgnoreCase( dbName ) );

            OwlTable ot = driver.getOwlTable( new OwlTableName(dbName, tableName ) );
            OwlTableName tName = ot.getName();
            // Database name and table name should be in lower case.
            Assert.assertTrue( tName.getDatabaseName().equalsIgnoreCase( dbName ) );
            Assert.assertTrue( tName.getTableName().equalsIgnoreCase( tableName ) );

            // Property key names should be stored in lower case
            List<OwlPropertyKey> propKeys = ot.getPropertyKeys();
            for( OwlPropertyKey key : propKeys ) {
                String keyName = key.getName();
                Assert.assertTrue( keyName.equalsIgnoreCase( propKey1 ) || keyName.equalsIgnoreCase( propKey2 ) );
            }

            // Similary, partition key should be stored in lower case.
            List<OwlPartitionKey> partKeys = ot.getPartitionKeys();
            for( OwlPartitionKey key : partKeys ) {
                String name = key.getName();
                Assert.assertTrue( name.equalsIgnoreCase( partKey1 ) || name.equalsIgnoreCase( partKey2 ) );
            }

            // column name should be in lower case.
            OwlSchema schema = ot.getSchema();
            int count = schema.getColumnCount();
            int match = 0;
            for( int i = 0; i < count; i++ ) {
                String colName = schema.columnAt(i).getName();
                if ((colName != null ) && ( colName.equalsIgnoreCase( colName1 ) || colName.equalsIgnoreCase( colName2 ) ))
                    match++;
            }
            Assert.assertEquals( 2, match );

            // case in search filter string shouldn't matter.
            List<OwlPartition> partitions = driver.getPartitions( 
                    new OwlTableName(dbName, tableName ), partKey1 + " = \"" + partKey1Value + "\"" );
            Assert.assertEquals( partitions.size(), 1 );
            schema = partitions.get( 0 ).getSchema();
            count = schema.getColumnCount();
            // Also, column name should be in lower case at partition level
            match = 0;
            for( int i = 0; i < count; i++ ) {
                String colName = schema.columnAt(i).getName();
                if ((colName != null ) && ( colName.equalsIgnoreCase( colName1 ) || colName.equalsIgnoreCase( colName2 ) ))
                    match++;
            }
            Assert.assertEquals( 2, match );
        } catch(OwlException ex) {
            System.out.println( "Test case failed with exception: " + ex );
            Assert.fail( "Test failed due to exception: " + ex );
        }
    }


}
