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
import org.apache.hadoop.owl.protocol.OwlKeyValue;
import org.apache.hadoop.owl.protocol.OwlPartition;
import org.apache.hadoop.owl.protocol.OwlPropertyKey;
import org.apache.hadoop.owl.protocol.OwlTable;
import org.apache.hadoop.owl.protocol.OwlTableName;
import org.apache.hadoop.owl.protocol.OwlKey.DataType;
import org.apache.hadoop.owl.protocol.OwlKey.KeyType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * 
 * Test add/modify/drop property keys.
 *
 */
public class TestAlterTable {
    private static String serverUrl = OwlTestCase.getUri();
    //private static String serverUrl = "http://localhost:8080/owl/rest";
    private static String dbName = "dbAlterTable";
    private static String tableName = "tableAlterTable";

    private static String partKey1 = "partitionKey1_string";
    private static String partKey2 = "partitionKey2_int";
    private static String partKey1Value = "us";
    private static int partKey2Value = 2;

    private static String propKey1 = "propertyKey1_string";
    private static String propKey2 = "propertyKey2_int";
    private static String propKey1Value = "prop key value 1";
    private static int propKey2Value = 3;


    private  static OwlClient client = null;
    private static OwlDriver driver = null;

    public TestAlterTable() {
    }

    static void dropTable(String tableName, String dbName) throws OwlException { 
        client.execute( "drop owltable " + tableName + " within owldatabase " + dbName  );
    }

    static void dropDatabase(String dbName) throws OwlException {
        client.execute( "drop owldatabase " + dbName );
    }

    static void createDatabase(String dbName) throws OwlException {
        client.execute( "create owldatabase " + dbName +
        " identified by storage directory \"dir\"" );
    }

    static void createTable(String tableName, String dbName) throws OwlException {
        String cmd = "create owltable type basic " + tableName + " within owldatabase " + dbName +
        " partitioned by LIST with partition key " + partKey1 + ":string " +
        " partitioned by LIST with partition key " + partKey2 + ":int" +
        " schema \"f1:float, f2:string\" loader \"abcloader\"";
        System.out.println( "cmd> " + cmd );
        client.execute( cmd );
    }

    void publishData() throws OwlException {
        String cmd = "publish dataelement to owltable " + tableName + " within owldatabase " +
        dbName + " partition ( " + partKey1 + " = \"" + partKey1Value + "\", " + partKey2 + " = " + partKey2Value + " ) " + 
        " schema \"f1:float, f2:string\" delocation \"location\"" +
        " loader \"abcloader\"";
        System.out.println( "cmd> " + cmd );
        client.execute( cmd );
    }

    void deletePropertyKeys(boolean tableLevel) throws OwlException {
        List<OwlKeyValue> partitionKeyValues = new ArrayList<OwlKeyValue>(2);
        OwlKeyValue kv = new OwlKeyValue();

        if( !tableLevel ) {
            kv.setKeyName( partKey1 );
            kv.setStringValue( partKey1Value );
            kv.setDataType( DataType.STRING );
            partitionKeyValues.add( kv );
            kv = new OwlKeyValue();
            kv.setKeyName( partKey2 );
            kv.setIntValue( partKey2Value );
            kv.setDataType( DataType.INT );
            partitionKeyValues.add( kv );
        }

        List<OwlKeyValue> propertyKeyValues = new ArrayList<OwlKeyValue>(2);
        kv = new OwlKeyValue();
        kv.setKeyName( propKey1 );
        kv.setStringValue( propKey1Value );
        kv.setDataType( DataType.STRING );
        propertyKeyValues.add( kv );
        kv = new OwlKeyValue();
        kv.setKeyName( propKey2 );
        kv.setIntValue( propKey2Value );
        kv.setDataType( DataType.INT );
        propertyKeyValues.add( kv );

        AlterOwlTableCommand cmd = AlterOwlTableCommand.createForDeletePropertyKeyValue(
                new OwlTableName( dbName, tableName ), partitionKeyValues, propertyKeyValues );

        String commandStr = cmd.buildCommand();
        System.out.println( "cmd> " + commandStr );
        client.execute( commandStr );
    }

    void modifyPropertyKeys(boolean tableLevel) throws OwlException {
        List<OwlKeyValue> partitionKeyValues = new ArrayList<OwlKeyValue>(2);
        OwlKeyValue kv = new OwlKeyValue();
        if( !tableLevel ) {
            kv.setKeyName( partKey1 );
            kv.setStringValue( partKey1Value );
            kv.setDataType( DataType.STRING );
            partitionKeyValues.add( kv );
            kv = new OwlKeyValue();
            kv.setKeyName( partKey2 );
            kv.setIntValue( partKey2Value );
            kv.setDataType( DataType.INT );
            partitionKeyValues.add( kv );
        }

        List<OwlKeyValue> propertyKeyValues = new ArrayList<OwlKeyValue>(2);
        kv = new OwlKeyValue();
        kv.setKeyName( propKey1 );
        kv.setStringValue( propKey1Value );
        kv.setDataType( DataType.STRING );
        propertyKeyValues.add( kv );
        kv = new OwlKeyValue();
        kv.setKeyName( propKey2 );
        kv.setIntValue( propKey2Value );
        kv.setDataType( DataType.INT );
        propertyKeyValues.add( kv );

        AlterOwlTableCommand cmd = AlterOwlTableCommand.createForUpdatePropertyKeyValue(
                new OwlTableName( dbName, tableName ), partitionKeyValues, propertyKeyValues );

        String commandStr = cmd.buildCommand();
        System.out.println( "cmd> " + commandStr );       
        client.execute( commandStr );
    }

    @BeforeClass
    public static void setUp() throws OwlException {
        client = new OwlClient( serverUrl );
        driver = new OwlDriver( serverUrl );
        try {
            dropTable( tableName, dbName );
        } catch(OwlException ex) {}

        try {
            dropDatabase( dbName );
        } catch(OwlException ex) {}

        // create db and table
        createDatabase( dbName );
        createTable( tableName, dbName );

        verifyPropertyKeyCount( 0 );
        addPropertyKeys();
    }

    @AfterClass
    public static void tearDown() throws OwlException {
        dropTable( tableName, dbName );
        dropDatabase( dbName );
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

    private static void verifyPropertyKeyCount(int count) throws OwlException {
        OwlTableName tn = new OwlTableName( dbName, tableName );
        OwlTable ot = driver.getOwlTable( tn );
        List<OwlPropertyKey> propKeys = ot.getPropertyKeys();
        Assert.assertEquals( count, propKeys.size() );
    }

    private static void verifyPropertyKeyValues(boolean tableLevel) throws OwlException {
        OwlTableName tn = new OwlTableName( dbName, tableName );
        OwlTable ot = driver.getOwlTable( tn );

        List<OwlKeyValue> kvs = null;
        if( tableLevel )
            kvs = ot.getPropertyValues();
        else {
            String filter = partKey1 + " = \"" + partKey1Value + "\" AND " +
            partKey2 + " = " + partKey2Value;
            List<OwlPartition> partitions = driver.getPartitions( tn, filter );
            Assert.assertEquals( 1, partitions.size() );
            kvs = partitions.get( 0 ).getKeyValues();
        }

        int match = 0;
        for( OwlKeyValue kv : kvs ) {
            if( kv.getKeyType() != KeyType.PROPERTY )
                continue;

            if( kv.getKeyName().equalsIgnoreCase(propKey1) )  {
                match++;
                Assert.assertEquals( kv.getStringValue(), propKey1Value );
            }
            else if( kv.getKeyName().equalsIgnoreCase( propKey2 ) ) {
                match++;
                Assert.assertEquals( kv.getIntValue().intValue(), propKey2Value );
            } else
                Assert.fail( "Unexpected property." );
        }
        Assert.assertEquals( match, 2 );
    }

    private void verifyPropertyKeyValueCount(int expected, boolean tableLevel) throws OwlException {
        OwlTableName tn = new OwlTableName( dbName, tableName );
        OwlTable ot = driver.getOwlTable( tn );

        List<OwlKeyValue> kvs = null;
        if( tableLevel )
            kvs = ot.getPropertyValues();
        else {
            String filter = partKey1 + " = \"" + partKey1Value + "\" AND " +
            partKey2 + " = " + partKey2Value;
            List<OwlPartition> partitions = driver.getPartitions( tn, filter );
            Assert.assertEquals( 1, partitions.size() );

            kvs = partitions.get( 0 ).getKeyValues();
        }

        int match = 0;
        for( OwlKeyValue kv : kvs ) {
            if( kv.getKeyType() != KeyType.PROPERTY )
                continue;

            if( kv.getKeyName().equalsIgnoreCase(propKey1) )  {
                match++;
            }
            else if( kv.getKeyName().equalsIgnoreCase( propKey2 ) ) {
                match++;
            } else
                Assert.fail( "Unexpected property." );
        }
        Assert.assertEquals( match, 0 );
    }

    @Test
    public void testAddProperty() throws OwlException {
        verifyPropertyKeyCount( 2 );
    }

    @Test
    public void testPartitionLevel() {
        try {
            publishData();
            modifyPropertyKeys( false );
            verifyPropertyKeyValues( false );
            deletePropertyKeys( false );
            verifyPropertyKeyValueCount( 0, false );
        } catch(OwlException ex) {
            System.out.println( "Test case failed with exception: " + ex );
            Assert.fail( "Test failed due to exception: " + ex );
        }
    }

    @Test
    public void testTableLevel() {
        try {
            modifyPropertyKeys( true );
            verifyPropertyKeyValues( true );
            deletePropertyKeys( true );
            verifyPropertyKeyValueCount( 0, true );
        } catch(OwlException ex) {
            System.out.println( "Test case failed with exception: " + ex );
            Assert.fail( "Test failed due to exception: " + ex );
        }
    }

}
