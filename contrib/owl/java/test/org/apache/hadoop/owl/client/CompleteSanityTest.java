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
import org.apache.hadoop.owl.common.OwlUtil;
import org.apache.hadoop.owl.protocol.OwlDatabase;
import org.apache.hadoop.owl.protocol.OwlDataElement;
import org.apache.hadoop.owl.protocol.OwlKey;
import org.apache.hadoop.owl.protocol.OwlKeyValue;
import org.apache.hadoop.owl.protocol.OwlObject;
import org.apache.hadoop.owl.protocol.OwlPartitionKey;
import org.apache.hadoop.owl.protocol.OwlPartitionProperty;
import org.apache.hadoop.owl.protocol.OwlPropertyKey;
import org.apache.hadoop.owl.protocol.OwlResultObject;
import org.apache.hadoop.owl.protocol.OwlTable;
import org.apache.hadoop.owl.protocol.OwlKey.DataType;
import org.apache.hadoop.owl.protocol.OwlKey.KeyType;
import org.apache.hadoop.owl.logical.SelectPartitionpropertyObjectsCommand;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.owl.client.KeyValueAssertionCriterion;

public class CompleteSanityTest extends OwlTestCase {

    private static OwlClient client;

    public CompleteSanityTest() {
        client = new OwlClient(getUri());

    }

    public static void verifyDataType(String expectedValue, DataType dataType) {
        if( expectedValue.toUpperCase().equals("INTEGER" ) ) {
            assertEquals(dataType, DataType.INT);
        } else {
            assertEquals(dataType.name(), expectedValue);
        }
    }

    @Before
    public static void testInitialize() {
    }

    public void createDatabase(String databaseName, String location)
    throws OwlException {

        try {
            client.execute("drop owldatabase " + databaseName);
        } catch(Exception e){}

        System.out.println("database name " + databaseName + " location "
                + location);

        client.execute("create owldatabase " + databaseName
                + " identified by storage directory \"" + location + "\"");

        verifyCreateDatabase(databaseName, location);

    }

    public void verifyCreateDatabase(String databaseName, String location)
    throws OwlException {

        OwlResultObject result = client
        .execute("select owldatabase objects where owldatabase in ("
                + databaseName + ")");
        assertNotNull(result);

        List<? extends OwlObject> output = result.getOutput();
        assertNotNull(output);
        assertEquals(1, output.size());

        OwlDatabase database = (OwlDatabase) output.get(0);
        assertTrue(databaseName.equalsIgnoreCase(database.getName()));
        assertEquals(location, database.getStorageLocation());

    }

    /*
     * create owltable type basic testOwlTable within owldatabase testDatabase define
     * property key retention : INTEGER partitioned by LIST with partition key
     * country : STRING define property key color : STRING shape : STRING
     */

    public void createPartitionedOwlTable(String owlTableName,
            String databaseName, String propertyKeyName, String type,
            String partitionKey, String partitionKeyType,
            String partPropertyKey1, String partPropertyKey1Type,
            String partPropertyKey2, String partPropertyKey2Type)
    throws OwlException {

        System.out.println("Owl Table name " + owlTableName
                + " within owldatabase " + databaseName);

        client.execute("create owltable type basic " + owlTableName
                + " within owldatabase " + databaseName + " define property key "
                + propertyKeyName + " : " + type
                + " partitioned by LIST with partition key " + partitionKey
                + " : " + partitionKeyType + " define property key "
                + partPropertyKey1 + " : " + partPropertyKey1Type + " , "
                + partPropertyKey2 + " : " + partPropertyKey2Type
                + " schema \"c1:collection(r1:record(f1:int, f2:int))\"");

        verifyCreatePartitionedOwlTable(owlTableName, databaseName,
                propertyKeyName, type, partitionKey, partitionKeyType,
                partPropertyKey1, partPropertyKey1Type, partPropertyKey2,
                partPropertyKey2Type);

    }

    public void verifyCreatePartitionedOwlTable(String owlTableName,
            String databaseName, String propertyKeyName, String type,
            String partitionKey, String partitionKeyType,
            String partPropertyKey1, String partPropertyKey1Type,
            String partPropertyKey2, String partPropertyKey2Type)
    throws OwlException {

        OwlResultObject result = client
        .execute("select OwlTable objects where owldatabase in ("
                + databaseName + ")");

        AssertionsForSelectAndDescribeOwlTable(result, owlTableName,
                databaseName, propertyKeyName, type, partitionKey,
                partitionKeyType, partPropertyKey1, partPropertyKey1Type,
                partPropertyKey2, partPropertyKey2Type);
    }

    public void AssertionsForSelectAndDescribeOwlTable(OwlResultObject result,
            String owlTableName, String databaseName, String propertyKeyName,
            String type, String partitionKey, String partitionKeyType,
            String partPropertyKey1, String partPropertyKey1Type,
            String partPropertyKey2, String partPropertyKey2Type)
    throws OwlException {

        assertNotNull(result);
        List<? extends OwlObject> output = result.getOutput();
        OwlTable owlTable = (OwlTable) output.get(0);

        assertEquals(1, output.size());
        assertTrue(owlTableName.equalsIgnoreCase(owlTable.getName().getTableName()));
        assertTrue(databaseName.equalsIgnoreCase(owlTable.getName().getDatabaseName()));
        assertEquals(3, owlTable.getPropertyKeys().size());

        List<OwlPropertyKey> owlPropertyKeys = owlTable.getPropertyKeys();
        OwlPropertyKey key1 = owlPropertyKeys.get(0);
        OwlPropertyKey key2 = owlPropertyKeys.get(1);
        OwlPropertyKey key3 = owlPropertyKeys.get(2);
        String basePartKey = key1.getName();
        String partKey1 = key2.getName();
        String partKey2 = key3.getName();

        // System.out.println(basePartKey + "\n" + partKey1 + "\n" + partKey2);
        assertNotNull(output);
        assertTrue(propertyKeyName.equalsIgnoreCase( basePartKey ));
        verifyDataType(type, key1.getDataType());

        assertTrue(partPropertyKey1.equalsIgnoreCase( partKey1 ));
        verifyDataType(partPropertyKey1Type, key2.getDataType());
        assertTrue(partPropertyKey2.equalsIgnoreCase( partKey2 ));
        verifyDataType(partPropertyKey2Type, key2.getDataType());

        List<OwlPartitionKey> partitionKeys = owlTable.getPartitionKeys();
        OwlPartitionKey partitionKey1 = partitionKeys.get(0);
        String partKeyName = partitionKey1.getName();
        assertTrue(partitionKey.equalsIgnoreCase( partKeyName ));
        verifyDataType(partitionKeyType, partitionKey1.getDataType());
        assertEquals(partitionKey1.getLevel(), 1);
    }

    public void describeOwlTable(String owlTableName, String databaseName,
            String propertyKeyName, String type, String partitionKey,
            String partitionKeyType, String partPropertyKey1,
            String partPropertyKey1Type, String partPropertyKey2,
            String partPropertyKey2Type) throws OwlException {

        OwlResultObject result = client.execute("describe owltable "
                + owlTableName + " within owldatabase " + databaseName);

        // AssertionsForSelectAndDescribeOwlTable(result, "testOwlTable",
        // "testDatabase", "retention", "INTEGER", "country", "STRING",
        // "color", "STRING", "shape", "STRING");

        AssertionsForSelectAndDescribeOwlTable(result, owlTableName,
                databaseName, propertyKeyName, type, partitionKey,
                partitionKeyType, partPropertyKey1, partPropertyKey1Type,
                partPropertyKey2, partPropertyKey2Type);

    }

    /*
     * {"database":"tcat1","creationTime":1254938491000,"modificationTime":1254938491000
     * ,"name":"testOwlTable","partitionKeys":[{"dataType":"STRING","level":1,
     * "listValues"
     * :[],"name":"country","partitioningType":"LIST"}],"propertyKeys"
     * :[{"dataType"
     * :"INTEGER","level":0,"name":"retention"},{"dataType":"STRING"
     * ,"level":1,"name"
     * :"color"},{"dataType":"STRING","level":1,"name":"shape"}]
     * ,"propertyValues":[]}
     */
    public void verifyDescribeOwlTable(OwlResultObject result,
            String owlTableName, String databaseName) throws OwlException {

        List<? extends OwlObject> output = result.getOutput();
        OwlTable owlTable = (OwlTable) output.get(0);
        assertEquals(owlTableName, owlTable.getName());
        assertEquals(databaseName, owlTable.getName().getDatabaseName());

    }

    public void publishDataElement(String owlTableName, String databaseName,
            String propertyKeyName, Integer propertyKeyValue,
            String propertyKeyType, String partitionKeyName,
            String partitionKeyValue, String partitionKeyType,
            String partPropertyKeyName1, String partPropertyKeyValue1,
            String partPropertyKeyType1, String partPropertyKeyName2,
            String partPropertyKeyValue2, String partPropertyKeyType2,
            String delocation) throws OwlException {

        client.execute("publish dataelement to owltable " + owlTableName
                + " within owldatabase " + databaseName + " partition ( "
                + partitionKeyName + " = \"" + partitionKeyValue
                + "\") property (" + propertyKeyName + " = " + propertyKeyValue
                + ", " + partPropertyKeyName1 + " = \"" + partPropertyKeyValue1
                + "\", " + partPropertyKeyName2 + " = \""
                + partPropertyKeyValue2 + "\" ) "
                + " schema \"c1:collection(r1:record(f1:int, f2:int))\"" + " delocation \""
                + delocation + "\"");

        verifyPublishDataElement(owlTableName, databaseName, propertyKeyName,
                propertyKeyValue, propertyKeyType, partitionKeyName,
                partitionKeyValue, partitionKeyType, partPropertyKeyName1,
                partPropertyKeyValue1, partPropertyKeyType1,
                partPropertyKeyName2, partPropertyKeyValue2,
                partPropertyKeyType2, delocation);

    }

    public void verifyPublishDataElement(String owlTableName,
            String databaseName, String propertyKeyName,
            Integer propertyKeyValue, String propertyKeyType,
            String partitionKeyName, String partitionKeyValue,
            String partitionKeyType, String partPropertyKeyName1,
            String partPropertyKeyValue1, String partPropertyKeyType1,
            String partPropertyKeyName2, String partPropertyKeyValue2,
            String partPropertyKeyType2, String delocation) throws OwlException {

        String testCmd = "select dataelement objects from owltable "
            + owlTableName + " within owldatabase " + databaseName
            + " with partition ( " + partitionKeyName + " = " + "\""
            + partitionKeyValue + "\"" + ") property ( "
            + partPropertyKeyName1 + " = " + "\"" + partPropertyKeyValue1
            + "\"" + ", " + partPropertyKeyName2 + " = " + "\""
            + partPropertyKeyValue2 + "\"" + ")";

        OwlResultObject result = client.execute(testCmd);

        AssertionsForPublishDataElement(result, owlTableName, databaseName,
                propertyKeyName, propertyKeyValue, propertyKeyType,
                partitionKeyName, partitionKeyValue, partitionKeyType,
                partPropertyKeyName1, partPropertyKeyValue1,
                partPropertyKeyType1, partPropertyKeyName2,
                partPropertyKeyValue2, partPropertyKeyType2, delocation);

    }

    public void AssertionsForPublishDataElement(OwlResultObject result,
            String owlTableName, String databaseName, String propertyKeyName,
            Integer propertyKeyValue, String propertyKeyType,
            String partitionKeyName, String partitionKeyValue,
            String partitionKeyType, String partPropertyKeyName1,
            String partPropertyKeyValue1, String partPropertyKeyType1,
            String partPropertyKeyName2, String partPropertyKeyValue2,
            String partPropertyKeyType2, String delocation) throws OwlException {

        assertNotNull(result);
        List<? extends OwlObject> dataElement = result.getOutput();
        OwlDataElement owlDataElement = (OwlDataElement) dataElement.get(0);
        assertNotNull(owlDataElement);
        assertEquals(1, dataElement.size());
        assertEquals(delocation, owlDataElement.getStorageLocation());

        List<OwlKeyValue> owlKeyValue = owlDataElement.getKeyValues();
        OwlKeyValue ptnKey = owlKeyValue.get(0);
        assertEquals(partitionKeyName, ptnKey.getKeyName());
        assertEquals(partitionKeyValue, ptnKey.getStringValue());
        verifyDataType(partitionKeyType, ptnKey.getDataType());
        assertEquals(201, ptnKey.getKeyType().getCode());

        OwlKeyValue[] owlPropertyKeyValues = new OwlKeyValue[] {owlKeyValue.get(1),owlKeyValue.get(2),owlKeyValue.get(3)};

        KeyValueAssertionCriterion[] criteria = new KeyValueAssertionCriterion[] {
                new KeyValueAssertionCriterion(propertyKeyName, propertyKeyType, propertyKeyValue),
                new KeyValueAssertionCriterion(partPropertyKeyName1, partPropertyKeyType1, partPropertyKeyValue1),
                new KeyValueAssertionCriterion(partPropertyKeyName2, partPropertyKeyType2, partPropertyKeyValue2)
        };

        for ( OwlKeyValue kv : owlPropertyKeyValues){
            KeyValueAssertionCriterion.verifyPropertyKeyValueValidity(kv,criteria);
        }

    }


    /*
     * publish dataelement to owltable foods within owldatabase fooca partition
     * (feed = "foo" , datastamp = "20090909") property ( publish_pathnames =
     * "hdfspathname" , schema_file_location = "hdfslocation" , format_of_data =
     * "zebra_format" , status_of_data_element = "online" , schema_version = "5"
     * , data_loader_type = "MR" , data_loader_version = "5" , data_publisher =
     * "ETL" , data_owner = "ETL" ) schema "bag{string,bag{string,string}}"
     * delocation "hdfs://tmp" loader {pig-1.0}
     */

    public void AssertionsForSelectPartitionProperty(OwlResultObject result,
            String owlTableName, String databaseName, String propertyKeyName,
            Integer propertyKeyValue, String propertyKeyType,
            String partitionKeyName, String partitionKeyValue,
            String partitionKeyType, String partPropertyKeyName1,
            String partPropertyKeyValue1, String partPropertyKeyType1,
            String partPropertyKeyName2, String partPropertyKeyValue2,
            String partPropertyKeyType2, String delocation) throws OwlException {

        assertNotNull(result);

        List<? extends OwlObject> partitionProperties = result.getOutput();
        OwlPartitionProperty partProperties = (OwlPartitionProperty) partitionProperties
        .get(0);
        assertNotNull(partProperties);

        List<OwlKeyValue> owlKeyValue = partProperties.getKeyValues();
        // Integer propertyKeys = owlKeyValue.size();

        OwlKeyValue[] owlPropertyKeyValues = new OwlKeyValue[] {owlKeyValue.get(0),owlKeyValue.get(1),owlKeyValue.get(2)};

        KeyValueAssertionCriterion[] criteria = new KeyValueAssertionCriterion[] {
                new KeyValueAssertionCriterion(propertyKeyName, propertyKeyType, propertyKeyValue),
                new KeyValueAssertionCriterion(partPropertyKeyName1, partPropertyKeyType1, partPropertyKeyValue1),
                new KeyValueAssertionCriterion(partPropertyKeyName2, partPropertyKeyType2, partPropertyKeyValue2)
        };

        for ( OwlKeyValue kv : owlPropertyKeyValues){
            KeyValueAssertionCriterion.verifyPropertyKeyValueValidity(kv,criteria);
        }

    }

    /*
     * {"executionTime":47,"output":[{"keyValues":[{"dataType":"STRING","keyName"
     * :"color","keyType":"PROPERTY","stringValue":"red"},{"dataType":"STRING",
     * "keyName"
     * :"shape","keyType":"PROPERTY","stringValue":"square"}]}],"status"
     * :"SUCCESS"}
     */

    /*
     * TODO Improve select partition to assert all fields of the output
     */

    public void selectPartition(String owlTableName, String databaseName,
            String propertyKeyName, Integer propertyKeyValue,
            String propertyKeyType, String partitionKeyName,
            String partitionKeyValue, String partitionKeyType,
            String partPropertyKeyName1, String partPropertyKeyValue1,
            String partPropertyKeyType1, String partPropertyKeyName2,
            String partPropertyKeyValue2, String partPropertyKeyType2,
            String delocation) throws OwlException {

        String testCmd = "select partitionproperty objects from owltable "
            + owlTableName + " within owldatabase " + databaseName
            + " with partition (" + partitionKeyName + " = \""
            + partitionKeyValue + "\")";
        System.out.println(testCmd);
        OwlResultObject result = client.execute(testCmd);

        AssertionsForSelectPartitionProperty(result, owlTableName, databaseName,
                propertyKeyName, propertyKeyValue, propertyKeyType,
                partitionKeyName, partitionKeyValue, partitionKeyType,
                partPropertyKeyName1, partPropertyKeyValue1,
                partPropertyKeyType1, partPropertyKeyName2,
                partPropertyKeyValue2, partPropertyKeyType2, delocation);

        // System.out.println(OwlUtil.getJSONFromObject(result));

    }

    /*
     * alter owltable foods Within database fooca with partition ( feed = "foo" ,
     * datestamp = "20080909" ) modify property ( color = "red" , shape =
     * "round" )
     */
    public void alterToModifyPropertyKeyValue(String owlTableName,
            String databaseName, String propertyKeyName,
            Integer propertyKeyValue, String propertyKeyType,
            String partitionKey, String partitionKeyValue,
            String partitionKeyType, String partPropertyKeyName1,
            String partPropertyKeyValue1, String partPropertyKeyType1,
            String partPropertyKeyName2, String partPropertyKeyValue2,
            String partPropertyKeyType2, String delocation) throws OwlException {

        String testCmd = "alter owltable " + owlTableName + " within owldatabase "
        + databaseName + " with partition (" + partitionKey + " = \""
        + partitionKeyValue + "\") modify property("
        + partPropertyKeyName2 + " = \"" + partPropertyKeyValue2
        + "\" )";

        // System.out.println(testCmd);
        client.execute(testCmd);
        verifyPublishDataElement(owlTableName, databaseName, propertyKeyName,
                propertyKeyValue, propertyKeyType, partitionKey,
                partitionKeyValue, partitionKeyType, partPropertyKeyName1,
                partPropertyKeyValue1, partPropertyKeyType1,
                partPropertyKeyName2, partPropertyKeyValue2,
                partPropertyKeyType2, delocation);

        selectPartition(owlTableName, databaseName, propertyKeyName,
                propertyKeyValue, propertyKeyType, partitionKey,
                partitionKeyValue, partitionKeyType, partPropertyKeyName1,
                partPropertyKeyValue1, partPropertyKeyType1,
                partPropertyKeyName2, partPropertyKeyValue2,
                partPropertyKeyType2, delocation);
    }

    public void alterToAddPropertyKey(String owlTableName,
            String databaseName, int partitionLevel,
            String keyName1, String keyType1,
            String keyName2, String keyType2,
            String partitionKeyName, String partitionKeyValue
    ) throws OwlException {

        // add property key
        String testCmd = "alter owltable " + owlTableName + " within owldatabase "
        + databaseName + " at partition level ( " + partitionLevel + " ) "
        + " add propertykey ( " + keyName1 + " : " + keyType1 + "," + keyName2 + " : " + keyType2
        + ")" ;

        client.execute(testCmd);
        System.out.println(testCmd);

        // set property keyvalue

        String testCmd2 = "alter owltable " + owlTableName + " within owldatabase "
        + databaseName + " with partition (" + partitionKeyName + " = \""
        + partitionKeyValue + "\") modify property("
        + keyName1 + " = \"" + "value1"+ "\" ," 
        + keyName2 + " = \"" + "value2"+ "\" " 
        +")";
        client.execute(testCmd2);


        // select partitionproperty
        String testCmd3 = "select partitionproperty objects from owltable "
            + owlTableName + " within owldatabase " + databaseName
            + " with partition (" + partitionKeyName + " = \""
            + partitionKeyValue + "\"" + ")";
        OwlResultObject result  = client.execute(testCmd3);


        List<? extends OwlObject> partitionProperties = result.getOutput();
        OwlPartitionProperty partProperties = (OwlPartitionProperty) partitionProperties
        .get(0);
        assertNotNull(partProperties);

        List<OwlKeyValue> owlPropertyKeyValue = partProperties.getKeyValues();
        assertEquals(owlPropertyKeyValue.size(), 5);

    }
    public void verifyDropProperty(String owlTableName, String databaseName,
            String propertyKeyName, Integer propertyKeyValue,
            String propertyKeyType, String partitionKeyName,
            String partitionKeyValue, String partitionKeyType,
            String partPropertyKeyName1, String partPropertyKeyValue1,
            String partPropertyKeyType1, String delocation) throws OwlException {

        String testCmd = "select dataelement objects from owltable "
            + owlTableName + " within owldatabase " + databaseName
            + " with partition ( " + partitionKeyName + " = " + "\""
            + partitionKeyValue + "\"" + ") property ( "
            + partPropertyKeyName1 + " = " + "\"" + partPropertyKeyValue1
            + "\"" + ")";

        OwlResultObject result = client.execute(testCmd);

        AssertionsForDropProperty(result, owlTableName, databaseName,
                propertyKeyName, propertyKeyValue, propertyKeyType,
                partitionKeyName, partitionKeyValue, partitionKeyType,
                partPropertyKeyName1, partPropertyKeyValue1,
                partPropertyKeyType1, delocation);

    }

    public void AssertionsForDropProperty(OwlResultObject result,
            String owlTableName, String databaseName, String propertyKeyName,
            Integer propertyKeyValue, String propertyKeyType,
            String partitionKeyName, String partitionKeyValue,
            String partitionKeyType, String partPropertyKeyName1,
            String partPropertyKeyValue1, String partPropertyKeyType1,
            String delocation) throws OwlException {

        assertNotNull(result);
        List<? extends OwlObject> dataElement = result.getOutput();
        OwlDataElement owlDataElement = (OwlDataElement) dataElement.get(0);
        assertNotNull(owlDataElement);
        assertEquals(1, dataElement.size());
        assertEquals(delocation, owlDataElement.getStorageLocation());

        List<OwlKeyValue> owlKeyValue = owlDataElement.getKeyValues();
        OwlKeyValue ptnKey = owlKeyValue.get(0);

        assertEquals(partitionKeyName, ptnKey.getKeyName());
        assertEquals(partitionKeyValue, ptnKey.getStringValue());
        verifyDataType(partitionKeyType, ptnKey.getDataType());
        assertEquals(201, ptnKey.getKeyType().getCode());

        // Integer propertyKeys = owlKeyValue.size();

        OwlKeyValue[] owlPropertyKeyValues = new OwlKeyValue[] {owlKeyValue.get(1),owlKeyValue.get(2)};

        KeyValueAssertionCriterion[] criteria = new KeyValueAssertionCriterion[] {
                new KeyValueAssertionCriterion(propertyKeyName, propertyKeyType, propertyKeyValue),
                new KeyValueAssertionCriterion(partPropertyKeyName1, partPropertyKeyType1, partPropertyKeyValue1),
        };


        for ( OwlKeyValue kv : owlPropertyKeyValues){
            KeyValueAssertionCriterion.verifyPropertyKeyValueValidity(kv,criteria);
        }

    }

    public void alterToDropProperty(String owlTableName, String databaseName,
            String propertyKeyName, Integer propertyKeyValue,
            String propertyKeyType, String partitionKey,
            String partitionKeyValue, String partitionKeyType,
            String partPropertyKeyName1, String partPropertyKeyValue1,
            String partPropertyKeyType1, String partPropertyKeyName2,
            String partPropertyKeyValue2, String partPropertyKeyType2,
            String delocation) throws OwlException {

        String testCmd = "alter owltable " + owlTableName + " within owldatabase "
        + databaseName + " with partition (" + partitionKey + " = \""
        + partitionKeyValue + "\") drop property("
        + partPropertyKeyName2 + " = \"" + partPropertyKeyValue2
        + "\" )";
        System.out.println(testCmd);
        client.execute(testCmd);

        verifyDropProperty(owlTableName, databaseName, propertyKeyName,
                propertyKeyValue, propertyKeyType, partitionKey,
                partitionKeyValue, partitionKeyType, partPropertyKeyName1,
                partPropertyKeyValue1, partPropertyKeyType1, delocation);

        selectPartitionwithDroppedProperty(owlTableName, databaseName,
                propertyKeyName, propertyKeyValue, propertyKeyType,
                partitionKey, partitionKeyValue, partitionKeyType,
                partPropertyKeyName1, partPropertyKeyValue1,
                partPropertyKeyType1, partPropertyKeyName2,
                partPropertyKeyValue2, partPropertyKeyType2, delocation);

    }

    public void selectPartitionwithDroppedProperty(String owlTableName,
            String databaseName, String propertyKeyName,
            Integer propertyKeyValue, String propertyKeyType,
            String partitionKeyName, String partitionKeyValue,
            String partitionKeyType, String partPropertyKeyName1,
            String partPropertyKeyValue1, String partPropertyKeyType1,
            String partPropertyKeyName2, String partPropertyKeyValue2,
            String partPropertyKeyType2, String delocation) throws OwlException {

        String testCmd = "select partitionproperty objects from owltable "
            + owlTableName + " within owldatabase " + databaseName
            + " with partition (" + partitionKeyName + " = \""
            + partitionKeyValue + "\")";
        System.out.println(testCmd);
        OwlResultObject result = client.execute(testCmd);

        AssertionsForSelectPartitionPropertyWithDroppedProperty(result,
                owlTableName, databaseName, propertyKeyName, propertyKeyValue,
                propertyKeyType, partitionKeyName, partitionKeyValue,
                partitionKeyType, partPropertyKeyName1, partPropertyKeyValue1,
                partPropertyKeyType1, partPropertyKeyName2,
                partPropertyKeyValue2, partPropertyKeyType2, delocation);

        // System.out.println(OwlUtil.getJSONFromObject(result));

    }

    public void AssertionsForSelectPartitionPropertyWithDroppedProperty(
            OwlResultObject result, String owlTableName, String databaseName,
            String propertyKeyName, Integer propertyKeyValue,
            String propertyKeyType, String partitionKeyName,
            String partitionKeyValue, String partitionKeyType,
            String partPropertyKeyName1, String partPropertyKeyValue1,
            String partPropertyKeyType1, String partPropertyKeyName2,
            String partPropertyKeyValue2, String partPropertyKeyType2,
            String delocation) throws OwlException {

        assertNotNull(result);

        List<? extends OwlObject> partitionProperties = result.getOutput();
        OwlPartitionProperty partProperties = (OwlPartitionProperty) partitionProperties
        .get(0);
        assertNotNull(partProperties);

        List<OwlKeyValue> owlKeyValue = partProperties.getKeyValues();

        // Integer propertyKeys = owlKeyValue.size();

        OwlKeyValue[] owlPropertyKeyValues = new OwlKeyValue[] {owlKeyValue.get(0),owlKeyValue.get(1)};

        KeyValueAssertionCriterion[] criteria = new KeyValueAssertionCriterion[] {
                new KeyValueAssertionCriterion(propertyKeyName, propertyKeyType, propertyKeyValue),
                new KeyValueAssertionCriterion(partPropertyKeyName1, partPropertyKeyType1, partPropertyKeyValue1),
        };


        for ( OwlKeyValue kv : owlPropertyKeyValues){
            KeyValueAssertionCriterion.verifyPropertyKeyValueValidity(kv,criteria);
        }

    }

    public void alterToDropDataElement(String owlTableName, String databaseName,
            String propertyKeyName, Integer propertyKeyValue,
            String propertyKeyType, String partitionKeyName,
            String partitionKeyValue, String partitionKeyType,
            String partPropertyKeyName1, String partPropertyKeyValue1,
            String partPropertyKeyType1, String partPropertyKeyName2,
            String partPropertyKeyValue2, String partPropertyKeyType2,
            String delocation) throws OwlException {

        String testCmd = "alter owlTable " + owlTableName + " within owldatabase "
        + databaseName + " with partition (" + partitionKeyName
        + " = \"" + partitionKeyValue + "\") delete dataelement";
        client.execute(testCmd);

        OwlResultObject result = client
        .execute("select dataelement objects from owltable "
                + owlTableName + " within owldatabase " + databaseName
                + " with partition ( " + partitionKeyName + " = "
                + "\"" + partitionKeyValue + "\"" + ") property ( "
                + partPropertyKeyName1 + " = " + "\""
                + partPropertyKeyValue1 + "\"" + ")");

        assertTrue(result.getOutput().isEmpty());

        // -----------------------------------------------------------------

        result = client.execute("select dataelement objects from owltable "
                + owlTableName + " within owldatabase " + databaseName
                + " with partition ( " + partitionKeyName + " = " + "\""
                + partitionKeyValue + "\"" + ") property ( "
                + partPropertyKeyName1 + " > = " + "\"" + partPropertyKeyValue1
                + "\"" + ")");

        assertTrue(result.getOutput().isEmpty());

        // -----------------------------------------------------------------

        // --------------------------------------------------------------------
        result = client.execute("select dataelement objects from owltable "
                + owlTableName + " within owldatabase " + databaseName
                + " with partition ( " + partitionKeyName + " = " + "\""
                + partitionKeyValue + "\"" + ") property ( "
                + partPropertyKeyName1 + " <= " + "\"" + partPropertyKeyValue1
                + "\"" + ")");

        assertTrue(result.getOutput().isEmpty());

        // ------------------------------------------------------------------------

    }

    public void alterToDropPartition(String owlTableName, String databaseName,
            String propertyKeyName, Integer propertyKeyValue,
            String propertyKeyType, String partitionKeyName,
            String partitionKeyValue, String partitionKeyType,
            String partPropertyKeyName1, String partPropertyKeyValue1,
            String partPropertyKeyType1, String partPropertyKeyName2,
            String partPropertyKeyValue2, String partPropertyKeyType2,
            String delocation) throws OwlException {

        String testCmd = "alter owlTable " + owlTableName + " within owldatabase "
        + databaseName + " drop partition (" + partitionKeyName
        + " = \"" + partitionKeyValue + "\")";
        System.out.println(testCmd);
        client.execute(testCmd);

        OwlResultObject result = client
        .execute("select partitionproperty objects from owltable "
                + owlTableName + " within owldatabase " + databaseName
                + " with partition (" + partitionKeyName + " = \""
                + partitionKeyValue + "\")");
        System.out.println(OwlUtil.getJSONFromObject(result));
        assertTrue(result.getOutput().isEmpty());
        assertEquals("SUCCESS", result.getStatus());
    }

    public void dropOwlTable(String owlTableName, String databaseName)
    throws OwlException {

        String testCmd = "drop owltable " + owlTableName + " within owldatabase "
        + databaseName;
        client.execute(testCmd);

        OwlResultObject result = client
        .execute("select OwlTable objects where owldatabase in ("
                + databaseName + ")");

        assertTrue(result.getOutput().isEmpty());
        assertEquals("SUCCESS", result.getStatus().toString());
    }

    public void dropDatabase(String databaseName) throws OwlException {
        String testCmd = "drop owldatabase " + databaseName;
        client.execute(testCmd);
        OwlException oe = null;
        try {
            client.execute("select owldatabase objects where owldatabase in ("
                    + databaseName + ")");
        } catch (OwlException e) {
            oe = e;
        }
        assertEquals(oe.getErrorCode(), ErrorType.ERROR_UNKNOWN_DATABASE
                .getErrorCode());
        assertNotNull(oe.getMessage());

    }

    public void cleanup(String databaseName, String owlTableName)
    throws OwlException {
        OwlException e = null;
        try {

            client.execute("drop owltable " + owlTableName + " within owldatabase "
                    + databaseName);
            client.execute("drop owldatabase " + databaseName);
        } catch (OwlException oe) {
            e = oe;
        }
        assertNotNull(e);
        assertEquals(e.getErrorCode(), ErrorType.ERROR_UNKNOWN_DATABASE
                .getErrorCode());
        assertNotNull(e.getMessage());
        assertTrue(e.getMessage().indexOf("Unable to map given database name") != -1);
    }
}
