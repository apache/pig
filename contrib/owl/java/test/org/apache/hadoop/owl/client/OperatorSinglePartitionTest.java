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
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.common.OwlUtil;
import org.apache.hadoop.owl.client.CompleteSanityTest;
import org.apache.hadoop.owl.protocol.OwlDataElement;
import org.apache.hadoop.owl.protocol.OwlKeyValue;
import org.apache.hadoop.owl.protocol.OwlObject;
import org.apache.hadoop.owl.protocol.OwlPartitionProperty;
import org.apache.hadoop.owl.protocol.OwlResultObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class OperatorSinglePartitionTest extends OwlTestCase {

    private static OwlClient client;
    private CompleteSanityTest opt;

    public OperatorSinglePartitionTest() {
        client = new OwlClient(getUri());
        this.opt = new CompleteSanityTest();

    }

    @Before
    public void testInitialize() {
    }

    public void publishDataElement(String owlTableName, String databaseName,
            String propertyKeyName, int propertyKeyValue,
            String propertyKeyType, String partitionKeyName,
            int partitionKeyValue, String partitionKeyType,
            String partPropertyKeyName1, int partPropertyKeyValue1,
            String partPropertyKeyType1, String partPropertyKeyName2,
            int partPropertyKeyValue2, String partPropertyKeyType2,
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

        // verifyPublishDataElement(owlTableName, databaseName, propertyKeyName,
        // propertyKeyValue, propertyKeyType, partitionKeyName,
        // partitionKeyValue, partitionKeyType, partPropertyKeyName1,
        // partPropertyKeyValue1, partPropertyKeyType1,
        // partPropertyKeyName2, partPropertyKeyValue2,
        // partPropertyKeyType2, delocation);

        verifyPublishDataElementWithOperator(owlTableName, databaseName,
                propertyKeyName, propertyKeyValue, propertyKeyType,
                partitionKeyName, partitionKeyValue, partitionKeyType,
                partPropertyKeyName1, partPropertyKeyValue1,
                partPropertyKeyType1, partPropertyKeyName2,
                partPropertyKeyValue2, partPropertyKeyType2, delocation);

    }

    public void verifyPublishDataElementWithOperator(String owlTableName,
            String databaseName, String propertyKeyName, int propertyKeyValue,
            String propertyKeyType, String partitionKeyName,
            int partitionKeyValue, String partitionKeyType,
            String partPropertyKeyName1, int partPropertyKeyValue1,
            String partPropertyKeyType1, String partPropertyKeyName2,
            int partPropertyKeyValue2, String partPropertyKeyType2,
            String delocation) throws OwlException {
        int i = 1;
        String op = " <= ";
        for (i = 1; i <= 2; i++) {
            String testCmd = "select dataelement objects from owltable "
                + owlTableName + " within owldatabase " + databaseName
                + " with partition ( " + partitionKeyName + op + "\""
                + partitionKeyValue + "\"" + ") property ( "
                + partPropertyKeyName1 + op + "\"" + partPropertyKeyValue1
                + "\"" + ", " + partPropertyKeyName2 + op + "\""
                + partPropertyKeyValue2 + "\"" + ")";

            System.out.println(testCmd);

            OwlResultObject result = client.execute(testCmd);

            System.out.println(OwlUtil.getJSONFromObject(result));

            AssertionsForPublishDataElement(result, owlTableName, databaseName,
                    propertyKeyName, propertyKeyValue, propertyKeyType,
                    partitionKeyName, partitionKeyValue, partitionKeyType,
                    partPropertyKeyName1, partPropertyKeyValue1,
                    partPropertyKeyType1, partPropertyKeyName2,
                    partPropertyKeyValue2, partPropertyKeyType2, delocation);
            op = " >= ";

        }
    }

    public void verifyPublishDataElement(String owlTableName,
            String databaseName, String propertyKeyName, int propertyKeyValue,
            String propertyKeyType, String partitionKeyName,
            int partitionKeyValue, String partitionKeyType,
            String partPropertyKeyName1, int partPropertyKeyValue1,
            String partPropertyKeyType1, String partPropertyKeyName2,
            int partPropertyKeyValue2, String partPropertyKeyType2,
            String delocation) throws OwlException {

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
            int propertyKeyValue, String propertyKeyType,
            String partitionKeyName, int partitionKeyValue,
            String partitionKeyType, String partPropertyKeyName1,
            int partPropertyKeyValue1, String partPropertyKeyType1,
            String partPropertyKeyName2, int partPropertyKeyValue2,
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
        assertEquals(partitionKeyValue, ptnKey.getIntValue().intValue());
        CompleteSanityTest.verifyDataType(partitionKeyType, ptnKey.getDataType());
        assertEquals(201, ptnKey.getKeyType().getCode());

        OwlKeyValue[] owlPropertyKeyValues = new OwlKeyValue[] {owlKeyValue.get(1),owlKeyValue.get(2),owlKeyValue.get(3)};
        KeyValueAssertionCriterion[] criteria = new KeyValueAssertionCriterion[] {
                new KeyValueAssertionCriterion(propertyKeyName,propertyKeyType,propertyKeyValue),
                new KeyValueAssertionCriterion(partPropertyKeyName1,partPropertyKeyType1,partPropertyKeyValue1),
                new KeyValueAssertionCriterion(partPropertyKeyName2,partPropertyKeyType2,partPropertyKeyValue2)
        };
        for ( OwlKeyValue kv : owlPropertyKeyValues ){
            KeyValueAssertionCriterion.verifyPropertyKeyValueValidity(kv,criteria);
        }

    }

    public void AssertionsForSelectPartitionProperty(OwlResultObject result,
            String owlTableName, String databaseName, String propertyKeyName,
            int propertyKeyValue, String propertyKeyType,
            String partitionKeyName, int partitionKeyValue,
            String partitionKeyType, String partPropertyKeyName1,
            int partPropertyKeyValue1, String partPropertyKeyType1,
            String partPropertyKeyName2, int partPropertyKeyValue2,
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
                new KeyValueAssertionCriterion(propertyKeyName,propertyKeyType,propertyKeyValue),
                new KeyValueAssertionCriterion(partPropertyKeyName1,partPropertyKeyType1,partPropertyKeyValue1),
                new KeyValueAssertionCriterion(partPropertyKeyName2,partPropertyKeyType2,partPropertyKeyValue2)
        };
        for ( OwlKeyValue kv : owlPropertyKeyValues ){
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
            String propertyKeyName, int propertyKeyValue,
            String propertyKeyType, String partitionKeyName,
            int partitionKeyValue, String partitionKeyType,
            String partPropertyKeyName1, int partPropertyKeyValue1,
            String partPropertyKeyType1, String partPropertyKeyName2,
            int partPropertyKeyValue2, String partPropertyKeyType2,
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

    public void alterToModifyPropertyKeyValue(String owlTableName,
            String databaseName, String propertyKeyName, int propertyKeyValue,
            String propertyKeyType, String partitionKey, int partitionKeyValue,
            String partitionKeyType, String partPropertyKeyName1,
            int partPropertyKeyValue1, String partPropertyKeyType1,
            String partPropertyKeyName2, int partPropertyKeyValue2,
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

    public void verifyDropProperty(String owlTableName, String databaseName,
            String propertyKeyName, int propertyKeyValue,
            String propertyKeyType, String partitionKeyName,
            int partitionKeyValue, String partitionKeyType,
            String partPropertyKeyName1, int partPropertyKeyValue1,
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
            int propertyKeyValue, String propertyKeyType,
            String partitionKeyName, int partitionKeyValue,
            String partitionKeyType, String partPropertyKeyName1,
            int partPropertyKeyValue1, String partPropertyKeyType1,
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
        assertEquals(partitionKeyValue, ptnKey.getIntValue().intValue());
        CompleteSanityTest.verifyDataType(partitionKeyType, ptnKey.getDataType());
        assertEquals(201, ptnKey.getKeyType().getCode());

        OwlKeyValue[] owlPropertyKeyValues = new OwlKeyValue[] {owlKeyValue.get(1),owlKeyValue.get(2)};

        KeyValueAssertionCriterion[] criteria = new KeyValueAssertionCriterion[] {
                new KeyValueAssertionCriterion(propertyKeyName,propertyKeyType,propertyKeyValue),
                new KeyValueAssertionCriterion(partPropertyKeyName1,partPropertyKeyType1,partPropertyKeyValue1),
        };

        for ( OwlKeyValue kv : owlPropertyKeyValues ){
            KeyValueAssertionCriterion.verifyPropertyKeyValueValidity(kv,criteria);
        }

    }

    public void alterToDropProperty(String owlTableName, String databaseName,
            String propertyKeyName, int propertyKeyValue,
            String propertyKeyType, String partitionKey, int partitionKeyValue,
            String partitionKeyType, String partPropertyKeyName1,
            int partPropertyKeyValue1, String partPropertyKeyType1,
            String partPropertyKeyName2, int partPropertyKeyValue2,
            String partPropertyKeyType2, String delocation) throws OwlException {

        String testCmd = "alter owltable " + owlTableName + " within owldatabase "
        + databaseName + " with partition (" + partitionKey + " = \""
        + partitionKeyValue + "\") drop property("
        + partPropertyKeyName2 + " = \"" + partPropertyKeyValue2
        + "\" )";

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
            String databaseName, String propertyKeyName, int propertyKeyValue,
            String propertyKeyType, String partitionKeyName,
            int partitionKeyValue, String partitionKeyType,
            String partPropertyKeyName1, int partPropertyKeyValue1,
            String partPropertyKeyType1, String partPropertyKeyName2,
            int partPropertyKeyValue2, String partPropertyKeyType2,
            String delocation) throws OwlException {

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
            String propertyKeyName, int propertyKeyValue,
            String propertyKeyType, String partitionKeyName,
            int partitionKeyValue, String partitionKeyType,
            String partPropertyKeyName1, int partPropertyKeyValue1,
            String partPropertyKeyType1, String partPropertyKeyName2,
            int partPropertyKeyValue2, String partPropertyKeyType2,
            String delocation) throws OwlException {

        assertNotNull(result);

        List<? extends OwlObject> partitionProperties = result.getOutput();
        OwlPartitionProperty partProperties = (OwlPartitionProperty) partitionProperties
        .get(0);
        assertNotNull(partProperties);

        List<OwlKeyValue> owlKeyValue = partProperties.getKeyValues();
        OwlKeyValue property1 = owlKeyValue.get(0);

        OwlKeyValue[] owlPropertyKeyValues = new OwlKeyValue[] {owlKeyValue.get(0),owlKeyValue.get(1)};

        KeyValueAssertionCriterion[] criteria = new KeyValueAssertionCriterion[] {
                new KeyValueAssertionCriterion(propertyKeyName,propertyKeyType,propertyKeyValue),
                new KeyValueAssertionCriterion(partPropertyKeyName1,partPropertyKeyType1,partPropertyKeyValue1),
        };

        for ( OwlKeyValue kv : owlPropertyKeyValues ){
            KeyValueAssertionCriterion.verifyPropertyKeyValueValidity(kv,criteria);
        }

    }

    @Test
    public void testCreateDatabase() throws OwlException {
        opt.createDatabase("testOperatorDatabase", "testloc");
    }

    @Test
    public void testCreatePartitionedOwlTable() throws OwlException {
        opt.createPartitionedOwlTable("testOperatorOwlTable", "testOperatorDatabase",
                "retention", "INTEGER", "year", "INTEGER", "month", "INTEGER",
                "date", "INTEGER");
    }

    @Test
    public void testDescribeOwlTable() throws OwlException {

        opt.describeOwlTable("testOperatorOwlTable", "testOperatorDatabase", "retention",
                "INTEGER", "year", "INTEGER", "month", "INTEGER", "date",
        "INTEGER");
    }

    @Test
    public void testPublishDataElement() throws OwlException {
        publishDataElement("testOperatorOwlTable", "testOperatorDatabase", "retention", 1,
                "INTEGER", "year", 2009, "INTEGER", "month", 8, "INTEGER",
                "date", 1, "INTEGER", "hdfs://dummytestlocation.blah/1234");

        publishDataElement("testOperatorOwlTable", "testOperatorDatabase", "retention", 10,
                "INTEGER", "year", 1984, "INTEGER", "month", 10, "INTEGER",
                "date", 30, "INTEGER", "hdfs://dummytestlocation.blah/2345");
    }

    @Test
    public void testSelectPartition() throws OwlException {
        selectPartition("testOperatorOwlTable", "testOperatorDatabase", "retention", 1,
                "INTEGER", "year", 2009, "INTEGER", "month", 8, "INTEGER",
                "date", 1, "INTEGER", "hdfs://dummytestlocation.blah/1234");
    }

    @Test
    public void testAlterToModifyPropertyKeyValue() throws OwlException {
        alterToModifyPropertyKeyValue("testOperatorOwlTable", "testOperatorDatabase", "retention", 1,
                "INTEGER", "year", 2009, "INTEGER", "month", 8, "INTEGER",
                "date", 20, "INTEGER", "hdfs://dummytestlocation.blah/1234");
    }

    @Test
    public void testAlterToDropProperty() throws OwlException {
        alterToDropProperty("testOperatorOwlTable", "testOperatorDatabase", "retention", 1,
                "INTEGER", "year", 2009, "INTEGER", "month", 8, "INTEGER",
                "date", 20, "INTEGER", "hdfs://dummytestlocation.blah/1234");
    }

    /*
     * FIXME
     * 
     * @Test public void testAlterToDropPartition() throws OwlException {
     * opt.alterToDropPartition("testOperatorOwlTable", "testOperatorDatabase", "retention", 1,
     * "INTEGER", "country", "USA", "STRING", "color", "red", "STRING", "shape",
     * "square", "STRING", "hdfs://dummytestlocation.blah/1234"); }
     * 
     * // Still results in the following after running: //
     * {"executionTime":73,"output"
     * :[{"keyValues":[{"dataType":"STRING","keyName"
     * :"color","keyType":"PROPERTY","stringValue":"red"}]}],"status":"SUCCESS"}
     * // BECK: result.getOutput() has [1] entries.
     */
    @Test
    public void testDropOwlTable() throws OwlException {
        opt.dropOwlTable("testOperatorOwlTable", "testOperatorDatabase");
    }

    @Test
    public void testDropDatabase() throws OwlException {
        opt.dropDatabase("testOperatorDatabase");
    }

    @After
    public void testCleanup() throws OwlException {
        opt.cleanup("testOperatorDatabase", "testOperatorOwlTable");
    }

}
