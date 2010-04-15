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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Date;
import org.apache.hadoop.owl.OwlTestCase;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.common.OwlUtil;
import org.apache.hadoop.owl.client.CompleteSanityTest;
import org.apache.hadoop.owl.protocol.OwlDataElement;
import org.apache.hadoop.owl.protocol.OwlKeyValue;
import org.apache.hadoop.owl.protocol.OwlObject;
import org.apache.hadoop.owl.protocol.OwlPartitionKey;
import org.apache.hadoop.owl.protocol.OwlPartitionProperty;
import org.apache.hadoop.owl.protocol.OwlPropertyKey;
import org.apache.hadoop.owl.protocol.OwlResultObject;
import org.apache.hadoop.owl.protocol.OwlTable;
import org.junit.Before;
import org.junit.Test;

public class MultiplePartitionIntervalTest extends OwlTestCase {
    // private int counter = 0;
    private static OwlClient client;
    private CompleteSanityTest csit;
    private MultiplePartitionTest mpit;

    public MultiplePartitionIntervalTest() {
        client = new OwlClient(getUri());
        this.csit = new CompleteSanityTest();
        this.mpit = new MultiplePartitionTest();
    }

    @Before
    public void testInitialize() {
    }

    public void createMultiplePartitionedIntervalOwlTable(String owlTableName,
            String databaseName, String propertyKeyName1, String type1,
            String propertyKeyName2, String type2, String ptnType,
            String partitionKey1, String partitionKey1Type1,
            String partitionKey2, String partitionKey2Type2,
            String part1PropertyKey1, String part1PropertyKey1Type1,
            String part1PropertyKey2, String part1PropertyKey2Type2,
            String part2PropertyKey1, String part2PropertyKey1Type1,
            String part2PropertyKey2, String part2PropertyKey2Type2)
    throws OwlException {

        System.out.println("Owl Table name " + owlTableName
                + " within owldatabase " + databaseName);

        String testCmd = "create owltable type basic " + owlTableName
        + " within owldatabase " + databaseName + " define property key "
        + propertyKeyName1 + " : " + type1 + " , " + propertyKeyName2
        + " : " + type2 + " partitioned by " + ptnType
        + " with partition key " + partitionKey1
        + " define property key " + part1PropertyKey1 + " : "
        + part1PropertyKey1Type1 + " , " + part1PropertyKey2 + " : "
        + part1PropertyKey2Type2
        + " partitioned by LIST with partition key " + partitionKey2
        + " : " + partitionKey2Type2 + " define property key "
        + part2PropertyKey1 + " : " + part2PropertyKey1Type1 + " ,"
        + part2PropertyKey2 + " : " + part2PropertyKey2Type2
        + " schema \"f1:int\"";

        System.out.println(testCmd);
        client.execute(testCmd);
        mpit.verifyCreateMultiplePartitionedOwlTable(owlTableName, databaseName,
                propertyKeyName1, type1, propertyKeyName2, type2,
                partitionKey1, partitionKey1Type1, partitionKey2,
                partitionKey2Type2, part1PropertyKey1, part1PropertyKey1Type1,
                part1PropertyKey2, part1PropertyKey2Type2, part2PropertyKey1,
                part2PropertyKey1Type1, part2PropertyKey2,
                part2PropertyKey2Type2);
    }

    public void publishDataElementToMultiplePartitionedIntervalOwlTable(
            String owlTableName, String databaseName, String propertyKeyName1,
            int propertyKeyValue1, String type1, String propertyKeyName2,
            String propertyKeyValue2, String type2, String partitionKey1,
            String partitionKey1Value1, String partitionKey1Type1,
            String partitionKey2, int partitionKey2Value2,
            String partitionKey2Type2, String part1PropertyKey1,
            String part1PropertyKey1Value1, String part1PropertyKey1Type1,
            String part1PropertyKey2, int part1PropertyKey2Value2,
            String part1PropertyKey2Type2, String part2PropertyKey1,
            String part2PropertyKey1Value1, String part2PropertyKey1Type1,
            String part2PropertyKey2, String part2PropertyKey2Value2,
            String part2PropertyKey2Type2, String delocation)
    throws OwlException {

        String testCmd = "publish dataelement to owltable " + owlTableName
        + " within owldatabase " + databaseName + " partition ( "
        + partitionKey1 + " = \"" + partitionKey1Value1 + "\" , "
        + partitionKey2 + " = \"" + partitionKey2Value2
        + "\") property (" + propertyKeyName1 + " = \""
        + propertyKeyValue1 + "\" , " + propertyKeyName2 + " = \""
        + propertyKeyValue2 + "\"" + " , " + part1PropertyKey1
        + " = \"" + part1PropertyKey1Value1 + "\" , "
        + part1PropertyKey2 + " = \"" + part1PropertyKey2Value2
        + "\" , " + part2PropertyKey1 + " = \""
        + part2PropertyKey1Value1 + "\" , " + part2PropertyKey2
        + " = \"" + part2PropertyKey2Value2 + "\") "
        + " schema \"f1:int\"" + " delocation \"" + delocation + "\"";

        System.out.println(testCmd);

        client.execute(testCmd);

        verifyPublishDataElementToMultiplePartitionedIntervalOwlTable(
                owlTableName, databaseName, propertyKeyName1, propertyKeyValue1,
                type1, propertyKeyName2, propertyKeyValue2, type2,
                partitionKey1, partitionKey1Value1, partitionKey1Type1,
                partitionKey2, partitionKey2Value2, partitionKey2Type2,
                part1PropertyKey1, part1PropertyKey1Value1,
                part1PropertyKey1Type1, part1PropertyKey2,
                part1PropertyKey2Value2, part1PropertyKey2Type2,
                part2PropertyKey1, part2PropertyKey1Value1,
                part2PropertyKey1Type1, part2PropertyKey2,
                part2PropertyKey2Value2, part2PropertyKey2Type2, delocation);

    }

    public void verifyPublishDataElementToMultiplePartitionedIntervalOwlTable(
            String owlTableName, String databaseName, String propertyKeyName1,
            int propertyKeyValue1, String type1, String propertyKeyName2,
            String propertyKeyValue2, String type2, String partitionKey1,
            String partitionKey1Value1, String partitionKey1Type1,
            String partitionKey2, int partitionKey2Value2,
            String partitionKey2Type2, String part1PropertyKey1,
            String part1PropertyKey1Value1, String part1PropertyKey1Type1,
            String part1PropertyKey2, int part1PropertyKey2Value2,
            String part1PropertyKey2Type2, String part2PropertyKey1,
            String part2PropertyKey1Value1, String part2PropertyKey1Type1,
            String part2PropertyKey2, String part2PropertyKey2Value2,
            String part2PropertyKey2Type2, String delocation)
    throws OwlException {

        String testCmd = "select dataelement objects from owltable "
            + owlTableName + " within owldatabase " + databaseName
            + " with partition ( " + partitionKey1 + " = " + "\""
            + partitionKey1Value1 + "\"," + partitionKey2 + " = \""
            + partitionKey2Value2 + "\") property (" + part1PropertyKey1
            + " = \"" + part1PropertyKey1Value1 + "\", "
            + part1PropertyKey2 + " = \"" + part1PropertyKey2Value2
            + "\", " + part2PropertyKey1 + " = \""
            + part2PropertyKey1Value1 + "\", " + part2PropertyKey2
            + " = \"" + part2PropertyKey2Value2 + "\")";

        // System.out.println("************ Select DE - " + testCmd
        // + "*****************\n");
        OwlResultObject result = client.execute(testCmd);
        //
        // System.out.println("1*********" + OwlUtil.getJSONFromObject(result));

        AssertionsForPublishDataElementToMultiplePartitionedIntervalOwlTable(
                result, owlTableName, databaseName, propertyKeyName1,
                propertyKeyValue1, type1, propertyKeyName2, propertyKeyValue2,
                type2, partitionKey1, partitionKey1Value1, partitionKey1Type1,
                partitionKey2, partitionKey2Value2, partitionKey2Type2,
                part1PropertyKey1, part1PropertyKey1Value1,
                part1PropertyKey1Type1, part1PropertyKey2,
                part1PropertyKey2Value2, part1PropertyKey2Type2,
                part2PropertyKey1, part2PropertyKey1Value1,
                part2PropertyKey1Type1, part2PropertyKey2,
                part2PropertyKey2Value2, part2PropertyKey2Type2, delocation);

    }

    public void AssertionsForPublishDataElementToMultiplePartitionedIntervalOwlTable(
            OwlResultObject result, String owlTableName, String databaseName,
            String propertyKeyName1, int propertyKeyValue1, String type1,
            String propertyKeyName2, String propertyKeyValue2, String type2,
            String partitionKey1, String partitionKey1Value1,
            String partitionKey1Type1, String partitionKey2,
            int partitionKey2Value2, String partitionKey2Type2,
            String part1PropertyKey1, String part1PropertyKey1Value1,
            String part1PropertyKey1Type1, String part1PropertyKey2,
            int part1PropertyKey2Value2, String part1PropertyKey2Type2,
            String part2PropertyKey1, String part2PropertyKey1Value1,
            String part2PropertyKey1Type1, String part2PropertyKey2,
            String part2PropertyKey2Value2, String part2PropertyKey2Type2,
            String delocation) throws OwlException {
        assertNotNull(result);
        List<? extends OwlObject> dataElement = result.getOutput();
        OwlDataElement owlDataElement = (OwlDataElement) dataElement.get(0);
        assertNotNull(owlDataElement);
        assertEquals(1, dataElement.size());
        assertEquals(delocation, owlDataElement.getStorageLocation());

        List<OwlKeyValue> owlKeyValue = owlDataElement.getKeyValues();
        OwlKeyValue ptnKey1 = owlKeyValue.get(0);
        OwlKeyValue ptnKey2 = owlKeyValue.get(1);
        OwlKeyValue property1 = owlKeyValue.get(2);
        OwlKeyValue property2 = owlKeyValue.get(3);
        OwlKeyValue property3 = owlKeyValue.get(4);
        OwlKeyValue property4 = owlKeyValue.get(5);

        assertEquals(partitionKey2, ptnKey1.getKeyName());
        assertEquals(partitionKey2Value2, ptnKey1.getIntValue().intValue());
        CompleteSanityTest.verifyDataType(partitionKey2Type2, ptnKey1.getDataType());
        assertEquals(201, ptnKey1.getKeyType().getCode());

        assertEquals(part2PropertyKey1, ptnKey2.getKeyName());
        assertEquals(part2PropertyKey1Value1, ptnKey2.getStringValue());
        CompleteSanityTest.verifyDataType(part2PropertyKey1Type1, ptnKey2.getDataType());
        assertEquals(202, ptnKey2.getKeyType().getCode());

        assertEquals(part2PropertyKey2, property1.getKeyName());
        assertEquals(part2PropertyKey2Value2, property1.getStringValue());
        CompleteSanityTest.verifyDataType(part2PropertyKey2Type2, property1.getDataType());
        assertEquals(202, property1.getKeyType().getCode());

        assertEquals(partitionKey1, property2.getKeyName());
        //assertEquals(partitionKey1Value1, property2.getLongValue());
        CompleteSanityTest.verifyDataType(partitionKey1Type1, property2.getDataType());
        assertEquals(201, property2.getKeyType().getCode());

        assertEquals(part1PropertyKey1, property3.getKeyName());
        assertEquals(part1PropertyKey1Value1, property3.getStringValue());
        CompleteSanityTest.verifyDataType(part1PropertyKey1Type1, property3.getDataType());
        assertEquals(202, property3.getKeyType().getCode());

        assertEquals(part1PropertyKey2, property4.getKeyName());
        assertEquals(part1PropertyKey2Value2, property4.getIntValue()
                .intValue());
        CompleteSanityTest.verifyDataType(part1PropertyKey2Type2, property4.getDataType());
        assertEquals(202, property4.getKeyType().getCode());

    }

    public void alterToModifyIntervalOwlTableProperty(String owlTableName,
            String databaseName, String propertyKeyName1, int propertyKeyValue1,
            String type1, String propertyKeyName2, String propertyKeyValue2,
            String type2, String partitionKey1, String partitionKey1Value1,
            String partitionKey1Type1, String partitionKey2,
            int partitionKey2Value2, String partitionKey2Type2,
            String part1PropertyKey1, String part1PropertyKey1Value1,
            String part1PropertyKey1Type1, String part1PropertyKey2,
            int part1PropertyKey2Value2, String part1PropertyKey2Type2,
            String part2PropertyKey1, String part2PropertyKey1Value1,
            String part2PropertyKey1Type1, String part2PropertyKey2,
            String part2PropertyKey2Value2, String part2PropertyKey2Type2,
            String delocation) throws OwlException {

        // String testCmd = "alter owltable " + owlTableName +
        // " within owldatabase "
        // + databaseName + " with partition (" + partitionKey1 + " = \""
        // + partitionKey1Value1 + "\", " + partitionKey2 + " = \""
        // + partitionKey2Value2 + "\"" + ") modify property("
        // + part1PropertyKey1 + " = \"" + part1PropertyKey1Value1
        // + "\", " + part1PropertyKey2 + " = \""
        // + part1PropertyKey2Value2 + "\", " + part2PropertyKey1
        // + " = \"" + part2PropertyKey1Value1 + "\", "
        // + part2PropertyKey2 + " = \"" + part2PropertyKey2Value2 + "\""
        // + ")";

        String TT = "alter owltable " + owlTableName + " within owldatabase "
        + databaseName + " with partition (" + partitionKey1 + " = \""
        + partitionKey1Value1 + "\"" + ") modify property("
        + part1PropertyKey1 + " = \"" + part1PropertyKey1Value1
        + "\", " + part1PropertyKey2 + " = \""
        + part1PropertyKey2Value2 + "\"" + ")";

        System.out.println(TT);
        client.execute(TT);

        verifyPublishDataElementToMultiplePartitionedIntervalOwlTable(
                owlTableName, databaseName, propertyKeyName1, propertyKeyValue1,
                type1, propertyKeyName2, propertyKeyValue2, type2,
                partitionKey1, partitionKey1Value1, partitionKey1Type1,
                partitionKey2, partitionKey2Value2, partitionKey2Type2,
                part1PropertyKey1, part1PropertyKey1Value1,
                part1PropertyKey1Type1, part1PropertyKey2,
                part1PropertyKey2Value2, part1PropertyKey2Type2,
                part2PropertyKey1, part2PropertyKey1Value1,
                part2PropertyKey1Type1, part2PropertyKey2,
                part2PropertyKey2Value2, part2PropertyKey2Type2, delocation);

        selectIntervalPartition(owlTableName, databaseName, propertyKeyName1,
                propertyKeyValue1, type1, propertyKeyName2, propertyKeyValue2,
                type2, partitionKey1, partitionKey1Value1, partitionKey1Type1,
                partitionKey2, partitionKey2Value2, partitionKey2Type2,
                part1PropertyKey1, part1PropertyKey1Value1,
                part1PropertyKey1Type1, part1PropertyKey2,
                part1PropertyKey2Value2, part1PropertyKey2Type2,
                part2PropertyKey1, part2PropertyKey1Value1,
                part2PropertyKey1Type1, part2PropertyKey2,
                part2PropertyKey2Value2, part2PropertyKey2Type2, delocation);

    }

    public void selectIntervalPartition(String owlTableName,
            String databaseName, String propertyKeyName1, int propertyKeyValue1,
            String type1, String propertyKeyName2, String propertyKeyValue2,
            String type2, String partitionKey1, String partitionKey1Value1,
            String partitionKey1Type1, String partitionKey2,
            int partitionKey2Value2, String partitionKey2Type2,
            String part1PropertyKey1, String part1PropertyKey1Value1,
            String part1PropertyKey1Type1, String part1PropertyKey2,
            int part1PropertyKey2Value2, String part1PropertyKey2Type2,
            String part2PropertyKey1, String part2PropertyKey1Value1,
            String part2PropertyKey1Type1, String part2PropertyKey2,
            String part2PropertyKey2Value2, String part2PropertyKey2Type2,
            String delocation) throws OwlException {

        String testCmd = "select partitionproperty objects from owltable "
            + owlTableName + " within owldatabase " + databaseName
            + " with partition (" + partitionKey1 + " = \""
            + partitionKey1Value1 + "\")";

        // System.out.println("???????????????? [CMD]" + testCmd);

        OwlResultObject result = client.execute(testCmd);

        // System.out
        // .println("???????????????????????????????????????????????????????????????[RESULT START]");
        //
        // System.out.println(OwlUtil.getJSONFromObject(result));
        //
        // System.out
        // .println("???????????????????????????????????????????????????????????????[RESULT END]");

        AssertionsForSelectIntervalPartitionProperty(result, owlTableName,
                databaseName, propertyKeyName1, propertyKeyValue1, type1,
                propertyKeyName2, propertyKeyValue2, type2, partitionKey1,
                partitionKey1Value1, partitionKey1Type1, partitionKey2,
                partitionKey2Value2, partitionKey2Type2, part1PropertyKey1,
                part1PropertyKey1Value1, part1PropertyKey1Type1,
                part1PropertyKey2, part1PropertyKey2Value2,
                part1PropertyKey2Type2, part2PropertyKey1,
                part2PropertyKey1Value1, part2PropertyKey1Type1,
                part2PropertyKey2, part2PropertyKey2Value2,
                part2PropertyKey2Type2, delocation);

    }

    public void AssertionsForSelectIntervalPartitionProperty(
            OwlResultObject result, String owlTableName, String databaseName,
            String propertyKeyName1, int propertyKeyValue1, String type1,
            String propertyKeyName2, String propertyKeyValue2, String type2,
            String partitionKey1, String partitionKey1Value1,
            String partitionKey1Type1, String partitionKey2,
            int partitionKey2Value2, String partitionKey2Type2,
            String part1PropertyKey1, String part1PropertyKey1Value1,
            String part1PropertyKey1Type1, String part1PropertyKey2,
            int part1PropertyKey2Value2, String part1PropertyKey2Type2,
            String part2PropertyKey1, String part2PropertyKey1Value1,
            String part2PropertyKey1Type1, String part2PropertyKey2,
            String part2PropertyKey2Value2, String part2PropertyKey2Type2,
            String delocation) throws OwlException {

        assertNotNull(result);

        List<? extends OwlObject> partitionProperties = result.getOutput();

        OwlPartitionProperty partProperties = (OwlPartitionProperty) partitionProperties
        .get(0);

        assertNotNull(partProperties);

        List<OwlKeyValue> owlKeyValue = partProperties.getKeyValues();

        OwlKeyValue property1 = owlKeyValue.get(0);
        OwlKeyValue property2 = owlKeyValue.get(1);
        // System.out.println("*********part1PropertyKey1" + part1PropertyKey1
        // + "*******");
        // System.out.println("*********property1.getKeyName()"
        // + property1.getKeyName() + "*******");

        assertEquals(part1PropertyKey1, property1.getKeyName());
        assertEquals(part1PropertyKey1Value1, property1.getStringValue());
        CompleteSanityTest.verifyDataType(part1PropertyKey1Type1, property1.getDataType());
        assertEquals(202, property1.getKeyType().getCode());

        // System.out.println("********************" +
        // property2.getIntValue().intValue() + " **********************");

        assertEquals(part1PropertyKey2, property2.getKeyName());
        assertEquals(part1PropertyKey2Value2, property2.getIntValue()
                .intValue());
        CompleteSanityTest.verifyDataType(part1PropertyKey2Type2, property2.getDataType());
        assertEquals(202, property2.getKeyType().getCode());

    }

    public void verifyDropIntervalProperty(String owlTableName,
            String databaseName, String propertyKeyName1, int propertyKeyValue1,
            String type1, String propertyKeyName2, String propertyKeyValue2,
            String type2, String partitionKey1, String partitionKey1Value1,
            String partitionKey1Type1, String partitionKey2,
            int partitionKey2Value2, String partitionKey2Type2,
            String part1PropertyKey1, String part1PropertyKey1Value1,
            String part1PropertyKey1Type1, String part1PropertyKey2,
            int part1PropertyKey2Value2, String part1PropertyKey2Type2,
            String part2PropertyKey1, String part2PropertyKey1Value1,
            String part2PropertyKey1Type1, String part2PropertyKey2,
            String part2PropertyKey2Value2, String part2PropertyKey2Type2,
            String delocation) throws OwlException {

        String testCmd = "select dataelement objects from owltable "
            + owlTableName + " within owldatabase " + databaseName
            + " with partition ( " + partitionKey1 + " = " + "\""
            + partitionKey1Value1 + "\"" + ") property ( "
            + part1PropertyKey1 + " = " + "\"" + part1PropertyKey1Value1
            + "\"" + ")";

        OwlResultObject result = client.execute(testCmd);

        AssertionsForDropIntervalProperty(result, owlTableName, databaseName,
                propertyKeyName1, propertyKeyValue1, type1, propertyKeyName2,
                propertyKeyValue2, type2, partitionKey1, partitionKey1Value1,
                partitionKey1Type1, partitionKey2, partitionKey2Value2,
                partitionKey2Type2, part1PropertyKey1, part1PropertyKey1Value1,
                part1PropertyKey1Type1, part1PropertyKey2,
                part1PropertyKey2Value2, part1PropertyKey2Type2,
                part2PropertyKey1, part2PropertyKey1Value1,
                part2PropertyKey1Type1, part2PropertyKey2,
                part2PropertyKey2Value2, part2PropertyKey2Type2, delocation);

    }

    public void AssertionsForSelectIntervalPartitionPropertyWithDroppedProperty(
            OwlResultObject result, String owlTableName, String databaseName,
            String propertyKeyName1, int propertyKeyValue1, String type1,
            String propertyKeyName2, String propertyKeyValue2, String type2,
            String partitionKey1, String partitionKey1Value1,
            String partitionKey1Type1, String partitionKey2,
            int partitionKey2Value2, String partitionKey2Type2,
            String part1PropertyKey1, String part1PropertyKey1Value1,
            String part1PropertyKey1Type1, String part1PropertyKey2,
            int part1PropertyKey2Value2, String part1PropertyKey2Type2,
            String part2PropertyKey1, String part2PropertyKey1Value1,
            String part2PropertyKey1Type1, String part2PropertyKey2,
            String part2PropertyKey2Value2, String part2PropertyKey2Type2,
            String delocation) throws OwlException {

        assertNotNull(result);

        List<? extends OwlObject> partitionProperties = result.getOutput();
        OwlPartitionProperty partProperties = (OwlPartitionProperty) partitionProperties
        .get(0);
        assertNotNull(partProperties);

        List<OwlKeyValue> owlKeyValue = partProperties.getKeyValues();
        OwlKeyValue property1 = owlKeyValue.get(0);

        assertEquals(1, owlKeyValue.size());
        assertEquals(part1PropertyKey1, property1.getKeyName());
        assertEquals(part1PropertyKey1Value1, property1.getStringValue());
        CompleteSanityTest.verifyDataType(part1PropertyKey1Type1, property1.getDataType());
        assertEquals(202, property1.getKeyType().getCode());

    }

    public void selectPartitionwithDroppedIntervalProperty(String owlTableName,
            String databaseName, String propertyKeyName1, int propertyKeyValue1,
            String type1, String propertyKeyName2, String propertyKeyValue2,
            String type2, String partitionKey1, String partitionKey1Value1,
            String partitionKey1Type1, String partitionKey2,
            int partitionKey2Value2, String partitionKey2Type2,
            String part1PropertyKey1, String part1PropertyKey1Value1,
            String part1PropertyKey1Type1, String part1PropertyKey2,
            int part1PropertyKey2Value2, String part1PropertyKey2Type2,
            String part2PropertyKey1, String part2PropertyKey1Value1,
            String part2PropertyKey1Type1, String part2PropertyKey2,
            String part2PropertyKey2Value2, String part2PropertyKey2Type2,
            String delocation) throws OwlException {

        String testCmd = "select partitionproperty objects from owltable "
            + owlTableName + " within owldatabase " + databaseName
            + " with partition (" + partitionKey1 + " = \""
            + partitionKey1Value1 + "\")";
        System.out.println(testCmd);
        OwlResultObject result = client.execute(testCmd);

        AssertionsForSelectIntervalPartitionPropertyWithDroppedProperty(result,
                owlTableName, databaseName, propertyKeyName1, propertyKeyValue1,
                type1, propertyKeyName2, propertyKeyValue2, type2,
                partitionKey1, partitionKey1Value1, partitionKey1Type1,
                partitionKey2, partitionKey2Value2, partitionKey2Type2,
                part1PropertyKey1, part1PropertyKey1Value1,
                part1PropertyKey1Type1, part1PropertyKey2,
                part1PropertyKey2Value2, part1PropertyKey2Type2,
                part2PropertyKey1, part2PropertyKey1Value1,
                part2PropertyKey1Type1, part2PropertyKey2,
                part2PropertyKey2Value2, part2PropertyKey2Type2, delocation);

        // System.out.println(OwlUtil.getJSONFromObject(result));

    }

    public void AssertionsForDropIntervalProperty(OwlResultObject result,
            String owlTableName, String databaseName, String propertyKeyName1,
            int propertyKeyValue1, String type1, String propertyKeyName2,
            String propertyKeyValue2, String type2, String partitionKey1,
            String partitionKey1Value1, String partitionKey1Type1,
            String partitionKey2, int partitionKey2Value2,
            String partitionKey2Type2, String part1PropertyKey1,
            String part1PropertyKey1Value1, String part1PropertyKey1Type1,
            String part1PropertyKey2, int part1PropertyKey2Value2,
            String part1PropertyKey2Type2, String part2PropertyKey1,
            String part2PropertyKey1Value1, String part2PropertyKey1Type1,
            String part2PropertyKey2, String part2PropertyKey2Value2,
            String part2PropertyKey2Type2, String delocation)
    throws OwlException {

        assertNotNull(result);
        List<? extends OwlObject> dataElement = result.getOutput();
        OwlDataElement owlDataElement = (OwlDataElement) dataElement.get(0);
        assertNotNull(owlDataElement);
        assertEquals(1, dataElement.size());
        assertEquals(delocation, owlDataElement.getStorageLocation());

        List<OwlKeyValue> owlKeyValue = owlDataElement.getKeyValues();
        OwlKeyValue ptnKey1 = owlKeyValue.get(0);
        OwlKeyValue ptnKey2 = owlKeyValue.get(1);
        OwlKeyValue property1 = owlKeyValue.get(2);
        OwlKeyValue property2 = owlKeyValue.get(3);
        OwlKeyValue property3 = owlKeyValue.get(4);
        assertEquals(partitionKey2, ptnKey1.getKeyName());
        assertEquals(partitionKey2Value2, ptnKey1.getIntValue().intValue());
        CompleteSanityTest.verifyDataType(partitionKey2Type2, ptnKey1.getDataType());
        assertEquals(201, ptnKey1.getKeyType().getCode());

        assertEquals(part2PropertyKey1, ptnKey2.getKeyName());
        assertEquals(part2PropertyKey1Value1, ptnKey2.getStringValue());
        assertEquals(part2PropertyKey1Type1, ptnKey2.getDataType().toString());
        assertEquals(202, ptnKey2.getKeyType().getCode());

        assertEquals(part2PropertyKey2, property1.getKeyName());
        assertEquals(part2PropertyKey2Value2, property1.getStringValue());
        CompleteSanityTest.verifyDataType(part2PropertyKey2Type2, property1.getDataType());
        assertEquals(202, property1.getKeyType().getCode());

        assertEquals(partitionKey1, property2.getKeyName());
        // assertEquals(partitionKey1Value1, property2.getLongValue());
        CompleteSanityTest.verifyDataType(partitionKey1Type1, property2.getDataType());
        assertEquals(201, property2.getKeyType().getCode());

        assertEquals(part1PropertyKey1, property3.getKeyName());
        assertEquals(part1PropertyKey1Value1, property3.getStringValue());
        CompleteSanityTest.verifyDataType(part1PropertyKey1Type1, property3.getDataType());
        assertEquals(202, property3.getKeyType().getCode());

    }

    public void alterToDropIntervalProperty(String owlTableName,
            String databaseName, String propertyKeyName1, int propertyKeyValue1,
            String type1, String propertyKeyName2, String propertyKeyValue2,
            String type2, String partitionKey1, String partitionKey1Value1,
            String partitionKey1Type1, String partitionKey2,
            int partitionKey2Value2, String partitionKey2Type2,
            String part1PropertyKey1, String part1PropertyKey1Value1,
            String part1PropertyKey1Type1, String part1PropertyKey2,
            int part1PropertyKey2Value2, String part1PropertyKey2Type2,
            String part2PropertyKey1, String part2PropertyKey1Value1,
            String part2PropertyKey1Type1, String part2PropertyKey2,
            String part2PropertyKey2Value2, String part2PropertyKey2Type2,
            String delocation) throws OwlException {

        String testCmd = "alter owltable " + owlTableName + " within owldatabase "
        + databaseName + " with partition (" + partitionKey1 + " = \""
        + partitionKey1Value1 + "\") drop property("
        + part1PropertyKey2 + " = \"" + part1PropertyKey2Value2
        + "\" )";

        client.execute(testCmd);

        verifyDropIntervalProperty(owlTableName, databaseName, propertyKeyName1,
                propertyKeyValue1, type1, propertyKeyName2, propertyKeyValue2,
                type2, partitionKey1, partitionKey1Value1, partitionKey1Type1,
                partitionKey2, partitionKey2Value2, partitionKey2Type2,
                part1PropertyKey1, part1PropertyKey1Value1,
                part1PropertyKey1Type1, part1PropertyKey2,
                part1PropertyKey2Value2, part1PropertyKey2Type2,
                part2PropertyKey1, part2PropertyKey1Value1,
                part2PropertyKey1Type1, part2PropertyKey2,
                part2PropertyKey2Value2, part2PropertyKey2Type2, delocation);

        selectPartitionwithDroppedIntervalProperty(owlTableName, databaseName,
                propertyKeyName1, propertyKeyValue1, type1, propertyKeyName2,
                propertyKeyValue2, type2, partitionKey1, partitionKey1Value1,
                partitionKey1Type1, partitionKey2, partitionKey2Value2,
                partitionKey2Type2, part1PropertyKey1, part1PropertyKey1Value1,
                part1PropertyKey1Type1, part1PropertyKey2,
                part1PropertyKey2Value2, part1PropertyKey2Type2,
                part2PropertyKey1, part2PropertyKey1Value1,
                part2PropertyKey1Type1, part2PropertyKey2,
                part2PropertyKey2Value2, part2PropertyKey2Type2, delocation);

    }

    @Test
    public void testCreateIntervalDatabase() throws OwlException {
        csit.createDatabase("testIntervalDatabase1", "testloc1");
        csit.createDatabase("testIntervalDatabase2", "testloc2");
        csit.createDatabase("testIntervalDatabase3", "testloc3");
    }

    @Test
    public void testCreateMultiplePartitionedIntervalOwlTable()
    throws OwlException {

        createMultiplePartitionedIntervalOwlTable("testIntervalOwlTable1",
                "testIntervalDatabase1", "retention", "INTEGER", "domain",
                "STRING", "INTERVAL (\"2009-10-28 00:00:00 PDT\", 15 Minutes)",
                "date", "LONG", "zip", "INTEGER", "climate", "STRING",
                "countrycode", "INTEGER", "city", "STRING", "state", "STRING");

        createMultiplePartitionedIntervalOwlTable("testIntervalOwlTable2",
                "testIntervalDatabase1", "percentComplete", "INTEGER", "name",
                "STRING", "INTERVAL (\"2009-10-28 00:10:00 PDT\", 30 Minutes)",
                "datest", "LONG", "age", "INTEGER", "nationality", "STRING",
                "yob", "INTEGER", "company", "STRING", "designation", "STRING");

    }

    @Test
    public void testDescribeMultiplePartitionedIntervalOwlTable()
    throws OwlException {
        mpit.describeMultiplePartitionedOwlTable("testIntervalOwlTable1",
                "testIntervalDatabase1", "retention", "INTEGER", "domain",
                "STRING", "date", "LONG", "zip", "INTEGER", "climate",
                "STRING", "countrycode", "INTEGER", "city", "STRING", "state",
        "STRING");

        mpit.describeMultiplePartitionedOwlTable("testIntervalOwlTable2",
                "testIntervalDatabase1", "percentComplete", "INTEGER", "name",
                "STRING", "datest", "LONG", "age", "INTEGER", "nationality",
                "STRING", "yob", "INTEGER", "company", "STRING", "designation",
        "STRING");
    }

    @Test
    public void testPublishToMutiplePartitionedIntervalOwlTable()
    throws OwlException {
        publishDataElementToMultiplePartitionedIntervalOwlTable(
                "testIntervalOwlTable1", "testIntervalDatabase1", "retention",
                10, "INTEGER", "Domain", "US", "STRING", "date",
                "2009-10-28 00:00:00 PDT", "LONG", "zip", 94086, "INTEGER",
                "climate", "sunny", "STRING", "countrycode", 100, "INTEGER",
                "city", "sunnyvale", "STRING", "state", "CA", "STRING", "hdfs");

        publishDataElementToMultiplePartitionedIntervalOwlTable(
                "testIntervalOwlTable2", "testIntervalDatabase1",
                "percentComplete", 50, "INTEGER", "name", "abc", "STRING",
                "datest", "2009-10-28 00:10:00 PDT", "LONG", "age", 25,
                "INTEGER", "nationality", "indian", "STRING", "yob", 1984,
                "INTEGER", "company", "def", "STRING", "designation", "swengg",
                "STRING", "hdfs1");

    }

    @Test
    public void testVerifyPublishDataElementToMultiplePartitionedIntervalOwlTable()
    throws OwlException {

        verifyPublishDataElementToMultiplePartitionedIntervalOwlTable(
                "testIntervalOwlTable2", "testIntervalDatabase1",
                "percentComplete", 50, "INTEGER", "name", "abc", "STRING",
                "datest", "2009-10-28 00:10:00 PDT", "LONG", "age", 25,
                "INTEGER", "nationality", "indian", "STRING", "yob", 1984,
                "INTEGER", "company", "def", "STRING", "designation", "swengg",
                "STRING", "hdfs1");
    }

    @Test
    public void testSelectIntervalPartition() throws OwlException {
        selectIntervalPartition("testIntervalOwlTable1",
                "testIntervalDatabase1", "retention", 10, "INTEGER", "domain",
                "US", "STRING", "date", "2009-10-28 00:00:00 PDT", "LONG",
                "zip", 94086, "INTEGER", "climate", "sunny", "STRING",
                "countrycode", 100, "INTEGER", "city", "sunnyvale", "STRING",
                "state", "CA", "STRING", "hdfs");

        selectIntervalPartition("testIntervalOwlTable2",
                "testIntervalDatabase1", "percentComplete", 50, "INTEGER",
                "name", "abc", "STRING", "datest", "2009-10-28 00:10:00 PDT",
                "LONG", "age", 25, "INTEGER", "nationality", "indian",
                "STRING", "yob", 1984, "INTEGER", "company", "def", "STRING",
                "designation", "swengg", "STRING", "hdfs1");
    }

    @Test
    public void testAlterToModifyIntervalOwlTableProperty() throws OwlException {

        alterToModifyIntervalOwlTableProperty("testIntervalOwlTable1",
                "testIntervalDatabase1", "retention", 10, "INTEGER", "domain",
                "US", "STRING", "date", "2009-10-28 00:00:00 PDT", "LONG",
                "zip", 94086, "INTEGER", "climate", "rainy", "STRING",
                "countrycode", 100, "INTEGER", "city", "sunnyvale", "STRING",
                "state", "CA", "STRING", "hdfs");

    }

    @Test
    public void testAlterToDropIntervalPartitionProperty() throws OwlException {

        alterToDropIntervalProperty("testIntervalOwlTable1",
                "testIntervalDatabase1", "retention", 10, "INTEGER", "domain",
                "US", "STRING", "date", "2009-10-28 00:00:00 PDT", "LONG",
                "zip", 94086, "INTEGER", "climate", "rainy", "STRING",
                "countrycode", 100, "INTEGER", "city", "sunnyvale", "STRING",
                "state", "CA", "STRING", "hdfs");

    }

    @Test
    public void testDropMultiplePartitionedIntervalOwlTable()
    throws OwlException {
        mpit.dropMultipleOwlTable("testIntervalOwlTable1",
        "testIntervalDatabase1");
        mpit.dropMultipleOwlTable("testIntervalOwlTable2",
        "testIntervalDatabase1");
    }

    @Test
    public void testDropDatabaseWithMultiplePartitionedIntervalOwlTable()
    throws OwlException {
        csit.dropDatabase("testIntervalDatabase1");
        csit.dropDatabase("testIntervalDatabase2");
        csit.dropDatabase("testIntervalDatabase3");
    }

    @Test
    public void cleanup() throws OwlException {
        csit.cleanup("testIntervalDatabase1", "testIntervalOwlTable1");
        csit.cleanup("testIntervalDatabase1", "testIntervalOwlTable2");
    }

}
