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
import org.apache.hadoop.owl.protocol.OwlPartitionKey;
import org.apache.hadoop.owl.protocol.OwlPartitionProperty;
import org.apache.hadoop.owl.protocol.OwlPropertyKey;
import org.apache.hadoop.owl.protocol.OwlResultObject;
import org.apache.hadoop.owl.protocol.OwlTable;
import org.junit.Before;
import org.junit.Test;

public class MultiplePartitionTest extends OwlTestCase {
    private int counter = 0;
    private static OwlClient client;
    private CompleteSanityTest mpt;

    public MultiplePartitionTest() {
        client = new OwlClient(getUri());
        this.mpt = new CompleteSanityTest();
    }

    @Before
    public void testInitialize() {
    }

    public void createMultiplePartitionedOwlTable(String owlTableName,
            String databaseName, String propertyKeyName1, String type1,
            String propertyKeyName2, String type2, String ptnType, String partitionKey1,
            String partitionKey1Type1, String partitionKey2,
            String partitionKey2Type2, String part1PropertyKey1,
            String part1PropertyKey1Type1, String part1PropertyKey2,
            String part1PropertyKey2Type2, String part2PropertyKey1,
            String part2PropertyKey1Type1, String part2PropertyKey2,
            String part2PropertyKey2Type2) throws OwlException {

        System.out.println("Owl Table name " + owlTableName
                + " within owldatabase " + databaseName);

        String testCmd = "create owltable type basic " + owlTableName
        + " within owldatabase " + databaseName + " define property key "
        + propertyKeyName1 + " : " + type1 + " , " + propertyKeyName2
        + " : " + type2 + " partitioned by " + ptnType + " with partition key "
        + partitionKey1 + " : " + partitionKey1Type1
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

        verifyCreateMultiplePartitionedOwlTable(owlTableName, databaseName,
                propertyKeyName1, type1, propertyKeyName2, type2,
                partitionKey1, partitionKey1Type1, partitionKey2,
                partitionKey2Type2, part1PropertyKey1, part1PropertyKey1Type1,
                part1PropertyKey2, part1PropertyKey2Type2, part2PropertyKey1,
                part2PropertyKey1Type1, part2PropertyKey2,
                part2PropertyKey2Type2);

    }

    public void verifyCreateMultiplePartitionedOwlTable(String owlTableName,
            String databaseName, String propertyKeyName1, String type1,
            String propertyKeyName2, String type2, String partitionKey1,
            String partitionKey1Type1, String partitionKey2,
            String partitionKey2Type2, String part1PropertyKey1,
            String part1PropertyKey1Type1, String part1PropertyKey2,
            String part1PropertyKey2Type2, String part2PropertyKey1,
            String part2PropertyKey1Type1, String part2PropertyKey2,
            String part2PropertyKey2Type2) throws OwlException {
        OwlResultObject result = client
        .execute("select OwlTable objects where owldatabase in ("
                + databaseName + ")");

        AssertionsForSelectAndDescribeMultiplePartitionedOwlTable(result,
                owlTableName, databaseName, propertyKeyName1, type1,
                propertyKeyName2, type2, partitionKey1, partitionKey1Type1,
                partitionKey2, partitionKey2Type2, part1PropertyKey1,
                part1PropertyKey1Type1, part1PropertyKey2,
                part1PropertyKey2Type2, part2PropertyKey1,
                part2PropertyKey1Type1, part2PropertyKey2,
                part2PropertyKey2Type2);

    }

    public void publishDataElementToMultiplePartitionedOwlTable(
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

        verifyPublishDataElementToMultiplePartitionedOwlTable(owlTableName,
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

    public void verifyPublishDataElementToMultiplePartitionedOwlTable(
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


        // System.out.println("************ Select DE - " + testCmd +
        //   "*****************\n");
        OwlResultObject result = client.execute(testCmd);

        // System.out.println("1*********" + OwlUtil.getJSONFromObject(result));

        AssertionsForPublishDataElementToMultiplePartitionedOwlTable(result,
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

    public void AssertionsForPublishDataElementToMultiplePartitionedOwlTable(
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
        // System.out.println(owlDataElement.getLoader());

        List<OwlKeyValue> owlKeyValue = owlDataElement.getKeyValues();
        OwlKeyValue ptnKey1 = owlKeyValue.get(0);
        OwlKeyValue ptnKey2 = owlKeyValue.get(1);
        OwlKeyValue property1 = owlKeyValue.get(2);
        OwlKeyValue property2 = owlKeyValue.get(3);
        OwlKeyValue property3 = owlKeyValue.get(4);
        OwlKeyValue property4 = owlKeyValue.get(5);
        /*
         * System.out.println("********* ptnKey1.getKeyName()" + " " +
         * owlKeyValue.get(0).getKeyName() + " - " +
         * owlKeyValue.get(0).getIntValue());
         * System.out.println("********* ptnKey2.getKeyName()" + " " +
         * owlKeyValue.get(1).getKeyName() + " - " +
         * owlKeyValue.get(1).getStringValue());
         * 
         * System.out.println("********* property1.getKeyName()" + " " +
         * owlKeyValue.get(2).getKeyName() + " - " +
         * owlKeyValue.get(2).getStringValue());
         * System.out.println("********* property2.getKeyName()" + " " +
         * owlKeyValue.get(3).getKeyName() + " - " +
         * owlKeyValue.get(3).getStringValue());
         * 
         * System.out.println("********* property3.getKeyName()" + " " +
         * owlKeyValue.get(4).getKeyName() + " - " +
         * owlKeyValue.get(4).getStringValue());
         * System.out.println("********* property4.getKeyName()" + " " +
         * owlKeyValue.get(5).getKeyName() + " - " +
         * owlKeyValue.get(5).getIntValue());
         */
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
        assertEquals(partitionKey1Value1, property2.getStringValue());
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

    public void describeMultiplePartitionedOwlTable(String owlTableName,
            String databaseName, String propertyKeyName1, String type1,
            String propertyKeyName2, String type2, String partitionKey1,
            String partitionKey1Type1, String partitionKey2,
            String partitionKey2Type2, String part1PropertyKey1,
            String part1PropertyKey1Type1, String part1PropertyKey2,
            String part1PropertyKey2Type2, String part2PropertyKey1,
            String part2PropertyKey1Type1, String part2PropertyKey2,
            String part2PropertyKey2Type2) throws OwlException {

        counter = 0;
        OwlResultObject result = client.execute("describe owltable "
                + owlTableName + " within owldatabase " + databaseName);

        AssertionsForSelectAndDescribeMultiplePartitionedOwlTable(result,
                owlTableName, databaseName, propertyKeyName1, type1,
                propertyKeyName2, type2, partitionKey1, partitionKey1Type1,
                partitionKey2, partitionKey2Type2, part1PropertyKey1,
                part1PropertyKey1Type1, part1PropertyKey2,
                part1PropertyKey2Type2, part2PropertyKey1,
                part2PropertyKey1Type1, part2PropertyKey2,
                part2PropertyKey2Type2);

    }

    public void AssertionsForSelectAndDescribeMultiplePartitionedOwlTable(
            OwlResultObject result, String owlTableName, String databaseName,
            String propertyKeyName1, String type1, String propertyKeyName2,
            String type2, String partitionKey1, String partitionKey1Type1,
            String partitionKey2, String partitionKey2Type2,
            String part1PropertyKey1, String part1PropertyKey1Type1,
            String part1PropertyKey2, String part1PropertyKey2Type2,
            String part2PropertyKey1, String part2PropertyKey1Type1,
            String part2PropertyKey2, String part2PropertyKey2Type2) {

        assertNotNull(result);
        List<? extends OwlObject> output = result.getOutput();
        OwlTable owlTable = (OwlTable) output.get(counter);
        counter++;
        assertEquals(counter, output.size());
        // System.out.println("owltableName : " + owlTableName);
        // System.out.println("owlTable.getName() : " + owlTable.getName());
        assertEquals(owlTableName, owlTable.getName().getTableName());
        assertEquals(databaseName, owlTable.getName().getDatabaseName());
        assertEquals(6, owlTable.getPropertyKeys().size());

        List<OwlPropertyKey> owlPropertyKeys = owlTable.getPropertyKeys();
        OwlPropertyKey key1 = owlPropertyKeys.get(0);
        OwlPropertyKey key2 = owlPropertyKeys.get(1);
        OwlPropertyKey key3 = owlPropertyKeys.get(2);
        OwlPropertyKey key4 = owlPropertyKeys.get(3);
        OwlPropertyKey key5 = owlPropertyKeys.get(4);
        OwlPropertyKey key6 = owlPropertyKeys.get(5);

        String basePropertyKey1 = key1.getName();
        String basePropertyKey2 = key2.getName();
        String partKey1 = key3.getName();
        String partKey2 = key4.getName();
        String partKey3 = key5.getName();
        String partKey4 = key6.getName();

        // System.out.println(basePartKey + "\n" + partKey1 + "\n" + partKey2);
        assertNotNull(output);
        assertEquals(propertyKeyName1, basePropertyKey1);
        CompleteSanityTest.verifyDataType(type1, key1.getDataType());

        assertEquals(propertyKeyName2, basePropertyKey2);
        CompleteSanityTest.verifyDataType(type2, key2.getDataType());

        assertEquals(part1PropertyKey1, partKey1);
        CompleteSanityTest.verifyDataType(part1PropertyKey1Type1, key3.getDataType());

        assertEquals(part1PropertyKey2, partKey2);
        CompleteSanityTest.verifyDataType(part1PropertyKey2Type2, key4.getDataType());

        assertEquals(part2PropertyKey1, partKey3);
        CompleteSanityTest.verifyDataType(part2PropertyKey1Type1, key5.getDataType());

        assertEquals(part2PropertyKey2, partKey4);
        CompleteSanityTest.verifyDataType(part2PropertyKey2Type2, key6.getDataType());

        List<OwlPartitionKey> partitionKeys = owlTable.getPartitionKeys();
        OwlPartitionKey ptnKey1 = partitionKeys.get(0);
        String partKeyName1 = ptnKey1.getName();
        assertEquals(partitionKey1, partKeyName1);
        CompleteSanityTest.verifyDataType(partitionKey1Type1, ptnKey1.getDataType());
        assertEquals(ptnKey1.getLevel(), 1);

        OwlPartitionKey ptnKey2 = partitionKeys.get(1);
        String partKeyName2 = ptnKey2.getName();
        assertEquals(partitionKey2, partKeyName2);
        // System.out.println(partKeyName2 + " : "
        // + ptnKey2.getDataType().toString());
        CompleteSanityTest.verifyDataType(partitionKey2Type2, ptnKey2.getDataType());
        assertEquals(ptnKey1.getLevel(), 1);

    }

    public void dropMultipleOwlTable(String owlTableName, String databaseName)
    throws OwlException {

        String testCmd = "drop owltable " + owlTableName + " within owldatabase "
        + databaseName;
        client.execute(testCmd);

        OwlResultObject result = client
        .execute("select OwlTable objects where owldatabase in ("
                + databaseName + ")");

        assertEquals("SUCCESS", result.getStatus().toString());
    }

    public void alterToModifyOwlTableProperty(String owlTableName,
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
        + partitionKey1Value1 + "\", " + partitionKey2 + " = \""
        + partitionKey2Value2 + "\"" + ") modify property("
        + part1PropertyKey1 + " = \"" + part1PropertyKey1Value1
        + "\", " + part1PropertyKey2 + " = \""
        + part1PropertyKey2Value2 + "\", " + part2PropertyKey1
        + " = \"" + part2PropertyKey1Value1 + "\", "
        + part2PropertyKey2 + " = \"" + part2PropertyKey2Value2 + "\""
        + ")";

        String TT = "alter owltable " + owlTableName + " within owldatabase "
        + databaseName + " with partition (" + partitionKey1 + " = \""
        + partitionKey1Value1 + "\"" + ") modify property("
        + part1PropertyKey1 + " = \"" + part1PropertyKey1Value1
        + "\", " + part1PropertyKey2 + " = \""
        + part1PropertyKey2Value2 + "\""
        + ")";

        System.out.println(TT);
        client.execute(TT);

        verifyPublishDataElementToMultiplePartitionedOwlTable(owlTableName,
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

        selectPartition(owlTableName, databaseName, propertyKeyName1,
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

    public void selectPartition(String owlTableName, String databaseName,
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

        String testCmd = "select partitionproperty objects from owltable "
            + owlTableName + " within owldatabase " + databaseName
            + " with partition (" + partitionKey1 + " = \""
            + partitionKey1Value1 + "\")";

        System.out.println("???????????????? [CMD]" + testCmd);
        //
        OwlResultObject result = client.execute(testCmd);
        //
        System.out.println("???????????????????????????????????????????????????????????????[RESULT START]");
        //
        System.out.println(OwlUtil.getJSONFromObject(result));
        //
        System.out.println("???????????????????????????????????????????????????????????????[RESULT END]");

        AssertionsForSelectPartitionProperty(result, owlTableName, databaseName,
                propertyKeyName1, propertyKeyValue1, type1, propertyKeyName2,
                propertyKeyValue2, type2, partitionKey1, partitionKey1Value1,
                partitionKey1Type1, partitionKey2, partitionKey2Value2,
                partitionKey2Type2, part1PropertyKey1, part1PropertyKey1Value1,
                part1PropertyKey1Type1, part1PropertyKey2,
                part1PropertyKey2Value2, part1PropertyKey2Type2,
                part2PropertyKey1, part2PropertyKey1Value1,
                part2PropertyKey1Type1, part2PropertyKey2,
                part2PropertyKey2Value2, part2PropertyKey2Type2, delocation);

        // testCmd = "select partitionproperty objects from owltable "
        // + owlTableName + " within owldatabase " + databaseName
        // + " with partition (" + partitionKey2 + " = \""
        // + partitionKey2Value2 + "\")";
        //
        // System.out.println(testCmd);
        //
        // result = client.execute(testCmd);
        //
        // AssertionsForSelectPartitionProperty(result, owlTableName,
        // databaseName,
        // propertyKeyName1, propertyKeyValue1, type1, propertyKeyName2,
        // propertyKeyValue2, type2, partitionKey1, partitionKey1Value1,
        // partitionKey1Type1, partitionKey2, partitionKey2Value2,
        // partitionKey2Type2, part1PropertyKey1, part1PropertyKey1Value1,
        // part1PropertyKey1Type1, part1PropertyKey2,
        // part1PropertyKey2Value2, part1PropertyKey2Type2,
        // part2PropertyKey1, part2PropertyKey1Value1,
        // part2PropertyKey1Type1, part2PropertyKey2,
        // part2PropertyKey2Value2, part2PropertyKey2Type2, delocation);

        // System.out.println(OwlUtil.getJSONFromObject(result));

    }

    public void AssertionsForSelectPartitionProperty(OwlResultObject result,
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

        List<? extends OwlObject> partitionProperties = result.getOutput();
        OwlPartitionProperty partProperties = (OwlPartitionProperty) partitionProperties
        .get(0);

        assertNotNull(partProperties);

        List<OwlKeyValue> owlKeyValue = partProperties.getKeyValues();

        OwlKeyValue property1 = owlKeyValue.get(0);
        OwlKeyValue property2 = owlKeyValue.get(1);
        // OwlKeyValue property3 = owlKeyValue.get(2);
        // OwlKeyValue property4 = owlKeyValue.get(3);

        // Integer propertyKeys = owlKeyValue.size();

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

        // assertEquals(part2PropertyKey1, property3.getKeyName());
        // assertEquals(part2PropertyKey1Value1, property3.getStringValue());
        // assertEquals(part2PropertyKey1Type1,
        // property3.getDataType().toString());
        // assertEquals(202, property3.getKeyType().getCode());
        //
        // assertEquals(part2PropertyKey2, property4.getKeyName());
        // assertEquals(part2PropertyKey2Value2, property4.getStringValue());
        // assertEquals(part2PropertyKey2Type2,
        // property4.getDataType().toString());
        // assertEquals(202, property4.getKeyType().getCode());

    }

    public void verifyDropProperty(String owlTableName, String databaseName,
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

        String testCmd = "select dataelement objects from owltable "
            + owlTableName + " within owldatabase " + databaseName
            + " with partition ( " + partitionKey1 + " = " + "\""
            + partitionKey1Value1 + "\"" + ") property ( "
            + part1PropertyKey1 + " = " + "\"" + part1PropertyKey1Value1
            + "\"" + ")";

        OwlResultObject result = client.execute(testCmd);

        AssertionsForDropProperty(result, owlTableName, databaseName,
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

    public void AssertionsForSelectPartitionPropertyWithDroppedProperty(
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

    public void selectPartitionwithDroppedProperty(String owlTableName,
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

        AssertionsForSelectPartitionPropertyWithDroppedProperty(result,
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

    public void AssertionsForDropProperty(OwlResultObject result,
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

        // List<OwlKeyValue> owlKeyValue = owlDataElement.getKeyValues();
        // OwlKeyValue ptnKey = (OwlKeyValue) owlKeyValue.get(0);
        // OwlKeyValue property1 = (OwlKeyValue) owlKeyValue.get(1);
        // OwlKeyValue property2 = (OwlKeyValue) owlKeyValue.get(2);
        // OwlKeyValue property3 = (OwlKeyValue) owlKeyValue.get(3);
        //
        // assertEquals(partitionKey2, ptnKey.getKeyName());
        // assertEquals(partitionKey2Value2, ptnKey.getStringValue());
        // assertEquals(partitionKey2Type2, ptnKey.getDataType().toString());
        // assertEquals(201, ptnKey.getKeyType().getCode());
        //
        // assertEquals(part2PropertyKey1, property1.getKeyName());
        // assertEquals(part2PropertyKey1Value1, property1.getStringValue());
        // assertEquals(part2PropertyKey1Type1,
        // property1.getDataType().toString());
        // assertEquals(202, property1.getKeyType().getCode());
        //
        // assertEquals(part2PropertyKey2, property1.getKeyName());
        // assertEquals(part2PropertyKey2Value2, property1.getStringValue());
        // assertEquals(part2PropertyKey2Type2,
        // property1.getDataType().toString());
        // assertEquals(202, property2.getKeyType().getCode());
        //
        // assertEquals(part1PropertyKey2, property1.getKeyName());
        // assertEquals(part2PropertyKey2Value2, property1.getStringValue());
        // assertEquals(part2PropertyKey2Type2,
        // property1.getDataType().toString());
        // assertEquals(202, property2.getKeyType().getCode());

        List<OwlKeyValue> owlKeyValue = owlDataElement.getKeyValues();
        OwlKeyValue ptnKey1 = owlKeyValue.get(0);
        OwlKeyValue ptnKey2 = owlKeyValue.get(1);
        OwlKeyValue property1 = owlKeyValue.get(2);
        OwlKeyValue property2 = owlKeyValue.get(3);
        OwlKeyValue property3 = owlKeyValue.get(4);
        // OwlKeyValue property4 = owlKeyValue.get(5);
        /*
         * System.out.println("********* ptnKey1.getKeyName()" + " " +
         * owlKeyValue.get(0).getKeyName() + " - " +
         * owlKeyValue.get(0).getIntValue());
         * System.out.println("********* ptnKey2.getKeyName()" + " " +
         * owlKeyValue.get(1).getKeyName() + " - " +
         * owlKeyValue.get(1).getStringValue());
         * 
         * System.out.println("********* property1.getKeyName()" + " " +
         * owlKeyValue.get(2).getKeyName() + " - " +
         * owlKeyValue.get(2).getStringValue());
         * System.out.println("********* property2.getKeyName()" + " " +
         * owlKeyValue.get(3).getKeyName() + " - " +
         * owlKeyValue.get(3).getStringValue());
         * 
         * System.out.println("********* property3.getKeyName()" + " " +
         * owlKeyValue.get(4).getKeyName() + " - " +
         * owlKeyValue.get(4).getStringValue());
         * System.out.println("********* property4.getKeyName()" + " " +
         * owlKeyValue.get(5).getKeyName() + " - " +
         * owlKeyValue.get(5).getIntValue());
         */
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
        assertEquals(partitionKey1Value1, property2.getStringValue());
        CompleteSanityTest.verifyDataType(partitionKey1Type1, property2.getDataType());
        assertEquals(201, property2.getKeyType().getCode());

        assertEquals(part1PropertyKey1, property3.getKeyName());
        assertEquals(part1PropertyKey1Value1, property3.getStringValue());
        CompleteSanityTest.verifyDataType(part1PropertyKey1Type1, property3.getDataType());
        assertEquals(202, property3.getKeyType().getCode());

    }

    public void alterToDropProperty(String owlTableName, String databaseName,
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

        String testCmd = "alter owltable " + owlTableName + " within owldatabase "
        + databaseName + " with partition (" + partitionKey1 + " = \""
        + partitionKey1Value1 + "\") drop property("
        + part1PropertyKey2 + " = \"" + part1PropertyKey2Value2
        + "\" )";

        client.execute(testCmd);

        verifyDropProperty(owlTableName, databaseName, propertyKeyName1,
                propertyKeyValue1, type1, propertyKeyName2, propertyKeyValue2,
                type2, partitionKey1, partitionKey1Value1, partitionKey1Type1,
                partitionKey2, partitionKey2Value2, partitionKey2Type2,
                part1PropertyKey1, part1PropertyKey1Value1,
                part1PropertyKey1Type1, part1PropertyKey2,
                part1PropertyKey2Value2, part1PropertyKey2Type2,
                part2PropertyKey1, part2PropertyKey1Value1,
                part2PropertyKey1Type1, part2PropertyKey2,
                part2PropertyKey2Value2, part2PropertyKey2Type2, delocation);

        selectPartitionwithDroppedProperty(owlTableName, databaseName,
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

    public void alterToDropDataElement(String owlTableName, String databaseName,
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

        String testCmd = "alter owlTable " + owlTableName + " within owldatabase "
        + databaseName + " with partition (" + partitionKey1 + " = \""
        + partitionKey1Value1 + "\") delete dataelement";
        client.execute(testCmd);

        OwlResultObject result = client
        .execute("select dataelement objects from owltable "
                + owlTableName + " within owldatabase " + databaseName
                + " with partition ( " + partitionKey1 + " = " + "\""
                + partitionKey1Value1 + "\"" + ") property ( "
                + part1PropertyKey1 + " = " + "\""
                + part1PropertyKey1Value1 + "\"" + ")");

        assertTrue(result.getOutput().isEmpty());

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

    @Test
    public void testCreateDatabase() throws OwlException {
        mpt.createDatabase("testDatabase1", "testloc1");
        mpt.createDatabase("testDatabase2", "testloc2");
        mpt.createDatabase("testDatabase3", "testloc3");
    }

    @Test
    public void testCreateMultiplePartitionedOwlTable() throws OwlException {

        createMultiplePartitionedOwlTable("testOwlTable1", "testDatabase1",
                "Retention", "INTEGER", "Domain", "STRING", "LIST", "country",
                "STRING", "zip", "INTEGER", "climate", "STRING", "countrycode",
                "INTEGER", "city", "STRING", "state", "STRING");

        createMultiplePartitionedOwlTable("testOwlTable2", "testDatabase1",
                "percentComplete", "INTEGER", "name", "STRING", "LIST", "city",
                "STRING", "age", "INTEGER", "nationality", "STRING", "yob",
                "INTEGER", "company", "STRING", "designation", "STRING");

    }

    @Test
    public void testDescribeMultiplePartitionedOwlTable() throws OwlException {
        describeMultiplePartitionedOwlTable("testOwlTable1", "testDatabase1",
                "Retention", "INTEGER", "Domain", "STRING", "country",
                "STRING", "zip", "INTEGER", "climate", "STRING", "countrycode",
                "INTEGER", "city", "STRING", "state", "STRING");

        describeMultiplePartitionedOwlTable("testOwlTable2", "testDatabase1",
                "percentComplete", "INTEGER", "name", "STRING", "city",
                "STRING", "age", "INTEGER", "nationality", "STRING", "yob",
                "INTEGER", "company", "STRING", "designation", "STRING");
    }

    @Test
    public void testPublishToMutiplePartitionedOwlTable() throws OwlException {
        publishDataElementToMultiplePartitionedOwlTable("testOwlTable1",
                "testDatabase1", "Retention", 10, "INTEGER", "Domain", "US",
                "STRING", "country", "USA", "STRING", "zip", 94086, "INTEGER",
                "climate", "sunny", "STRING", "countrycode", 100, "INTEGER",
                "city", "sunnyvale", "STRING", "state", "CA", "STRING", "hdfs");

        publishDataElementToMultiplePartitionedOwlTable("testOwlTable2",
                "testDatabase1", "percentComplete", 50, "INTEGER", "name",
                "abc", "STRING", "city", "sunnyvale", "STRING", "age", 25,
                "INTEGER", "nationality", "indian", "STRING", "yob", 1984,
                "INTEGER", "company", "def", "STRING", "designation", "swengg",
                "STRING", "hdfs1");

    }

    @Test
    public void testVerifyPublishDataElementToMultiplePartitionedOwlTable()
    throws OwlException {

        verifyPublishDataElementToMultiplePartitionedOwlTable("testOwlTable2",
                "testDatabase1", "percentComplete", 50, "INTEGER", "name",
                "abc", "STRING", "city", "sunnyvale", "STRING", "age", 25,
                "INTEGER", "nationality", "indian", "STRING", "yob", 1984,
                "INTEGER", "company", "def", "STRING", "designation", "swengg",
                "STRING", "hdfs1");
    }

    @Test
    public void testSelectPartition() throws OwlException {
        selectPartition("testOwlTable1", "testDatabase1", "Retention", 10,
                "INTEGER", "Domain", "US", "STRING", "country", "USA",
                "STRING", "zip", 94086, "INTEGER", "climate", "sunny",
                "STRING", "countrycode", 100, "INTEGER", "city", "sunnyvale",
                "STRING", "state", "CA", "STRING", "hdfs");

        selectPartition("testOwlTable2", "testDatabase1", "percentComplete", 50,
                "INTEGER", "name", "abc", "STRING", "city", "sunnyvale",
                "STRING", "age", 25, "INTEGER", "nationality", "indian",
                "STRING", "yob", 1984, "INTEGER", "company", "def", "STRING",
                "designation", "swengg", "STRING", "hdfs1");
    }

    @Test
    public void testAlterToModifyOwlTableProperty() throws OwlException {

        alterToModifyOwlTableProperty("testOwlTable1", "testDatabase1",
                "Retention", 10, "INTEGER", "Domain", "US", "STRING",
                "country", "USA", "STRING", "zip", 94086, "INTEGER", "climate",
                "rainy", "STRING", "countrycode", 100, "INTEGER", "city",
                "sunnyvale", "STRING", "state", "CA", "STRING", "hdfs");

        //alterToModifyOwlTableProperty("testOwlTable1", "testDatabase1",
        //"Retention", 10, "INTEGER", "Domain", "US", "STRING",
        //"country", "USA", "STRING", "zip", 94086, "INTEGER", "climate",
        //"windy", "STRING", "countrycode", 91, "INTEGER", "city",
        //"portland", "STRING", "state", "OR", "STRING", "hdfs");
        //
    }
    @Test
    public void testAlterToDropPartitionProperty() throws OwlException {

        alterToDropProperty("testOwlTable1", "testDatabase1", "Retention", 10,
                "INTEGER", "Domain", "US", "STRING", "country", "USA",
                "STRING", "zip", 94086, "INTEGER", "climate", "rainy",
                "STRING", "countrycode", 100, "INTEGER", "city", "sunnyvale",
                "STRING", "state", "CA", "STRING", "hdfs");

    }

    @Test
    public void testDropMultiplePartitionedOwlTable() throws OwlException {
        dropMultipleOwlTable("testOwlTable1", "testDatabase1");
        dropMultipleOwlTable("testOwlTable2", "testDatabase1");
    }

    @Test
    public void testDropDatabaseWithMultiplePartitionedOwlTable()
    throws OwlException {
        mpt.dropDatabase("testDatabase1");
        mpt.dropDatabase("testDatabase2");
        mpt.dropDatabase("testDatabase3");
    }

    @Test
    public void cleanup() throws OwlException {
        mpt.cleanup("testDatabase1", "testOwlTable1");
        mpt.cleanup("testDatabase1", "testOwlTable2");
    }

}
