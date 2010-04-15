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
import org.apache.hadoop.owl.backend.OwlBackend;
import org.apache.hadoop.owl.client.OwlClient;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.protocol.OwlDataElement;
import org.apache.hadoop.owl.protocol.OwlObject;
import org.apache.hadoop.owl.protocol.OwlPartition;
import org.apache.hadoop.owl.protocol.OwlPartitionKey;
import org.apache.hadoop.owl.protocol.OwlPropertyKey;
import org.apache.hadoop.owl.protocol.OwlResultObject;
import org.apache.hadoop.owl.protocol.OwlSchema;
import org.apache.hadoop.owl.protocol.OwlTable;
import org.apache.hadoop.owl.common.OwlUtil;
import org.apache.hadoop.owl.common.ErrorType;
import org.apache.hadoop.owl.common.TestOwlUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class PublishDataElementTest extends OwlTestCase {

    private static OwlClient client;

    private static OwlBackend backend;

    private static String dbLocation = "\"hdfs://dblocationpublishdataelementclientest\"";

    public PublishDataElementTest() {
    }

    @Test
    public static void testInitialize() {
        client = new OwlClient(getUri());
        try {
            try {
                client.execute("drop owlTable testOwlTable within owldatabase Database1");
                client.execute("drop owlTable testOwlTable2 within owldatabase Database1");
                client.execute("drop owlTable testOwlTable3 within owldatabase Database1");
                client.execute("drop owldatabase Database1");
            } catch (Exception e) {
            } // cleanup of database
            client
            .execute("create owldatabase Database1 identified by storage directory "+ dbLocation);
            // create owltable with zebra schema
            client
            .execute("create owltable type basic testOwlTable within owldatabase Database1 define property key feed : STRING partitioned by LIST with partition key country : STRING  define property key color : STRING , shape : STRING partitioned by LIST with partition key datestamp : STRING schema \"c1:collection(record(f1:int, f2:int))\"" );
            client
            .execute("create owltable type basic testOwlTable2 within owldatabase Database1 define property key feed : STRING partitioned by LIST with partition key country : STRING  define property key color : STRING , shape : STRING partitioned by LIST with partition key datestamp : STRING schema \"c1:collection(record(f1:int, f2:int))\"" );
            client
            .execute("create owltable type basic testOwlTable3 within owldatabase Database1 define property key feed : STRING partitioned by LIST with partition key country : STRING  define property key color : STRING , shape : STRING partitioned by LIST with partition key datestamp : STRING schema \"c1:collection(record(f1:int, f2:int))\"" );
        } catch (OwlException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    //@Test
    public static void publishDataElement(String owlTableName,
            String databaseName, String partValue, String propertyKeyName, String dESchema, String expectedSchema, String dELocation) throws OwlException {

        System.out.println("Owl Table name " + owlTableName
                + " within owldatabase Database1");

        String schemaString ="";
        if (dESchema != null){
            schemaString = "schema " + dESchema;
        }
        String execCmd = "publish dataelement to owltable " 
            + owlTableName 
            + " within owldatabase "
            + databaseName
            + " partition ( country = \""+partValue+"\" , datestamp = \"20090918\" ) property ( color = \"red\" , shape = \"round\") " 
            + schemaString
            + " delocation \"" + dELocation + "\""; 

        System.out.println(execCmd);

        OwlResultObject resultOwlTable = client.execute("describe owltable " + owlTableName + " within owldatabase " + databaseName);
        assertNotNull(resultOwlTable);
        List<? extends OwlObject> output = resultOwlTable.getOutput();
        OwlTable owltable = (OwlTable) output.get(0);
        String owltablename = owltable.getName().getTableName();
        System.out.println("Owl table name " + owltablename);

        client.execute(execCmd);
        System.out.println("select dataelement objects from owltable "+owlTableName);

        OwlResultObject result = client.execute("select dataelement objects from owltable " + owlTableName + " within owldatabase " + databaseName + " with partition (country = \"USA\" , datestamp = \"20090918\" ) property ( color = \"red\" , shape = \"round\" ) ");
        assertNotNull(result);
        List<? extends OwlObject> output1 = result.getOutput();
        OwlDataElement owlDataElement = (OwlDataElement) output1.get(0);

        assertNotNull(output1);
        assertEquals(1, output1.size());
        assertEquals(dELocation, owlDataElement.getStorageLocation());
        //assertEquals("ETL", owlDataElement.getOwner());  owner not set currently

        // test with the correctness of schema object with "select partition objects"
        List<OwlPartitionKey> partitionKeys = owltable.getPartitionKeys();
        OwlPartitionKey partitionKeyName = partitionKeys
        .get(0);
        String partKeyName = partitionKeyName.getName();
        assertTrue("country".equalsIgnoreCase( partKeyName ));
        List<OwlPropertyKey> owlPropertyKeys = owltable.getPropertyKeys();
        OwlPropertyKey key = owlPropertyKeys.get(0);
        String keyName = key.getName();
        assertEquals(propertyKeyName, keyName);

        OwlSchema owlSchema = owlDataElement.getOwlSchema();
        System.out.println("Result of OwlSchema of DE is " + owlSchema.getSchemaString());
        assertEquals(expectedSchema, owlSchema.getSchemaString());

        // test with the correctness of schema object with "select partition objects"
        String selectString = "select partition objects from owltable "+owlTableName
        + " within owldatabase "+databaseName 
        + " with partition ( country = \"USA\" and datestamp = \"20090918\" )"
        + " at partition level ( datestamp)";
        System.out.println(selectString);
        OwlResultObject partitionResult = client.execute(selectString);

        assertNotNull(partitionResult);
        OwlPartition partitionOutput = (OwlPartition) partitionResult.getOutput().get(0);
        OwlSchema owlPartitionSchema = partitionOutput.getSchema();
        System.out.println("Result of OwlSchema of partition is " + owlPartitionSchema.getSchemaString());
        assertEquals(expectedSchema, owlPartitionSchema.getSchemaString());
    }

    @Test
    public static void testPublishDataElement() throws OwlException {
        publishDataElement("testOwlTable", "Database1", "USA","feed", "\"c1:collection(record(f1:int,f2:int))\"", "c1:collection(record(f1:int,f2:int))", "hdfs://dummytestlocation.blah/1234");
        publishDataElement("testOwlTable2", "Database1", "USA","feed", "\"c1:collection(record(f1:int,f2:int))\"", "c1:collection(record(f1:int,f2:int))", "hdfs://dummytestlocation.blah/2345");
        // negative test case to validate the length of the deLocation
        String deLocation_negative = TestOwlUtil.generateLongString(OwlUtil.LOCATION_LIMIT+1);
        try{
            publishDataElement("testOwlTable2", "Database1", "UK","feed", "\"c1:collection(record(f1:int,f2:int))\"", "c1:collection(record(f1:int,f2:int))", deLocation_negative);
            Assert.fail("There is no OwlException thrown.  We are expecting deLocation length validation fails");
        }catch(OwlException e){
            assertEquals(e.getErrorType(), ErrorType.ERROR_IDENTIFIER_LENGTH_VALIDATION_FAILED);
        }
        // negative test case to validate the uniqueness of deLocation -- this is guaranteed by db unique key
        try{
            publishDataElement("testOwlTable2", "Database1", "UN","feed", "\"c1:collection(record(f1:int,f2:int))\"", "c1:collection(record(f1:int,f2:int))", "hdfs://dummytestlocation.blah/2345");
            Assert.fail("There is no OwlException thrown.  We are expecting duplicated deLocation validation fails");
        }catch(OwlException e){
            assertTrue((e.getErrorType() == ErrorType.ERROR_RESOURCE_CREATE)|| (e.getErrorType() == ErrorType.ERROR_UNIQUE_KEY_CONSTRAINT));

        }
    }

    @After
    public static void testCleanup() throws OwlException {
        client.execute("drop owlTable testOwlTable within owldatabase Database1");
        client.execute("drop owlTable testOwlTable2 within owldatabase Database1");
        client.execute("drop owlTable testOwlTable3 within owldatabase Database1");
        client.execute("drop owldatabase Database1");

    }
}
