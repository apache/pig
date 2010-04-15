/**
 * 
 */
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
import org.apache.hadoop.owl.common.TestOwlUtil;
import org.apache.hadoop.owl.driver.DriverVerificationUtil;
import org.apache.hadoop.owl.protocol.OwlGlobalKey;
import org.apache.hadoop.owl.protocol.OwlResultObject;
import junit.framework.Assert;
import org.junit.Test;

public class GlobalKeyTest extends OwlTestCase {

    private static OwlClient client = new OwlClient(getUri());
    private static String owlDBName = "owlDBName";
    private static String gKeyName = "testGKeyName";

    public GlobalKeyTest() {
    }


    @Test
    public static void testCreateGlobalKey() throws OwlException {

        createOwlDatabase(owlDBName);
        // test case: positive case
        createGlobalKey(gKeyName, "INT");
        selectGlobalKey(gKeyName);

        validateGlobalKeyNameLength();

        validateUniquenessOfGKey(gKeyName);

        validateGKeyWithPartitionKeyName();

        validateGKeyWithPropertyKeyName();

        validatePartitionKeyWithGKeyName(gKeyName);

        validatePropertyKeyWithGKeyName(gKeyName);

        // clean up
        DriverVerificationUtil.dropOwlDatabaseIfExists(client, owlDBName);
        // now all the tests are done. we clean up by drop the global key we used.
        dropGlobalKey(gKeyName);

    }
    /**
     * negative test --create a PROPERTY key with the same name of a global key that already existed
     * @param gKeyName
     * @throws OwlException
     */
    private static void validatePropertyKeyWithGKeyName(String inputGKey) throws OwlException {
        // make sure we have gkey created
        selectGlobalKey(inputGKey);

        // we first create a owltable with a property key
        String owlTableName = "owlTableName_4";
        String partitionKeyName = "partitionKeyName";
        String partitionKeyType = "INT";
        String propertyKeyName = inputGKey;
        String propertyKeyType = "INT";

        try{
            // global key is already existed. Yet, we try to create a property key with the same name.
            createOwlTable(owlDBName, owlTableName, partitionKeyName, partitionKeyType, propertyKeyName, propertyKeyType);
            Assert.fail("There is no OwlException thrown.  We are expecting duplicated property key name validation fails.");
        }catch (OwlException e){
            assertEquals(e.getErrorType(), ErrorType.ERROR_DUPLICATED_RESOURCENAME);
        }
        DriverVerificationUtil.dropOwlTableIfExists(client, owlTableName, owlDBName);
    }
    /**
     * negative test --create a PARTITION key with the same name of a global key that already existed
     * @param gKeyName
     * @throws OwlException
     */
    private static void validatePartitionKeyWithGKeyName(
            String gKeyName) throws OwlException {
        // make sure we have gkey created
        selectGlobalKey(gKeyName);

        // we first create a owltable with a partition key
        String owlTableName = "owlTableName_3";
        String partitionKeyName = gKeyName;
        String partitionKeyType = "INT";
        String propertyKeyName = "propertyKeyName";
        String propertyKeyType = "INT";

        try{
            // global key is already existed. Yet, we try to create a partition key with the same name.
            createOwlTable(owlDBName, owlTableName, partitionKeyName, partitionKeyType, propertyKeyName, propertyKeyType);
            Assert.fail("There is no OwlException thrown.  We are expecting duplicated partition key name validation fails");
        }catch (OwlException e){
            assertEquals(e.getErrorType(), ErrorType.ERROR_DUPLICATED_RESOURCENAME);
        }
        DriverVerificationUtil.dropOwlTableIfExists(client, owlTableName, owlDBName);
    }
    /**
     * negative test -- create a global key with the same name of a PROPERTY key that existed already
     * @throws OwlException
     */
    private static void validateGKeyWithPropertyKeyName()
    throws OwlException {
        // we first create a owltable with a partition key
        String owlTableName = "owlTableName_2";
        String partitionKeyName = "partitionKeyName";
        String partitionKeyType = "INT";
        String propertyKeyName = "repeatedKeyName";
        String propertyKeyType = "STRING";

        createOwlTable(owlDBName, owlTableName, partitionKeyName, partitionKeyType, propertyKeyName, propertyKeyType);
        try{
            // partition key is already existed. Yet, we try to create a GKey with the same name.
            createGlobalKey(propertyKeyName, propertyKeyType);
            Assert.fail("There is no OwlException thrown.  We are expecting duplicated global key name validation fails");
        }catch (OwlException e){
            assertEquals(e.getErrorType(), ErrorType.ERROR_DUPLICATED_RESOURCENAME);
        }
        DriverVerificationUtil.dropOwlTableIfExists(client, owlTableName, owlDBName);
    }
    /**
     * negative test -- create a global key with the same name of a PARTITION key that existed already
     * @throws OwlException
     */
    private static void validateGKeyWithPartitionKeyName()
    throws OwlException {
        // we first create a owltable with a partition key
        String owlTableName = "owlTableName_1";
        String partitionKeyName = "repeatedKeyName";
        String partitionKeyType = "INT";
        String propertyKeyName = "propertyKeyName";
        String propertyKeyType = "STRING";

        createOwlTable(owlDBName, owlTableName, partitionKeyName, partitionKeyType, propertyKeyName, propertyKeyType);
        try{
            // partition key is already existed. Yet, we try to create a GKey with the same name.
            createGlobalKey(partitionKeyName, partitionKeyType);
            Assert.fail("There is no OwlException thrown.  We are expecting duplicated global key name validation fails");
        }catch (OwlException e){
            assertEquals(e.getErrorType(), ErrorType.ERROR_DUPLICATED_RESOURCENAME);
        }
        DriverVerificationUtil.dropOwlTableIfExists(client, owlTableName, owlDBName);
    }
    /**
     * negative test -- validate the uniqueness of the Gkey
     * @param gKeyName
     */
    private static void validateUniquenessOfGKey(String gKeyName) {
        try{
            // create a global key with the same name of a global key that existed already.
            // this is prevented by global key name is unique key in db.
            createGlobalKey(gKeyName, "INT");
            Assert.fail("There is no OwlException thrown.  We are expecting duplicated global key name validation fails");
        }catch (OwlException e){
            assertTrue((e.getErrorType() == ErrorType.ERROR_RESOURCE_CREATE)|| (e.getErrorType() == ErrorType.ERROR_UNIQUE_KEY_CONSTRAINT));
        }
    }
    /**
     * negative case that validate global key name length
     */
    private static void validateGlobalKeyNameLength() {
        String gkeyName_negative = TestOwlUtil.generateLongString(OwlUtil.IDENTIFIER_LIMIT + 1);
        try{
            createGlobalKey(gkeyName_negative, "INT");
            // expected exception to be thrown by now, we should not reach the next statement
            Assert.fail("There is no OwlException thrown.  We are expecting gkey name variable length validation fails");
        }catch (OwlException e){
            assertEquals(e.getErrorType(), ErrorType.ERROR_IDENTIFIER_LENGTH_VALIDATION_FAILED);
        }
    }

    // Utility functions
    /**
     * Creates the global key.
     * 
     * @param gKeyName the g key name
     * @param keyType the key type
     * @throws OwlException the owl exception
     */
    private static void createGlobalKey(String gKeyName, String keyType) throws OwlException {
        String command = "create globalkey "+gKeyName+" : "+keyType;
        client.execute(command);
    }

    /**
     * Select global key.
     * 
     * @param gKeyName the g key name
     * @throws OwlException the owl exception
     */
    private static void selectGlobalKey(String gKeyName) throws OwlException{
        // check if the gkey has been properly created
        String selectGKeyCommand = "select globalkey objects where globalkey in ("+ gKeyName + ")";
        OwlResultObject result = client.execute(selectGKeyCommand);
        assertNotNull(result);
        @SuppressWarnings("unchecked") List<OwlGlobalKey> output = (List<OwlGlobalKey>) result.getOutput();
        OwlGlobalKey owlGKey = output.get(0);
        System.out.println("owlGKey.getName() = " + owlGKey.getName());
        System.out.println("gKeyName.toLowerCase() = " + gKeyName.toLowerCase());
        assertEquals(owlGKey.getName(), gKeyName.toLowerCase());
    }

    /**
     * Drop global key.
     * 
     * @param gKeyName the g key name
     * @throws OwlException the owl exception
     */
    private static void dropGlobalKey(String gKeyName) throws OwlException{
        String dropGKeyCommand = "drop globalkey "+gKeyName;
        client.execute(dropGKeyCommand);
    }

    /**
     * Creates the owl database.
     * 
     * @param dbName the db name
     * @throws OwlException the owl exception
     */
    private static void createOwlDatabase(String dbName) throws OwlException{
        String createOwlDatabaseCommand = "create owldatabase " +dbName+ " identified by storage directory \"hdfs://GlobalKeyClientTest\"";
        client.execute(createOwlDatabaseCommand);
    }

    /**
     * Creates the owl table.
     * 
     * @param owlDBName the owl db name
     * @param owlTableName the owl table name
     * @param partitionKeyName the partition key name
     * @param partitionKeyType the partition key type
     * @param propertyKeyName the property key name
     * @param propertyKeyType the property key type
     * @throws OwlException the owl exception
     */
    private static void createOwlTable(String owlDBName, String owlTableName, 
            String partitionKeyName,String partitionKeyType,
            String propertyKeyName, String propertyKeyType) throws OwlException{

        String createOwlTableCommand = "create owltable type basic " + owlTableName
        + " within owldatabase "+ owlDBName 
        + " define property key "
        + propertyKeyName + " : " + propertyKeyType
        + " partitioned by LIST with partition key " + partitionKeyName
        + " : " + partitionKeyType 
        + " schema \"c1:collection(r1:record(f1:int, f2:int))\"";
        client.execute(createOwlTableCommand);
    }
}
