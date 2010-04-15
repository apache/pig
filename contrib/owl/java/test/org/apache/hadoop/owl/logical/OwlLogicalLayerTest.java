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

package org.apache.hadoop.owl.logical;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.owl.backend.OwlBackend;
import org.apache.hadoop.owl.client.DatabaseTest;
import org.apache.hadoop.owl.client.OwlClient;
import org.apache.hadoop.owl.common.ErrorType;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.common.OwlUtil;
import org.apache.hadoop.owl.entity.DatabaseEntity;
import org.apache.hadoop.owl.entity.DataElementEntity;
import org.apache.hadoop.owl.entity.GlobalKeyEntity;
import org.apache.hadoop.owl.entity.KeyValueEntity;
import org.apache.hadoop.owl.entity.OwlTableEntity;
import org.apache.hadoop.owl.entity.OwlTableKeyValueEntity;
import org.apache.hadoop.owl.entity.PartitionEntity;
import org.apache.hadoop.owl.entity.PartitionKeyEntity;
import org.apache.hadoop.owl.entity.PropertyKeyEntity;
import org.apache.hadoop.owl.protocol.OwlDataElement;
import org.apache.hadoop.owl.protocol.OwlKey;
import org.apache.hadoop.owl.protocol.OwlObject;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import junit.framework.TestCase;

public class OwlLogicalLayerTest extends TestCase {

    private static final String RETENTION = "retention";
    private static final String VUTS = "vuts";
    private static final String EIGHT = "8";
    private static final String NINE = "9";
    private static final String ONE = "1";
    private static final String ZYXW = "zyxw";
    private static final String ABCD = "ABCD";

    private static final String PROP_B_INT = "prbi";
    private static final String PTN_B_INT = "pnbi";
    private static final String PROP_A_STRING = "pras";
    private static final String PTN_A_STRING = "pnas";

    private static final String TESTDATABASE_LOCN = "/tmp/ll_testcat";
    private static final String TESTDATABASE_NAME = "ll_testcat";

    private static final String TESTOWLTABLE_NAME = "ll_testowltable";
    private static final String TESTGKEYOWLTABLE_NAME = "ll_testgkeyowltable";

    private static final String TESTDE_LOCN = TESTDATABASE_LOCN + "/" + TESTOWLTABLE_NAME + "/ll_testde";
    private static final String TESTDE_LOCN2 = TESTDATABASE_LOCN + "/" + TESTOWLTABLE_NAME + "/ll_testde_2";

    private static final String TEST_GKEY = "ll_gkey";

    private static OwlBackend backend;

    private static int databaseId = 0;
    private static int otableId = 0;
    private static int ptnAStringKeyId = 0;
    private static int ptnBIntegerKeyId = 0;
    private static int propAStringKeyId = 0;
    private static int propBIntegerKeyId = 0;

    private static boolean gkeyBasicTested;
    private static boolean gkeyTested;

    private static boolean databaseCreated;
    private static boolean owlTableCreated;
    private static boolean dePublished;
    private static boolean deSelected;

    private static boolean deAltered1;
    private static boolean deAltered2;
    private static boolean deAltered3;

    @BeforeClass
    public void setUp() throws OwlException {
        System.out.println("@BeforeClass running");


        OwlException connectException = null;
        //Retry backend connection in case previous test has not closed its derbydb connection yet
        for(int i = 0; i < DatabaseTest.CONNECTION_RETRY_COUNT;i++) {
            try {
                backend = new OwlBackend();
                cleanup();
                connectException  = null;
                break;
            } catch(OwlException e) {
                if( e.getErrorType() != ErrorType.ERROR_DB_INIT ) {
                    throw e;
                } else {
                    connectException = e;
                    long retryTime = DatabaseTest.RETRY_SLEEP_MILLIS * (i + 1);
                    System.out.println("Connection failed " + e.toString() + ". Retrying after " + retryTime + " ms");
                    try { Thread.sleep(retryTime); } catch(Exception e2) {}
                }
            }
        }

        if( connectException != null ) {
            throw connectException;
        }
    }

    @AfterClass
    public void tearDown() throws OwlException {
        System.out.println("@AfterClass running");
        cleanup();
        backend.close();
    }

    private static void localTestBasicGkey() throws OwlException {
        if (!gkeyBasicTested){

            // create global key
            CreateGlobalKeyCommand createGlobalKeyCmd  = (CreateGlobalKeyCommand) CommandFactory.getCommand("CREATE GLOBALKEY");
            createGlobalKeyCmd.addPropertyKey(TEST_GKEY, "STRING");
            createGlobalKeyCmd.execute(backend);

            List<GlobalKeyEntity> listOfGlobalKeys;

            // get list of global keys to see if it exists
            listOfGlobalKeys = backend.find(GlobalKeyEntity.class, 
                    "name = \""+TEST_GKEY+"\"");
            assertEquals(1, listOfGlobalKeys.size() );
            assertEquals(OwlKey.DataType.STRING.getCode(),listOfGlobalKeys.get(0).getDataType());

            // delete global key
            DropGlobalKeyCommand dropGlobalKeyCmd = (DropGlobalKeyCommand) CommandFactory.getCommand("DROP GLOBALKEY");
            dropGlobalKeyCmd.addPropertyKey(TEST_GKEY, null);
            dropGlobalKeyCmd.execute(backend);

            // verify that global key is deleted
            listOfGlobalKeys = backend.find(GlobalKeyEntity.class, 
                    "name = \""+TEST_GKEY+"\"");
            assertTrue(listOfGlobalKeys.size() == 0);

            gkeyBasicTested = true;
        }
    }

    private static void localTestCreateDatabase() throws OwlException {
        if (!databaseCreated){
            localTestBasicGkey();

            CreateDatabaseCommand createDatabaseCmd = (CreateDatabaseCommand) CommandFactory.getCommand("CREATE OWLDATABASE");
            createDatabaseCmd.setName(TESTDATABASE_NAME);
            createDatabaseCmd.setExternalStorageIdentifier(TESTDATABASE_LOCN);
            createDatabaseCmd.execute(backend);

            List<DatabaseEntity> listOfDatabases = backend.find(DatabaseEntity.class, 
                    "name = \""+TESTDATABASE_NAME+"\"");
            assertTrue(listOfDatabases.size() == 1);
            databaseCreated = true;

            databaseId = listOfDatabases.get(0).getId();
            assertTrue(databaseId > 0);
            assertTrue(listOfDatabases.get(0).getName().equals(TESTDATABASE_NAME));
            assertTrue(listOfDatabases.get(0).getLocation().equals(TESTDATABASE_LOCN));
        }
    }

    private static void localTestCreateOwlTable() throws OwlException {

        if (!owlTableCreated){
            localTestCreateDatabase();

            CreateOwlTableCommand createOwlTableCmd = (CreateOwlTableCommand) CommandFactory.getCommand("CREATE OWLTABLE");

            createOwlTableCmd.setBasicDataSetType();
            createOwlTableCmd.setName(TESTOWLTABLE_NAME);
            createOwlTableCmd.setParentDatabase(TESTDATABASE_NAME);
            createOwlTableCmd.addPropertyKey(RETENTION, "INTEGER");
            createOwlTableCmd.addPartitionKey(PTN_A_STRING, "STRING", "LIST", null, null, null, null);
            createOwlTableCmd.addPropertyKey(PROP_A_STRING,"STRING");
            createOwlTableCmd.addPartitionKey(PTN_B_INT, "INTEGER", "LIST", null, null, null, null);
            createOwlTableCmd.addPropertyKey(PROP_B_INT,"INTEGER");
            createOwlTableCmd.addPropertyKeyValue(RETENTION, "60");
            createOwlTableCmd.addSchemaElement("c1:int");
            createOwlTableCmd.execute(backend);

            String owlTableFilter = "name = \""+TESTOWLTABLE_NAME+"\" and databaseId = "+databaseId;
            System.out.println("Selecting owltables matching filter : "+ owlTableFilter);
            List<OwlTableEntity> listOfOwlTables = backend.find(OwlTableEntity.class, 
                    owlTableFilter
            );

            System.out.println("Matches : "+listOfOwlTables.size());

            assertTrue(listOfOwlTables.size() == 1);
            owlTableCreated = true;

            OwlTableEntity otable = listOfOwlTables.get(0);
            otableId = otable.getId();

            assertTrue(otable.getName().equals(TESTOWLTABLE_NAME));
            assertTrue(otable.getDatabaseId() == databaseId);
            assertTrue(otableId > 0);

            List<OwlTableKeyValueEntity> okvs = otable.getKeyValues();
            assertTrue (okvs.size() == 1);
            OwlTableKeyValueEntity retentionKeyValue = okvs.get(0);
            int retentionKeyId = retentionKeyValue.getPropertyKeyId();
            assertTrue(retentionKeyValue.getIntValue() == 60);

            Map<String,PropertyKeyEntity> propKeysByName = new HashMap<String,PropertyKeyEntity>();
            for (PropertyKeyEntity propKey : otable.getPropertyKeys()){
                propKeysByName.put(propKey.getName(),propKey);
            }

            assertTrue(propKeysByName.size() == 3);

            assertTrue(propKeysByName.containsKey(RETENTION));
            PropertyKeyEntity retentionKey = propKeysByName.get(RETENTION);
            assertTrue(retentionKey.getId() == retentionKeyId);

            assertTrue(propKeysByName.containsKey( PROP_A_STRING));
            PropertyKeyEntity propAString = propKeysByName.get( PROP_A_STRING);
            propAStringKeyId = propAString.getId();
            assertTrue(propAStringKeyId > 0);

            assertTrue(propKeysByName.containsKey(PROP_B_INT));
            PropertyKeyEntity propBInteger = propKeysByName.get(PROP_B_INT);
            propBIntegerKeyId = propBInteger.getId();
            assertTrue(propBIntegerKeyId > 0);

            Map<String,PartitionKeyEntity> ptnKeysByName = new HashMap<String,PartitionKeyEntity>();
            for (PartitionKeyEntity ptnKey : otable.getPartitionKeys()){
                ptnKeysByName.put(ptnKey.getName(), ptnKey);
            }

            assertTrue(ptnKeysByName.size() == 2);

            assertTrue(ptnKeysByName.containsKey(PTN_A_STRING));
            PartitionKeyEntity ptnAString = ptnKeysByName.get(PTN_A_STRING);
            assertTrue(ptnAString.getPartitionLevel() == 1);
            ptnAStringKeyId = ptnAString.getId();
            assertTrue(ptnAStringKeyId > 0);

            assertTrue(ptnKeysByName.containsKey( PTN_B_INT));
            PartitionKeyEntity ptnBInteger = ptnKeysByName.get(PTN_B_INT);
            assertTrue(ptnBInteger.getPartitionLevel() == 2);
            ptnBIntegerKeyId = ptnBInteger.getId();
            assertTrue(ptnBIntegerKeyId > 0);
        }
    }

    private static void verifyDE(String valPNAS, String valPNBI, String valPRAS, String valPRASAtLowerPtn , String valPRBI) throws OwlException {

        // TODO : modify to allow nulls, and gkey possibilities - be more generic testing for the DE structure

        List<PartitionEntity> listOfPartitionsInOwlTable = backend.find(PartitionEntity.class,
                "owlTableId = " + otableId
        );
        System.out.println("BECK: listOfPartitionsInOwlTable.size : " + listOfPartitionsInOwlTable.size());

        List<PartitionEntity> listOfPartitions = backend.find(PartitionEntity.class, 
                "owlTableId = " + otableId
                + " and ["+ PTN_A_STRING+"] = \""+valPNAS+"\""
                + " and ["+ PTN_B_INT+"] = "+valPNBI
        );
        System.out.println("BECK: listOfPartitions.size : " + listOfPartitions.size());
        assertTrue(listOfPartitions.size() == 1);

        List<DataElementEntity> listOfDataElements = backend.find(DataElementEntity.class, 
                "partitionId = "+listOfPartitions.get(0).getId());

        assertTrue(listOfDataElements.size() == 1);

        assertTrue(listOfDataElements.get(0).getLocation().equals(TESTDE_LOCN));

        List<PartitionEntity> listOfTopLevelPartitions = backend.find(PartitionEntity.class,
                "owlTableId = " + otableId
                + " and ["+ PTN_A_STRING+"] = \""+valPNAS+"\""
                //                    + " and partitionLevel = 1"
                + " and parentPartitionId is null"
        );

        assertTrue(listOfTopLevelPartitions.size() == 1);

        PartitionEntity abcdPartition = listOfTopLevelPartitions.get(0);
        assertTrue(abcdPartition.getId() > 0);

        List<KeyValueEntity> abcdKeyValues = abcdPartition.getKeyValues();

        assertTrue(abcdKeyValues.size() >= 1);

        Map <Integer,KeyValueEntity> abcdPropKeyValuesByKeyId = new HashMap<Integer,KeyValueEntity>();
        Map <Integer,KeyValueEntity> abcdPtnKeyValuesByKeyId = new HashMap<Integer,KeyValueEntity>();
        for (KeyValueEntity kve : abcdKeyValues){
            System.out.println(
                    " otable["+ kve.getOwlTableId()
                    + "] ptn[" + kve.getPartition().getId()
                    + "] ptnKeyId[" + nullGuardedPrint(kve.getPartitionKeyId())
                    + "] propKeyId[" + nullGuardedPrint(kve.getPropertyKeyId())
                    + "] globalKeyId[" + nullGuardedPrint(kve.getGlobalKeyId())
                    + "] stringValue[" + nullGuardedPrint(kve.getStringValue())
                    + "] intValue[" + nullGuardedPrint(kve.getIntValue())
                    + "]"
            );

            if (kve.getPropertyKeyId() != null){
                abcdPropKeyValuesByKeyId.put(kve.getPropertyKeyId(),kve);
            }
            if (kve.getPartitionKeyId() != null){
                abcdPtnKeyValuesByKeyId.put(kve.getPartitionKeyId(),kve);
            }
        }


        assertTrue(abcdPtnKeyValuesByKeyId.containsKey(ptnAStringKeyId));
        KeyValueEntity abcdPtnKeyValue = abcdPtnKeyValuesByKeyId.get(ptnAStringKeyId);
        assertTrue(abcdPtnKeyValue.getStringValue().equals(valPNAS));

        if (valPRAS != null){
            assertTrue(abcdPropKeyValuesByKeyId.containsKey(propAStringKeyId));
            KeyValueEntity abcdPropKeyValue = abcdPropKeyValuesByKeyId.get(propAStringKeyId);
            assertTrue(abcdPropKeyValue.getStringValue().equals(valPRAS));
        }

        List<PartitionEntity> listOfBottomLevelPartitions = backend.find(PartitionEntity.class,
                "owlTableId = " + otableId
                + "and ["+ PTN_A_STRING+"] = \""+valPNAS+"\""
                + " and ["+ PTN_B_INT+"] = "+valPNBI
                //                    + " and partitionLevel = 2"
                + " and parentPartitionId = " + abcdPartition.getId()
        );

        assertTrue(listOfBottomLevelPartitions.size() == 1);

        PartitionEntity abcd1Partition = listOfBottomLevelPartitions.get(0);
        assertTrue(abcd1Partition.getId() > 0);
        List<KeyValueEntity> abcd1KeyValues = abcd1Partition.getKeyValues();

        assertTrue (abcd1KeyValues.size() >= 2);

        Map <Integer,KeyValueEntity> abcd1PropKeyValuesByKeyId = new HashMap<Integer,KeyValueEntity>();
        Map <Integer,KeyValueEntity> abcd1PtnKeyValuesByKeyId = new HashMap<Integer,KeyValueEntity>();

        for (KeyValueEntity kve : abcd1KeyValues){
            //                System.out.println("JIMI kve id=[" + kve.getId() +"] , otableId = ["+kve.getOwlTableId()+"]");
            //                if (kve.getIntValue() != null){ System.out.println("JIMI kve intval=["+kve.getIntValue()+"]");}
            //                if (kve.getStringValue() != null){ System.out.println("JIMI kve stringval=["+kve.getStringValue()+"]");}
            //                if (kve.getPropertyKeyId() != null){ System.out.println("JIMI kve PropertyKeyId()=["+kve.getPropertyKeyId()+"]");}
            //                if (kve.getPartitionKeyId() != null){ System.out.println("JIMI kve PartitionKeyId()=["+kve.getPartitionKeyId()+"]");}
            //                if (kve.getGlobalKeyId() != null){ System.out.println("JIMI kve GlobalKeyId()=["+kve.getGlobalKeyId()+"]");}

            if (kve.getPropertyKeyId() != null){
                abcd1PropKeyValuesByKeyId.put(kve.getPropertyKeyId(),kve);
            }
            if (kve.getPartitionKeyId() != null){
                abcd1PtnKeyValuesByKeyId.put(kve.getPartitionKeyId(),kve);
            }

        }

        assertTrue(abcd1PtnKeyValuesByKeyId.containsKey(ptnBIntegerKeyId));
        assertTrue(abcd1PtnKeyValuesByKeyId.get(ptnBIntegerKeyId).getIntValue().intValue() == Integer.valueOf(valPNBI).intValue());

        if (valPRBI != null){
            assertTrue(abcd1PropKeyValuesByKeyId.containsKey(propBIntegerKeyId));
            assertEquals(abcd1PropKeyValuesByKeyId.get(propBIntegerKeyId).getIntValue(), Integer.valueOf(valPRBI));
        }

        assertTrue(abcd1PtnKeyValuesByKeyId.containsKey(ptnAStringKeyId));  
        assertTrue(abcd1PtnKeyValuesByKeyId.get(ptnAStringKeyId).getStringValue().equals(valPNAS));

        if (valPRASAtLowerPtn != null){
            assertTrue(abcd1PropKeyValuesByKeyId.containsKey(propAStringKeyId));
            assertTrue(abcd1PropKeyValuesByKeyId.get(propAStringKeyId).getStringValue().equals(valPRASAtLowerPtn));
        }

        assertTrue(listOfDataElements.get(0).getPartitionId() == abcd1Partition.getId());

    }


    private static void localPublishDataElement() throws OwlException{
        if (! dePublished){
            localTestCreateOwlTable();

            PublishDataElementCommand publishCmd = (PublishDataElementCommand) CommandFactory.getCommand("PUBLISH DATAELEMENT");
            publishCmd.setName(TESTOWLTABLE_NAME);
            publishCmd.setParentDatabase(TESTDATABASE_NAME);
            publishCmd.setLocation(TESTDE_LOCN);
            publishCmd.addPartitionKeyValue(PTN_A_STRING, ABCD);
            publishCmd.addPropertyKeyValue(PROP_A_STRING, ZYXW);
            publishCmd.addPartitionKeyValue(PTN_B_INT, ONE);
            publishCmd.addPropertyKeyValue(PROP_B_INT, NINE);
            publishCmd.addSchemaElement("c1:int");
            publishCmd.execute(backend);

            verifyDE(ABCD,ONE,null,ZYXW,NINE);
            dePublished = true;

        }
    }

    private static void localSelectDataElementObjects() throws OwlException {
        if (! deSelected){
            localPublishDataElement();

            SelectDataelementObjectsCommand selectDeCmd1 = (SelectDataelementObjectsCommand) CommandFactory.getCommand("SELECT DATAELEMENT OBJECTS");
            selectDeCmd1.addAdditionalActionInfo(TESTOWLTABLE_NAME);
            selectDeCmd1.setParentDatabase(TESTDATABASE_NAME);
            selectDeCmd1.addPartitionFilter(PTN_A_STRING, CommandInfo.Operator.EQUAL.getOp(), ABCD);
            selectDeCmd1.addPropertyFilter(PROP_A_STRING, CommandInfo.Operator.EQUAL.getOp(), ZYXW);
            selectDeCmd1.addPartitionFilter(PTN_B_INT, CommandInfo.Operator.EQUAL.getOp(), ONE);
            selectDeCmd1.addPropertyFilter(PROP_B_INT, CommandInfo.Operator.EQUAL.getOp(), NINE);

            //List<OwlDataElement>
            List<? extends OwlObject> results = selectDeCmd1.execute(backend);

            System.out.println("testSelectDataelementObjects \n");

            assertTrue(results.size() == 1);

            OwlDataElement de = (OwlDataElement) results.get(0);
            //TODO: verify de.getKeyValues

            deSelected = true;
        }
    }


    private static void localAlterOwlTableModifyPartitionProperty() throws OwlException {
        if (! deAltered1){

            localSelectDataElementObjects();

            AlterOwlTableCommand alterModifyCmd1 = (AlterOwlTableCommand) CommandFactory.getCommand("ALTER OWLTABLE");
            alterModifyCmd1.setName(TESTOWLTABLE_NAME);
            alterModifyCmd1.setParentDatabase(TESTDATABASE_NAME);
            alterModifyCmd1.addAdditionalActionInfo("MODIFY PARTITION PROPERTY");
            alterModifyCmd1.addPartitionFilter(PTN_A_STRING, CommandInfo.Operator.EQUAL.getOp(), ABCD);
            alterModifyCmd1.addPartitionFilter(PTN_B_INT, CommandInfo.Operator.EQUAL.getOp(),ONE);
            alterModifyCmd1.addPropertyKeyValue(PROP_B_INT, EIGHT);
            alterModifyCmd1.execute(backend);

            verifyDE(ABCD,ONE,null,ZYXW,EIGHT);

            AlterOwlTableCommand alterModifyCmd2 = (AlterOwlTableCommand) CommandFactory.getCommand("ALTER OWLTABLE");
            alterModifyCmd2.setName(TESTOWLTABLE_NAME);
            alterModifyCmd2.setParentDatabase(TESTDATABASE_NAME);
            alterModifyCmd2.addAdditionalActionInfo("MODIFY PARTITION PROPERTY");
            alterModifyCmd2.addPartitionFilter(PTN_A_STRING, CommandInfo.Operator.EQUAL.getOp(), ABCD);
            alterModifyCmd2.addPropertyKeyValue(PROP_A_STRING, VUTS);
            alterModifyCmd2.execute(backend);

            verifyDE(ABCD,ONE,VUTS,VUTS,EIGHT); // change to verifyDE(ABCD,ONE,VUTS,ZYXW,EIGHT) after property pushdown removal

            AlterOwlTableCommand alterModifyCmd3 = (AlterOwlTableCommand) CommandFactory.getCommand("ALTER OWLTABLE");
            alterModifyCmd3.setName(TESTOWLTABLE_NAME);
            alterModifyCmd3.setParentDatabase(TESTDATABASE_NAME);
            alterModifyCmd3.addAdditionalActionInfo("MODIFY PARTITION PROPERTY");
            alterModifyCmd3.addPartitionFilter(PTN_A_STRING, CommandInfo.Operator.EQUAL.getOp(),ABCD);
            alterModifyCmd3.addPartitionFilter(PTN_B_INT, CommandInfo.Operator.EQUAL.getOp(),ONE);
            alterModifyCmd3.addPropertyKeyValue(PROP_A_STRING, ZYXW);
            alterModifyCmd3.execute(backend);

            verifyDE(ABCD,ONE,VUTS,ZYXW,EIGHT);

            deAltered1 = true;
        }
    }

    private static void localTestAlterOwlTableDropPartitionProperty() throws OwlException {
        if (!deAltered2){

            localAlterOwlTableModifyPartitionProperty();

            AlterOwlTableCommand alterDropCmd1 = (AlterOwlTableCommand) CommandFactory.getCommand("ALTER OWLTABLE");
            alterDropCmd1.setName(TESTOWLTABLE_NAME);
            alterDropCmd1.setParentDatabase(TESTDATABASE_NAME);
            alterDropCmd1.addAdditionalActionInfo("DROP PARTITION PROPERTY");
            alterDropCmd1.addPartitionFilter(PTN_A_STRING, CommandInfo.Operator.EQUAL.getOp(), ABCD);
            alterDropCmd1.addPartitionFilter(PTN_B_INT, CommandInfo.Operator.EQUAL.getOp(), ONE);
            alterDropCmd1.addPropertyKeyValue(PROP_A_STRING, ""); // TODO : Grammar needs to change to let this just be addPropertyKey
            alterDropCmd1.execute(backend);

            /* TODO note:
             *  Drop Partition Property takes the same command form as Modify Partition Property,
             *  so it uses addPropertyKeyValue, even though it ignores the value - this is not good/clear,
             *  and should be changed from DDL onwards.
             */ 

            verifyDE(ABCD,ONE,VUTS,null,EIGHT);

            //            AlterOwlTableCommand alterModifyCmd1 = (AlterOwlTableCommand) CommandFactory.getCommand("ALTER OWLTABLE");
            //            alterModifyCmd1.setName(TESTOWLTABLE_NAME);
            //            alterModifyCmd1.setParentDatabase(TESTDATABASE_NAME);
            //            alterModifyCmd1.addAdditionalActionInfo("MODIFY PARTITION PROPERTY");
            //            alterModifyCmd1.addPartitionFilter(PTN_A_STRING, CommandInfo.Operator.EQUAL.getOp(), ABCD);
            //            alterModifyCmd1.addPropertyKeyValue(PROP_A_STRING, ZYXW);
            //            alterModifyCmd1.execute(backend);
            //
            //            verifyDE(ABCD,ONE,ZYXW,ZYXW,EIGHT);
            //            
            //            AlterOwlTableCommand alterDropCmd2 = (AlterOwlTableCommand) CommandFactory.getCommand("ALTER OWLTABLE");
            //            alterDropCmd2.setName(TESTOWLTABLE_NAME);
            //            alterDropCmd2.setParentDatabase(TESTDATABASE_NAME);
            //            alterDropCmd2.addAdditionalActionInfo("DROP PARTITION PROPERTY");
            //            alterDropCmd2.addPartitionFilter(PTN_A_STRING, CommandInfo.Operator.EQUAL.getOp(), ABCD);
            //            alterDropCmd2.addPropertyKeyValue(PROP_A_STRING, ""); // TODO : Grammar needs to change to let this just be addPropertyKey
            //            alterDropCmd2.execute(backend);
            //
            //            verifyDE(ABCD,ONE,null,null,EIGHT);
            //
            //            AlterOwlTableCommand alterModifyCmd2 = (AlterOwlTableCommand) CommandFactory.getCommand("ALTER OWLTABLE");
            //            alterModifyCmd2.setName(TESTOWLTABLE_NAME);
            //            alterModifyCmd2.setParentDatabase(TESTDATABASE_NAME);
            //            alterModifyCmd2.addAdditionalActionInfo("MODIFY PARTITION PROPERTY");
            //            alterModifyCmd2.addPartitionFilter(PTN_A_STRING, CommandInfo.Operator.EQUAL.getOp(), ABCD);
            //            alterModifyCmd2.addPartitionFilter(PTN_B_INT, CommandInfo.Operator.EQUAL.getOp(), ONE);
            //            alterModifyCmd2.addPropertyKeyValue(PROP_A_STRING, VUTS);
            //            alterModifyCmd2.execute(backend);
            //
            //            verifyDE(ABCD,ONE,null,VUTS,EIGHT);

            deAltered2 = true;
        }
    }

    @Test
    public static void testGkey() throws OwlException {
        if (!gkeyTested){
            backend.beginTransaction();
            localTestAlterOwlTableDropPartitionProperty();

            // create global key
            CreateGlobalKeyCommand createGlobalKeyCmd  = (CreateGlobalKeyCommand) CommandFactory.getCommand("CREATE GLOBALKEY");
            createGlobalKeyCmd.addPropertyKey(TEST_GKEY, "STRING");
            createGlobalKeyCmd.execute(backend);

            List<GlobalKeyEntity> listOfGlobalKeys;

            // get list of global keys to see if it exists
            listOfGlobalKeys = backend.find(GlobalKeyEntity.class, 
                    "name = \""+TEST_GKEY+"\"");
            assertTrue(listOfGlobalKeys.size() == 1);
            assertEquals(OwlKey.DataType.STRING.getCode(),listOfGlobalKeys.get(0).getDataType());
            int gKeyId = listOfGlobalKeys.get(0).getId();

            // create a simpletable, set a gkey-value during table create.
            CreateOwlTableCommand createOwlTableCmd = (CreateOwlTableCommand) CommandFactory.getCommand("CREATE OWLTABLE");

            createOwlTableCmd.setBasicDataSetType();
            createOwlTableCmd.setName(TESTGKEYOWLTABLE_NAME);
            createOwlTableCmd.setParentDatabase(TESTDATABASE_NAME);
            createOwlTableCmd.addPartitionKey(PTN_A_STRING, "STRING", "LIST", null, null, null, null);
            createOwlTableCmd.addPropertyKeyValue(TEST_GKEY, VUTS);
            createOwlTableCmd.addSchemaElement("c1:int");
            createOwlTableCmd.execute(backend);

            // check that the table was created correctly with the gkey-value as an otkv

            String owlTableFilter = "name = \""+TESTGKEYOWLTABLE_NAME+"\" and databaseId = "+databaseId;
            System.out.println("Selecting owltables matching filter : "+ owlTableFilter);
            List<OwlTableEntity> listOfOwlTables = backend.find(OwlTableEntity.class, 
                    owlTableFilter
            );

            System.out.println("Matches : "+listOfOwlTables.size());

            assertTrue(listOfOwlTables.size() == 1);

            OwlTableEntity otable = listOfOwlTables.get(0);

            assertTrue(otable.getPartitionKeys().size() == 1);
            int ptnKeyId = otable.getPartitionKeys().get(0).getId();
            int gkeyOtableId = otable.getId();

            List<OwlTableKeyValueEntity> okvs = otable.getKeyValues();
            assertTrue (okvs.size() == 1);
            OwlTableKeyValueEntity gKeyValue = okvs.get(0);
            assertTrue(gKeyId == gKeyValue.getGlobalKeyId());
            assertTrue(gKeyValue.getStringValue().equalsIgnoreCase(VUTS));

            // publish a de to the table, check that there's an appropriate kv at the leaf ptn

            PublishDataElementCommand publishCmd = (PublishDataElementCommand) CommandFactory.getCommand("PUBLISH DATAELEMENT");
            publishCmd.setName(TESTGKEYOWLTABLE_NAME);
            publishCmd.setParentDatabase(TESTDATABASE_NAME);
            publishCmd.setLocation(TESTDE_LOCN2);
            publishCmd.addPartitionKeyValue(PTN_A_STRING, ABCD);
            publishCmd.addPropertyKeyValue(TEST_GKEY, ZYXW);
            publishCmd.addSchemaElement("c2:collection(r1:record(f1:int,f2:int))");
            publishCmd.execute(backend);

            verifyDECase2(gKeyId, ptnKeyId, gkeyOtableId,ZYXW);

            // alter table to alter the kv at the leaf ptn

            AlterOwlTableCommand alterModifyCmd1 = (AlterOwlTableCommand) CommandFactory.getCommand("ALTER OWLTABLE");
            alterModifyCmd1.setName(TESTGKEYOWLTABLE_NAME);
            alterModifyCmd1.setParentDatabase(TESTDATABASE_NAME);
            alterModifyCmd1.addAdditionalActionInfo("MODIFY PARTITION PROPERTY");
            alterModifyCmd1.addPartitionFilter(PTN_A_STRING, CommandInfo.Operator.EQUAL.getOp(), ABCD);
            alterModifyCmd1.addPropertyKeyValue(TEST_GKEY, ABCD);
            alterModifyCmd1.execute(backend);

            verifyDECase2(gKeyId, ptnKeyId, gkeyOtableId,ABCD);

            // alter table to alter the kv at the root node

            //            AlterOwlTableCommand alterModifyCmd2 = (AlterOwlTableCommand) CommandFactory.getCommand("ALTER OWLTABLE");
            //            alterModifyCmd2.setName(TESTGKEYOWLTABLE_NAME);
            //            alterModifyCmd2.setParentDatabase(TESTDATABASE_NAME);
            //            alterModifyCmd2.addAdditionalActionInfo("MODIFY PARTITION PROPERTY");
            //            alterModifyCmd2.addPropertyKeyValue(TEST_GKEY, ZYXW);
            //            alterModifyCmd2.execute(backend);
            //
            //            verifyDECase2(gKeyId, ptnKeyId, gkeyOtableId,ABCD); // verify that the gkey-value at the leaf ptn did not change
            //
            //            System.out.println("Selecting owltables matching filter : "+ owlTableFilter);
            //            listOfOwlTables = backend.find(OwlTableEntity.class, 
            //                    owlTableFilter
            //                    );
            //
            //            System.out.println("Matches : "+listOfOwlTables.size());
            //
            //            assertTrue(listOfOwlTables.size() == 1);
            //            
            //            otable = listOfOwlTables.get(0);
            //            
            //            assertTrue(otable.getPartitionKeys().size() == 1);
            //            assertTrue(ptnKeyId == otable.getPartitionKeys().get(0).getId());
            //            assertTrue(gkeyOtableId == otable.getId());
            //            
            //            okvs = otable.getKeyValues();
            //            assertTrue (okvs.size() == 1);
            //            gKeyValue = okvs.get(0);
            //            assertTrue(gKeyId == gKeyValue.getGlobalKeyId());
            //            assertTrue(gKeyValue.getStringValue().equalsIgnoreCase(ZYXW)); // verify that the gkey-value at the table level changed

            // nuke table
            DropOwlTableCommand dropTableCommand = (DropOwlTableCommand) CommandFactory.getCommand("DROP OWLTABLE");
            dropTableCommand.setName(TESTGKEYOWLTABLE_NAME);
            dropTableCommand.setParentDatabase(TESTDATABASE_NAME);
            dropTableCommand.execute(backend);

            // delete global key
            DropGlobalKeyCommand dropGlobalKeyCmd = (DropGlobalKeyCommand) CommandFactory.getCommand("DROP GLOBALKEY");
            dropGlobalKeyCmd.addPropertyKey(TEST_GKEY, null);
            dropGlobalKeyCmd.execute(backend);

            // verify that global key is deleted
            listOfGlobalKeys = backend.find(GlobalKeyEntity.class, 
                    "name = \""+ TEST_GKEY+"\"");
            assertTrue(listOfGlobalKeys.size() == 0);
            backend.rollbackTransaction();

            gkeyTested = true;
        }
    }

    private static void verifyDECase2(int gKeyId, int ptnKeyId, int gkeyOtableId, String gkeyValue)
    throws OwlException {
        List<PartitionEntity> listOfTopLevelPartitions = backend.find(PartitionEntity.class,
                "owlTableId = " + gkeyOtableId
                + " and ["+ PTN_A_STRING+"] = \""+ABCD+"\""
                + " and parentPartitionId is null"
        );

        assertTrue(listOfTopLevelPartitions.size() == 1);
        PartitionEntity abcdPartition = listOfTopLevelPartitions.get(0);
        assertTrue(abcdPartition.getId() > 0);

        List<KeyValueEntity> abcdKeyValues = abcdPartition.getKeyValues();
        assertTrue(abcdKeyValues.size() == 2); // one ptnkey-value, one gkey-value

        int keyValuesStillToCheck = 2;
        for (KeyValueEntity kve : abcdKeyValues){
            System.out.println(
                    " otable["+ kve.getOwlTableId()
                    + "] ptn[" + kve.getPartition().getId()
                    + "] ptnKeyId[" + nullGuardedPrint(kve.getPartitionKeyId())
                    + "] propKeyId[" + nullGuardedPrint(kve.getPropertyKeyId())
                    + "] globalKeyId[" + nullGuardedPrint(kve.getGlobalKeyId())
                    + "] stringValue[" + nullGuardedPrint(kve.getStringValue())
                    + "] intValue[" + nullGuardedPrint(kve.getIntValue())
                    + "]"
            );
            if (kve.getGlobalKeyId() != null){
                assertTrue((kve.getGlobalKeyId() == gKeyId) && kve.getStringValue().equals(gkeyValue));
                keyValuesStillToCheck--;
            }
            if (kve.getPartitionKeyId() != null){
                assertTrue((kve.getPartitionKeyId() == ptnKeyId) && kve.getStringValue().equals(ABCD));
                keyValuesStillToCheck--;
            }
        }
        assertTrue(keyValuesStillToCheck == 0);
    }

    private static void cleanup() throws OwlException {

        try {
            backend.beginTransaction();
            List<DatabaseEntity> databases = backend.find(DatabaseEntity.class, "name = \"" + TESTDATABASE_NAME + "\"");
            if( databases.size() == 1 ) {
                int catId = databases.get(0).getId();
                backend.delete(OwlTableEntity.class, "databaseId = " + catId +  " and name = \""+TESTOWLTABLE_NAME+"\"");
                backend.delete(DatabaseEntity.class, "name = \""+TESTDATABASE_NAME+"\"");
            }

            backend.commitTransaction();

        } catch(Exception e) {
            e.printStackTrace();
            assertNull(e); // An error during cleanup is critical, should not be ignored, is a failure.
        } 

        gkeyBasicTested = false;

        databaseCreated = false; databaseId = 0;
        owlTableCreated = false; otableId = 0;
        dePublished = false; deSelected = false;
        deAltered1 = false;
        deAltered2 = false;
        deAltered3 = false;

        ptnAStringKeyId = 0; ptnBIntegerKeyId = 0;
        propAStringKeyId = 0; propBIntegerKeyId = 0;
    }

    private static String nullGuardedPrint(Object o) {
        if (o != null) {
            return o.toString();
        }else{
            return "null";
        }
    }

}
