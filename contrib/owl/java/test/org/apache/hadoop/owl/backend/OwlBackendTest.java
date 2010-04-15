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

package org.apache.hadoop.owl.backend;

import java.util.*;
import org.apache.hadoop.owl.common.*;
import org.apache.hadoop.owl.protocol.*;
import org.apache.hadoop.owl.entity.*;
import org.apache.hadoop.owl.logical.ConvertEntity;
import org.apache.hadoop.owl.backend.OwlBackend;

import org.junit.Test;
import junit.framework.TestCase;

public class OwlBackendTest extends TestCase{

    static private OwlBackend backend;
    static private OwlTableEntity otable;

    static private int firstLevelPartitionId;
    static private int part1keyId;
    static private int part2keyId;

    public OwlBackendTest() {
    }

    @Test
    public static void testInitialize() throws OwlException
    {
        backend = new OwlBackend();
        backend.beginTransaction();

        backend.delete(OwlTableEntity.class, "name = \"be_testtable\"");
        backend.delete(DatabaseEntity.class, "name = \"be_testcat\"");
        backend.delete(GlobalKeyEntity.class, "name = \"test_gk\"");


        //Create database
        DatabaseEntity cat = new DatabaseEntity("be_testcat", "testown", "testdesc", "/tmp");
        cat = backend.create(cat);

        //Create owltable
        otable = new OwlTableEntity(cat.getId(), "be_testtable", "testown", "testtabdesc", "/tmp/tableloc", null, "pig-1.0" );

        PartitionKeyEntity[] pk = {
                new PartitionKeyEntity(otable, 1, "part1", OwlKey.DataType.INT.getCode(), OwlPartitionKey.PartitioningType.LIST.getCode()), 
                new PartitionKeyEntity(otable, 2, "part2",  OwlKey.DataType.STRING.getCode(), OwlPartitionKey.PartitioningType.LIST.getCode())
        };


        PropertyKeyEntity[] propk = {
                new PropertyKeyEntity(otable, "prop1", OwlKey.DataType.STRING.getCode()), 
                new PropertyKeyEntity(otable, "prop2", OwlKey.DataType.STRING.getCode()),
        };

        otable.setPartitionKeys(Arrays.asList(pk));
        otable.setPropertyKeys(Arrays.asList(propk));


        //Set bounded list values
        KeyListValueEntity[] valueList1 = {
                new KeyListValueEntity(2008, null),
                new KeyListValueEntity(2009, null),
                new KeyListValueEntity(2010, null)
        };
        otable.getPartitionKeys().get(0).setListValues(Arrays.asList(valueList1)); 
        otable.getPartitionKeys().get(1).addListValue(null, "us");
        otable.getPartitionKeys().get(1).addListValue(null, "uk");
        otable.getPartitionKeys().get(1).addListValue(null, "fr");

        otable = backend.create(otable);

        part1keyId = pk[0].getId();
        part2keyId = pk[1].getId();


        //Insert partitions
        //branch 1
        PartitionEntity pe1 = new PartitionEntity("testowner", "part1 2008 desc", otable.getId(), null, 0);

        KeyValueEntity[] kv1 = {
                new KeyValueEntity(otable.getId(), pe1, pk[0].getId(), null, null, 2008, null),
                new KeyValueEntity(otable.getId(), pe1, null, propk[1].getId(), null, null, "prop key value for 2008")
        };

        pe1.setKeyValues(Arrays.asList(kv1));

        pe1 = backend.create(pe1);
        firstLevelPartitionId = pe1.getId();

        PartitionEntity pe2 = new PartitionEntity("testowner", "part2 uk desc", otable.getId(), pe1.getId(), 0);

        KeyValueEntity[] kv2 = {
                new KeyValueEntity(otable.getId(), pe2, pk[1].getId(), null, null, null, "uk"),
                new KeyValueEntity(otable.getId(), pe2, null, propk[1].getId(), null, null, "prop key value for 2008 uk")
        };

        pe2.setKeyValues(Arrays.asList(kv2));
        pe2 = backend.create(pe2);


        //branch 2
        PartitionEntity pe3 = new PartitionEntity("testowner", "part1 2009 desc", otable.getId(), null, 0);

        KeyValueEntity[] kv3 = {
                new KeyValueEntity(otable.getId(), pe3, pk[0].getId(), null, null, 2009, null),
                new KeyValueEntity(otable.getId(), pe3, null, propk[1].getId(), null, null, "prop key value for 2009")
        };

        pe3.setKeyValues(Arrays.asList(kv3));

        pe3 = backend.create(pe3);

        PartitionEntity pe4 = new PartitionEntity("testowner", "part2 us desc", otable.getId(), pe3.getId(), 0);

        KeyValueEntity[] kv4 = {
                new KeyValueEntity(otable.getId(), pe4, pk[1].getId(), null, null, null, "us"),
                new KeyValueEntity(otable.getId(), pe4, null, propk[1].getId(), null, null, "prop key value for 2009 us")
        };

        pe4.setKeyValues(Arrays.asList(kv4));
        pe4 = backend.create(pe4);


        PartitionEntity pe5 = new PartitionEntity("testowner", "part2 uk desc", otable.getId(), pe3.getId(), 0);

        KeyValueEntity[] kv5 = {
                new KeyValueEntity(otable.getId(), pe5, pk[1].getId(), null, null, null, "uk"),
                new KeyValueEntity(otable.getId(), pe5, null, propk[1].getId(), null, null, "prop key value for 2009 uk")
        };

        pe5.setKeyValues(Arrays.asList(kv5));
        pe5 = backend.create(pe5);

        DataElementEntity de1 = new DataElementEntity("testown", "testdesc", "/tmp/ttt1", otable.getId(), pe1.getId(), "pig-1.0");
        DataElementEntity de2 = new DataElementEntity("testown", "testdesc", "/tmp/ttt2", otable.getId(), pe2.getId(), "pig-1.0");
        DataElementEntity de3 = new DataElementEntity("testown", "testdesc", "/tmp/ttt3", otable.getId(), pe3.getId(), "pig-1.0");
        DataElementEntity de4 = new DataElementEntity("testown", "testdesc", "/tmp/ttt4", otable.getId(), pe4.getId(), "pig-1.0");
        DataElementEntity de5 = new DataElementEntity("testown", "testdesc", "/tmp/ttt5", otable.getId(), pe5.getId(), "pig-1.0");

        de1 = backend.create(de1);
        de2 = backend.create(de2);
        de3 = backend.create(de3);
        de4 = backend.create(de4);
        de5 = backend.create(de5);

        backend.commitTransaction();
        backend.beginTransaction();

        List<OwlTableEntity> tableList = backend.find(OwlTableEntity.class, "name = \"be_testtable\"");
        otable = tableList.get(0);
        otable.setDescription("updated desc");
        backend.update(otable);
        backend.commitTransaction();

        List<GlobalKeyEntity> gk = backend.find(GlobalKeyEntity.class, null);

        OwlDatabase catBean = ConvertEntity.convert(cat);
        OwlTable otBean = ConvertEntity.convert(otable, "be_testcat", gk, backend);
        OwlDataElement ode = ConvertEntity.convert(de5, pe5, otable, gk, backend);
        OwlPartitionProperty partProp = ConvertEntity.convert(pe5, otable, gk);


        System.out.println("Cat bean " + OwlUtil.getJSONFromObject(catBean));
        System.out.println("OwlTable bean " + OwlUtil.getJSONFromObject(otBean));
        System.out.println("OwlDataElement bean " + OwlUtil.getJSONFromObject(ode));
        System.out.println("OwlPartProperty bean " + OwlUtil.getJSONFromObject(partProp));
    }

    public static void runKeyQuery(String filter, int expectedCount) throws OwlException {
        List<PartitionEntity> partitions = backend.find(PartitionEntity.class, "owlTableId = " + otable.getId() + " and " + filter);
        assertEquals(expectedCount, partitions.size());
    }


    @Test
    public static void testTwoPartitionsQuery() throws OwlException {
        runKeyQuery("[part1] = 2008 and [part2] = \"uk\"", 1);
        runKeyQuery("[part1] <> 2008 and [part2] = \"uk\"", 1);
    }

    @Test
    public static void testOnePartitions1Query() throws OwlException {
        runKeyQuery("[part1] = 2009", 3);
    }

    @Test
    public static void testOnePartitions2Query() throws OwlException {
        runKeyQuery("[part2] = \"uk\"", 2);
    }

    @Test
    public static void testLikePropertyQuery1() throws OwlException {
        runKeyQuery("[prop2] like(\"%2009%\")", 3);
    }

    @Test
    public static void testLikePropertyQuery2() throws OwlException {
        runKeyQuery("[prop2] like(\"%2009 us%\")", 1);
    }

    @Test
    public static void testInvalidListInsertInt() throws OwlException  {
        backend.beginTransaction();
        PartitionEntity pe = new PartitionEntity("testowner", "part1 invalid desc", otable.getId(), null, 0);

        KeyValueEntity[] kv = {
                new KeyValueEntity(otable.getId(), pe, part1keyId, null, null, 2000, null)
        };

        pe.setKeyValues(Arrays.asList(kv));

        OwlException exception = null;

        try {
            backend.create(pe);
        } catch(OwlException oe) {
            exception = oe;
        }

        assertNotNull(exception);
        assertEquals(ErrorType.ERROR_INVALID_LIST_VALUE, exception.getErrorType());
        assertTrue(exception.getMessage().indexOf("2000") != -1); //error must mention the invalid value 2000
        //transaction gets rolled back when exception is thrown
    }


    @Test
    public static void testInvalidListInsertString() throws OwlException {
        backend.beginTransaction();
        PartitionEntity pe = new PartitionEntity("testowner", "part2 invalid desc", otable.getId(), firstLevelPartitionId, 0);

        KeyValueEntity[] kv = {
                new KeyValueEntity(otable.getId(), pe, part2keyId, null, null, null, "INVALID_INP")
        };

        pe.setKeyValues(Arrays.asList(kv));

        OwlException exception = null;

        try {
            backend.create(pe);
        } catch(OwlException oe) {
            exception = oe;
        }

        assertNotNull(exception);
        assertEquals(ErrorType.ERROR_INVALID_LIST_VALUE, exception.getErrorType());
        assertTrue(exception.getMessage().indexOf("INVALID_INP") != -1); //error must mention the invalid value INVALID_INP
        //transaction gets rolled back when exception is thrown
    }

    @Test
    public static void testGlobalKey() throws OwlException {
        backend.beginTransaction();

        GlobalKeyEntity gk = new GlobalKeyEntity("test_gk", OwlKey.DataType.STRING.getCode());
        gk = backend.create(gk);

        PartitionEntity pe = new PartitionEntity("testowner", "part1 desc", otable.getId(), null, 0);

        KeyValueEntity[] kv = {
                new KeyValueEntity(otable.getId(), pe, null, null, gk.getId(), null, "gk_value"),
                new KeyValueEntity(otable.getId(), pe, part1keyId, null, null, 2008, null)
        };

        pe.setKeyValues(Arrays.asList(kv));

        pe = backend.create(pe);

        List<PartitionEntity> plist = backend.find(PartitionEntity.class, "owlTableId = " + otable.getId() + " and [test_gk] = \"gk_value\"");
        backend.delete(PartitionEntity.class, "id = " + pe.getId());
        backend.delete(GlobalKeyEntity.class, "name = \"test_gk\"");
        backend.commitTransaction();

        assertEquals(1, plist.size());
    }

    @Test
    public static void testLevel() throws OwlException {
        runKeyQuery("partitionLevel = 1", 2);
        runKeyQuery("partitionLevel = 2", 3);
    }

    @Test
    public static void testIsLeaf() throws OwlException {
        runKeyQuery("isLeaf = 'N'", 2);
        runKeyQuery("isLeaf = 'Y'", 3);
    }

    @Test
    public static void testCleanup() throws OwlException {
        backend.close();
    }
}
