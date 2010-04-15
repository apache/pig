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

public class OwlBackendNotEquals extends TestCase{

    static private OwlBackend backend;
    static private OwlTableEntity otable;

    static private int firstLevelPartitionId;
    static private int part1keyId;
    static private int part2keyId;

    public OwlBackendNotEquals() {
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

        //Create partition key entity
        PartitionKeyEntity[] pk = {
                new PartitionKeyEntity(otable, 1, "part1", OwlKey.DataType.INT.getCode(), OwlPartitionKey.PartitioningType.LIST.getCode()), 
                new PartitionKeyEntity(otable, 2, "part2",  OwlKey.DataType.STRING.getCode(), OwlPartitionKey.PartitioningType.LIST.getCode())
        };

        // set partition keys to owltable
        otable.setPartitionKeys(Arrays.asList(pk));

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

        //Insert partitions
        //branch 1
        PartitionEntity pe1 = new PartitionEntity("testowner", "part1 2008 desc", otable.getId(), null, 0);

        KeyValueEntity[] kv1 = {
                new KeyValueEntity(otable.getId(), pe1, pk[0].getId(), null, null, 2008, null),
        };

        pe1.setKeyValues(Arrays.asList(kv1));

        pe1 = backend.create(pe1);
        firstLevelPartitionId = pe1.getId();

        PartitionEntity pe2 = new PartitionEntity("testowner", "part2 uk desc", otable.getId(), pe1.getId(), 0);

        KeyValueEntity[] kv2 = {
                new KeyValueEntity(otable.getId(), pe2, pk[1].getId(), null, null, null, "uk"),
        };

        pe2.setKeyValues(Arrays.asList(kv2));
        pe2 = backend.create(pe2);


        //branch 2
        PartitionEntity pe3 = new PartitionEntity("testowner", "part1 2009 desc", otable.getId(), null, 0);

        KeyValueEntity[] kv3 = {
                new KeyValueEntity(otable.getId(), pe3, pk[0].getId(), null, null, 2009, null),
        };

        pe3.setKeyValues(Arrays.asList(kv3));

        pe3 = backend.create(pe3);

        PartitionEntity pe4 = new PartitionEntity("testowner", "part2 us desc", otable.getId(), pe3.getId(), 0);

        KeyValueEntity[] kv4 = {
                new KeyValueEntity(otable.getId(), pe4, pk[1].getId(), null, null, null, "us"),
        };

        pe4.setKeyValues(Arrays.asList(kv4));
        pe4 = backend.create(pe4);


        PartitionEntity pe5 = new PartitionEntity("testowner", "part2 uk desc", otable.getId(), pe3.getId(), 0);

        KeyValueEntity[] kv5 = {
                new KeyValueEntity(otable.getId(), pe5, pk[1].getId(), null, null, null, "uk"),
        };

        pe5.setKeyValues(Arrays.asList(kv5));
        pe5 = backend.create(pe5);
        // create dataelement
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
    }

    public static void runKeyQuery(String filter, int expectedCount) throws OwlException {
        List<PartitionEntity> partitions = backend.find(PartitionEntity.class, "owlTableId = " + otable.getId() + " and " + filter);
        assertEquals(expectedCount, partitions.size());
    }


    @Test
    public static void testOnePartitions1Query() throws OwlException {
        runKeyQuery("[part1] = 2009", 3);
        // not equals in JPWL is <>
        runKeyQuery("[part1] <> 2009", 2);
        runKeyQuery("[part1] <> 2008", 3);
        runKeyQuery("[part2] <> \"us\"", 4);
    }
}
